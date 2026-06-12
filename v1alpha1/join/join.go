// Package join implements libetcd's peer-port join protocol: a joining node
// drives its entire join over three HTTP verbs served on the cluster's peer
// (raft) listener, instead of dialing a networked clientv3. The node therefore
// needs only the peer transport to join — discovery, a remote client, and a
// client-side distributed lock all disappear — and a fully headless cluster
// (no client listeners anywhere) is joinable.
//
// The protocol is libetcd-to-libetcd: a stock etcd cluster doesn't serve these
// verbs, so From against one fails at the add step.
//
// Server side (Server, mounted by the peer handler) executes each verb through
// the receiving member's own in-process client. Client side (Add/Promote/
// Remove) is what the joiner calls. Both halves share the wire constants below.
package join

import (
	"context"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/cnuss/libetcd/v1alpha1/snapshot"
)

// Wire contract. One resource under /libetcd/, method-dispatched: POST adds the
// caller as a learner (and returns a seed snapshot), PUT promotes it to a
// voter, DELETE removes it (the rollback). Namespaced under /libetcd/ so it
// can't collide with etcd's own peer mux.
const (
	Path = "/libetcd/v1/join"

	// TokenHeader carries the caller's cluster token. The receiving member
	// constant-time-compares it against its own InitialClusterToken: a caller
	// must already know the token to add itself or pull a snapshot. The token
	// travels in the request, so this gate is only meaningful over a TLS peer
	// listener — on a cleartext listener it is sniffable, exactly like etcd's
	// own auth without TLS.
	TokenHeader = "X-Libetcd-Cluster-Token"

	// POST-response headers: the leader-assigned identity the joiner needs to
	// restore the snapshot as an existing member. The membership rides a
	// base64-JSON header so the response body stays a pure snapshot stream.
	selfIDHeader    = "X-Libetcd-Self-Id"
	clusterIDHeader = "X-Libetcd-Cluster-Id"
	membersHeader   = "X-Libetcd-Members"
)

// Paths returns the join resource's URL path, for the peer mux's served-path
// list.
func Paths() []string { return []string{Path} }

// ErrPermanent marks a reconfig rejection the joiner must not retry (the member
// was removed). Add wraps it for the 404 case; callers test with errors.Is.
var ErrPermanent = errors.New("join: permanent reconfig failure")

// ---- server side ----

// Server serves the join verbs. It executes each through the receiving
// member's in-process client (Self), gated on the cluster token. The lock
// dependency is injected so the package needn't import the lock package's
// concrete type; libetcd passes its peer-join lock acquirer.
type Server struct {
	// Self returns the member's in-process clientv3 client (may be nil before
	// the server is ready).
	Self func() *clientv3.Client
	// Token is the cluster's InitialClusterToken, the join credential.
	Token string
	// Acquire takes the cluster-wide join lock for the add critical section,
	// serializing concurrent joiners (including ones reaching other members).
	// It returns a release func.
	Acquire func(ctx context.Context, cli *clientv3.Client) (release func() error, err error)
	Logger  *zap.Logger
}

var _ http.Handler = (*Server)(nil)

// ServeHTTP dispatches the join resource by HTTP method: POST add, PUT promote,
// DELETE remove. Mount it at Path on the peer mux.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		s.add(w, r)
	case http.MethodPut:
		s.reconfig(w, r, func(ctx context.Context, cli *clientv3.Client, id uint64) error {
			_, err := cli.MemberPromote(ctx, id)
			if errors.Is(err, rpctypes.ErrMemberNotLearner) {
				return nil // already a voter — a prior attempt's promote committed
			}
			return err
		})
	case http.MethodDelete:
		s.reconfig(w, r, func(ctx context.Context, cli *clientv3.Client, id uint64) error {
			_, err := cli.MemberRemove(ctx, id)
			if errors.Is(err, rpctypes.ErrMemberNotFound) {
				return nil // already gone
			}
			return err
		})
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// authorize gates a verb on a matching cluster token and returns the in-process
// client. The HTTP method is already dispatched by handle.
func (s *Server) authorize(w http.ResponseWriter, r *http.Request) (*clientv3.Client, bool) {
	if subtle.ConstantTimeCompare([]byte(r.Header.Get(TokenHeader)), []byte(s.Token)) != 1 {
		http.Error(w, "invalid cluster token", http.StatusForbidden)
		return nil, false
	}
	cli := s.Self()
	if cli == nil {
		http.Error(w, "server not ready", http.StatusServiceUnavailable)
		return nil, false
	}
	return cli, true
}

// add adds the caller as a learner and streams back a snapshot taken after the
// add, so the joiner boots as an existing member already applied past its own
// learner-add. The add + snapshot run under the cluster-wide join lock so
// concurrent joiners (even ones reaching different members) serialize.
func (s *Server) add(w http.ResponseWriter, r *http.Request) {
	cli, ok := s.authorize(w, r)
	if !ok {
		return
	}

	peerURLs := r.FormValue("peerURLs")
	if peerURLs == "" {
		http.Error(w, "peerURLs required", http.StatusBadRequest)
		return
	}
	list := strings.Split(peerURLs, ",")
	for _, u := range list {
		if pu, err := url.Parse(u); err != nil || pu.Host == "" {
			http.Error(w, fmt.Sprintf("invalid peer URL %q", u), http.StatusBadRequest)
			return
		}
	}

	release, err := s.Acquire(r.Context(), cli)
	if err != nil {
		http.Error(w, fmt.Sprintf("acquiring join lock: %v", err), http.StatusServiceUnavailable)
		return
	}
	defer func() { _ = release() }()

	added, err := cli.MemberAddAsLearner(r.Context(), list)
	if err != nil {
		http.Error(w, fmt.Sprintf("adding member: %v", err), http.StatusInternalServerError)
		return
	}

	members := make([]snapshot.MemberInfo, 0, len(added.Members))
	for _, m := range added.Members {
		members = append(members, snapshot.MemberInfo{
			ID: m.ID, Name: m.Name, PeerURLs: m.PeerURLs,
			ClientURLs: m.ClientURLs, IsLearner: m.IsLearner,
		})
	}
	membersJSON, err := json.Marshal(members)
	if err != nil {
		http.Error(w, fmt.Sprintf("encoding membership: %v", err), http.StatusInternalServerError)
		return
	}

	snap, err := cli.Snapshot(r.Context())
	if err != nil {
		http.Error(w, fmt.Sprintf("snapshot: %v", err), http.StatusInternalServerError)
		return
	}
	defer snap.Close()

	w.Header().Set(selfIDHeader, strconv.FormatUint(added.Member.ID, 10))
	w.Header().Set(clusterIDHeader, strconv.FormatUint(added.Header.ClusterId, 10))
	w.Header().Set(membersHeader, base64.StdEncoding.EncodeToString(membersJSON))
	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	if _, err := io.Copy(w, snap); err != nil {
		// Status already sent; the joiner sees a short read and retries.
		if s.Logger != nil {
			s.Logger.Warn("join: streaming snapshot to joiner", zap.Error(err))
		}
	}
}

// reconfig is the shared promote (PUT) / remove (DELETE) body: authorize, parse
// memberID, run op, and map the result to a status code the joiner keys off —
// 200 done, 404 permanent (member not found), 409 retryable (everything else,
// e.g. an unhealthy cluster or a not-yet-ready learner). The reconfig forwards
// to the leader through raft, so any member can serve it.
func (s *Server) reconfig(w http.ResponseWriter, r *http.Request, op func(context.Context, *clientv3.Client, uint64) error) {
	cli, ok := s.authorize(w, r)
	if !ok {
		return
	}
	id, err := strconv.ParseUint(r.FormValue("memberID"), 10, 64)
	if err != nil {
		http.Error(w, "valid memberID required", http.StatusBadRequest)
		return
	}
	switch err := op(r.Context(), cli, id); {
	case err == nil:
		w.WriteHeader(http.StatusOK)
	case errors.Is(err, rpctypes.ErrMemberNotFound):
		http.Error(w, err.Error(), http.StatusNotFound)
	default:
		http.Error(w, err.Error(), http.StatusConflict)
	}
}

// ---- client side (the joiner) ----

// Client drives the join protocol from the joining node, over the peer
// transport. The zero value is unusable; set HTTP (its TLS config must match
// the peers' peer listeners) and Token (the cluster token, the join
// credential). Each call targets a base peer (raft) URL, e.g.
// http://10.0.0.1:2380.
type Client struct {
	HTTP  *http.Client
	Token string
}

// AddResult is what Add returns: the leader-assigned identity plus the snapshot
// stream the joiner restores into its data dir. The caller must close Snapshot.
type AddResult struct {
	SelfID    uint64
	ClusterID uint64
	Members   []snapshot.MemberInfo
	Snapshot  io.ReadCloser
}

// Add POSTs this node's peer URLs to a peer's join resource and returns the
// leader-assigned identity and a snapshot stream.
func (c *Client) Add(ctx context.Context, peerURL string, peerURLs []string) (*AddResult, error) {
	body := url.Values{"peerURLs": {strings.Join(peerURLs, ",")}}
	resp, err := c.do(ctx, http.MethodPost, peerURL, nil, body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, statusError(resp)
	}

	selfID, err := strconv.ParseUint(resp.Header.Get(selfIDHeader), 10, 64)
	if err != nil {
		resp.Body.Close()
		return nil, fmt.Errorf("add: bad %s header: %w", selfIDHeader, err)
	}
	clusterID, err := strconv.ParseUint(resp.Header.Get(clusterIDHeader), 10, 64)
	if err != nil {
		resp.Body.Close()
		return nil, fmt.Errorf("add: bad %s header: %w", clusterIDHeader, err)
	}
	membersJSON, err := base64.StdEncoding.DecodeString(resp.Header.Get(membersHeader))
	if err != nil {
		resp.Body.Close()
		return nil, fmt.Errorf("add: bad %s header: %w", membersHeader, err)
	}
	var members []snapshot.MemberInfo
	if err := json.Unmarshal(membersJSON, &members); err != nil {
		resp.Body.Close()
		return nil, fmt.Errorf("add: decoding membership: %w", err)
	}
	return &AddResult{SelfID: selfID, ClusterID: clusterID, Members: members, Snapshot: resp.Body}, nil
}

// Promote asks a peer (PUT) to promote memberID to a voter. It returns nil on
// success, an error wrapping ErrPermanent when the member is gone (404), or a
// plain (retryable) error otherwise.
func (c *Client) Promote(ctx context.Context, peerURL string, memberID uint64) error {
	return c.reconfig(ctx, http.MethodPut, peerURL, memberID)
}

// Remove asks a peer (DELETE) to remove memberID (the joiner's rollback).
func (c *Client) Remove(ctx context.Context, peerURL string, memberID uint64) error {
	return c.reconfig(ctx, http.MethodDelete, peerURL, memberID)
}

func (c *Client) reconfig(ctx context.Context, method, peerURL string, memberID uint64) error {
	// memberID rides the query string, not the body: net/http's FormValue reads
	// the body only for POST/PUT/PATCH, so a DELETE body would be dropped. A
	// query param is read for every method.
	q := url.Values{"memberID": {strconv.FormatUint(memberID, 10)}}
	resp, err := c.do(ctx, method, peerURL, q, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK:
		return nil
	case http.StatusNotFound:
		return fmt.Errorf("%w: %s", ErrPermanent, readBody(resp))
	default:
		return statusError(resp)
	}
}

func (c *Client) do(ctx context.Context, method, peerURL string, query url.Values, body url.Values) (*http.Response, error) {
	full := peerURL + Path
	if len(query) > 0 {
		full += "?" + query.Encode()
	}
	var r io.Reader
	if body != nil {
		r = strings.NewReader(body.Encode())
	}
	req, err := http.NewRequestWithContext(ctx, method, full, r)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}
	req.Header.Set(TokenHeader, c.Token)
	return c.HTTP.Do(req)
}

func statusError(resp *http.Response) error {
	defer resp.Body.Close()
	return fmt.Errorf("%s: %s", resp.Status, readBody(resp))
}

func readBody(resp *http.Response) string {
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	return strings.TrimSpace(string(b))
}
