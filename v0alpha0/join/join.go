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
	"crypto/sha256"
	"crypto/subtle"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/cnuss/libetcd/v0alpha0/snapshot"
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
)

// addPrelude is the JSON metadata the POST response carries ahead of the
// snapshot stream (length-prefixed; see add): the leader-assigned identity and
// the post-add membership the joiner restores the snapshot against.
type addPrelude struct {
	SelfID    uint64
	ClusterID uint64
	Members   []snapshot.MemberInfo
}

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
	// Token is the cluster credential. In plain mode it's the cluster's
	// InitialClusterToken (a shared secret), constant-time-matched against the
	// caller's. In JWT mode (Userinfo set) it's the cluster's sub: the caller's
	// credential is a JWT whose verified sub must equal Token.
	Token string
	// Userinfo, when set, switches /join to JWT mode: the caller's credential is
	// forwarded as a bearer to the discovery seed's userinfo endpoint, which
	// verifies it and returns its sub; that sub must equal Token (the cluster's
	// sub). This keeps libetcd crypto-free — the seed is the JWT verifier — at
	// the cost of the seed being reachable during a join. Empty keeps the plain
	// shared-secret match.
	Userinfo string
	// HTTP is the client for the userinfo call (defaults to http.DefaultClient).
	HTTP *http.Client
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
		s.remove(w, r)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// authorize gates a verb on the cluster credential and returns the in-process
// client. The HTTP method is already dispatched by ServeHTTP.
//
// JWT mode (Userinfo set): the credential is forwarded as a bearer to the seed's
// userinfo endpoint, which verifies it and returns its sub; that sub must equal
// Token (the cluster's sub). Plain mode: the credential is compared to Token as
// a fixed-length SHA-256 digest, so the comparison time doesn't vary with the
// (attacker-controlled) token length — subtle.ConstantTimeCompare short-circuits
// on a length mismatch, which would otherwise leak it.
func (s *Server) authorize(w http.ResponseWriter, r *http.Request) (*clientv3.Client, bool) {
	cred := r.Header.Get(TokenHeader)
	if s.Userinfo != "" {
		if !s.verifyViaUserinfo(r.Context(), cred) {
			http.Error(w, "invalid join token", http.StatusForbidden)
			return nil, false
		}
	} else {
		got := sha256.Sum256([]byte(cred))
		want := sha256.Sum256([]byte(s.Token))
		if subtle.ConstantTimeCompare(got[:], want[:]) != 1 {
			http.Error(w, "invalid cluster token", http.StatusForbidden)
			return nil, false
		}
	}
	cli := s.Self()
	if cli == nil {
		http.Error(w, "server not ready", http.StatusServiceUnavailable)
		return nil, false
	}
	return cli, true
}

// verifyViaUserinfo forwards the credential as a bearer to the seed's userinfo
// endpoint and accepts the join iff the verified sub equals Token (the cluster's
// sub). The seed does the cryptographic JWT verification; libetcd only matches
// the returned sub, so it stays crypto-free. Fail-closed: an unreachable seed,
// a non-200, or a sub mismatch all reject.
func (s *Server) verifyViaUserinfo(ctx context.Context, raw string) bool {
	if raw == "" {
		return false
	}
	hc := s.HTTP
	if hc == nil {
		hc = http.DefaultClient
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.Userinfo, nil)
	if err != nil {
		return false
	}
	req.Header.Set("Authorization", "Bearer "+raw)
	req.Header.Set("Accept", "application/json")
	resp, err := hc.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return false
	}
	var u struct {
		Sub string `json:"sub"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<16)).Decode(&u); err != nil {
		return false
	}
	return u.Sub != "" && u.Sub == s.Token
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
		// Reject bad input with 400 (a permanent client error) rather than
		// letting a non-http scheme reach MemberAddAsLearner, whose failure
		// would be mapped to a retryable 409 and spun on.
		if pu, err := url.Parse(u); err != nil || pu.Host == "" || (pu.Scheme != "http" && pu.Scheme != "https") {
			http.Error(w, fmt.Sprintf("invalid peer URL %q", u), http.StatusBadRequest)
			return
		}
	}

	release, err := s.Acquire(r.Context(), cli)
	if err != nil {
		http.Error(w, fmt.Sprintf("acquiring join lock: %v", err), http.StatusServiceUnavailable)
		return
	}
	defer func() {
		if rerr := release(); rerr != nil && s.Logger != nil {
			// The lock's lease will still expire on its own; log so a stuck
			// release (blocking concurrent joins until expiry) is diagnosable.
			s.Logger.Warn("join: releasing join lock", zap.Error(rerr))
		}
	}()

	out, status, err := addLearner(r.Context(), cli, list)
	if err != nil {
		// 409 transient (e.g. "unhealthy cluster" while a prior reconfig
		// settles — the joiner retries) vs 404 permanent (too many learners,
		// auth, a started member already holds the URL).
		http.Error(w, fmt.Sprintf("adding member: %v", err), status)
		return
	}

	// Metadata rides a length-prefixed JSON prelude at the front of the body,
	// not response headers: the full membership can be larger than common
	// header-size limits (8–16 KiB) on a big cluster, which would fail the join
	// as an opaque transport error. The body is: 4-byte big-endian prelude
	// length, that many bytes of JSON (selfID, clusterID, members), then the
	// raw snapshot stream.
	preludeJSON, err := json.Marshal(addPrelude{out.memberID, out.clusterID, out.members})
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

	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	var lenbuf [4]byte
	binary.BigEndian.PutUint32(lenbuf[:], uint32(len(preludeJSON)))
	if _, err = w.Write(lenbuf[:]); err == nil {
		_, err = w.Write(preludeJSON)
	}
	if err == nil {
		_, err = io.Copy(w, snap)
	}
	if err != nil {
		// Status already sent; the joiner sees a short read and retries.
		if s.Logger != nil {
			s.Logger.Warn("join: streaming join response to joiner", zap.Error(err))
		}
	}
}

// addOutcome is the identity + membership the snapshot is restored against.
type addOutcome struct {
	memberID  uint64
	clusterID uint64
	members   []snapshot.MemberInfo
}

// addLearner adds the peer URLs as a learner and returns the outcome, or a
// status code classifying the failure: 409 for a transient rejection the joiner
// should retry (an unhealthy cluster mid-reconfig, no leader yet), 404 for a
// permanent one. A lost-response retry that finds the add already committed —
// ErrPeerURLExist for an unstarted learner holding these URLs — is recovered as
// success; the same URL on a started or voting member is a real conflict (404).
func addLearner(ctx context.Context, cli *clientv3.Client, urls []string) (addOutcome, int, error) {
	added, err := cli.MemberAddAsLearner(ctx, urls)
	if err == nil {
		return addOutcome{added.Member.ID, added.Header.ClusterId, toMemberInfos(added.Members)}, http.StatusOK, nil
	}
	if errors.Is(err, rpctypes.ErrPeerURLExist) {
		ml, lerr := cli.MemberList(ctx)
		if lerr != nil {
			return addOutcome{}, http.StatusConflict, lerr // membership unreadable; retry
		}
		want := make(map[string]struct{}, len(urls))
		for _, u := range urls {
			want[u] = struct{}{}
		}
		for _, m := range ml.Members {
			if !m.IsLearner || m.Name != "" {
				continue
			}
			for _, pu := range m.PeerURLs {
				if _, ok := want[pu]; ok {
					return addOutcome{m.ID, ml.Header.ClusterId, toMemberInfos(ml.Members)}, http.StatusOK, nil
				}
			}
		}
		return addOutcome{}, http.StatusNotFound, fmt.Errorf("peer URL already held by an existing member: %w", err)
	}
	if errors.Is(err, rpctypes.ErrTooManyLearners) ||
		errors.Is(err, rpctypes.ErrPermissionDenied) ||
		errors.Is(err, rpctypes.ErrUserEmpty) {
		return addOutcome{}, http.StatusNotFound, err
	}
	return addOutcome{}, http.StatusConflict, err // transient: unhealthy cluster, no leader, …
}

func toMemberInfos(ms []*etcdserverpb.Member) []snapshot.MemberInfo {
	out := make([]snapshot.MemberInfo, 0, len(ms))
	for _, m := range ms {
		out = append(out, snapshot.MemberInfo{
			ID: m.ID, Name: m.Name, PeerURLs: m.PeerURLs,
			ClientURLs: m.ClientURLs, IsLearner: m.IsLearner,
		})
	}
	return out
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

// remove (DELETE) takes either a memberID or a peerURLs query. The peerURLs
// form is the joiner's lost-response rollback: a POST can commit the member-add
// server-side while the joiner fails before reading the response, leaving it
// without a member ID; it then asks each peer to remove the unstarted learner
// holding its peer URLs. Resolving none (the add never committed, or it was
// already cleaned up) is idempotent success.
func (s *Server) remove(w http.ResponseWriter, r *http.Request) {
	cli, ok := s.authorize(w, r)
	if !ok {
		return
	}

	id, idErr := strconv.ParseUint(r.FormValue("memberID"), 10, 64)
	if idErr != nil {
		urls := r.FormValue("peerURLs")
		if urls == "" {
			http.Error(w, "memberID or peerURLs required", http.StatusBadRequest)
			return
		}
		resolved, found, lerr := findUnstartedLearner(r.Context(), cli, strings.Split(urls, ","))
		if lerr != nil {
			http.Error(w, lerr.Error(), http.StatusConflict) // membership unreadable; retry
			return
		}
		if !found {
			w.WriteHeader(http.StatusOK) // nothing to roll back
			return
		}
		id = resolved
	}

	_, err := cli.MemberRemove(r.Context(), id)
	switch {
	case err == nil || errors.Is(err, rpctypes.ErrMemberNotFound):
		w.WriteHeader(http.StatusOK) // removed, or already gone
	default:
		http.Error(w, err.Error(), http.StatusConflict)
	}
}

// findUnstartedLearner resolves the member ID of an unstarted learner
// (IsLearner with an empty name — the only shape a half-committed join leaves)
// advertising any of the given peer URLs. A started or voting member holding
// one is a different member, not matched.
func findUnstartedLearner(ctx context.Context, cli *clientv3.Client, urls []string) (uint64, bool, error) {
	ml, err := cli.MemberList(ctx)
	if err != nil {
		return 0, false, err
	}
	want := make(map[string]struct{}, len(urls))
	for _, u := range urls {
		want[strings.TrimSpace(u)] = struct{}{}
	}
	for _, m := range ml.Members {
		if !m.IsLearner || m.Name != "" {
			continue
		}
		for _, pu := range m.PeerURLs {
			if _, ok := want[pu]; ok {
				return m.ID, true, nil
			}
		}
	}
	return 0, false, nil
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
		if resp.StatusCode == http.StatusNotFound {
			err := fmt.Errorf("%w: %s", ErrPermanent, readBody(resp))
			resp.Body.Close()
			return nil, err
		}
		return nil, statusError(resp) // 409 and 5xx: retryable (closes the body)
	}

	// Read the length-prefixed JSON prelude, then the body's remainder is the
	// snapshot stream (returned to the caller to restore and close).
	var lenbuf [4]byte
	if _, err := io.ReadFull(resp.Body, lenbuf[:]); err != nil {
		resp.Body.Close()
		return nil, fmt.Errorf("add: reading prelude length: %w", err)
	}
	preludeJSON := make([]byte, binary.BigEndian.Uint32(lenbuf[:]))
	if _, err := io.ReadFull(resp.Body, preludeJSON); err != nil {
		resp.Body.Close()
		return nil, fmt.Errorf("add: reading prelude: %w", err)
	}
	var p addPrelude
	if err := json.Unmarshal(preludeJSON, &p); err != nil {
		resp.Body.Close()
		return nil, fmt.Errorf("add: decoding prelude: %w", err)
	}
	return &AddResult{SelfID: p.SelfID, ClusterID: p.ClusterID, Members: p.Members, Snapshot: resp.Body}, nil
}

// Promote asks a peer (PUT) to promote memberID to a voter. It returns nil on
// success, an error wrapping ErrPermanent when the member is gone (404), or a
// plain (retryable) error otherwise.
func (c *Client) Promote(ctx context.Context, peerURL string, memberID uint64) error {
	return c.reconfig(ctx, http.MethodPut, peerURL, url.Values{
		"memberID": {strconv.FormatUint(memberID, 10)},
	})
}

// Remove asks a peer (DELETE) to remove memberID (the joiner's rollback when it
// learned its ID).
func (c *Client) Remove(ctx context.Context, peerURL string, memberID uint64) error {
	return c.reconfig(ctx, http.MethodDelete, peerURL, url.Values{
		"memberID": {strconv.FormatUint(memberID, 10)},
	})
}

// RemoveByPeerURLs asks a peer (DELETE) to remove the unstarted learner holding
// these peer URLs. It is the rollback for a lost-response add: the joiner never
// learned a member ID, but a half-committed add may have left a learner with
// its URLs. Resolving none is success.
func (c *Client) RemoveByPeerURLs(ctx context.Context, peerURL string, peerURLs []string) error {
	return c.reconfig(ctx, http.MethodDelete, peerURL, url.Values{
		"peerURLs": {strings.Join(peerURLs, ",")},
	})
}

func (c *Client) reconfig(ctx context.Context, method, peerURL string, q url.Values) error {
	// The selector (memberID or peerURLs) rides the query string, not the body:
	// net/http's FormValue reads the body only for POST/PUT/PATCH, so a DELETE
	// body would be dropped. A query param is read for every method.
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
	// Build the URL by parsing the base and joining the path, so a peer URL
	// with its own path component (e.g. behind a reverse proxy at host/etcd)
	// produces host/etcd/libetcd/v1/join rather than a string-concat mistake.
	base, err := url.Parse(peerURL)
	if err != nil {
		return nil, fmt.Errorf("parsing peer URL %q: %w", peerURL, err)
	}
	full := base.JoinPath(Path)
	full.RawQuery = query.Encode()
	var r io.Reader
	if body != nil {
		r = strings.NewReader(body.Encode())
	}
	req, err := http.NewRequestWithContext(ctx, method, full.String(), r)
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
