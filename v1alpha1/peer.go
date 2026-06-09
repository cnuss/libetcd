package v1alpha1

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/etcdserver/api/membership"

	v1 "github.com/cnuss/libetcd/v1"
)

// peerJoiner is the join-only builder returned by From. It wraps a concrete
// *EtcdImpl (which carries the real config + Client accessors) but exposes only
// the v1.EtcdPeer surface: the With* setters chain back to EtcdPeer (not Etcd,
// so there is no Start), and Join() discovers a client from the peer URLs rather
// than taking one. The embedded *EtcdImpl can't itself be EtcdPeer because its
// With* return v1.Etcd; this wrapper re-types them.
type peerJoiner struct {
	*EtcdImpl
	peers v1.Peers
}

var _ v1.EtcdPeer = (*peerJoiner)(nil)

// With* re-type the embedded EtcdImpl setters to return v1.EtcdPeer so chaining
// stays on the join-only surface.

func (p *peerJoiner) WithName(name string) v1.EtcdPeer {
	p.EtcdImpl.WithName(name)
	return p
}

func (p *peerJoiner) WithDir(dir string) v1.EtcdPeer {
	p.EtcdImpl.WithDir(dir)
	return p
}

func (p *peerJoiner) WithClusterToken(token string) v1.EtcdPeer {
	p.EtcdImpl.WithClusterToken(token)
	return p
}

func (p *peerJoiner) WithLog(level string, writer io.Writer) v1.EtcdPeer {
	p.EtcdImpl.WithLog(level, writer)
	return p
}

func (p *peerJoiner) WithContext(ctx context.Context) v1.EtcdPeer {
	p.EtcdImpl.WithContext(ctx)
	return p
}

func (p *peerJoiner) WithClientServing(lis net.Listener, srv *http.Server) v1.EtcdPeer {
	p.EtcdImpl.WithClientServing(lis, srv)
	return p
}

func (p *peerJoiner) WithPeerServing(lis net.Listener, srv *http.Server) v1.EtcdPeer {
	p.EtcdImpl.WithPeerServing(lis, srv)
	return p
}

// Join brings this node into the cluster reachable at the configured peer URLs.
// It discovers a client endpoint by scraping the peer (raft) handler's /members
// endpoint on those URLs, then runs the managed join (add-as-learner, seed from
// a leader snapshot, promote). It blocks until the node is voting or the
// bounding context elapses.
func (p *peerJoiner) Join() error {
	if len(p.peers) == 0 {
		return errors.New("join: no peer URLs")
	}

	p.mu.Lock()
	uctx := p.userCtx
	lg := p.cfg.GetLogger()
	p.mu.Unlock()

	ctx := context.Background()
	if uctx != nil {
		ctx = uctx
	}

	// Discover the cluster's client endpoints from the peer handlers.
	dctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	eps, err := clientEndpointsFromPeers(dctx, p.peers)
	if err != nil {
		return fmt.Errorf("join: discover endpoints: %w", err)
	}

	mc, err := clientv3.New(clientv3.Config{
		Endpoints:   eps,
		DialTimeout: 5 * time.Second,
		Logger:      lg,
	})
	if err != nil {
		return fmt.Errorf("join: dial discovered endpoints: %w", err)
	}
	defer mc.Close()

	return p.EtcdImpl.joinWith(mc)
}

// clientEndpointsFromPeers asks each peer's raft handler for the cluster
// membership (GET <peer>/members) concurrently, and returns the client URLs of
// the first peer that answers with at least one voting member. Learners are
// excluded: they don't serve raft, and their client URLs are no better an
// entrypoint than a voter's. First non-empty answer wins; the rest are dropped.
func clientEndpointsFromPeers(ctx context.Context, peers v1.Peers) ([]string, error) {
	type result struct{ eps []string }
	ch := make(chan result, len(peers))

	for _, peer := range peers {
		go func(peer *url.URL) {
			members, err := fetchMembers(ctx, peer)
			if err != nil {
				return
			}
			var eps []string
			for _, m := range members {
				if m.IsLearner {
					continue
				}
				eps = append(eps, m.ClientURLs...)
			}
			if len(eps) > 0 {
				select {
				case ch <- result{eps}:
				default:
				}
			}
		}(peer)
	}

	select {
	case r := <-ch:
		return r.eps, nil
	case <-ctx.Done():
		return nil, fmt.Errorf("no peer returned a member list: %w", ctx.Err())
	}
}

// fetchMembers GETs <peer>/members and decodes the JSON []*membership.Member the
// peer (raft) handler serves there over HTTP/1.1.
func fetchMembers(ctx context.Context, peer *url.URL) ([]*membership.Member, error) {
	u := *peer
	u.Path = "/members"

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s: %s", u.String(), resp.Status)
	}

	var members []*membership.Member
	if err := json.NewDecoder(resp.Body).Decode(&members); err != nil {
		return nil, err
	}
	return members, nil
}
