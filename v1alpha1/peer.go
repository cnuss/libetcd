package v1alpha1

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"go.etcd.io/etcd/server/v3/etcdserver/api/membership"

	v1 "github.com/cnuss/libetcd/v1"
	"github.com/cnuss/libetcd/v1alpha1/lock"
)

// peerJoiner is the join-only builder returned by From. It wraps a concrete
// *EtcdImpl (which carries the real config + Client accessors) but exposes only
// the v1.EtcdPeer surface: the With* setters chain back to EtcdPeer (not Etcd,
// so there is no Start), and Join() discovers a client from the peer URLs rather
// than taking one. The embedded *EtcdImpl can't itself be EtcdPeer because its
// With* return v1.Etcd; this wrapper re-types them.
type peerJoiner struct {
	*EtcdImpl
	peers []string
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
// Not implemented: the managed-join flow is being rebuilt as the single join
// path. See https://github.com/cnuss/libetcd/issues/36.
func (p *peerJoiner) Join() error {
	logger := p.Logger()
	if err := p.ensureListeners(); err != nil {
		return fmt.Errorf("ensuring listeners: %w", err)
	}

	p.mu.Lock()
	ctx := p.userCtx
	name := p.cfg.Name
	advertisePeerUrls := p.cfg.AdvertisePeerUrls
	advertiseClientUrls := p.cfg.AdvertiseClientUrls
	p.mu.Unlock()

	if ctx == nil {
		ctx = context.Background()
	}

	peers := sanitizePeers(p.peers)
	if len(peers) == 0 {
		return fmt.Errorf("no valid peer URLs: %v", p.peers)
	}

	endpoints, err := clientEndpointsFromPeers(ctx, peers)
	if err != nil {
		return fmt.Errorf("discovering client endpoints from peers: %w", err)
	}

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
		Logger:      logger,
	})
	if err != nil {
		return fmt.Errorf("creating client: %w", err)
	}
	defer cli.Close()

	joinLock, err := lock.Acquire(ctx, cli, "peer-join")
	if err != nil {
		return fmt.Errorf("acquiring join lock: %w", err)
	}
	defer joinLock.Release()
	log.Printf("!!! got lock at %s, joining cluster... (key: %s)\n", time.Now().Format(time.RFC3339), joinLock.Key())

	selfAddrs := func() []string {
		addrs := []string{}
		for _, u := range advertisePeerUrls {
			addrs = append(addrs, u.String())
		}
		return addrs
	}()

	// Add self as a learner, blocking through transient rejections: a prior
	// joiner's promotion raises quorum and the leader's StrictReconfigCheck
	// reports "unhealthy cluster" until the new voter's raft stream goes active.
	var member *clientv3.MemberAddResponse
	if err := retryUntil(ctx, time.Second, 5*time.Second, "adding self as learner", func(actx context.Context) error {
		m, err := cli.MemberAddAsLearner(actx, selfAddrs)
		if err == nil {
			member = m
		}
		return err
	}); err != nil {
		return err
	}
	log.Printf("!!! added self as learner: %v\n", member)

	members, err := cli.MemberList(ctx)
	if err != nil {
		return fmt.Errorf("listing members: %w", err)
	}

	initialCluster := []string{}
	for _, m := range members.Members {
		if m.Name == "" || m.IsLearner {
			continue
		}
		for _, pu := range m.PeerURLs {
			initialCluster = append(initialCluster, fmt.Sprintf("%s=%s", m.Name, pu))
		}
	}
	for _, pu := range advertisePeerUrls {
		initialCluster = append(initialCluster, fmt.Sprintf("%s=%s", name, pu.String()))
	}
	log.Printf("!!! initial cluster: %v\n", initialCluster)

	p.mutate(func() error {
		p.cfg.InitialCluster = strings.Join(initialCluster, ",")
		p.cfg.ClusterState = embed.ClusterStateFlagExisting
		p.clusterSet.Store(true)
		return nil
	})

	if err := p.Start(); err != nil {
		return fmt.Errorf("starting etcd server: %w", err)
	}
	log.Printf("!!! started etcd server, now promoting to voter...\n")

	// Promote learner -> voter, blocking until it sticks. etcd rejects promotion
	// of a learner that isn't ~caught up (ErrLearnerNotReady); without a seed the
	// node catches up over the live log, so promotion only succeeds once it has.
	if err := retryUntil(ctx, time.Second, 5*time.Second, "promoting to voter", func(actx context.Context) error {
		_, err := cli.MemberPromote(actx, member.Member.ID)
		return err
	}); err != nil {
		return err
	}
	log.Printf("!!! promoted to voter\n")

	// Block until this just-promoted voter has caught up to the leader before we
	// release the join lock. We compare networked Status (not the loopback Self
	// client, which reads a path that can transiently panic): our RaftAppliedIndex
	// reaching the leader's RaftIndex (committed) means the leader has been
	// successfully replicating to us — a leader-side "this member is active"
	// signal, which is what the next joiner's reconfig health check needs.
	// Holding the lock across this keeps the next joiner out of the unhealthy
	// window. The retry backstops the residual leader-side settle time.
	selfClientURL := advertiseClientUrls[0].String()
	if err := retryUntil(ctx, time.Second, 5*time.Second, "confirming voter caught up", func(actx context.Context) error {
		self, err := cli.Status(actx, selfClientURL)
		if err != nil {
			return err
		}
		if self.IsLearner {
			return fmt.Errorf("still a learner")
		}
		if self.Leader == 0 {
			return fmt.Errorf("no leader in contact yet")
		}

		// Resolve the leader's client URL from the membership, then read its
		// committed index to compare against ours.
		ml, err := cli.MemberList(actx)
		if err != nil {
			return err
		}
		var leaderURL string
		for _, m := range ml.Members {
			if m.ID == self.Leader && len(m.ClientURLs) > 0 {
				leaderURL = m.ClientURLs[0]
				break
			}
		}
		if leaderURL == "" {
			return fmt.Errorf("leader %x has no client URL yet", self.Leader)
		}
		leader, err := cli.Status(actx, leaderURL)
		if err != nil {
			return err
		}
		if self.RaftAppliedIndex < leader.RaftIndex {
			return fmt.Errorf("catching up: applied %d < leader committed %d", self.RaftAppliedIndex, leader.RaftIndex)
		}
		return nil
	}); err != nil {
		return err
	}
	log.Printf("!!! voter caught up to leader, cluster healthy\n")

	members, err = cli.MemberList(ctx)
	if err != nil {
		return fmt.Errorf("listing members: %w", err)
	}
	log.Printf("!!! final member list:")
	for _, m := range members.Members {
		log.Printf("  %x %s learner=%t peers=%v", m.ID, m.Name, m.IsLearner, m.PeerURLs)
	}

	return nil
}

// retryUntil calls fn every interval until it returns nil, or ctx is done (then
// it returns ctx's error wrapped with what). Each attempt gets its own context
// bounded to perAttempt and derived from ctx, so a single blocked RPC can't
// stall the loop — it's cancelled and retried.
func retryUntil(ctx context.Context, interval, perAttempt time.Duration, what string, fn func(context.Context) error) error {
	for {
		actx, cancel := context.WithTimeout(ctx, perAttempt)
		err := fn(actx)
		cancel()
		if err == nil {
			return nil
		}
		log.Printf("!!! %s: %v; retrying in %s\n", what, err, interval)
		select {
		case <-ctx.Done():
			return fmt.Errorf("%s: %w", what, ctx.Err())
		case <-time.After(interval):
		}
	}
}

// sanitizePeers normalizes the caller-supplied list of peer (raft) URLs into a
// clean, de-duplicated list ready to scrape. For each entry it trims surrounding
// whitespace, defaults a missing scheme to http (so a bare "host:port" works),
// and parses it as an absolute http/https URL with a host. Anything that doesn't
// parse — empty, malformed, wrong scheme, no host — is silently dropped rather
// than failing the whole join: a few bad entries shouldn't sink a list that also
// holds reachable peers. Duplicates are removed, first-seen order preserved.
func sanitizePeers(peers []string) []string {
	out := make([]string, 0, len(peers))
	seen := make(map[string]struct{}, len(peers))
	for _, raw := range peers {
		s := strings.TrimSpace(raw)
		if s == "" {
			continue
		}
		if !strings.Contains(s, "://") {
			s = "http://" + s
		}
		u, err := url.Parse(s)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			continue
		}
		u.Path = strings.TrimRight(u.Path, "/")
		norm := u.String()
		if _, dup := seen[norm]; dup {
			continue
		}
		seen[norm] = struct{}{}
		out = append(out, norm)
	}
	return out
}

// clientEndpointsFromPeers asks each peer's raft handler for the cluster
// membership (GET <peer>/members) concurrently, and returns the client URLs of
// the first peer that answers with at least one voting member. Learners are
// excluded: they don't serve raft, and their client URLs are no better an
// entrypoint than a voter's. First non-empty answer wins; the rest are dropped.
func clientEndpointsFromPeers(ctx context.Context, peers []string) ([]string, error) {
	type result struct{ eps []string }
	ch := make(chan result, len(peers))

	for _, peer := range peers {
		go func(peer string) {
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
func fetchMembers(ctx context.Context, peer string) ([]*membership.Member, error) {
	u, err := url.Parse(peer)
	if err != nil {
		return nil, err
	}
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
