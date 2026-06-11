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
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"go.etcd.io/etcd/client/pkg/v3/fileutil"
	"go.etcd.io/etcd/client/pkg/v3/types"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"go.etcd.io/etcd/server/v3/etcdserver/api/membership"
	"go.uber.org/zap"

	v1 "github.com/cnuss/libetcd/v1"
	"github.com/cnuss/libetcd/v1alpha1/lock"
	"github.com/cnuss/libetcd/v1alpha1/snapshot"
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

	// joining makes Join single-flight: a second call while one is running (or
	// after one succeeded) errors instead of re-adding this node to the cluster.
	joining atomic.Bool
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

// defaultJoinTimeout bounds a Join whose WithContext carries no deadline, so a
// wedged cluster surfaces as an error instead of blocking forever.
const defaultJoinTimeout = 90 * time.Second

// Join brings this node into the cluster reachable at the configured peer URLs:
// it discovers a client endpoint by scraping the peers' /members handlers, takes
// a cluster-wide join lock (so concurrent joiners — including ones in other
// processes — serialize), adds itself as a learner, starts, promotes itself to a
// voting member once caught up, and confirms the new voter is replicating before
// releasing the lock. It blocks until the node is a voting member or the
// bounding context elapses (defaultJoinTimeout if WithContext set no deadline).
//
// On failure after the member-add, Join rolls back: it removes the half-joined
// member from the cluster, then stops the local server if it started — in that
// order, because after a promote this node is a voter and stopping it first
// could drop the cluster below quorum. A failed join therefore doesn't strand a
// zombie learner that would trip the cluster's reconfig health checks. Join is
// single-flight per node; a second call errors.
func (p *peerJoiner) Join() (err error) {
	if !p.joining.CompareAndSwap(false, true) {
		return errors.New("join: already joined or join in progress")
	}
	defer func() {
		if err != nil {
			p.joining.Store(false) // failed (and rolled back if needed); allow a retry
		}
	}()

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
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, defaultJoinTimeout)
		defer cancel()
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
	defer func() {
		if rerr := joinLock.Release(); rerr != nil {
			logger.Warn("join: releasing join lock", zap.Error(rerr))
		}
	}()
	logger.Info("join: lock acquired", zap.String("key", joinLock.Key()))

	selfAddrs := urlsToEndpoints(advertisePeerUrls)

	// A failure from here on can leave a half-joined member behind — the add
	// itself can commit while its response is lost — so the rollback is armed
	// before the first membership mutation. When the add never reported an ID,
	// abortJoin recovers it from the membership by peer URL.
	var memberID uint64
	started := false
	defer func() {
		if err != nil {
			p.abortJoin(cli, memberID, selfAddrs, started, logger)
		}
	}()

	// Add self as a learner, blocking through transient rejections: a prior
	// joiner's promotion raises quorum and the leader's StrictReconfigCheck
	// reports "unhealthy cluster" until the new voter's raft stream goes active.
	// If a previous attempt's add committed but its response was lost (a per-
	// attempt timeout), the retry gets ErrPeerURLExist — recover our member ID
	// from the membership instead of failing. If the URL is instead held by a
	// started or voting member, the collision is real (a stale incarnation or a
	// misconfigured peer), not our lost add: adopting that ID would hand a live
	// member to the rollback, so fail permanently.
	var clusterMembers []*etcdserverpb.Member
	if err = retryUntil(ctx, time.Second, 5*time.Second, "adding self as learner", func(actx context.Context) error {
		m, aerr := cli.MemberAddAsLearner(actx, selfAddrs)
		if aerr == nil {
			memberID = m.Member.ID
			clusterMembers = m.Members // the full membership after the add
			return nil
		}
		if isPeerURLExist(aerr) {
			id, found, ferr := findMemberByPeerURLs(actx, cli, selfAddrs)
			switch {
			case ferr != nil:
				return aerr // membership unreadable; retry
			case found:
				memberID = id
				return nil
			default:
				return permanent(fmt.Errorf("peer URL already held by an existing cluster member: %w", aerr))
			}
		}
		return aerr
	}); err != nil {
		return err
	}
	logger.Info("join: added as learner", zap.String("member-id", types.ID(memberID).String()))

	// The ErrPeerURLExist recovery path adopts a prior attempt's member ID
	// without an add response to read the membership from; list it instead.
	if clusterMembers == nil {
		ml, lerr := cli.MemberList(ctx)
		if lerr != nil {
			return fmt.Errorf("listing members: %w", lerr)
		}
		clusterMembers = ml.Members
	}

	// initial-cluster is name=peerURL for every started voting member, plus this
	// node. Learners and not-yet-started members (empty name) are excluded; their
	// membership reaches us via the raft log, not the bootstrap string.
	//
	// Excluding learners is only safe because the seeded boot path skips etcd's
	// membership validation: seedFromLeader writes a full data dir before Start,
	// so etcd boots from existing data and never reaches
	// bootstrapExistingClusterNoWAL → ValidateClusterAndAssignIDs, whose exact
	// member-count check compares this voters-only view against the full remote
	// membership (learners included). If seeding is ever bypassed for a *fresh*
	// join — today the only skip is a non-empty caller-supplied dir, i.e. a
	// restart — any standing learner in the cluster would fail Start with
	// "member count is unequal".
	initialCluster := types.URLsMap{}
	for _, m := range clusterMembers {
		if m.Name == "" || m.IsLearner {
			continue
		}
		urls, uerr := types.NewURLs(m.PeerURLs)
		if uerr != nil {
			return fmt.Errorf("parsing peer URLs of member %s: %w", m.Name, uerr)
		}
		initialCluster[m.Name] = urls
	}
	initialCluster[name] = advertisePeerUrls

	p.mutate(func() error {
		p.cfg.InitialCluster = initialCluster.String()
		p.cfg.ClusterState = embed.ClusterStateFlagExisting
		p.clusterSet.Store(true)
		return nil
	})

	if err = p.seedFromLeader(ctx, cli, memberID, name); err != nil {
		return fmt.Errorf("seeding from leader snapshot: %w", err)
	}

	if err = p.Start(); err != nil {
		return fmt.Errorf("starting etcd server: %w", err)
	}
	started = true
	logger.Info("join: server started, promoting to voter")

	// Promote learner -> voter, blocking until it sticks. etcd rejects promotion
	// of a learner that isn't ~caught up (ErrLearnerNotReady). With the join seed,
	// the node boots already applied at the leader's index, so promotion should
	// succeed promptly; retries cover residual leader-side settle time only.
	// A member-not-learner means a prior attempt's promote committed but its
	// response was lost: the promotion already happened, so treat it as success —
	// spinning on it until the deadline would roll back a healthy voter. A
	// member-not-found is permanent (someone removed us): fail, don't spin.
	if err = retryUntil(ctx, time.Second, 5*time.Second, "promoting to voter", func(actx context.Context) error {
		_, perr := cli.MemberPromote(actx, memberID)
		switch {
		case perr == nil || isMemberNotLearner(perr):
			return nil
		case isMemberNotFound(perr):
			return permanent(perr)
		default:
			return perr
		}
	}); err != nil {
		return err
	}
	logger.Info("join: promoted to voter")

	// Block until this just-promoted voter has caught up to the leader before we
	// release the join lock. We compare networked Status (not the loopback Self
	// client, which reads a path that can transiently panic under write load):
	// our RaftAppliedIndex reaching ~90% of the leader's committed RaftIndex —
	// etcd's own learner-readiness threshold — means the leader is successfully
	// replicating to us, which is what the next joiner's reconfig health check
	// needs. Holding the lock across this keeps the next joiner out of the
	// unhealthy window; its add-learner retry backstops the residual leader-side
	// settle time, which no client API exposes.
	selfClientURL := advertiseClientUrls[0].String()
	if err = retryUntil(ctx, time.Second, 5*time.Second, "confirming voter caught up", func(actx context.Context) error {
		self, serr := cli.Status(actx, selfClientURL)
		if serr != nil {
			return serr
		}
		if self.IsLearner {
			return errors.New("still a learner")
		}
		if self.Leader == 0 {
			return errors.New("no leader in contact yet")
		}

		// Resolve the leader's client URL from the membership, then read its
		// committed index to compare against ours.
		ml, lerr := cli.MemberList(actx)
		if lerr != nil {
			return lerr
		}
		var leaderURL string
		for _, m := range ml.Members {
			if m.ID == self.Leader && len(m.ClientURLs) > 0 {
				leaderURL = m.ClientURLs[0]
				break
			}
		}
		if leaderURL == "" {
			return fmt.Errorf("leader %s has no client URL yet", types.ID(self.Leader))
		}
		leader, serr := cli.Status(actx, leaderURL)
		if serr != nil {
			return serr
		}
		if self.RaftAppliedIndex < leader.RaftIndex*9/10 {
			return fmt.Errorf("catching up: applied %d < 90%% of leader committed %d", self.RaftAppliedIndex, leader.RaftIndex)
		}
		return nil
	}); err != nil {
		return err
	}

	logger.Info("join: complete, voter caught up", zap.String("member-id", types.ID(memberID).String()))
	return nil
}

// seedFromLeader pulls a point-in-time db snapshot from the cluster and restores
// it into this node's data directory, pre-seeded with the leader-assigned member
// ID (selfID), the live cluster ID, and the full membership (learner status
// preserved). The seeded node boots as a follower already applied to the
// snapshot's raft index, so the leader replicates forward over the log and never
// sends a raft snapshot — applying one panics the embedded host on Windows
// (see v1alpha1/snapshot/snapshot.md). It must run after the learner-add (so
// selfID and the membership are known) and before Start.
func (p *peerJoiner) seedFromLeader(ctx context.Context, cli *clientv3.Client, selfID uint64, selfName string) error {
	p.mu.Lock()
	lg := p.cfg.GetLogger()
	dir := p.cfg.Dir
	p.mu.Unlock()

	// A concrete data directory for the restore target. Restore requires it
	// empty; if the caller supplied a dir that already has data (a restart,
	// not a fresh join), skip seeding — etcd will boot from what's there.
	if dir == "" {
		d, err := os.MkdirTemp("", "libetcd-")
		if err != nil {
			return fmt.Errorf("data dir: %w", err)
		}
		dir = d
		p.mutate(func() error {
			p.cfg.Dir = dir
			return nil
		})
	} else if fileutil.Exist(dir) && !fileutil.DirEmpty(dir) {
		return nil
	}

	// Full membership (including this node, still a learner) and the cluster ID,
	// taken verbatim from the leader so the seed agrees on every ID.
	ml, err := cli.MemberList(ctx)
	if err != nil {
		return fmt.Errorf("member list: %w", err)
	}
	members := make([]snapshot.MemberInfo, 0, len(ml.Members))
	for _, m := range ml.Members {
		name := m.Name
		if m.ID == selfID {
			name = selfName // the leader records the new learner with an empty name
		}
		members = append(members, snapshot.MemberInfo{
			ID:         m.ID,
			Name:       name,
			PeerURLs:   m.PeerURLs,
			ClientURLs: m.ClientURLs,
			IsLearner:  m.IsLearner,
		})
	}

	// Pull the snapshot into a scratch dir (kept out of the restore target,
	// which must be empty). Save wants exactly one endpoint: the snapshot is
	// the point-in-time state of that node.
	scratch, err := os.MkdirTemp("", "libetcd-seed-")
	if err != nil {
		return fmt.Errorf("scratch dir: %w", err)
	}
	defer os.RemoveAll(scratch)
	dbPath := filepath.Join(scratch, "leader.db")

	mgr := snapshot.NewV3(lg)
	saveCfg := clientv3.Config{Endpoints: cli.Endpoints()[:1], DialTimeout: 5 * time.Second, Logger: lg}
	if _, err := mgr.Save(ctx, saveCfg, dbPath); err != nil {
		return fmt.Errorf("snapshot save: %w", err)
	}

	return mgr.Restore(snapshot.RestoreConfig{
		SnapshotPath:  dbPath,
		Name:          selfName,
		SelfID:        selfID,
		ClusterID:     ml.Header.ClusterId,
		Members:       members,
		OutputDataDir: dir,
	})
}

// abortJoin best-effort rolls back a partial join so the cluster isn't left
// with a zombie member. Order matters: the member is removed from the cluster
// first, while the local server (if started) is still serving — after a
// successful promote this node is a voter, and stopping it before the remove
// commits can drop the cluster below quorum, wedging it with a dead voter no
// reconfig can fix. The remove is retried because the rollback runs exactly
// when the cluster is mid-reconfig, so it sees the same transient "unhealthy
// cluster" rejections the forward path does. If the remove ultimately fails
// while the server is running, the server is left running — a live voter keeps
// quorum reachable for a manual remove — and the caller can Stop() afterwards.
// It runs on a fresh bounded context because the join's own context is
// typically already dead here.
func (p *peerJoiner) abortJoin(cli *clientv3.Client, memberID uint64, selfAddrs []string, started bool, logger *zap.Logger) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// The add can commit while its response is lost, failing the join before a
	// member ID was ever learned. Sweep the membership for the zombie: an
	// unstarted learner advertising our peer URLs.
	if memberID == 0 {
		id, found, ferr := findMemberByPeerURLs(ctx, cli, selfAddrs)
		if ferr != nil || !found {
			return // no member known or discoverable; nothing to roll back
		}
		memberID = id
	}

	if rerr := retryUntil(ctx, time.Second, 5*time.Second, "removing half-joined member", func(actx context.Context) error {
		_, err := cli.MemberRemove(actx, memberID)
		if err != nil && !isMemberNotFound(err) {
			return err
		}
		return nil
	}); rerr != nil {
		msg := "join rollback: removing half-joined member failed; remove it manually"
		if started {
			msg = "join rollback: removing half-joined member failed; leaving local server running so the cluster keeps quorum — remove the member manually, then Stop()"
		}
		logger.Warn(msg, zap.String("member-id", types.ID(memberID).String()), zap.Error(rerr))
		return
	}
	logger.Info("join rollback: removed half-joined member",
		zap.String("member-id", types.ID(memberID).String()))

	if started {
		if serr := p.Stop(); serr != nil {
			logger.Warn("join rollback: stopping local server", zap.Error(serr))
		}
	}
}

// findMemberByPeerURLs scans the membership for an unstarted learner — the only
// shape a lost member-add can leave behind: IsLearner with an empty name —
// advertising any of the given peer URLs, and returns its ID. A started or
// voting member holding one of the URLs is deliberately not matched: that's a
// pre-existing member (a stale incarnation at the same address, or operator
// misconfiguration), not our half-add, and adopting its ID would hand a live
// member to the rollback. The error is non-nil only when the membership itself
// couldn't be read.
func findMemberByPeerURLs(ctx context.Context, cli *clientv3.Client, urls []string) (uint64, bool, error) {
	ml, err := cli.MemberList(ctx)
	if err != nil {
		return 0, false, err
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
				return m.ID, true, nil
			}
		}
	}
	return 0, false, nil
}

// isPeerURLExist matches etcd's "Peer URLs already exists" member-add
// rejection. Typed match only: clientv3 pre-converts every cluster-op error
// via rpctypes.Error before returning it, so errors.Is against the sentinel
// already covers all shapes the client hands back.
func isPeerURLExist(err error) bool {
	return errors.Is(err, rpctypes.ErrPeerURLExist)
}

// isMemberNotFound matches etcd's "member not found" rejection. Typed match
// only, for the same reason as isPeerURLExist.
func isMemberNotFound(err error) bool {
	return errors.Is(err, rpctypes.ErrMemberNotFound)
}

// isMemberNotLearner matches etcd's ErrMemberNotLearner ("can only promote a
// learner member") promote rejection. Seeing it for our own member ID means the
// promotion already committed (a prior attempt's response was lost). Typed
// match only, deliberately: a substring fallback would also match
// ErrLearnerNotReady ("can only promote a learner member which is in sync with
// leader") — the retryable not-caught-up-yet rejection, the opposite meaning.
func isMemberNotLearner(err error) bool {
	return errors.Is(err, rpctypes.ErrMemberNotLearner)
}

// permanentError marks an error retryUntil must not retry: the condition will
// never self-heal (e.g. the member we're promoting was removed).
type permanentError struct{ error }

func (p permanentError) Unwrap() error { return p.error }

// permanent wraps err so retryUntil stops and returns it instead of retrying.
func permanent(err error) error { return permanentError{err} }

// retryUntil calls fn every interval until it returns nil, or ctx is done (then
// it returns ctx's error wrapped with what), or fn returns an error marked with
// permanent (returned immediately). Each attempt gets its own context bounded to
// perAttempt and derived from ctx, so a single blocked RPC can't stall the loop
// — it's cancelled and retried.
func retryUntil(ctx context.Context, interval, perAttempt time.Duration, what string, fn func(context.Context) error) error {
	for {
		actx, cancel := context.WithTimeout(ctx, perAttempt)
		err := fn(actx)
		cancel()
		if err == nil {
			return nil
		}
		var perm permanentError
		if errors.As(err, &perm) {
			return fmt.Errorf("%s: %w", what, perm.Unwrap())
		}
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
