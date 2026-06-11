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

	// exhausted latches when a failed join's rollback stopped a started server.
	// The embedded server is single-use (its lifecycle is once-guarded), so a
	// retried Join would re-add a member to the remote cluster while Start
	// silently no-ops on the dead server — burning the whole join budget and
	// churning membership with no chance of success. A latched handle rejects
	// Join immediately instead.
	exhausted atomic.Bool
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

func (p *peerJoiner) WithoutClientServing() v1.EtcdPeer {
	p.EtcdImpl.WithoutClientServing()
	return p
}

func (p *peerJoiner) WithPeerServing(lis net.Listener, srv *http.Server) v1.EtcdPeer {
	p.EtcdImpl.WithPeerServing(lis, srv)
	return p
}

func (p *peerJoiner) WithoutPeerServing(advertiseURLs ...string) v1.EtcdPeer {
	p.EtcdImpl.WithoutPeerServing(advertiseURLs...)
	return p
}

// defaultJoinTimeout bounds a Join whose WithContext carries no deadline, so a
// wedged cluster surfaces as an error instead of blocking forever.
const defaultJoinTimeout = 90 * time.Second

// discoveryTimeout bounds the /members scrape of the supplied peers. Discovery
// either answers in connect-time scale or never will; without its own bound,
// unreachable peers would eat the entire join budget before failing.
const discoveryTimeout = 10 * time.Second

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
// could drop the cluster below quorum. If the remove itself ultimately fails,
// the local server is deliberately left running (a live voter keeps quorum
// reachable for a manual member remove); call Stop to shut it down. A failed
// join therefore doesn't strand a zombie learner that would trip the cluster's
// reconfig health checks. Join is single-flight per node; a second call errors.
//
// A failure before the local server started leaves the handle reusable — Join
// may simply be called again. A failure after the server started exhausts the
// handle (the embedded server is single-use): further Join calls fail
// immediately, and another attempt needs a fresh From(...) handle.
func (p *peerJoiner) Join() (err error) {
	if p.exhausted.Load() {
		return errors.New("join: this handle's server was started and stopped by a failed join and cannot be reused — build a fresh From(...) handle and try again")
	}
	if !p.joining.CompareAndSwap(false, true) {
		return errors.New("join: already joined or join in progress")
	}
	defer func() {
		if err != nil && !p.exhausted.Load() {
			// Failed before the server started (and was rolled back if
			// needed): the handle is intact, allow a retry. Post-start
			// failures latch exhausted in abortJoin and keep joining set.
			p.joining.Store(false)
		}
	}()

	// Fail fast on local misconfiguration before touching the remote cluster:
	// a join mutates the target's membership, so a doomed attempt must not get
	// that far.
	//
	// A latched builder error (e.g. a bad WithLog level) makes every later
	// mutate a no-op: ensureListeners would leak its freshly bound listeners,
	// and the member-add would register embed's default localhost advertise URL
	// with the live cluster, only for Start to surface the cause after the
	// damage is done.
	if cause := context.Cause(p.ctx); cause != nil {
		return fmt.Errorf("join: configuration error: %w", cause)
	}
	// A server minted before Join (any client accessor — Self/Leader/Voters/
	// Peers — mints it) was built from the bootstrap config; Join's later
	// InitialCluster/ClusterState mutations can't reach the cached server, so
	// it would boot a divergent single-node cluster and the join would fail
	// only after the full promote timeout, churning remote membership on the
	// way. Best-effort check: a concurrent accessor can still race past it,
	// but the sequential misuse is caught with a clear error.
	p.mu.Lock()
	minted := p.srv != nil
	p.mu.Unlock()
	if minted {
		return errors.New("join: server already minted (a client accessor like Self/Leader/Voters/Peers was called before Join); call Join first, or build a fresh From(...) handle")
	}

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

	peers, droppedPeers := sanitizePeers(p.peers)
	if len(droppedPeers) > 0 {
		logger.Warn("join: ignoring unparseable peer URLs", zap.Strings("dropped", droppedPeers))
	}
	if len(peers) == 0 {
		return fmt.Errorf("no valid peer URLs: %v", p.peers)
	}

	// A loopback advertise URL is unreachable from any other machine. When the
	// target peers are non-loopback (a remote cluster), the join is
	// structurally doomed: the cluster would accept the member-add but could
	// never dial us back, the learner would never sync, and the join would die
	// at the promote timeout — leaving rollback churn behind. Catch it before
	// the member-add. Same-host joins (loopback on both sides) pass.
	if loopbackOnly(advertisePeerUrls) && !allLoopbackPeers(peers) {
		return fmt.Errorf("join: advertise peer URLs %v are all loopback but target peers %v are not; a remote cluster can never dial back — serve a routable address via WithPeerServing", urlsToEndpoints(advertisePeerUrls), peers)
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
		if isPermanentMemberAdd(aerr) {
			return permanent(aerr)
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

	// start with the join deadline bounding the ready wait: the user context
	// often has no deadline, and a joiner that can't reach ready (e.g. the
	// cluster lost quorum after the member-add) must fail within the join
	// budget so the rollback runs, not hang on ReadyNotify forever. The run
	// loop may be live even when start errors, so flag started first — the
	// rollback must Stop it either way.
	startErr := p.start(ctx)
	started = true
	if startErr != nil {
		return fmt.Errorf("starting etcd server: %w", startErr)
	}
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

	// Block until this just-promoted voter has caught up before we release the
	// join lock. We prefer networked Status on both sides (not the loopback
	// Self client, which reads a path that can transiently panic under write
	// load): our RaftAppliedIndex reaching ~90% of the leader's committed
	// RaftIndex — etcd's own learner-readiness threshold — means the leader is
	// successfully replicating to us, which is what the next joiner's reconfig
	// health check needs. Holding the lock across this keeps the next joiner
	// out of the unhealthy window; its add-learner retry backstops the residual
	// leader-side settle time, which no client API exposes.
	//
	// Headless members (WithoutClientServing) have no client URL to Status, so
	// both sides degrade explicitly rather than assume one:
	//   - self headless: read our own status through the in-process Self client
	//     (see selfStatus) — the only vantage point a headless node has.
	//   - leader headless: compare against any *other* serving voter instead.
	//     A voter's committed RaftIndex trails the leader's, so the bar is
	//     slightly weaker but still evidences cluster-wide replication progress;
	//     it's the best networked proxy available. With no serving member to
	//     measure against at all, skip the comparison with a logged caveat —
	//     the promote itself already required etcd's learner-readiness check,
	//     so the joiner was caught up moments ago.
	var selfClientURL string
	if len(advertiseClientUrls) > 0 {
		selfClientURL = advertiseClientUrls[0].String()
	}
	if err = retryUntil(ctx, time.Second, 5*time.Second, "confirming voter caught up", func(actx context.Context) error {
		self, serr := p.selfStatus(actx, cli, selfClientURL)
		if serr != nil {
			return serr
		}
		if self.IsLearner {
			return errors.New("still a learner")
		}
		if self.Leader == 0 {
			return errors.New("no leader in contact yet")
		}

		// Resolve a serving vantage point from the membership: the leader's
		// client URL, or — when the leader is headless — any other serving
		// voter's.
		ml, lerr := cli.MemberList(actx)
		if lerr != nil {
			return lerr
		}
		var vantageURL string
		for _, m := range ml.Members {
			if m.ID == self.Leader && len(m.ClientURLs) > 0 {
				vantageURL = m.ClientURLs[0]
				break
			}
		}
		if vantageURL == "" {
			for _, m := range ml.Members {
				if m.ID != memberID && !m.IsLearner && len(m.ClientURLs) > 0 {
					vantageURL = m.ClientURLs[0]
					break
				}
			}
		}
		if vantageURL == "" {
			logger.Warn("join: no serving member to confirm catch-up against (leader and all other voters are headless); relying on the promote's learner-readiness check")
			return nil
		}
		ref, serr := cli.Status(actx, vantageURL)
		if serr != nil {
			return serr
		}
		if self.RaftAppliedIndex < ref.RaftIndex*9/10 {
			return fmt.Errorf("catching up: applied %d < 90%% of committed %d at %s", self.RaftAppliedIndex, ref.RaftIndex, vantageURL)
		}
		return nil
	}); err != nil {
		return err
	}

	logger.Info("join: complete, voter caught up", zap.String("member-id", types.ID(memberID).String()))
	return nil
}

// selfStatus reads this node's Status for the catch-up gate: networked via the
// advertise client URL when the node serves client traffic, otherwise — a
// headless node (WithoutClientServing) has no networked vantage point on
// itself — through the in-process Self client. The in-process read path can
// transiently panic under write load (why the networked path is preferred when
// available), so a panic is recovered into a retryable error instead of
// crashing the join.
func (p *peerJoiner) selfStatus(actx context.Context, cli *clientv3.Client, selfClientURL string) (st *clientv3.StatusResponse, err error) {
	if selfClientURL != "" {
		return cli.Status(actx, selfClientURL)
	}
	defer func() {
		if r := recover(); r != nil {
			st, err = nil, fmt.Errorf("in-process status panicked: %v", r)
		}
	}()
	return p.Self().Status(actx, "") // loopback ignores the endpoint arg
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

	if started {
		// The embedded server's once-guarded lifecycle is consumed: no retry
		// on this handle could ever start it again, so latch the handle dead
		// before anything else (even if the remove below fails).
		p.exhausted.Store(true)
	}

	// The add can commit while its response is lost, failing the join before a
	// member ID was ever learned. Sweep the membership for the zombie: an
	// unstarted learner advertising our peer URLs.
	if memberID == 0 {
		id, found, ferr := findMemberByPeerURLs(ctx, cli, selfAddrs)
		if ferr != nil {
			logger.Warn("join rollback: could not read membership to check for a half-added member; verify manually",
				zap.Strings("peer-urls", selfAddrs), zap.Error(ferr))
			return
		}
		if !found {
			return // the add never committed; nothing to roll back
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

// isPermanentMemberAdd matches member-add rejections no amount of retrying can
// heal within one join: auth demands credentials this client doesn't carry
// (none are configurable on the join path), and the learner cap is the target
// cluster's standing config (stock etcd allows one learner). Spinning on these
// burns the whole join budget while holding the cluster-wide join lock.
func isPermanentMemberAdd(err error) bool {
	return errors.Is(err, rpctypes.ErrPermissionDenied) ||
		errors.Is(err, rpctypes.ErrUserEmpty) ||
		errors.Is(err, rpctypes.ErrTooManyLearners)
}

// isMemberNotLearner matches etcd's ErrMemberNotLearner ("can only promote a
// learner member") promote rejection across the shapes clientv3 returns (typed
// rpctypes error or raw gRPC status). Seeing it for our own member ID means the
// promotion already committed (a prior attempt's response was lost). The string
// form is matched by exact suffix, deliberately: a Contains match would also
// hit ErrLearnerNotReady ("can only promote a learner member which is in sync
// with leader") — the retryable not-caught-up rejection, the opposite meaning.
func isMemberNotLearner(err error) bool {
	return err != nil && (errors.Is(err, rpctypes.ErrMemberNotLearner) ||
		strings.HasSuffix(err.Error(), "can only promote a learner member"))
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
			// The deadline is the symptom; the last attempt's error is the
			// diagnosis (and stays matchable via errors.Is).
			return fmt.Errorf("%s: %w (last attempt: %w)", what, ctx.Err(), err)
		case <-time.After(interval):
		}
	}
}

// loopbackOnly reports whether every URL's host is a loopback address
// (localhost, 127.0.0.0/8, ::1). Empty input is not "only loopback".
func loopbackOnly(urls []url.URL) bool {
	for _, u := range urls {
		if !isLoopbackHost(u.Hostname()) {
			return false
		}
	}
	return len(urls) > 0
}

// allLoopbackPeers reports whether every peer URL targets a loopback host —
// the same-host multi-process case, where a loopback advertise URL works.
func allLoopbackPeers(peers []string) bool {
	for _, s := range peers {
		u, err := url.Parse(s)
		if err != nil || !isLoopbackHost(u.Hostname()) {
			return false
		}
	}
	return len(peers) > 0
}

// isLoopbackHost reports whether host is a loopback name or address. Non-IP
// hostnames other than "localhost" count as non-loopback without resolving
// them: a wrong answer only changes when a doomed join fails, and resolving
// would put DNS on the join path.
func isLoopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// sanitizePeers normalizes the caller-supplied list of peer (raft) URLs into a
// clean, de-duplicated list ready to scrape. For each entry it trims surrounding
// whitespace, defaults a missing scheme to http (so a bare "host:port" works),
// and parses it as an absolute http/https URL with a host. Anything that doesn't
// parse — malformed, wrong scheme, no host — is dropped rather than failing the
// whole join (a few bad entries shouldn't sink a list that also holds reachable
// peers) and returned in dropped so the caller can report what was discarded:
// a silently shrunk list turns a typo into an unattributable discovery timeout.
// Empty entries and duplicates are removed without being reported; first-seen
// order is preserved.
func sanitizePeers(peers []string) (out, dropped []string) {
	out = make([]string, 0, len(peers))
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
			dropped = append(dropped, raw)
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
	return out, dropped
}

// clientEndpointsFromPeers asks each peer's raft handler for the cluster
// membership (GET <peer>/members) concurrently, and returns the client URLs of
// the first peer that answers with at least one voting member. Learners are
// excluded: they don't serve raft, and their client URLs are no better an
// entrypoint than a voter's. First usable answer wins and cancels the rest;
// when every peer fails, the per-peer errors are returned immediately instead
// of waiting out the context. The scrape carries its own bound
// (discoveryTimeout) so unreachable peers can't eat the whole join budget.
func clientEndpointsFromPeers(ctx context.Context, peers []string) ([]string, error) {
	dctx, cancel := context.WithTimeout(ctx, discoveryTimeout)
	defer cancel() // also reels in straggler scrapes once a winner is picked

	type result struct {
		eps []string
		err error
	}
	ch := make(chan result, len(peers)) // buffered: losers must not leak

	for _, peer := range peers {
		go func(peer string) {
			members, err := fetchMembers(dctx, peer)
			if err != nil {
				ch <- result{err: fmt.Errorf("%s: %w", peer, err)}
				return
			}
			var eps []string
			for _, m := range members {
				if m.IsLearner {
					continue
				}
				eps = append(eps, m.ClientURLs...)
			}
			if len(eps) == 0 {
				ch <- result{err: fmt.Errorf("%s: no voting members with client URLs", peer)}
				return
			}
			ch <- result{eps: eps}
		}(peer)
	}

	var errs []error
	for range peers {
		select {
		case r := <-ch:
			if r.err != nil {
				errs = append(errs, r.err)
				continue
			}
			return r.eps, nil
		case <-dctx.Done():
			return nil, fmt.Errorf("no peer returned a member list: %w", errors.Join(append(errs, context.Cause(dctx))...))
		}
	}
	return nil, fmt.Errorf("no peer returned a member list: %w", errors.Join(errs...))
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
