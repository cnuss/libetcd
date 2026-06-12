package v1alpha1

import (
	"context"
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

	"go.etcd.io/etcd/client/pkg/v3/fileutil"
	"go.etcd.io/etcd/client/pkg/v3/types"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/zap"

	v1 "github.com/cnuss/libetcd/v1"
	"github.com/cnuss/libetcd/v1alpha1/join"
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

func (p *peerJoiner) WithClientListener(lis net.Listener) v1.EtcdPeer {
	p.EtcdImpl.WithClientListener(lis)
	return p
}

func (p *peerJoiner) WithPeerListener(lis net.Listener) v1.EtcdPeer {
	p.EtcdImpl.WithPeerListener(lis)
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
	// mutate a no-op: the listener materialization below would bind sockets the
	// latched builder silently discards, and the member-add would register
	// embed's default localhost advertise URL with the live cluster, only for
	// Start to surface the cause after the damage is done.
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

	// Materialize the listeners now — the member-add below registers the
	// advertise URLs they derive. The accessors invoke each side's factory
	// exactly once; a headless client side (nil factory) stays unbound and
	// registers no client URLs. The peer listener is mandatory: its factory
	// failing (or having been nil'd) latches the cause.
	p.PeerListener()
	p.ClientListener()
	if cause := context.Cause(p.ctx); cause != nil {
		return fmt.Errorf("join: materializing listeners: %w", cause)
	}

	p.mu.Lock()
	ctx := p.userCtx
	name := p.cfg.Name
	advertisePeerUrls := p.cfg.AdvertisePeerUrls
	p.mu.Unlock()

	if len(advertisePeerUrls) == 0 {
		return errors.New("join: no advertise peer URLs; a raft member must advertise a peer URL")
	}

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
		return fmt.Errorf("join: advertise peer URLs %v are all loopback but target peers %v are not; a remote cluster can never dial back — serve a routable address via WithPeerListener", urlsToEndpoints(advertisePeerUrls), peers)
	}

	selfAddrs := urlsToEndpoints(advertisePeerUrls)

	p.mu.Lock()
	token := p.cfg.InitialClusterToken
	p.mu.Unlock()
	jc := &join.Client{HTTP: &http.Client{}, Token: token}

	// A failure from here on can leave a half-joined member behind — the add
	// itself can commit while its response is lost — so the rollback is armed
	// before the first membership mutation. joinPeer is the peer that answered
	// the add: promote and rollback target the same one.
	var memberID uint64
	var joinPeer string
	started := false
	defer func() {
		if err != nil {
			p.abortJoin(jc, peers, selfAddrs, joinPeer, memberID, started, logger)
		}
	}()

	// Add self as a learner over the peer transport: POST the join resource on
	// each peer until one answers. The answering member adds us under the
	// cluster-wide join lock (server-side) and streams back a snapshot taken
	// after the add, plus the leader-assigned identity and membership. No
	// networked clientv3, no client-side lock, no discovery scrape.
	//
	// Retried on the join ctx (not retryUntil, whose per-attempt cancel would
	// abort the snapshot download the success returns): a fresh joiner racing a
	// prior joiner's still-settling reconfig gets a transient "unhealthy
	// cluster" (mapped to a retryable error server-side); a permanent rejection
	// (ErrPermanent) stops immediately.
	var add *join.AddResult
	for {
		a, peer, aerr := joinAddToAny(ctx, jc, peers, selfAddrs)
		if aerr == nil {
			add, joinPeer, memberID = a, peer, a.SelfID
			break
		}
		if errors.Is(aerr, join.ErrPermanent) {
			return fmt.Errorf("adding self as learner: %w", aerr)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("adding self as learner: %w (last attempt: %w)", context.Cause(ctx), aerr)
		case <-time.After(time.Second):
		}
	}
	defer add.Snapshot.Close()
	logger.Info("join: added as learner", zap.String("member-id", types.ID(memberID).String()), zap.String("via", joinPeer))

	// Pin the cluster as existing so mutate stops single-member auto-sync; the
	// restore below writes the real membership into the data dir, and a node
	// booting from a WAL ignores InitialCluster anyway.
	p.mutate(func() error {
		p.cfg.ClusterState = embed.ClusterStateFlagExisting
		p.clusterSet.Store(true)
		return nil
	})

	if err = p.seedFromSnapshot(add, name); err != nil {
		return fmt.Errorf("seeding from snapshot: %w", err)
	}

	// Start with the join deadline bounding the ready wait (startWaitCtx): the
	// user context often has no deadline, and a joiner that can't reach ready
	// (e.g. the cluster lost quorum after the member-add) must fail within the
	// join budget so the rollback runs, not hang on ReadyNotify forever. The
	// run loop may be live even when Start errors, so flag started first — the
	// rollback must Stop it either way.
	p.mu.Lock()
	p.startWaitCtx = ctx
	p.mu.Unlock()
	startErr := p.Start()
	started = true
	if startErr != nil {
		return fmt.Errorf("starting etcd server: %w", startErr)
	}
	logger.Info("join: server started, promoting to voter")

	// Promote learner -> voter via the same peer, blocking until it sticks.
	// etcd rejects a not-yet-caught-up learner (ErrLearnerNotReady) — retryable;
	// with the seed the node boots already applied, so it succeeds promptly. The
	// server maps a removed member to ErrPermanent (don't spin) and a
	// lost-response promote (already a voter) to success.
	if err = retryUntil(ctx, time.Second, 5*time.Second, "promoting to voter", func(actx context.Context) error {
		perr := jc.Promote(actx, joinPeer, memberID)
		if errors.Is(perr, join.ErrPermanent) {
			return permanent(perr)
		}
		return perr
	}); err != nil {
		return err
	}
	logger.Info("join: promoted to voter")

	// Confirm caught up before returning: poll our own in-process status until
	// we're a voter with a leader in contact. There is no networked vantage by
	// design (the whole join is peer-transport only), and etcd's promote gate
	// already enforced learner-readiness, so reaching voter status with a leader
	// is the confirmation. The in-process read can transiently panic under write
	// load, so selfStatus recovers it into a retryable error.
	if err = retryUntil(ctx, time.Second, 5*time.Second, "confirming voter", func(actx context.Context) error {
		st, serr := p.selfStatus(actx)
		if serr != nil {
			return serr
		}
		if st.IsLearner {
			return errors.New("still a learner")
		}
		if st.Leader == 0 {
			return errors.New("no leader in contact yet")
		}
		return nil
	}); err != nil {
		return err
	}

	logger.Info("join: complete, voter caught up", zap.String("member-id", types.ID(memberID).String()))
	return nil
}

// joinAddToAny POSTs the join resource to each peer in turn and returns the
// first usable response and the peer that gave it. The request runs on the full
// join ctx, not a per-peer sub-timeout, because the returned snapshot body is
// read later (in seedFromSnapshot) and a cancel would abort the download mid-
// stream; an unreachable peer fails fast on its own (connection refused), and a
// blackholed one is bounded by the join ctx. Per-peer errors are joined if all
// fail.
func joinAddToAny(ctx context.Context, jc *join.Client, peers, selfAddrs []string) (*join.AddResult, string, error) {
	var errs []error
	for _, peer := range peers {
		add, err := jc.Add(ctx, peer, selfAddrs)
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", peer, err))
			continue
		}
		return add, peer, nil
	}
	return nil, "", errors.Join(errs...)
}

// selfStatus reads this node's Status in-process for the catch-up gate. The
// join is peer-transport only, so there is no networked vantage; the in-process
// read path can transiently panic under write load, so a panic is recovered
// into a retryable error instead of crashing the join.
func (p *peerJoiner) selfStatus(actx context.Context) (st *clientv3.StatusResponse, err error) {
	defer func() {
		if r := recover(); r != nil {
			st, err = nil, fmt.Errorf("in-process status panicked: %v", r)
		}
	}()
	return p.Self().Status(actx, "") // loopback ignores the endpoint arg
}

// seedFromSnapshot restores the snapshot streamed by the join's add response
// into this node's data directory, pre-seeded with the leader-assigned member
// ID, the live cluster ID, and the post-add membership. The seeded node boots
// as a follower already applied to the snapshot's raft index, so the leader
// replicates forward over the log and never sends a raft snapshot — applying
// one panics the embedded host on Windows (see v1alpha1/snapshot/snapshot.md).
// It runs after the add (selfID + membership known) and before Start.
func (p *peerJoiner) seedFromSnapshot(add *join.AddResult, selfName string) error {
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

	// The leader records the new learner with an empty name until it starts and
	// publishes; fill in our real name so the seed boots as this member.
	members := make([]snapshot.MemberInfo, len(add.Members))
	copy(members, add.Members)
	for i := range members {
		if members[i].ID == add.SelfID {
			members[i].Name = selfName
		}
	}

	// Stream the snapshot body to a scratch file (the restore target must be
	// empty), then restore from it.
	scratch, err := os.MkdirTemp("", "libetcd-seed-")
	if err != nil {
		return fmt.Errorf("scratch dir: %w", err)
	}
	defer os.RemoveAll(scratch)
	dbPath := filepath.Join(scratch, "join.db")
	f, err := os.Create(dbPath)
	if err != nil {
		return fmt.Errorf("snapshot file: %w", err)
	}
	if _, err := io.Copy(f, add.Snapshot); err != nil {
		f.Close()
		return fmt.Errorf("writing snapshot: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("closing snapshot: %w", err)
	}

	return snapshot.NewV3(lg).Restore(snapshot.RestoreConfig{
		SnapshotPath:  dbPath,
		Name:          selfName,
		SelfID:        add.SelfID,
		ClusterID:     add.ClusterID,
		Members:       members,
		OutputDataDir: dir,
	})
}

// abortJoin best-effort rolls back a partial join so the cluster isn't left
// with a zombie member, by asking a peer to remove us (DELETE). Order matters:
// the member is removed from the cluster first, while the local server (if
// started) is still serving — after a successful promote this node is a voter,
// and stopping it before the remove commits can drop the cluster below quorum,
// wedging it with a dead voter no reconfig can fix. The remove is retried
// because the rollback runs exactly when the cluster is mid-reconfig, so it
// sees the same transient rejections the forward path does. If the remove
// ultimately fails while the server is running, the server is left running — a
// live voter keeps quorum reachable for a manual remove — and the caller can
// Stop() afterwards. It runs on a fresh bounded context because the join's own
// context is typically already dead here.
//
// When the add never reported a member ID — it failed, or it committed
// server-side but the response was lost before the joiner parsed it (an HTTP
// POST is not atomic with its response) — there may still be a zombie learner
// holding our peer URLs. We can't name it, so we sweep every peer asking it to
// remove the unstarted learner with our URLs (RemoveByPeerURLs), idempotent if
// there's nothing to remove.
func (p *peerJoiner) abortJoin(jc *join.Client, peers, selfAddrs []string, joinPeer string, memberID uint64, started bool, logger *zap.Logger) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if started {
		// The embedded server's once-guarded lifecycle is consumed: no retry
		// on this handle could ever start it again, so latch the handle dead
		// before anything else (even if the remove below fails).
		p.exhausted.Store(true)
	}

	// Lost-response (or pre-ID failure) case: no member ID to name, so sweep
	// peers to remove any learner holding our peer URLs. Best-effort — a server
	// not running locally means there's nothing to stop afterwards.
	if memberID == 0 {
		for _, peer := range peers {
			if rerr := jc.RemoveByPeerURLs(ctx, peer, selfAddrs); rerr == nil {
				logger.Info("join rollback: removed half-joined learner by peer URL", zap.String("via", peer))
				return
			}
		}
		return
	}

	if rerr := retryUntil(ctx, time.Second, 5*time.Second, "removing half-joined member", func(actx context.Context) error {
		// Remove maps an already-gone member to success server-side; a removed
		// member (ErrPermanent) is also nothing to roll back.
		err := jc.Remove(actx, joinPeer, memberID)
		if errors.Is(err, join.ErrPermanent) {
			return nil
		}
		return err
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
