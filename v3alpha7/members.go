package v3alpha7

import (
	"context"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"time"

	"go.uber.org/zap"

	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/etcd/server/v3/auth"
	"go.etcd.io/etcd/server/v3/etcdserver/api/membership"
	"go.etcd.io/etcd/server/v3/etcdserver/api/rafthttp"
	servererrors "go.etcd.io/etcd/server/v3/etcdserver/errors"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

const (
	// healthInterval is the minimum time the cluster should be healthy
	// before accepting add/remove member requests (upstream HealthInterval).
	healthInterval = 5 * time.Second

	// readyPercentThreshold: learner must have caught up to at least 90% of
	// the leader's match index to be promotable.
	readyPercentThreshold = 0.9

	// applyTimeout bounds waitAppliedIndex (upstream v3_server.go).
	applyTimeout = time.Second
)

// AddMember proposes adding memb through raft; returns the resulting
// member list (EtcdServer.AddMember upstream).
func (s *serverImpl) AddMember(ctx context.Context, memb membership.Member) ([]*membership.Member, error) {
	if err := s.checkMembershipOperationPermission(ctx); err != nil {
		return nil, err
	}

	b, err := json.Marshal(memb)
	if err != nil {
		return nil, err
	}

	// by default StrictReconfigCheck is enabled; reject new members if unhealthy
	if err := s.mayAddMember(memb); err != nil {
		return nil, err
	}

	cc := raftpb.ConfChange{
		Type:    raftpb.ConfChangeAddNode.Enum(),
		NodeId:  new(uint64(memb.ID)),
		Context: b,
	}
	if memb.IsLearner {
		cc.Type = raftpb.ConfChangeAddLearnerNode.Enum()
	}

	return s.configure(ctx, &cc)
}

func (s *serverImpl) mayAddMember(memb membership.Member) error {
	cfg := s.cfg()
	if !cfg.StrictReconfigCheck {
		return nil
	}
	cl := s.raftCluster()

	// protect quorum when adding voting member
	if !memb.IsLearner && !cl.IsReadyToAddVotingMember() {
		s.lg().Warn(
			"rejecting member add request; not enough healthy members",
			zap.String("local-member-id", s.MemberID().String()),
			zap.String("requested-member-add", fmt.Sprintf("%+v", memb)),
			zap.Error(servererrors.ErrNotEnoughStartedMembers),
		)
		return servererrors.ErrNotEnoughStartedMembers
	}

	// treat the new member as unavailable when checking quorum safety
	if !isConnectedToQuorumAfterAddingNewMemberSince(s.transport(), time.Now().Add(-healthInterval), s.MemberID(), cl.VotingMembers()) {
		s.lg().Warn(
			"rejecting member add request; local member has not been connected to majority peers, reconfigure breaks active quorum",
			zap.String("local-member-id", s.MemberID().String()),
			zap.String("requested-member-add", fmt.Sprintf("%+v", memb)),
			zap.Error(servererrors.ErrUnhealthy),
		)
		return servererrors.ErrUnhealthy
	}

	return nil
}

// RemoveMember proposes removing the member with the given id through raft.
func (s *serverImpl) RemoveMember(ctx context.Context, id uint64) ([]*membership.Member, error) {
	if err := s.checkMembershipOperationPermission(ctx); err != nil {
		return nil, err
	}

	// by default StrictReconfigCheck is enabled; reject removal if it leads
	// to quorum loss
	if err := s.mayRemoveMember(types.ID(id)); err != nil {
		return nil, err
	}

	cc := raftpb.ConfChange{
		Type:   raftpb.ConfChangeRemoveNode.Enum(),
		NodeId: new(id),
	}
	return s.configure(ctx, &cc)
}

func (s *serverImpl) mayRemoveMember(id types.ID) error {
	cfg := s.cfg()
	if !cfg.StrictReconfigCheck {
		return nil
	}
	cl := s.raftCluster()

	member := cl.Member(id)
	// no need to check quorum when removing non-voting member
	if member != nil && member.IsLearner {
		return nil
	}

	if !cl.IsReadyToRemoveVotingMember(uint64(id)) {
		s.lg().Warn(
			"rejecting member remove request; not enough healthy members",
			zap.String("local-member-id", s.MemberID().String()),
			zap.String("requested-member-remove-id", id.String()),
			zap.Error(servererrors.ErrNotEnoughStartedMembers),
		)
		return servererrors.ErrNotEnoughStartedMembers
	}

	// downed member is safe to remove since it's not part of the active quorum
	if t := s.transport().ActiveSince(id); id != s.MemberID() && t.IsZero() {
		return nil
	}

	// protect quorum if some members are down
	m := cl.VotingMembers()
	active := numConnectedSince(s.transport(), time.Now().Add(-healthInterval), s.MemberID(), m)
	if (active - 1) < 1+((len(m)-1)/2) {
		s.lg().Warn(
			"rejecting member remove request; local member has not been connected to all peers, reconfigure breaks active quorum",
			zap.String("local-member-id", s.MemberID().String()),
			zap.String("requested-member-remove", id.String()),
			zap.Int("active-peers", active),
			zap.Error(servererrors.ErrUnhealthy),
		)
		return servererrors.ErrUnhealthy
	}

	return nil
}

// UpdateMember proposes updating a member's raft attributes through raft.
func (s *serverImpl) UpdateMember(ctx context.Context, memb membership.Member) ([]*membership.Member, error) {
	b, merr := json.Marshal(memb)
	if merr != nil {
		return nil, merr
	}

	if err := s.checkMembershipOperationPermission(ctx); err != nil {
		return nil, err
	}
	cc := raftpb.ConfChange{
		Type:    raftpb.ConfChangeUpdateNode.Enum(),
		NodeId:  new(uint64(memb.ID)),
		Context: b,
	}
	return s.configure(ctx, &cc)
}

// PromoteMember promotes a learner node to a voting node. Only the leader
// can judge learner readiness.
// TODO: upstream forwards to the leader over peer HTTP on ErrNotLeader
// (promoteMemberHTTP, unexported); until ported, callers must retry against
// the leader themselves.
func (s *serverImpl) PromoteMember(ctx context.Context, id uint64) ([]*membership.Member, error) {
	resp, err := s.promoteMember(ctx, id)
	if err != nil && stderrors.Is(err, servererrors.ErrNotLeader) {
		s.lg().Warn("cannot promote member locally; forward the request to the leader (not yet implemented)",
			zap.String("member-id", types.ID(id).String()))
	}
	return resp, err
}

// promoteMember checks learner readiness and proposes the promotion.
// Returns ErrNotLeader when the local node lacks the information to judge
// readiness, ErrLearnerNotReady when it has it and the learner lags.
func (s *serverImpl) promoteMember(ctx context.Context, id uint64) ([]*membership.Member, error) {
	if err := s.checkMembershipOperationPermission(ctx); err != nil {
		return nil, err
	}

	if err := s.mayPromoteMember(types.ID(id)); err != nil {
		return nil, err
	}

	// mark IsLearner false and IsPromote true in the conf-change context
	promoteChangeContext := membership.ConfigChangeContext{
		Member: membership.Member{
			ID: types.ID(id),
		},
		IsPromote: true,
	}

	b, err := json.Marshal(promoteChangeContext)
	if err != nil {
		return nil, err
	}

	cc := raftpb.ConfChange{
		Type:    raftpb.ConfChangeAddNode.Enum(),
		NodeId:  new(id),
		Context: b,
	}
	return s.configure(ctx, &cc)
}

func (s *serverImpl) mayPromoteMember(id types.ID) error {
	if err := s.isLearnerReady(uint64(id)); err != nil {
		return err
	}

	cfg := s.cfg()
	if !cfg.StrictReconfigCheck {
		return nil
	}
	if !s.raftCluster().IsReadyToPromoteMember(uint64(id)) {
		s.lg().Warn(
			"rejecting member promote request; not enough healthy members",
			zap.String("local-member-id", s.MemberID().String()),
			zap.String("requested-member-remove-id", id.String()),
			zap.Error(servererrors.ErrNotEnoughStartedMembers),
		)
		return servererrors.ErrNotEnoughStartedMembers
	}

	return nil
}

// isLearnerReady checks whether the learner has caught up with the leader.
// Returns nil if the member is missing or not a learner — those conditions
// are re-checked at apply time.
func (s *serverImpl) isLearnerReady(id uint64) error {
	if err := s.waitAppliedIndex(); err != nil {
		return err
	}

	rs := s.raftNode().Status()

	// only the leader has a non-nil Progress
	if rs.Progress == nil {
		return servererrors.ErrNotLeader
	}

	var learnerMatch uint64
	isFound := false
	leaderID := rs.ID
	for memberID, progress := range rs.Progress {
		if id == memberID {
			learnerMatch = progress.Match
			isFound = true
			break
		}
	}

	if !isFound {
		return membership.ErrIDNotFound
	}

	leaderMatch := rs.Progress[leaderID].Match
	learnerReadyPercent := float64(learnerMatch) / float64(leaderMatch)

	if learnerReadyPercent < readyPercentThreshold {
		s.lg().Error(
			"rejecting promote learner: learner is not ready",
			zap.Float64("learner-ready-percent", learnerReadyPercent),
			zap.Float64("ready-percent-threshold", readyPercentThreshold),
		)
		return servererrors.ErrLearnerNotReady
	}

	return nil
}

// configure proposes a conf change through raft and waits for it to be
// applied (EtcdServer.configure upstream). Completing a proposal requires
// the whole pipeline, so it pulls runLoop() — first caller boots the server.
func (s *serverImpl) configure(ctx context.Context, cc *raftpb.ConfChange) ([]*membership.Member, error) {
	if s.runLoop() == nil {
		return nil, context.Cause(s.ctx)
	}
	cc.Id = new(s.reqIDGen().Next())
	ch := s.w().Register(cc.GetId())

	start := time.Now()
	if err := s.raftNode().ProposeConfChange(ctx, cc); err != nil {
		s.w().Trigger(cc.GetId(), nil)
		return nil, err
	}

	select {
	case x := <-ch:
		if x == nil {
			s.lg().Panic("failed to configure")
		}
		resp := x.(*confChangeResponse)
		// ensure raft has advanced before responding, or the next config
		// change may be rejected (etcd-io/etcd#15528)
		<-resp.raftAdvancedC
		s.lg().Info(
			"applied a configuration change through raft",
			zap.String("local-member-id", s.MemberID().String()),
			zap.String("raft-conf-change", cc.GetType().String()),
			zap.String("raft-conf-change-node-id", types.ID(cc.GetNodeId()).String()),
		)
		return resp.membs, resp.err

	case <-ctx.Done():
		s.w().Trigger(cc.GetId(), nil) // GC wait
		return nil, s.parseProposeCtxErr(ctx.Err(), start)

	case <-s.ctx.Done():
		return nil, servererrors.ErrStopped
	}
}

// checkMembershipOperationPermission checks auth for membership operations;
// both membership change and role management require root, so the API-layer
// TOCTOU window is acceptable (see upstream comment).
func (s *serverImpl) checkMembershipOperationPermission(ctx context.Context) error {
	as := s.authStore()
	if as == nil {
		return context.Cause(s.ctx)
	}
	authInfo, err := s.authInfoFromCtx(ctx)
	if err != nil {
		return err
	}
	return as.IsAdminPermitted(authInfo)
}

func (s *serverImpl) authInfoFromCtx(ctx context.Context) (*auth.AuthInfo, error) {
	authInfo, err := s.authStore().AuthInfoFromCtx(ctx)
	if authInfo != nil || err != nil {
		return authInfo, err
	}
	cfg := s.cfg()
	if !cfg.ClientCertAuthEnabled {
		return nil, nil
	}
	authInfo = s.authStore().AuthInfoFromTLS(ctx)
	return authInfo, nil
}

// waitAppliedIndex blocks until the applied index catches the committed
// index (or times out).
func (s *serverImpl) waitAppliedIndex() error {
	select {
	case <-s.applyWait().Wait(s.committedIdx.Load()):
	case <-s.ctx.Done():
		return servererrors.ErrStopped
	case <-time.After(applyTimeout):
		return servererrors.ErrTimeoutWaitAppliedIndex
	}
	return nil
}

func (s *serverImpl) parseProposeCtxErr(err error, start time.Time) error {
	switch {
	case stderrors.Is(err, context.Canceled):
		return servererrors.ErrCanceled

	case stderrors.Is(err, context.DeadlineExceeded):
		s.leadTimeMu.RLock()
		curLeadElected := s.leadElectedTime
		s.leadTimeMu.RUnlock()
		cfg := s.cfg()
		prevLeadLost := curLeadElected.Add(-2 * time.Duration(cfg.ElectionTicks) * time.Duration(cfg.TickMs) * time.Millisecond)
		if start.After(prevLeadLost) && start.Before(curLeadElected) {
			return servererrors.ErrTimeoutDueToLeaderFail
		}
		lead := types.ID(s.lead.Load())
		switch lead {
		case types.ID(raft.None):
			// TODO: specify that it happens because the cluster has no leader now
		case s.MemberID():
			if !isConnectedToQuorumSince(s.transport(), start, s.MemberID(), s.raftCluster().Members()) {
				return servererrors.ErrTimeoutDueToConnectionLost
			}
		default:
			if !isConnectedSince(s.transport(), start, lead) {
				return servererrors.ErrTimeoutDueToConnectionLost
			}
		}
		return servererrors.ErrTimeout

	default:
		return err
	}
}

// --- connectivity helpers (etcdserver/util.go upstream, unexported there) ---

func quorum(num int) int {
	return num/2 + 1
}

func isConnectedSince(transport rafthttp.Transporter, since time.Time, remote types.ID) bool {
	t := transport.ActiveSince(remote)
	return !t.IsZero() && t.Before(since)
}

func numConnectedSince(transport rafthttp.Transporter, since time.Time, self types.ID, members []*membership.Member) int {
	connectedNum := 0
	for _, m := range members {
		if m.ID == self || isConnectedSince(transport, since, m.ID) {
			connectedNum++
		}
	}
	return connectedNum
}

func isConnectedToQuorumSince(transport rafthttp.Transporter, since time.Time, self types.ID, members []*membership.Member) bool {
	return numConnectedSince(transport, since, self, members) >= quorum(len(members))
}

func isConnectedToQuorumAfterAddingNewMemberSince(transport rafthttp.Transporter, since time.Time, self types.ID, members []*membership.Member) bool {
	if len(members) == 1 {
		// single-member cluster: always allow adding a new member
		return true
	}
	return numConnectedSince(transport, since, self, members) >= quorum(len(members)+1)
}
