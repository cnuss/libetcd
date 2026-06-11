package v1alpha1

import (
	"context"
	"fmt"
	"net/url"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3client"
	"go.uber.org/zap"
)

// Logger returns the zap logger the node is configured with (the one wired up
// by WithLog, or the silent default). Read under the config mutex.
func (b *EtcdImpl) Logger() *zap.Logger {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.cfg.GetLogger()
}

// Self returns an in-process clientv3.Client wired to this node's minted server
// (via v3client), minted at most once. Returns nil if the server can't be
// minted.
func (b *EtcdImpl) Self() *clientv3.Client {
	srv := b.Server()
	if srv == nil {
		return nil
	}
	b.loopbackOnce.Do(func() {
		b.loopbackCli = v3client.New(srv)
	})
	return b.loopbackCli
}

// Leader returns a clientv3.Client pinned to the cluster leader's client URLs,
// discovered via this node's Self client, or nil if it can't be determined. The
// caller closes the returned client.
func (b *EtcdImpl) Leader() *clientv3.Client {
	self := b.Self()
	if self == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(b.ctx, 5*time.Second)
	defer cancel()

	ml, err := self.MemberList(ctx)
	if err != nil {
		return nil
	}
	st, err := self.Status(ctx, "") // loopback ignores the endpoint arg
	if err != nil {
		return nil
	}
	var leader []string
	for _, m := range ml.Members {
		if m.ID == st.Leader {
			leader = m.ClientURLs
			break
		}
	}
	if len(leader) == 0 {
		return nil
	}

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   leader,
		DialTimeout: 5 * time.Second,
		Logger:      b.Logger(),
	})
	if err != nil {
		return nil
	}
	return cli
}

// Voters returns a networked clientv3.Client that dials the cluster's voting
// members (learners excluded). It discovers the voters via the in-process Self
// client's MemberList; if that's unavailable it falls back to this node's own
// client URLs. Returns nil if the configuration is invalid or the client can't
// be built (the underlying error is latched as the builder cause).
func (b *EtcdImpl) Voters() *clientv3.Client {
	b.mu.Lock()
	cause := context.Cause(b.ctx)
	eps := urlsToEndpoints(b.cfg.AdvertiseClientUrls) // fallback: self
	b.mu.Unlock()

	if cause != nil {
		return nil
	}

	// Prefer the cluster's voting members.
	if lb := b.Self(); lb != nil {
		ctx, cancel := context.WithTimeout(b.ctx, 5*time.Second)
		ml, err := lb.MemberList(ctx)
		cancel()
		if err == nil {
			var voters []string
			for _, m := range ml.Members {
				if m.IsLearner {
					continue
				}
				voters = append(voters, m.ClientURLs...)
			}
			if len(voters) > 0 {
				eps = voters
			}
		}
	}

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   eps,
		DialTimeout: 5 * time.Second,
		Logger:      b.Logger(),
	})
	if err != nil {
		b.cancel(fmt.Errorf("dial client: %w", err))
		return nil
	}
	return cli
}

// Peers returns the flat list of every member's peer (raft) URLs, discovered by
// calling MemberList through the in-process Self client. Learners are included.
// The list is what From consumes to join a node to this cluster (it scrapes each
// peer's /members handler). Returns an empty slice if the server can't be minted
// or the member list is unavailable.
func (b *EtcdImpl) Peers() []string {
	self := b.Self()
	if self == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(b.ctx, 5*time.Second)
	defer cancel()

	ml, err := self.MemberList(ctx)
	if err != nil {
		return nil
	}

	peers := []string{}
	for _, m := range ml.Members {
		for _, u := range m.PeerURLs {
			parsed, err := url.Parse(u)
			if err != nil {
				continue // skip a member with unparseable peer URLs
			}
			peers = append(peers, parsed.String())
		}
	}
	return peers
}
