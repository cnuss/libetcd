// Package examples holds shared helpers for the runnable examples.
package examples

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	v1 "github.com/cnuss/libetcd/v1"
)

const loadWorkers = 8

// Load drives read/write traffic against one or more etcd nodes and prints a
// throughput + latency summary, plus the current member list, every interval.
// Registering a node with WithEtcd kicks off load against it immediately.
type Load struct {
	ctx      context.Context
	interval time.Duration

	ops   atomic.Int64
	nanos atomic.Int64
	errs  atomic.Int64

	once    sync.Once
	mu      sync.Mutex
	members *clientv3.Client // client used to read the member list for summaries
}

// NewLoad returns a Load that reports every interval until ctx is cancelled.
func NewLoad(ctx context.Context, interval time.Duration) *Load {
	return &Load{ctx: ctx, interval: interval}
}

// WithEtcd registers e as a load target and kicks off load against it. The first
// call also starts the periodic summary printer. Returns the Load for chaining.
func (l *Load) WithEtcd(e v1.Etcd) *Load {
	cli := e.Client()
	if cli == nil {
		return l
	}
	l.mu.Lock()
	l.members = cli
	l.mu.Unlock()

	for w := range loadWorkers {
		go l.worker(cli, w)
	}
	go l.status(e) // per-node endpoint status printer
	l.once.Do(func() { go l.report() })
	return l
}

// status prints e's endpoint status (via its in-process loopback client) every
// interval until ctx is cancelled.
func (l *Load) status(e v1.Etcd) {
	lb := e.Loopback()
	if lb == nil {
		return
	}
	ticker := time.NewTicker(l.interval)
	defer ticker.Stop()
	for {
		select {
		case <-l.ctx.Done():
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(l.ctx, time.Second)
			st, err := lb.Status(ctx, "") // loopback ignores the endpoint arg
			cancel()
			if err != nil {
				continue
			}
			fmt.Printf("status %16x: v%s db=%dB leader=%x term=%d index=%d\n",
				st.Header.MemberId, st.Version, st.DbSize, st.Leader, st.RaftTerm, st.RaftIndex)
		}
	}
}

// worker hammers Put+Get round-trips on a per-worker key until ctx is cancelled.
func (l *Load) worker(cli *clientv3.Client, w int) {
	key := fmt.Sprintf("load/%d", w)
	for l.ctx.Err() == nil {
		start := time.Now()
		_, err := cli.Put(l.ctx, key, "v")
		if err == nil {
			_, err = cli.Get(l.ctx, key)
		}
		if err != nil {
			l.errs.Add(1)
			continue
		}
		l.nanos.Add(time.Since(start).Nanoseconds())
		l.ops.Add(1)
	}
}

// report prints a summary every interval until ctx is cancelled.
func (l *Load) report() {
	ticker := time.NewTicker(l.interval)
	defer ticker.Stop()

	var lastOps, lastNanos int64
	last := time.Now()
	for {
		select {
		case <-l.ctx.Done():
			return
		case now := <-ticker.C:
			o, n, e := l.ops.Load(), l.nanos.Load(), l.errs.Load()
			dOps, dNanos := o-lastOps, n-lastNanos

			var tput, avgMs float64
			if secs := now.Sub(last).Seconds(); secs > 0 {
				tput = float64(dOps) / secs
			}
			if dOps > 0 {
				avgMs = float64(dNanos) / float64(dOps) / 1e6
			}
			fmt.Printf("load: %7.0f rtrips/s  avg %6.2f ms  (total %d, errs %d)\n", tput, avgMs, o, e)
			l.printMembers()

			lastOps, lastNanos, last = o, n, now
		}
	}
}

// printMembers prints the cluster's current members under the summary line.
func (l *Load) printMembers() {
	l.mu.Lock()
	cli := l.members
	l.mu.Unlock()
	if cli == nil {
		return
	}
	ctx, cancel := context.WithTimeout(l.ctx, time.Second)
	defer cancel()
	ml, err := cli.MemberList(ctx)
	if err != nil {
		return
	}
	for _, m := range ml.Members {
		fmt.Printf("  %16x  %-22s learner=%-5v  peers=%v\n", m.ID, m.Name, m.IsLearner, m.PeerURLs)
	}
}
