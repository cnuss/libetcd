// Package examples holds shared helpers for the runnable examples.
package examples

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	v1 "github.com/cnuss/libetcd/v1"
)

const loadWorkers = 8

// Load drives read/write traffic against one or more etcd nodes and prints a
// throughput + latency summary with a per-node status table every interval.
// Registering a node with WithEtcd kicks off load against it immediately.
type Load struct {
	ctx      context.Context
	interval time.Duration

	ops   atomic.Int64
	nanos atomic.Int64
	errs  atomic.Int64

	once    sync.Once
	mu      sync.Mutex
	targets []v1.Etcd
	start   time.Time

	cli atomic.Value // *clientv3.Client, updated on each WithEtcd; workers load from it on each roundtrip
}

// NewLoad returns a Load that reports every interval until ctx is cancelled.
func NewLoad(ctx context.Context, interval time.Duration) *Load {
	return &Load{ctx: ctx, interval: interval}
}

// WithEtcd registers e as a load target and kicks off load against it. The first
// call also starts the periodic reporter. Returns the Load for chaining.
func (l *Load) WithEtcd(e v1.Etcd) *Load {
	l.cli.Store(e.Voters())

	l.mu.Lock()
	l.targets = append(l.targets, e)
	l.mu.Unlock()

	for w := range loadWorkers {
		go l.worker(w)
	}
	l.once.Do(func() {
		l.start = time.Now()
		go l.report()
	})
	return l
}

// worker hammers Put+Get round-trips on a per-worker key until ctx is cancelled.
func (l *Load) worker(w int) {
	key := fmt.Sprintf("load/%d", w)
	for l.ctx.Err() == nil {
		start := time.Now()
		cli := l.cli.Load().(*clientv3.Client)
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

// report prints one consolidated summary block every interval (single goroutine,
// so lines never interleave).
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
			lastOps, lastNanos, last = o, n, now

			fmt.Print(l.block(now.Sub(l.start), tput, avgMs, o, e))
		}
	}
}

// block renders one summary block: a header, the load line, and a node table.
func (l *Load) block(elapsed time.Duration, tput, avgMs float64, total, errs int64) string {
	var b strings.Builder
	rule := strings.Repeat("─", 64)

	fmt.Fprintf(&b, "\n┌─ load · %s %s\n", elapsed.Truncate(time.Second), rule[:48])
	fmt.Fprintf(&b, "│  %.0f rtrips/s   avg %.1f ms   total %d   errs %d\n", tput, avgMs, total, errs)
	fmt.Fprintf(&b, "│  %-24s %-7s %-6s %-9s %10s %4s\n", "NODE", "ROLE", "LEADER", "DB", "INDEX", "TERM")

	names := l.memberNames()
	for _, e := range l.snapshotTargets() {
		st := l.statusOf(e)
		if st == nil {
			continue
		}
		id := st.Header.MemberId
		name := names[id]
		if name == "" {
			name = fmt.Sprintf("%x", id)
		}
		role := "voter"
		if st.IsLearner {
			role = "learner"
		}
		leader := ""
		if st.Leader == id {
			leader = "★"
		}
		fmt.Fprintf(&b, "│  %-24s %-7s %-6s %-9s %10d %4d\n",
			name, role, leader, humanBytes(st.DbSize), st.RaftIndex, st.RaftTerm)
	}
	fmt.Fprintf(&b, "└%s\n", rule)
	return b.String()
}

func (l *Load) snapshotTargets() []v1.Etcd {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]v1.Etcd(nil), l.targets...)
}

// memberNames returns a member-id -> name map, queried from the first reachable
// node's loopback client.
func (l *Load) memberNames() map[uint64]string {
	for _, e := range l.snapshotTargets() {
		lb := e.Self()
		if lb == nil {
			continue
		}
		ctx, cancel := context.WithTimeout(l.ctx, time.Second)
		ml, err := lb.MemberList(ctx)
		cancel()
		if err != nil {
			continue
		}
		m := make(map[uint64]string, len(ml.Members))
		for _, mem := range ml.Members {
			m[mem.ID] = mem.Name
		}
		return m
	}
	return nil
}

// statusOf returns e's endpoint status via its in-process loopback client.
func (l *Load) statusOf(e v1.Etcd) *clientv3.StatusResponse {
	lb := e.Self()
	if lb == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(l.ctx, time.Second)
	defer cancel()
	st, err := lb.Status(ctx, "") // loopback ignores the endpoint arg
	if err != nil {
		return nil
	}
	return st
}

func humanBytes(n int64) string {
	switch {
	case n >= 1<<20:
		return fmt.Sprintf("%.1f MB", float64(n)/(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.1f KB", float64(n)/(1<<10))
	default:
		return fmt.Sprintf("%d B", n)
	}
}
