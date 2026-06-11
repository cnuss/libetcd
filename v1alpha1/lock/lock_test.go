package lock_test

import (
	"context"
	"strings"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/cnuss/libetcd/v1alpha1"
	"github.com/cnuss/libetcd/v1alpha1/lock"
)

// testClient starts a real single-node etcd on a temp dir and returns its
// in-process client, registering Stop for cleanup. The lock talks to a live
// cluster, so the tests need one.
func testClient(t *testing.T) *clientv3.Client {
	t.Helper()
	e := v1alpha1.New()
	e.WithDir(t.TempDir())
	if err := e.Start(); err != nil {
		t.Fatalf("start node: %v", err)
	}
	t.Cleanup(func() { _ = e.Stop() })
	cli := e.Self()
	if cli == nil {
		t.Fatal("nil Self client")
	}
	return cli
}

// TestAcquireReleaseKey checks the happy path: a lock is acquired, exposes a key
// namespaced under the libetcd prefix + name, and releases cleanly.
func TestAcquireReleaseKey(t *testing.T) {
	cli := testClient(t)

	l, err := lock.Acquire(context.Background(), cli, "join")
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}

	if want := "libetcd/lock/join"; !strings.HasPrefix(l.Key(), want) {
		t.Errorf("Key() = %q, want prefix %q", l.Key(), want)
	}

	if err := l.Release(); err != nil {
		t.Errorf("Release: %v", err)
	}
}

// TestMutualExclusion checks a second Acquire on the same name blocks while the
// lock is held and proceeds once it's released.
func TestMutualExclusion(t *testing.T) {
	cli := testClient(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	first, err := lock.Acquire(ctx, cli, "x")
	if err != nil {
		t.Fatalf("first Acquire: %v", err)
	}

	// No t.Errorf/t.Fatalf inside the goroutine — it could fire after the test
	// has completed and panic the binary. Send the outcome over a (buffered)
	// channel and assert from the test goroutine instead.
	type result struct {
		l   *lock.Lock
		err error
	}
	acquired := make(chan result, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		l, err := lock.Acquire(ctx, cli, "x")
		acquired <- result{l, err}
	}()
	// Join the contender on every exit path: cancel its Acquire, wait for the
	// goroutine to finish, and release any lock it won before the node stops.
	defer func() {
		cancel()
		<-done
		select {
		case r := <-acquired:
			if r.l != nil {
				_ = r.l.Release()
			}
		default:
		}
	}()

	// The contender must not get the lock while first holds it.
	select {
	case r := <-acquired:
		if r.l != nil {
			_ = r.l.Release()
		}
		if r.err != nil {
			t.Fatalf("second Acquire: %v", r.err)
		}
		t.Fatal("second Acquire succeeded while the lock was held")
	case <-time.After(500 * time.Millisecond):
	}

	if err := first.Release(); err != nil {
		t.Fatalf("release first: %v", err)
	}

	// Now it should proceed promptly.
	select {
	case r := <-acquired:
		if r.err != nil {
			t.Fatalf("second Acquire: %v", r.err)
		}
		if err := r.l.Release(); err != nil {
			t.Errorf("release second: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("second Acquire did not proceed after release")
	}
}

// TestAcquireContextDeadline checks Acquire honors its context: a contender with
// a deadline gives up (returns an error) rather than blocking forever on a held
// lock.
func TestAcquireContextDeadline(t *testing.T) {
	cli := testClient(t)

	held, err := lock.Acquire(context.Background(), cli, "y")
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	defer func() { _ = held.Release() }()

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if l, err := lock.Acquire(ctx, cli, "y"); err == nil {
		_ = l.Release()
		t.Fatal("Acquire succeeded despite a held lock and an elapsed deadline; want error")
	}
}
