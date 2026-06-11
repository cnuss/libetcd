// Package lock is a thin wrapper over etcd's concurrency primitives for taking
// a cluster-wide distributed lock. It serializes operations across separate
// processes by holding the lock in the etcd cluster itself, not in process
// memory — so it works where a sync.Mutex can't reach (e.g. several nodes each
// joining the same cluster from their own process).
package lock

import (
	"context"
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

// keyPrefix namespaces every lock key so libetcd locks don't collide with
// application keys in the same cluster.
const keyPrefix = "libetcd/lock/"

// sessionTTL bounds how long a crashed holder keeps the lock before etcd's
// lease expires and another waiter can take it.
const sessionTTL = 30

// releaseTimeout caps the unlock + session-close on Release so a wedged cluster
// can't block teardown indefinitely.
const releaseTimeout = 5 * time.Second

// Lock is a held distributed lock. Release it when done.
type Lock struct {
	sess *concurrency.Session
	mu   *concurrency.Mutex
}

// Acquire blocks until it holds the named lock on the cluster reachable through
// cli, or ctx is done (returning its error). It makes a single attempt; callers
// that must ride out transient cluster unavailability should retry Acquire.
func Acquire(ctx context.Context, cli *clientv3.Client, name string) (*Lock, error) {
	sess, err := concurrency.NewSession(cli, concurrency.WithContext(ctx), concurrency.WithTTL(sessionTTL))
	if err != nil {
		return nil, fmt.Errorf("lock %q: new session: %w", name, err)
	}
	mu := concurrency.NewMutex(sess, keyPrefix+name)
	if err := mu.Lock(ctx); err != nil {
		_ = sess.Close()
		return nil, fmt.Errorf("lock %q: acquire: %w", name, err)
	}
	return &Lock{sess: sess, mu: mu}, nil
}

// Release unlocks and closes the underlying session, returning the first error.
// It uses a fresh bounded context so it still releases even if the context that
// took the lock is already cancelled.
func (l *Lock) Release() error {
	ctx, cancel := context.WithTimeout(context.Background(), releaseTimeout)
	defer cancel()
	uerr := l.mu.Unlock(ctx)
	serr := l.sess.Close()
	if uerr != nil {
		return uerr
	}
	return serr
}

func (l *Lock) Key() string {
	return l.mu.Key()
}
