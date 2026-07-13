package v3alpha7

import "sync"

// lazy memoizes a single computation. The zero value is ready to use: do
// runs f exactly once and every call returns the cached result. serverImpl's
// accessors use it so components build on first use, in dependency order,
// without an eager bootstrap phase.
type lazy[T any] struct {
	once sync.Once
	val  T
}

func (l *lazy[T]) do(f func() T) T {
	l.once.Do(func() { l.val = f() })
	return l.val
}
