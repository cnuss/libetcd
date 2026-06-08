// Package v1alpha1 is the current implementation behind the v1.Builder
// interface. The root golib façade wraps this; callers reaching directly into
// v1alpha1 use it for the concrete struct. Anything here may change between
// alpha revisions — depend on the v1 contract, not these internals.
package v1alpha1

import (
	"sync"

	v1 "github.com/cnuss/golib/v1"
)

// New returns an unconfigured BuilderImpl for values of type T. The root
// golib.New façade wraps this and returns the v1.Builder[T] interface.
func New[T any]() *BuilderImpl[T] {
	return &BuilderImpl[T]{}
}

// BuilderImpl is the default Builder implementation. T is the produced value's
// type (and what Build wraps in a Result).
type BuilderImpl[T any] struct {
	name  string
	value T // zero value of T until WithValue is called

	// Build assembles once; subsequent calls return the cached result.
	builtOnce sync.Once
	built     v1.Result[T]
}
