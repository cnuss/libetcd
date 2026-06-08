// Package v1 is the stable public surface for golib. The Builder interface and
// Result type here are the contract callers depend on across releases; the
// implementation lives in v1alpha1 and may change between alpha revisions.
package v1

// Builder assembles a value of type T from optional configuration. Configure it
// with the With* methods (each returns the Builder for chaining), then call the
// terminal Build to produce a Result. Obtain one from golib.New.
type Builder[T any] interface {
	// WithName sets a display name carried into the Result. Unset, the name is
	// empty.
	WithName(name string) Builder[T]
	// WithValue sets the payload the builder produces. Unset, Build returns the
	// zero value of T.
	WithValue(v T) Builder[T]
	// Build assembles the configured value and returns it. It is the terminal
	// step; calling it more than once returns the same Result.
	Build() Result[T]
	// Name returns the configured name (empty if WithName was never called).
	Name() string
}

// Result is the structured output of Builder.Build. The json tags make it drop
// straight into encoding/json and compatible marshalers.
type Result[T any] struct {
	Name  string `json:"name,omitempty"`
	Value T      `json:"value"`
}
