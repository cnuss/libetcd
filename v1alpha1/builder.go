package v1alpha1

import v1 "github.com/cnuss/golib/v1"

// WithName sets the display name carried into the Result.
func (b *BuilderImpl[T]) WithName(name string) v1.Builder[T] {
	b.name = name
	return b
}

// WithValue sets the payload Build produces.
func (b *BuilderImpl[T]) WithValue(v T) v1.Builder[T] {
	b.value = v
	return b
}

// Build assembles the configured value and returns it. It is the terminal step;
// the result is computed once and cached, so repeated calls are cheap and
// stable.
func (b *BuilderImpl[T]) Build() v1.Result[T] {
	b.builtOnce.Do(func() {
		b.built = v1.Result[T]{Name: b.name, Value: b.value}
	})
	return b.built
}

// Name returns the configured name (empty if WithName was never called).
func (b *BuilderImpl[T]) Name() string {
	return b.name
}
