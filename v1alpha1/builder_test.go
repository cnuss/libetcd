package v1alpha1_test

import (
	"testing"

	"github.com/cnuss/golib/v1alpha1"
)

func TestBuildCarriesNameAndValue(t *testing.T) {
	res := v1alpha1.New[string]().
		WithName("greeting").
		WithValue("hello").
		Build()

	if res.Name != "greeting" {
		t.Errorf("Name = %q, want %q", res.Name, "greeting")
	}
	if res.Value != "hello" {
		t.Errorf("Value = %q, want %q", res.Value, "hello")
	}
}

func TestZeroValueWhenUnset(t *testing.T) {
	res := v1alpha1.New[int]().Build()

	if res.Name != "" {
		t.Errorf("Name = %q, want empty", res.Name)
	}
	if res.Value != 0 {
		t.Errorf("Value = %d, want 0", res.Value)
	}
}

func TestNameAccessor(t *testing.T) {
	b := v1alpha1.New[int]().WithName("widget")
	if got := b.Name(); got != "widget" {
		t.Errorf("Name() = %q, want %q", got, "widget")
	}
}

func TestBuildIsStable(t *testing.T) {
	b := v1alpha1.New[string]().WithValue("first")
	first := b.Build()

	// Mutating after the first Build must not change a subsequent Build: the
	// result is cached on the first call.
	b.WithValue("second")
	second := b.Build()

	if first != second {
		t.Errorf("Build not stable: first=%+v second=%+v", first, second)
	}
}

// FuzzBuildName checks that any name round-trips through the builder unchanged.
func FuzzBuildName(f *testing.F) {
	f.Add("")
	f.Add("greeting")
	f.Add("emoji-✓")

	f.Fuzz(func(t *testing.T, name string) {
		res := v1alpha1.New[int]().WithName(name).Build()
		if res.Name != name {
			t.Errorf("Name = %q, want %q", res.Name, name)
		}
	})
}
