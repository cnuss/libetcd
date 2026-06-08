.PHONY: all check fmt fmt-check vet build windows test e2e run

# Default: everything CI runs except the auto-bump release step.
all: fmt-check vet build windows test e2e

# Compose the common pre-push checklist. Mirrors the CI matrix.
check: fmt-check vet windows test e2e

# gofmt the tree in place.
fmt:
	gofmt -w .

# Fail if anything in the tree is not gofmt-clean.
fmt-check:
	@out=$$(gofmt -l .); \
	if [ -n "$$out" ]; then echo "gofmt found unformatted files:"; echo "$$out"; exit 1; fi

# Static analysis across every package.
vet:
	go vet ./...

# Build the whole module for the host platform.
build:
	go build ./...

# Cross-compile + vet for Windows. A build-only smoke so a host-only library
# doesn't quietly stop building on the other major target.
windows:
	GOOS=windows go vet ./...
	GOOS=windows go build ./...

# Library unit + fuzz tests (v1alpha1) plus the godoc examples (v1).
test:
	go test ./...

# End-to-end: the harness builds and drives every example binary. -count=1 disables
# go test caching, since the harness builds the example binaries at runtime and the
# cache key wouldn't otherwise pick up example source changes.
e2e:
	go test -count=1 -v ./e2e

# Run an example by name, forwarding any trailing words as args:
#   make run basic
#   make run named
run:
	cd examples/$(word 2,$(MAKECMDGOALS)) && go run . $(wordlist 3,$(words $(MAKECMDGOALS)),$(MAKECMDGOALS))

# Swallow the example name and forwarded args (extra goals) so make doesn't error.
%:
	@:
