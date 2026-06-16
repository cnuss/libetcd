.PHONY: all check fmt fmt-check vet build windows test e2e run

# The library and its deps are pure Go; build everything cgo-free, matching CI
# (workflow env CGO_ENABLED=0). This also keeps the cgo-heavy with-tunnel deps
# (cloudflared) building pure-Go on platforms with no working cgo toolchain.
export CGO_ENABLED := 0

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

# Library unit tests (v1alpha1).
test:
	go test ./...

# End-to-end: the harness builds and drives every example binary, plus the
# in-process TestMultiNodeTunnel. -count=1 disables go test caching, since the
# harness builds the example binaries at runtime and the cache key wouldn't
# otherwise pick up example source changes. e2e is its own module (it imports
# libtunnel, which the library module must not), so run it from its own dir.
e2e:
	cd e2e && go test -count=1 -v ./...

# Run an example by name, forwarding any trailing words as args:
#   make run single-node
#   make run with-tunnel
# Each example is its own module (own go.mod + replace ../..). Build it
# standalone (GOWORK=off, ignoring the dev go.work) to its own dir, then run
# the binary — the same build the e2e harness and CI do, so `make run` proves
# the example resolves on its own. The binary is gitignored.
run:
	cd examples/$(word 2,$(MAKECMDGOALS)) && \
		GOWORK=off go build -o $(word 2,$(MAKECMDGOALS)) . && \
		./$(word 2,$(MAKECMDGOALS)) $(wordlist 3,$(words $(MAKECMDGOALS)),$(MAKECMDGOALS))

# Swallow the example name and forwarded args (extra goals) so make doesn't error.
%:
	@:
