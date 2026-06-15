package e2e

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// runner builds one example binary, then runs it. The harness builds at test
// time (not via `go build ./...`) so example source changes are always picked
// up — that's why `make e2e` passes -count=1 to defeat the test cache.
type runner struct {
	name string
	bin  string
}

func newRunner(t *testing.T, name string) *runner {
	t.Helper()
	bin := filepath.Join(t.TempDir(), name)
	if runtime.GOOS == "windows" {
		bin += ".exe"
	}
	// Build the example as its own standalone module: cd into its directory
	// (each examples/<name> carries its own go.mod + replace) and disable the
	// workspace (GOWORK=off) so this proves the example resolves on its own,
	// not via the repo's go.work. bin is absolute, so -o still lands there.
	cmd := exec.Command("go", "build", "-o", bin, ".")
	cmd.Dir = filepath.Join("..", "examples", name)
	cmd.Env = append(os.Environ(), "GOWORK=off")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build %s: %v\n%s", name, err, out)
	}
	return &runner{name: name, bin: bin}
}

// run executes the built example with args and returns (output, exitCode).
// exitCode is -1 if the process could not be started.
func (r *runner) run(t *testing.T, args ...string) (string, int) {
	t.Helper()
	out, err := exec.Command(r.bin, args...).CombinedOutput()
	code := 0
	if err != nil {
		if ee, ok := err.(*exec.ExitError); ok {
			code = ee.ExitCode()
		} else {
			code = -1
		}
	}
	t.Logf("$ %s %s (exit %d)\n%s", r.name, strings.Join(args, " "), code, out)
	return string(out), code
}

// assertExample builds an example, runs it, and checks the exit code is 0 and
// stdout contains want. Each example added under examples/ should get a row in
// the table below.
func assertExample(t *testing.T, name, want string) {
	t.Helper()
	r := newRunner(t, name)
	out, code := r.run(t)
	if code != 0 {
		t.Errorf("%s exited %d, want 0", name, code)
	}
	if !strings.Contains(out, want) {
		t.Errorf("%s output %q does not contain %q", name, out, want)
	}
}

func TestExamples(t *testing.T) {
	gateE2E(t)
	cases := []struct {
		name string
		want string
	}{
		{"single-node", "greeting: hello world"},
		{"multi-node", "greeting from node 2: hello world"},
		{"async-join", "all 3 puts survived"},
		{"load-join", "load-join success: verified"},
		{"dir-handoff", "dir-handoff success: verified 16/16 keys"},
		{"restart-cycle", "restart-cycle success: verified 24 keys on 2 members across 2 restart cycles"},
		{"headless-leader", "headless-leader success: verified 3 voters, 1 headless"},
		{"with-tunnel", "with-tunnel success:"},
	}
	// Examples run serially: each boots a real embedded node binding loopback
	// ports, and concurrent runs contend for ports and CPU.
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assertExample(t, tc.name, tc.want)
		})
	}
}

// gateE2E skips the whole e2e suite on CI cells not chosen to run it. Each
// example boots real etcd nodes (with-tunnel also dials real Cloudflare
// tunnels), so CI runs the suite on just a few variants (the workflow sets
// LIBETCD_E2E=1 there) rather than all matrix cells; the examples are still
// built on every cell by a separate CI step. Outside CI (CI unset) the suite
// always runs, so `make e2e` covers it locally.
func gateE2E(t *testing.T) {
	t.Helper()
	if os.Getenv("CI") == "true" && os.Getenv("LIBETCD_E2E") != "1" {
		t.Skip("e2e gated off on this CI cell (runs on a few variants); set LIBETCD_E2E=1 to force")
	}
}
