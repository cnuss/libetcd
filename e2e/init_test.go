package e2e

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cnuss/libetcd"
)

func TestInitThenStart(t *testing.T) {
	gateE2E(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	dir := filepath.Join(t.TempDir(), "m0")

	// Offline init through the builder.
	if err := libetcd.New().WithName("m0").WithDir(dir).Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}
	// Idempotent rerun (validates instead of clobbering).
	if err := libetcd.New().WithName("m0").WithDir(dir).Init(); err != nil {
		t.Fatalf("reinit: %v", err)
	}
	// Wrong token must be rejected.
	if err := libetcd.New().WithName("m0").WithDir(dir).WithClusterToken("other").Init(); err == nil {
		t.Fatal("wrong-token Init succeeded")
	}
	// Sanity: dir has member/wal + member/snap/db.
	if _, err := os.Stat(filepath.Join(dir, "member", "snap", "db")); err != nil {
		t.Fatal(err)
	}

	// Boot a real node over the initialized dir — no --initial-* equivalents,
	// the dir carries identity and membership.
	n := libetcd.New().WithDir(dir)
	if err := n.Start(); err != nil {
		t.Fatalf("Start over initialized dir: %v", err)
	}
	defer n.Stop()

	cli := n.Self()
	if _, err := cli.Put(ctx, "initk", "initv"); err != nil {
		t.Fatal(err)
	}
	resp, err := cli.Get(ctx, "initk")
	if err != nil || len(resp.Kvs) != 1 || string(resp.Kvs[0].Value) != "initv" {
		t.Fatalf("get: %v %+v", err, resp)
	}
	t.Logf("node booted over Init()-produced dir; roundtrip ok")
}
