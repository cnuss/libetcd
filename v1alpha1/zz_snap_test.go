package v1alpha1_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cnuss/libetcd/v1alpha1"
)

func reproSnap(t *testing.T, catchUp uint64) {
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	e1 := v1alpha1.New()
	e1.WithDir(t.TempDir()).WithLogLevel("info").WithSnapshotCatchUpEntries(catchUp).WithContext(ctx)
	if err := e1.Start(); err != nil {
		t.Fatal(err)
	}
	cli := e1.Self()
	// blast > 8000 writes
	for i := 0; i < 8000; i++ {
		if _, err := cli.Put(ctx, fmt.Sprintf("k%d", i%64), "v"); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	st, _ := cli.Status(ctx, "")
	t.Logf("node1 raftIndex before join = %d (catchUp=%d)", st.RaftIndex, catchUp)
	n := v1alpha1.New()
	n.WithDir(t.TempDir()).WithLogLevel("info").WithSnapshotCatchUpEntries(catchUp).WithContext(ctx)
	if err := n.Join(e1); err != nil {
		t.Logf("join: %v", err)
	}
	time.Sleep(2 * time.Second)
}

func TestSnapDefault(t *testing.T) { reproSnap(t, 5000) }
func TestSnapHigh(t *testing.T)    { reproSnap(t, 100_000_000) }
