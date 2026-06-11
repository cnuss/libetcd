// Command external-peer-serving carries the raft (peer) transport on a
// caller-owned HTTP server instead of a libetcd-bound socket.
//
// The node opts out of managed peer serving with WithoutPeerServing, passing
// the address of a listener the application bound itself — that address becomes
// the advertise-peer-URL the rest of the cluster dials. After Start, the
// application mounts PeerHandler on the raft PeerPaths of its own mux,
// alongside its own routes (here /healthz), and serves it. A second node then
// joins through that server — the leader has no other peer endpoint, so the
// join completing proves raft (including the 206 stream negotiation inside
// PeerHandler) flowed through the application's transport.
package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"sync/atomic"

	"github.com/cnuss/libetcd"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // cancelling the context gracefully stops both nodes

	// The application owns the peer transport: bind the listener first so its
	// address is known at configuration time.
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatal(err)
	}

	leader := libetcd.New().WithoutPeerServing(lis.Addr().String()).WithContext(ctx)
	if err := leader.Start(); err != nil {
		log.Fatal(err)
	}

	// Mount the raft paths on the application's own mux — after Start, so
	// PeerHandler resolves against the started server — next to an application
	// route. Count raft requests to show the traffic really came through here.
	var raftHits atomic.Int64
	ph := leader.PeerHandler()
	counted := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raftHits.Add(1)
		ph.ServeHTTP(w, r)
	})
	mux := http.NewServeMux()
	for _, p := range leader.PeerPaths() {
		mux.Handle(p, counted)
	}
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		io.WriteString(w, "ok")
	})
	srv := &http.Server{Handler: mux}
	go func() { _ = srv.Serve(lis) }()
	defer srv.Close()

	if _, err := leader.Voters().Put(ctx, "greeting", "hello world"); err != nil {
		log.Fatal(err)
	}

	// The leader's only peer endpoint is the application server above; the join
	// scrapes /members there, and raft replication streams through it.
	peer := libetcd.From(leader.Peers()...).WithContext(ctx)
	if err := peer.Join(); err != nil {
		log.Fatal(err)
	}

	// The pre-join write replicated to the joiner...
	resp, err := peer.Self().Get(ctx, "greeting")
	if err != nil || len(resp.Kvs) != 1 || string(resp.Kvs[0].Value) != "hello world" {
		log.Fatalf("read on the joiner: %v %v", resp, err)
	}
	// ...and a post-join write replicates back to the leader.
	if _, err := peer.Self().Put(ctx, "after-join", "ok"); err != nil {
		log.Fatal(err)
	}
	resp, err = leader.Self().Get(ctx, "after-join")
	if err != nil || len(resp.Kvs) != 1 || string(resp.Kvs[0].Value) != "ok" {
		log.Fatalf("read back on the leader: %v %v", resp, err)
	}

	// The application's own route coexists with raft on the same server.
	hres, err := http.Get("http://" + lis.Addr().String() + "/healthz")
	if err != nil {
		log.Fatal(err)
	}
	body, _ := io.ReadAll(hres.Body)
	hres.Body.Close()
	if string(body) != "ok" {
		log.Fatalf("/healthz = %q, want ok", body)
	}

	if raftHits.Load() == 0 {
		log.Fatal("no raft requests hit the application server")
	}
	fmt.Printf("raft requests served by the application server: %d\n", raftHits.Load())
	fmt.Println("external-peer-serving success: verified")
}
