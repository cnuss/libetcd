package e2e

import (
	"context"
	"net"
	"net/http"
	"testing"

	"github.com/cnuss/libetcd"
	"github.com/cnuss/libtunnel"
)

// TestSingleNodeTunnel boots a single libetcd node fronted by a public
// Cloudflare tunnel (libtunnel): the node serves its own peer (raft) HTTP on a
// local socket and advertises the tunnel URL, so other members could join it
// without a routable address of its own.
//
// It is the minimal shape of BYO peer serving:
//   - WithPeerListener(nil, tunnelURL) — libetcd binds no peer socket; we serve
//     PeerHandler() ourselves at the advertised tunnel URL.
//   - From() with no peers bootstraps a single-member cluster; Join() starts it.
//
// In-process, dials a real Cloudflare tunnel — gated like the rest of the suite
// (gateE2E), needs outbound network.
func TestSingleNodeTunnel(t *testing.T) {
	gateE2E(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // cancelling the context gracefully stops the node + tunnel

	// Bind a local socket and front it with a public Cloudflare tunnel.
	// WithContext makes tunnel.URL() block until the tunnel is up and routable.
	lis, err := net.Listen("tcp", "0.0.0.0:0")
	if err != nil {
		t.Fatal(err)
	}
	tunnel := libtunnel.New(libtunnel.Cloudflare()).WithContext(ctx).WithListener(lis)

	// Bootstrap a single node that advertises the tunnel URL as its peer
	// address. BYO peer serving: libetcd binds no peer socket — we serve the
	// raft HTTP ourselves. From() with no peers bootstraps; Join() starts it.
	e := libetcd.From().WithPeerListener(nil, tunnel.URL().String()).WithContext(ctx)
	if err := e.Join(); err != nil {
		t.Fatal(err)
	}
	// Stop the node, but only after we stop serving (defers run LIFO): Stop
	// closes the etcd backend, and a peer request reaching the handler after
	// that panics in etcd's handler.
	defer e.Stop()

	// Serve the peer protocol on the tunnel-fronted listener — only after Join
	// returns, so PeerHandler() doesn't mint the server prematurely.
	mux := http.NewServeMux()
	for _, path := range e.PeerPaths() {
		mux.Handle(path, e.PeerHandler())
	}
	srv := &http.Server{Handler: mux}
	go srv.Serve(lis)
	defer srv.Close()

	cli := e.Client()
	if _, err := cli.Put(ctx, "greeting", "hello world"); err != nil {
		t.Fatal(err)
	}
	resp, err := cli.Get(ctx, "greeting")
	if err != nil {
		t.Fatal(err)
	}
	if got := string(resp.Kvs[0].Value); got != "hello world" {
		t.Fatalf("greeting = %q, want %q", got, "hello world")
	}
}
