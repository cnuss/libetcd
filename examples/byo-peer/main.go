// Command byo-peer shows WithPeerServing with a caller-supplied http.Server:
// an application HTTP route shares the same listener as the raft peer protocol.
// libetcd routes the raft PeerPaths to its peer handler and everything else to
// the supplied handler, so both work on one port.
package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"time"

	"github.com/cnuss/libetcd"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // cancelling the context gracefully stops the node

	// One listener carries both raft and the application's routes.
	peerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatal(err)
	}
	peerAddr := peerListener.Addr().String()

	// The caller's server: any non-raft path is handled here.
	peerHTTP := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprint(w, "hello from app")
		}),
	}

	e := libetcd.New().WithContext(ctx).WithPeerServing(peerListener, peerHTTP)
	if err := e.Start(); err != nil {
		log.Fatal(err)
	}

	// etcd works: round-trip a key through the in-process client.
	cli := e.Voters()
	if _, err := cli.Put(ctx, "greeting", "hello world"); err != nil {
		log.Fatal(err)
	}
	resp, err := cli.Get(ctx, "greeting")
	if err != nil {
		log.Fatal(err)
	}

	// The application route works on the same port the peer listener owns.
	httpCli := &http.Client{Timeout: 5 * time.Second}
	r, err := httpCli.Get("http://" + peerAddr + "/app")
	if err != nil {
		log.Fatal(err)
	}
	defer r.Body.Close()
	body, _ := io.ReadAll(r.Body)

	fmt.Printf("etcd: %s | app: %s\n", resp.Kvs[0].Value, body)
	// Output: etcd: hello world | app: hello from app
}
