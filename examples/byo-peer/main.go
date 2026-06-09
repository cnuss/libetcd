// Command byo-peer shows WithPeerServing with a caller-supplied http.Server:
// an application HTTP route shares the same listener as the raft peer protocol.
// libetcd routes the raft PeerPaths to its peer handler and everything else to
// the supplied handler, so both work on one port.
package main

import (
	"context"
	"log"
	"net"
	"net/http"
	"time"

	"github.com/cnuss/libetcd"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // cancelling the context gracefully stops the node

	node1 := libetcd.New().WithContext(ctx).WithPeerServing(listener(), nil)
	if err := node1.Start(); err != nil {
		log.Fatal(err)
	}

	cli := node1.Voters()
	cli.Put(ctx, "now", time.Now().String())

	node2 := libetcd.From(node1.Peers()).WithContext(ctx)
	if err := node2.Join(); err != nil {
		log.Fatal(err)
	}
}

func listener() net.Listener {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatal(err)
	}
	return listener
}

func server(handler http.Handler) *http.Server {
	return &http.Server{Handler: handler}
}
