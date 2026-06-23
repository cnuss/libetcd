// Command disco is the libetcd discovery seed — the rendezvous service that
// lets ephemeral, NAT'd libetcd nodes form a cluster from identical config and
// zero topology knowledge (issue #108).
//
// It serves three HTTP/1.1 endpoints — claim, register, roster — translating
// them into kvdb.io operations (https://kvdb.io/docs/api/): an atomic
// PATCH "+1" is the bootstrap claim (the caller that reads 1 wins), and a
// TTL'd, prefix-listed key set is the roster of live join targets. The seed
// holds no cluster state of its own and is NOT a raft member of the clusters it
// brokers, so it has no WAL, no embedded etcd, and no Lambda-freeze concerns.
//
// Deployment: a plain go-restful HTTP server on $PORT, packaged as a container
// and fronted by rowdy (rowdy.run) — which serves any container-on-a-port as a
// Lambda. See routes.yml and Dockerfile. Because all durable state lives in
// kvdb.io (external, shared),
// the Lambda is stateless and may scale to any number of instances: the atomic
// claim is enforced in kvdb, not in process memory, so there is no split-brain
// across instances and no reserved-concurrency pin.
//
// Authentication is seed-side (issue #108): nodes carry the cluster JWT as a
// bearer, the seed verifies it and uses its sub claim as the cluster identity
// (roster namespace). The kvdb access token is the seed's secret, held in the
// environment and never exposed to nodes.
//
// Layout (internal/, private to this binary):
//
//	internal/seed        the go-restful API + JWT gate (the HTTP surface)
//	internal/store       the backing-state contract (Store interface)
//	internal/store/kvdb  the kvdb.io implementation of Store
package main

import (
	"log"
	"net"
	"net/http"
	"os"

	restful "github.com/emicklei/go-restful/v3"

	"github.com/cnuss/libetcd/cmd/disco/internal/seed"
	"github.com/cnuss/libetcd/cmd/disco/internal/store/kvdb"
)

func main() {
	backing, err := kvdb.New()
	if err != nil {
		log.Fatalf("disco: store init: %v", err)
	}

	srv := seed.New(backing)
	defer srv.Close()

	container := restful.NewContainer()
	container.Add(srv.WebService())

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("disco: listening on %s", listener.Addr())
	log.Fatal(http.Serve(listener, container))
}
