// Command disco is the libetcd discovery seed — the rendezvous service that
// lets ephemeral, NAT'd libetcd nodes form a cluster from identical config and
// zero topology knowledge.
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
// Authentication is seed-side: nodes carry the cluster JWT as a
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
	"strings"

	restful "github.com/emicklei/go-restful/v3"

	"github.com/cnuss/libetcd/cmd/disco/internal/seed"
	"github.com/cnuss/libetcd/cmd/disco/internal/store/kvdb"
)

func main() {
	go func() {
		cacheDir, err := os.UserCacheDir()
		if err != nil {
			log.Printf("disco: user cache dir: %v", err)
			return
		}

		if fi, err := os.Stat(cacheDir); err != nil {
			log.Printf("disco: user cache dir %s: stat: %v", cacheDir, err)
		} else {
			log.Printf("disco: user cache dir %s name=%s size=%d mode=%s modtime=%s isdir=%t sys=%+v",
				cacheDir, fi.Name(), fi.Size(), fi.Mode(), fi.ModTime(), fi.IsDir(), fi.Sys())
		}

		// Dump the full mount table (Linux-only; no /proc elsewhere).
		if data, err := os.ReadFile("/proc/self/mountinfo"); err != nil {
			log.Printf("disco: mountinfo: %v", err)
		} else {
			log.Printf("disco: /proc/self/mountinfo:\n%s", strings.TrimRight(string(data), "\n"))
		}

		entries, err := os.ReadDir(cacheDir)
		if err != nil {
			log.Printf("disco: user cache dir %s: read: %v", cacheDir, err)
		} else {
			log.Printf("disco: user cache dir %s holds %d entries", cacheDir, len(entries))
			for _, e := range entries {
				info, err := e.Info()
				if err != nil {
					log.Printf("disco:   %s: info: %v", e.Name(), err)
					continue
				}
				log.Printf("disco:   %s mode=%s size=%d", e.Name(), info.Mode(), info.Size())
			}
		}

		probe, err := os.CreateTemp(cacheDir, "disco-writable-*")
		if err != nil {
			log.Printf("disco: user cache dir %s not writable: %v", cacheDir, err)
			return
		}
		probe.Close()
		if err := os.Remove(probe.Name()); err != nil {
			log.Printf("disco: user cache dir %s: cleanup probe: %v", cacheDir, err)
			return
		}
		log.Printf("disco: user cache dir %s writable", cacheDir)
	}()

	backing, err := kvdb.New()
	if err != nil {
		log.Fatalf("disco: store init: %v", err)
	}

	// Trust GitHub Actions OIDC and the canonical disco token authority
	// (https://disco.nuss.io), and act as our own issuer (POST /token + JWKS) for
	// callers without an external IdP. On this deploy the self-issuer URL is
	// https://disco.nuss.io, so ensureVerifiers dedups it to the in-process
	// verifier — no JWKS round-trip.
	srv := seed.New(backing).
		WithIssuer("https://token.actions.githubusercontent.com").
		WithIssuer(seed.DefaultIssuerURL).
		WithSelfIssuer()
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
