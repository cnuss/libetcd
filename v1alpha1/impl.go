// Package v1alpha1 is the current implementation behind the v1.Builder
// interface. The root libetcd façade wraps this; callers reaching directly into
// v1alpha1 use it for the concrete types. Anything here may change between alpha
// revisions — depend on the v1 contract, not these internals.
package v1alpha1

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"os"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"

	v1 "github.com/cnuss/libetcd/v1"
)

// New returns an unconfigured BuilderImpl. The root libetcd.New façade wraps this
// and returns it as the v1.Builder interface.
func New() *BuilderImpl {
	return &BuilderImpl{}
}

// BuilderImpl is the default Builder implementation: it accumulates configuration
// from the With* methods and translates it into an embed.Config at Start.
type BuilderImpl struct {
	name       string
	dir        string
	clientPort *int     // nil = default 2379; set (incl. 0) overrides
	peerPort   *int     // nil = default 2380; set (incl. 0) overrides
	clientURLs []string // non-empty overrides clientPort
	peerURLs   []string // non-empty overrides peerPort
	peers      map[string]string
	token      string
	existing   bool
	logLevel   string
}

// defaults applied when the corresponding field is left unset.
const (
	defaultName       = "default"
	defaultClientPort = 2379
	defaultPeerPort   = 2380
	defaultToken      = "libetcd-cluster"
	defaultLogLevel   = "error"
	startTimeout      = 30 * time.Second
)

// Start translates the accumulated configuration into an embed.Config, boots the
// server, waits until it is ready (bounded by ctx or startTimeout), dials a
// client, and returns the running handle.
func (b *BuilderImpl) Start(ctx context.Context) (v1.Etcd, error) {
	cfg, err := b.config()
	if err != nil {
		return nil, err
	}

	e, err := embed.StartEtcd(cfg)
	if err != nil {
		return nil, fmt.Errorf("start etcd: %w", err)
	}

	// Bound startup independently so a context without a deadline still can't
	// hang forever on a node that never reports ready.
	waitCtx := ctx
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		waitCtx, cancel = context.WithTimeout(ctx, startTimeout)
		defer cancel()
	}

	select {
	case <-e.Server.ReadyNotify():
	case err := <-e.Err():
		e.Close()
		return nil, fmt.Errorf("etcd failed during startup: %w", err)
	case <-waitCtx.Done():
		e.Close()
		return nil, fmt.Errorf("etcd not ready: %w", waitCtx.Err())
	}

	endpoints := clientEndpoints(e)
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
		Context:     ctx,
	})
	if err != nil {
		e.Close()
		return nil, fmt.Errorf("dial client: %w", err)
	}

	return &etcdHandle{e: e, cli: cli, endpoints: endpoints}, nil
}

// config builds the embed.Config from the accumulated settings, resolving any
// auto (port 0) ports to concrete free ports so advertised URLs are never ":0".
func (b *BuilderImpl) config() (*embed.Config, error) {
	cfg := embed.NewConfig()
	cfg.Logger = "zap"

	cfg.Name = b.name
	if cfg.Name == "" {
		cfg.Name = defaultName
	}

	if b.dir != "" {
		cfg.Dir = b.dir
	} else {
		dir, err := os.MkdirTemp("", "libetcd-"+cfg.Name+"-")
		if err != nil {
			return nil, fmt.Errorf("create data dir: %w", err)
		}
		cfg.Dir = dir
	}

	clientURLs, err := b.resolveURLs(b.clientURLs, b.clientPort, defaultClientPort)
	if err != nil {
		return nil, fmt.Errorf("client url: %w", err)
	}
	peerURLs, err := b.resolveURLs(b.peerURLs, b.peerPort, defaultPeerPort)
	if err != nil {
		return nil, fmt.Errorf("peer url: %w", err)
	}
	cfg.ListenClientUrls, cfg.AdvertiseClientUrls = clientURLs, clientURLs
	cfg.ListenPeerUrls, cfg.AdvertisePeerUrls = peerURLs, peerURLs

	if len(b.peers) > 0 {
		parts := make([]string, 0, len(b.peers))
		for name, purl := range b.peers {
			parts = append(parts, name+"="+purl)
		}
		cfg.InitialCluster = strings.Join(parts, ",")
	} else {
		cfg.InitialCluster = cfg.Name + "=" + peerURLs[0].String()
	}

	cfg.InitialClusterToken = b.token
	if cfg.InitialClusterToken == "" {
		cfg.InitialClusterToken = defaultToken
	}

	if b.existing {
		cfg.ClusterState = embed.ClusterStateFlagExisting
	} else {
		cfg.ClusterState = embed.ClusterStateFlagNew
	}

	cfg.LogLevel = b.logLevel
	if cfg.LogLevel == "" {
		cfg.LogLevel = defaultLogLevel
	}

	return cfg, nil
}

// resolveURLs turns explicit URL strings, or a localhost port, into []url.URL. A
// nil port means "use fallback"; a port of 0 (explicit or fallback path) is
// resolved to a concrete free port so advertised URLs are dialable.
func (b *BuilderImpl) resolveURLs(explicit []string, port *int, fallback int) ([]url.URL, error) {
	if len(explicit) > 0 {
		out := make([]url.URL, 0, len(explicit))
		for _, raw := range explicit {
			u, err := url.Parse(raw)
			if err != nil {
				return nil, fmt.Errorf("parse %q: %w", raw, err)
			}
			out = append(out, *u)
		}
		return out, nil
	}

	p := fallback
	if port != nil {
		p = *port
	}
	if p == 0 {
		free, err := freePort()
		if err != nil {
			return nil, err
		}
		p = free
	}
	return []url.URL{{Scheme: "http", Host: fmt.Sprintf("localhost:%d", p)}}, nil
}

// freePort asks the OS for an unused TCP port on the loopback interface and
// returns it, closing the probe listener so the port is free to rebind.
func freePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, fmt.Errorf("find free port: %w", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

// clientEndpoints returns the server's actually-bound client endpoints, reading
// the live listener addresses so auto-selected ports are concrete.
func clientEndpoints(e *embed.Etcd) []string {
	eps := make([]string, 0, len(e.Clients))
	for _, l := range e.Clients {
		eps = append(eps, "http://"+l.Addr().String())
	}
	return eps
}

// etcdHandle is the running-node implementation of v1.Etcd.
type etcdHandle struct {
	e         *embed.Etcd
	cli       *clientv3.Client
	endpoints []string
}

func (h *etcdHandle) Client() *clientv3.Client { return h.cli }
func (h *etcdHandle) Endpoints() []string      { return h.endpoints }
func (h *etcdHandle) Server() *embed.Etcd      { return h.e }

// Close closes the client first (so in-flight calls drain), then stops the
// server. The data directory is left in place.
func (h *etcdHandle) Close() error {
	err := h.cli.Close()
	h.e.Close()
	return err
}
