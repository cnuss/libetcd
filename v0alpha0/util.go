package v0alpha0

import (
	"crypto/rand"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"net"
	"net/url"
	"reflect"
	"sort"
	"time"

	"go.etcd.io/etcd/pkg/v3/idutil"
	"go.uber.org/zap"
)

// idGen is a process-global unique id generator using etcd's own idutil scheme
// (random member-id prefix | timestamp | counter), seeded once at startup.
var idGen = idutil.NewGenerator(randUint16(), time.Now())

// randUint16 returns a random 16-bit value to seed the generator's prefix.
func randUint16() uint16 {
	var b [2]byte
	if _, err := rand.Read(b[:]); err != nil {
		return 0
	}
	return binary.BigEndian.Uint16(b[:])
}

// defaultName returns a unique member name from the global id generator, so a
// node created with New() doesn't collide with others or trip etcd's "default
// name" warning. WithName overrides it.
func defaultName() string {
	return fmt.Sprintf("node-%x", idGen.Next())
}

// listenerURL builds a URL from a listener's bound address, inferring the scheme
// (https if the listener is TLS-wrapped, http otherwise).
func listenerURL(l net.Listener) url.URL {
	scheme := "http"
	if isTLS(l) {
		scheme = "https"
	}
	return url.URL{Scheme: scheme, Host: l.Addr().String()}
}

// parseAdvertiseURLs parses the WithPeerListener advertise-URL overrides into
// peer URLs etcd will accept: an explicit host:port (a missing port is filled
// from the scheme — https→443, http→80, since etcd requires host:port) and no
// path (etcd peer URLs carry none, so a trailing slash is dropped). A public
// tunnel URL like https://x.trycloudflare.com/ becomes https://x.trycloudflare.com:443.
// Unparseable or hostless entries are dropped. Returns nil when none are given
// or none survive (the caller falls back to the listener's own address); the
// logger notes the fallback. lg is passed in (not read via b.Logger()) because
// callers hold b.mu, which Logger() would re-lock.
func parseAdvertiseURLs(advertiseURLs []string, lg *zap.Logger) []url.URL {
	if len(advertiseURLs) == 0 {
		return nil
	}
	var urls []url.URL
	seen := make(map[string]struct{}, len(advertiseURLs))
	for _, s := range advertiseURLs {
		u, err := url.Parse(s)
		if err != nil || u.Hostname() == "" {
			continue
		}
		if u.Port() == "" {
			port := "80"
			if u.Scheme == "https" {
				port = "443"
			}
			u.Host = net.JoinHostPort(u.Hostname(), port)
		}
		u.Path = "" // etcd peer URLs carry no path (drops a trailing slash)
		// Dedup after normalization, so entries that differ only by a trailing
		// slash or an implicit port collapse to one.
		if _, dup := seen[u.String()]; dup {
			continue
		}
		seen[u.String()] = struct{}{}
		urls = append(urls, *u)
	}
	if len(urls) == 0 && lg != nil {
		lg.Warn("no valid advertise peer URLs, falling back to the listener address",
			zap.Strings("advertiseURLs", advertiseURLs))
	}
	// Sort for a stable advertise order regardless of argument order.
	sort.Slice(urls, func(i, j int) bool { return urls[i].String() < urls[j].String() })
	return urls
}

// applyPeerURLs sets the peer listen + advertise URLs for the bound listener:
// listen is always the listener's own address; advertise is the explicit
// override (peerAdvertise) when set, otherwise the listener's address too.
// Callers hold b.mu (it runs inside mutate or the PeerListener materialization).
func (b *EtcdImpl) applyPeerURLs(lis net.Listener) {
	u := listenerURL(lis)
	b.cfg.ListenPeerUrls = []url.URL{u}
	if len(b.peerAdvertise) > 0 {
		b.cfg.AdvertisePeerUrls = b.peerAdvertise
	} else {
		b.cfg.AdvertisePeerUrls = []url.URL{u}
	}
}

// listenerScheme returns "https" if l carries TLS, "http" otherwise. A TLS
// listener (from tls.NewListener / tls.Listen) is an unexported *tls.listener
// that holds a non-nil *tls.Config field; we dig that out by reflection since
// the type isn't exported for a direct assertion.
func isTLS(l net.Listener) bool {
	v := reflect.ValueOf(l)
	for v.Kind() == reflect.Pointer {
		v = v.Elem()
	}
	if v.Kind() == reflect.Struct {
		tlsConfigType := reflect.TypeFor[*tls.Config]()
		for i := 0; i < v.NumField(); i++ {
			f := v.Field(i)
			if f.Type() == tlsConfigType && !f.IsNil() {
				return true
			}
		}
	}
	return false
}

// urlsToEndpoints renders URLs as endpoint strings for clientv3.Config.
func urlsToEndpoints(urls []url.URL) []string {
	eps := make([]string, len(urls))
	for i, u := range urls {
		eps[i] = u.String()
	}
	return eps
}
