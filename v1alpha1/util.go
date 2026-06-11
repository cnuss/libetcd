package v1alpha1

import (
	"crypto/rand"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"net/url"
	"reflect"
	"strings"
	"time"

	"go.etcd.io/etcd/pkg/v3/idutil"
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

// parseAdvertiseURLs parses caller-supplied advertise URLs (WithoutPeerServing)
// into url.URLs. Entries take the same shapes From accepts — bare host:port,
// http://, or https:// (a missing scheme defaults to http) — but unlike From's
// peer sanitization, a bad entry is an error rather than silently dropped:
// these are this node's own advertised identity, so a typo must fail loudly. At
// least one URL is required — a raft member with no advertise-peer-URL can't be
// dialed by the rest of the cluster.
func parseAdvertiseURLs(addrs []string) ([]url.URL, error) {
	if len(addrs) == 0 {
		return nil, errors.New("at least one advertise URL is required (the address of the caller-owned peer server)")
	}
	out := make([]url.URL, 0, len(addrs))
	for _, raw := range addrs {
		s := strings.TrimSpace(raw)
		if !strings.Contains(s, "://") {
			s = "http://" + s
		}
		u, err := url.Parse(s)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			return nil, fmt.Errorf("advertise URL %q: must be host:port, http://host:port, or https://host:port", raw)
		}
		out = append(out, *u)
	}
	return out, nil
}

// urlsToEndpoints renders URLs as endpoint strings for clientv3.Config.
func urlsToEndpoints(urls []url.URL) []string {
	eps := make([]string, len(urls))
	for i, u := range urls {
		eps[i] = u.String()
	}
	return eps
}
