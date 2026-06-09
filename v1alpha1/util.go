package v1alpha1

import (
	"crypto/rand"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"net"
	"net/url"
	"reflect"
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

// urlsToEndpoints renders URLs as endpoint strings for clientv3.Config.
func urlsToEndpoints(urls []url.URL) []string {
	eps := make([]string, len(urls))
	for i, u := range urls {
		eps[i] = u.String()
	}
	return eps
}
