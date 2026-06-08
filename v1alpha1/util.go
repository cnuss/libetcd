package v1alpha1

import (
	"crypto/tls"
	"net"
	"net/url"
	"reflect"
)

// listenerURL builds a URL from a listener's bound address, inferring the scheme
// (https if the listener is TLS-wrapped, http otherwise).
func listenerURL(l net.Listener) url.URL {
	return url.URL{Scheme: listenerScheme(l), Host: l.Addr().String()}
}

// listenerScheme returns "https" if l carries TLS, "http" otherwise. A TLS
// listener (from tls.NewListener / tls.Listen) is an unexported *tls.listener
// that holds a non-nil *tls.Config field; we dig that out by reflection since
// the type isn't exported for a direct assertion.
func listenerScheme(l net.Listener) string {
	v := reflect.ValueOf(l)
	for v.Kind() == reflect.Pointer {
		v = v.Elem()
	}
	if v.Kind() == reflect.Struct {
		tlsConfigType := reflect.TypeFor[*tls.Config]()
		for i := 0; i < v.NumField(); i++ {
			f := v.Field(i)
			if f.Type() == tlsConfigType && !f.IsNil() {
				return "https"
			}
		}
	}
	return "http"
}

// urlsToEndpoints renders URLs as endpoint strings for clientv3.Config.
func urlsToEndpoints(urls []url.URL) []string {
	eps := make([]string, len(urls))
	for i, u := range urls {
		eps[i] = u.String()
	}
	return eps
}
