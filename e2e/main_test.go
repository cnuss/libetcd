package e2e

import (
	"net"
	"os"
	"testing"
)

// TestMain prefers Go's built-in DNS resolver for the whole e2e process. macOS's
// cgo resolver (mDNSResponder) negative-caches an NXDOMAIN, so a peer/tunnel
// hostname that hasn't propagated yet stays "no such host" past the join budget
// — the recurring TestMultiNodeTunnel flake (issue #116). The pure-Go resolver
// re-queries each lookup, so a joiner sees the name the moment DNS propagates.
func TestMain(m *testing.M) {
	net.DefaultResolver.PreferGo = true
	os.Exit(m.Run())
}
