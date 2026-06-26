package e2e

import (
	"context"
	"net"
	"os"
	"testing"
	"time"
)

// TestMain hardens DNS for the whole e2e process against the recurring
// TestMultiNodeTunnel flake (issue #116), where a freshly-created
// *.trycloudflare.com tunnel hostname stays "no such host" past the join budget.
//
// Two layers:
//
//   - PreferGo swaps macOS's cgo resolver (mDNSResponder), which negative-caches
//     an NXDOMAIN, for the pure-Go resolver, which re-queries each lookup — so a
//     joiner sees the name the moment DNS propagates.
//   - A custom Dial sends those queries to a public resolver instead of the
//     system one. On the macos-26 CI runner the system path is a VM NAT-gateway
//     resolver (192.168.64.1) that is slow to learn just-created tunnel names and
//     NXDOMAINs them past the budget even with PreferGo; public resolvers see the
//     propagated record promptly. Falls over 1.1.1.1 -> 8.8.8.8 so a single
//     unreachable resolver doesn't break the suite.
func TestMain(m *testing.M) {
	net.DefaultResolver.PreferGo = true
	net.DefaultResolver.Dial = func(ctx context.Context, network, _ string) (net.Conn, error) {
		d := net.Dialer{Timeout: 5 * time.Second}
		var err error
		for _, ns := range []string{"1.1.1.1:53", "8.8.8.8:53"} {
			var c net.Conn
			if c, err = d.DialContext(ctx, network, ns); err == nil {
				return c, nil
			}
		}
		return nil, err
	}
	os.Exit(m.Run())
}
