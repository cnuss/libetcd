package v1alpha1

import "testing"

// TestSanitizePeers covers the normalization From applies to a caller's peer
// URLs: trim, drop-empty, default-scheme, dedup, the preserved first-seen
// order, and silently dropping anything that doesn't parse.
func TestSanitizePeers(t *testing.T) {
	cases := []struct {
		name string
		in   []string
		want []string
	}{
		{
			name: "bare host:port gets http scheme",
			in:   []string{"10.0.0.1:2380"},
			want: []string{"http://10.0.0.1:2380"},
		},
		{
			name: "trims surrounding whitespace",
			in:   []string{"  http://10.0.0.1:2380  ", "\thttp://10.0.0.2:2380\n"},
			want: []string{"http://10.0.0.1:2380", "http://10.0.0.2:2380"},
		},
		{
			name: "drops empty and whitespace-only entries",
			in:   []string{"", "   ", "http://10.0.0.1:2380"},
			want: []string{"http://10.0.0.1:2380"},
		},
		{
			name: "dedups, preserving first-seen order",
			in:   []string{"http://b:2380", "http://a:2380", "http://b:2380", "b:2380"},
			want: []string{"http://b:2380", "http://a:2380"},
		},
		{
			name: "trailing slash normalized away (dedups with bare)",
			in:   []string{"http://a:2380/", "http://a:2380"},
			want: []string{"http://a:2380"},
		},
		{
			name: "keeps https as-is",
			in:   []string{"https://a:2380"},
			want: []string{"https://a:2380"},
		},
		{
			name: "drops bad entries, keeps the good ones",
			in:   []string{"ftp://a:2380", "http://", "://2380", "http://good:2380"},
			want: []string{"http://good:2380"},
		},
		{
			name: "all-bad-or-empty yields empty",
			in:   []string{"", "  ", "ftp://x:2380", "http://"},
			want: nil,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := sanitizePeers(tc.in)
			if len(got) != len(tc.want) {
				t.Fatalf("sanitizePeers(%q) = %q, want %q", tc.in, got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("sanitizePeers(%q) = %q, want %q", tc.in, got, tc.want)
				}
			}
		})
	}
}
