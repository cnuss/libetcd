package seed

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-jose/go-jose/v4"
	"github.com/go-jose/go-jose/v4/jwt"

	restful "github.com/emicklei/go-restful/v3"

	"github.com/cnuss/libetcd/cmd/disco/internal/issuer"
)

// fakeStore records the sub each op is called with so the test can assert the
// verified identity propagated from the JWT into the handlers.
type fakeStore struct {
	won  bool
	urls []string

	mu   sync.Mutex
	subs []string
}

func (f *fakeStore) note(sub string) {
	f.mu.Lock()
	f.subs = append(f.subs, sub)
	f.mu.Unlock()
}

func (f *fakeStore) Claim(_ context.Context, sub string) (bool, error) {
	f.note(sub)
	return f.won, nil
}
func (f *fakeStore) Register(_ context.Context, sub, _, _ string) error {
	f.note(sub)
	return nil
}
func (f *fakeStore) Roster(_ context.Context, sub string) ([]string, error) {
	f.note(sub)
	return f.urls, nil
}

// newMockIssuer stands up an OIDC issuer: a discovery document pointing at a
// JWKS endpoint that publishes one RSA verification key. Returns the signing key
// + its kid so the test can mint tokens the seed will accept.
func newMockIssuer(t *testing.T) (priv *rsa.PrivateKey, kid string, srv *httptest.Server) {
	t.Helper()
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("rsa key: %v", err)
	}
	kid = "test-key"
	jwks := jose.JSONWebKeySet{Keys: []jose.JSONWebKey{{
		Key: priv.Public(), KeyID: kid, Algorithm: "RS256", Use: "sig",
	}}}

	var issURL string // set after the server starts; handlers read it at request time
	mux := http.NewServeMux()
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"issuer":                                issURL,
			"jwks_uri":                              issURL + "/jwks",
			"id_token_signing_alg_values_supported": []string{"RS256"},
		})
	})
	mux.HandleFunc("/jwks", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(jwks)
	})
	srv = httptest.NewServer(mux)
	issURL = srv.URL
	return priv, kid, srv
}

// signToken mints an RS256 JWT for the issuer with the given subject.
func signToken(t *testing.T, priv *rsa.PrivateKey, kid, iss, sub string) string {
	t.Helper()
	sig, err := jose.NewSigner(
		jose.SigningKey{Algorithm: jose.RS256, Key: jose.JSONWebKey{Key: priv, KeyID: kid}},
		(&jose.SignerOptions{}).WithType("JWT"),
	)
	if err != nil {
		t.Fatalf("signer: %v", err)
	}
	raw, err := jwt.Signed(sig).Claims(jwt.Claims{
		Issuer:   iss,
		Subject:  sub,
		Audience: jwt.Audience{"disco"},
		Expiry:   jwt.NewNumericDate(time.Now().Add(time.Hour)),
		IssuedAt: jwt.NewNumericDate(time.Now()),
	}).Serialize()
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	return raw
}

// TestSeedVerifiedRequests drives the full chain — verify filter against a real
// (mock) OIDC issuer, then the handler — for each authenticated route, and
// asserts the verified sub reached the store.
func TestSeedVerifiedRequests(t *testing.T) {
	priv, kid, iss := newMockIssuer(t)
	defer iss.Close()

	fs := &fakeStore{won: true, urls: []string{"http://n1:2380", "http://n2:2380"}}
	container := restful.NewContainer()
	container.Add(New(fs).WithIssuer(iss.URL).WebService())
	ts := httptest.NewServer(container)
	defer ts.Close()

	const sub = "repo:cnuss/libetcd:ref:refs/heads/main"
	token := signToken(t, priv, kid, iss.URL, sub)

	req := func(method, path, body string) *http.Request {
		r, err := http.NewRequest(method, ts.URL+path, strings.NewReader(body))
		if err != nil {
			t.Fatalf("new request: %v", err)
		}
		r.Header.Set("Authorization", "Bearer "+token)
		r.Header.Set("Content-Type", restful.MIME_JSON)
		r.Header.Set("Accept", restful.MIME_JSON)
		return r
	}

	// claim -> 200 {"won":true}
	t.Run("claim", func(t *testing.T) {
		resp, body := do(t, req(http.MethodPost, "/claim", ""))
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status %d: %s", resp.StatusCode, body)
		}
		var got struct {
			Won bool `json:"won"`
		}
		mustJSON(t, body, &got)
		if !got.Won {
			t.Fatalf("won=false, want true")
		}
	})

	// register -> 204
	t.Run("register", func(t *testing.T) {
		resp, body := do(t, req(http.MethodPost, "/register", `{"id":"n1","url":"http://n1:2380"}`))
		if resp.StatusCode != http.StatusNoContent {
			t.Fatalf("status %d: %s", resp.StatusCode, body)
		}
	})

	// roster -> 200 {"urls":[...]}
	t.Run("roster", func(t *testing.T) {
		resp, body := do(t, req(http.MethodGet, "/roster", ""))
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status %d: %s", resp.StatusCode, body)
		}
		var got struct {
			URLs []string `json:"urls"`
		}
		mustJSON(t, body, &got)
		if !slices.Equal(got.URLs, fs.urls) {
			t.Fatalf("urls=%v, want %v", got.URLs, fs.urls)
		}
	})

	// userinfo -> 200 with the full verified JWT payload (sub, iss, aud, ...) —
	// the hook a joining node's /join handler uses to verify a JWT and read its
	// claims without crypto. It touches no store op, so fs.subs is unaffected.
	t.Run("userinfo", func(t *testing.T) {
		resp, body := do(t, req(http.MethodGet, "/userinfo", ""))
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status %d: %s", resp.StatusCode, body)
		}
		var got map[string]any
		mustJSON(t, body, &got)
		if got["sub"] != sub {
			t.Fatalf("userinfo sub=%v, want %q", got["sub"], sub)
		}
		if got["iss"] != iss.URL {
			t.Fatalf("userinfo iss=%v, want %q", got["iss"], iss.URL)
		}
		// aud is part of the payload too — proves the full claim set, not just sub.
		if got["aud"] == nil {
			t.Fatalf("userinfo missing aud; payload=%v", got)
		}
	})

	// Every store op ran under the verified subject.
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if len(fs.subs) != 3 {
		t.Fatalf("store calls=%d, want 3", len(fs.subs))
	}
	for _, got := range fs.subs {
		if got != sub {
			t.Fatalf("store saw sub=%q, want %q", got, sub)
		}
	}
}

// TestVerifyRejectsUntrustedAndForged: the issuer is read from the unverified
// token, so the trust check and the signature check both matter. A token
// claiming an untrusted issuer is rejected (before any key fetch); a token
// claiming a trusted issuer but signed by the wrong key fails verification.
func TestVerifyRejectsUntrustedAndForged(t *testing.T) {
	priv, kid, iss := newMockIssuer(t)
	defer iss.Close()

	s := New(&fakeStore{won: true}).WithIssuer(iss.URL) // only iss.URL trusted
	container := restful.NewContainer()
	container.Add(s.WebService())
	ts := httptest.NewServer(container)
	defer ts.Close()

	status := func(token string) int {
		r, _ := http.NewRequest(http.MethodGet, ts.URL+"/roster", nil)
		r.Header.Set("Authorization", "Bearer "+token)
		r.Header.Set("Accept", restful.MIME_JSON)
		resp, _ := do(t, r)
		return resp.StatusCode
	}

	// Untrusted issuer: rejected by the trust check, no verifier ever built.
	if got := status(signToken(t, priv, kid, "https://evil.example", "sub")); got != http.StatusUnauthorized {
		t.Fatalf("untrusted issuer: status %d, want 401", got)
	}
	if _, built := s.verifiers.Load("https://evil.example"); built {
		t.Fatal("built a verifier for an untrusted issuer")
	}

	// Trusted issuer claim, but signed by a different key: signature check fails.
	other, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("rsa key: %v", err)
	}
	if got := status(signToken(t, other, kid, iss.URL, "sub")); got != http.StatusUnauthorized {
		t.Fatalf("forged signature: status %d, want 401", got)
	}
}

// TestSeedRejectsUnverified covers the fail-closed paths without a valid token.
func TestSeedRejectsUnverified(t *testing.T) {
	_, _, iss := newMockIssuer(t)
	defer iss.Close()

	container := restful.NewContainer()
	container.Add(New(&fakeStore{}).WithIssuer(iss.URL).WebService())
	ts := httptest.NewServer(container)
	defer ts.Close()

	cases := []struct{ name, auth string }{
		{"no bearer", ""},
		{"malformed", "Bearer not.a.jwt"},
		{"bad signature", "Bearer eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJ4In0.bad"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r, _ := http.NewRequest(http.MethodGet, ts.URL+"/roster", nil)
			if c.auth != "" {
				r.Header.Set("Authorization", c.auth)
			}
			r.Header.Set("Accept", restful.MIME_JSON)
			resp, body := do(t, r)
			if resp.StatusCode != http.StatusUnauthorized {
				t.Fatalf("status %d (%s), want 401", resp.StatusCode, body)
			}
		})
	}
}

// TestDiscoveryDescriptor checks the sniff endpoint: unauthenticated, advertises
// the endpoints, and only lists /token when the self-issuer is enabled.
func TestDiscoveryDescriptor(t *testing.T) {
	get := func(srv *Seed) descriptor {
		t.Helper()
		c := restful.NewContainer()
		c.Add(srv.WebService())
		ts := httptest.NewServer(c)
		defer ts.Close()
		r, _ := http.NewRequest(http.MethodGet, ts.URL+DiscoveryDescriptorPath, nil)
		r.Header.Set("Accept", restful.MIME_JSON)
		resp, body := do(t, r)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status %d: %s", resp.StatusCode, body)
		}
		var d descriptor
		mustJSON(t, body, &d)
		return d
	}

	// Without the self-issuer: endpoints present, no token.
	d := get(New(&fakeStore{}))
	if d.Discovery != discoveryVersion {
		t.Fatalf("discovery=%q, want %q", d.Discovery, discoveryVersion)
	}
	if d.Claim != "/claim" || d.Register != "/register" || d.Roster != "/roster" {
		t.Fatalf("endpoints wrong: %+v", d)
	}
	if d.Userinfo != "/userinfo" {
		t.Fatalf("userinfo=%q, want /userinfo", d.Userinfo)
	}
	if d.Token != "" {
		t.Fatalf("token advertised without self-issuer: %q", d.Token)
	}

	// With the self-issuer: token advertised.
	t.Setenv("DISCO_ISSUER_URL", "https://disco.example")
	if got := get(New(&fakeStore{}).WithSelfIssuer()).Token; got != "/token" {
		t.Fatalf("token=%q, want /token", got)
	}
}

// jwtPayload base64-decodes a JWT's claims segment. The token is freshly minted
// by the seed under test, so the test trusts it without re-verifying the
// signature — it only inspects the claims.
func jwtPayload(t *testing.T, token string) map[string]any {
	t.Helper()
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		t.Fatalf("token has %d segments, want 3", len(parts))
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	return m
}

// TestTokenCloudFrontClaims: /token folds the CloudFront viewer headers into the
// minted token as top-level claims (device flags as bools), and never folds in
// authorization/host or other non-cloudfront headers.
func TestTokenCloudFrontClaims(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	url := "http://" + ln.Addr().String()
	t.Setenv("DISCO_ISSUER_URL", url)

	container := restful.NewContainer()
	container.Add(New(&fakeStore{}).WithSelfIssuer().WebService())
	srv := &http.Server{Handler: container}
	go func() { _ = srv.Serve(ln) }()
	defer srv.Close()

	req, err := http.NewRequest(http.MethodGet, url+"/token", nil)
	if err != nil {
		t.Fatalf("token request: %v", err)
	}
	req.Header.Set("CloudFront-Viewer-Country", "US")
	req.Header.Set("CloudFront-Viewer-City", "Peachtree Corners")
	req.Header.Set("CloudFront-Viewer-Latitude", "33.97330")
	req.Header.Set("CloudFront-Viewer-Asn", "8075")
	req.Header.Set("CloudFront-Is-Mobile-Viewer", "false")
	req.Header.Set("CloudFront-Is-Desktop-Viewer", "true")
	req.Header.Set("Authorization", "Bearer should-not-be-folded-in")
	req.Header.Set("Host", "should-not-appear")

	resp, body := do(t, req)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("token status %d: %s", resp.StatusCode, body)
	}
	var tok struct {
		Token string `json:"token"`
		Sub   string `json:"sub"`
	}
	mustJSON(t, body, &tok)

	p := jwtPayload(t, tok.Token)
	// Viewer string claims carried through under their mapped names.
	for name, want := range map[string]string{
		"country": "US", "city": "Peachtree Corners", "latitude": "33.97330", "asn": "8075",
	} {
		if p[name] != want {
			t.Fatalf("claim %q = %v, want %q", name, p[name], want)
		}
	}
	// Device flags mapped to bools.
	if p["is_desktop"] != true {
		t.Fatalf("is_desktop = %v, want true", p["is_desktop"])
	}
	if v, ok := p["is_mobile"]; !ok || v != false {
		t.Fatalf("is_mobile = %v (present=%v), want false", v, ok)
	}
	// Sensitive / non-cloudfront headers never become claims.
	for _, k := range []string{"authorization", "host", "Authorization", "Host"} {
		if _, ok := p[k]; ok {
			t.Fatalf("claim %q leaked into token: %v", k, p[k])
		}
	}
	// Registered claims intact.
	if p["sub"] != tok.Sub {
		t.Fatalf("sub claim = %v, want %q", p["sub"], tok.Sub)
	}
	if p["iss"] != url {
		t.Fatalf("iss claim = %v, want %q", p["iss"], url)
	}
}

// TestVerifierDedup: an issuer named twice (as it is in production — the
// self-issuer URL also passed via WithIssuer(DefaultIssuerURL)) builds a single
// verifier, not one OIDC-discovery round-trip per duplicate. Drives a real
// verify and inspects the cached set.
func TestVerifierDedup(t *testing.T) {
	priv, kid, iss := newMockIssuer(t)
	defer iss.Close()

	s := New(&fakeStore{won: true}).WithIssuer(iss.URL).WithIssuer(iss.URL)
	container := restful.NewContainer()
	container.Add(s.WebService())
	ts := httptest.NewServer(container)
	defer ts.Close()

	r, _ := http.NewRequest(http.MethodGet, ts.URL+"/roster", nil)
	r.Header.Set("Authorization", "Bearer "+signToken(t, priv, kid, iss.URL, "sub-x"))
	r.Header.Set("Accept", restful.MIME_JSON)
	resp, body := do(t, r)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("roster status %d: %s", resp.StatusCode, body)
	}

	n := 0
	s.verifiers.Range(func(_, _ any) bool { n++; return true })
	if n != 1 {
		t.Fatalf("verifiers=%d, want 1 (deduped)", n)
	}
}

func do(t *testing.T, r *http.Request) (*http.Response, string) {
	t.Helper()
	resp, err := http.DefaultClient.Do(r)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp, string(b)
}

func mustJSON(t *testing.T, body string, v any) {
	t.Helper()
	if err := json.Unmarshal([]byte(body), v); err != nil {
		t.Fatalf("decode %q: %v", body, err)
	}
}

// TestSelfIssuer exercises disco-as-issuer end to end: it publishes a discovery
// document, mints a token at /token, and accepts that token on an authenticated
// route — verified against its own JWKS (fetched over HTTP by the verifier),
// with the minted sub reaching the store.
func TestSelfIssuer(t *testing.T) {
	// The issuer URL must equal the live server URL (the verifier fetches its
	// discovery there), so bind a listener first and point the self-issuer at
	// its address via env. No DISCO_SIGNING_KEY -> ephemeral key, fine here.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	url := "http://" + ln.Addr().String()
	t.Setenv("DISCO_ISSUER_URL", url)

	fs := &fakeStore{won: true, urls: []string{"http://n1:2380"}}
	container := restful.NewContainer()
	container.Add(New(fs).WithSelfIssuer().WebService())
	srv := &http.Server{Handler: container}
	go func() { _ = srv.Serve(ln) }()
	defer srv.Close()

	jsonReq := func(method, path, body string) *http.Request {
		r, err := http.NewRequest(method, url+path, strings.NewReader(body))
		if err != nil {
			t.Fatalf("new request: %v", err)
		}
		r.Header.Set("Content-Type", restful.MIME_JSON)
		r.Header.Set("Accept", restful.MIME_JSON)
		return r
	}

	// Discovery advertises this issuer.
	{
		resp, body := do(t, jsonReq(http.MethodGet, issuer.DiscoveryPath, ""))
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("discovery status %d: %s", resp.StatusCode, body)
		}
		var d struct {
			Issuer   string `json:"issuer"`
			JWKSURI  string `json:"jwks_uri"`
			UserInfo string `json:"userinfo_endpoint"`
		}
		mustJSON(t, body, &d)
		if d.Issuer != url {
			t.Fatalf("discovery issuer=%q, want %q", d.Issuer, url)
		}
		if d.JWKSURI != url+issuer.JWKSPath {
			t.Fatalf("discovery jwks_uri=%q, want %q", d.JWKSURI, url+issuer.JWKSPath)
		}
		if d.UserInfo != url+"/userinfo" {
			t.Fatalf("discovery userinfo_endpoint=%q, want %q", d.UserInfo, url+"/userinfo")
		}
	}

	// The root route aliases the JWKS: a bare GET of the issuer URL returns the
	// same verification key set as the well-known jwks path.
	{
		resp, body := do(t, jsonReq(http.MethodGet, "/", ""))
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("root status %d: %s", resp.StatusCode, body)
		}
		var jwks struct {
			Keys []json.RawMessage `json:"keys"`
		}
		mustJSON(t, body, &jwks)
		if len(jwks.Keys) != 1 {
			t.Fatalf("root jwks keys=%d, want 1", len(jwks.Keys))
		}
	}

	// Mint a disco-native identity. /token takes no body and accepts GET or POST
	// with any content type — use a bare GET (no headers) to prove that.
	greq, err := http.NewRequest(http.MethodGet, url+"/token", nil)
	if err != nil {
		t.Fatalf("token request: %v", err)
	}
	resp, body := do(t, greq)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("token status %d: %s", resp.StatusCode, body)
	}
	var tok struct {
		Token     string `json:"token"`
		Sub       string `json:"sub"`
		ExpiresIn int    `json:"expires_in"`
	}
	mustJSON(t, body, &tok)
	if tok.Token == "" || tok.Sub == "" || tok.ExpiresIn <= 0 {
		t.Fatalf("bad token response: %s", body)
	}

	// The minted token is accepted on an authenticated route (verified against
	// the seed's own JWKS), and its sub reaches the store.
	r := jsonReq(http.MethodGet, "/roster", "")
	r.Header.Set("Authorization", "Bearer "+tok.Token)
	resp, body = do(t, r)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("roster status %d: %s", resp.StatusCode, body)
	}
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if len(fs.subs) != 1 || fs.subs[0] != tok.Sub {
		t.Fatalf("store saw subs=%v, want [%s]", fs.subs, tok.Sub)
	}
}
