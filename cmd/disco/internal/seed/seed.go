// Package seed serves the disco discovery API: claim, register, and roster,
// each a thin translation of a verified request onto a store.Store operation.
// The seed holds no cluster state of its own.
package seed

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
	restful "github.com/emicklei/go-restful/v3"
	"github.com/go-jose/go-jose/v4"
	"github.com/go-jose/go-jose/v4/jwt"

	"github.com/cnuss/libetcd/cmd/disco/internal/issuer"
	"github.com/cnuss/libetcd/cmd/disco/internal/store"
)

// DefaultIssuerURL is the canonical disco token authority: the self-issuer's URL
// when DISCO_ISSUER_URL is unset, and the issuer callers pass to WithIssuer to
// trust disco-minted tokens. A var so a caller can repoint it.
var DefaultIssuerURL = "https://disco.nuss.io"

// tokenTTL is the self-issued token lifetime.
const tokenTTL = time.Hour

// discoveryTimeout bounds an issuer's OIDC discovery fetch in GetVerifier.
const discoveryTimeout = 15 * time.Second

// Seed is the discovery rendezvous service — a stateless shim over a Store.
type Seed struct {
	store   store.Store
	issuers []string       // OIDC issuers to trust for cluster JWTs
	issuer  *issuer.Issuer // optional self-issuer: mints + publishes disco-native tokens

	// verifiers caches one *oidc.IDTokenVerifier per issuer URL, built lazily on
	// first use. Keyed by URL, so issuers named twice (the self-issuer URL also
	// passed to WithIssuer) share a single entry — dedup for free.
	verifiers sync.Map // issuer URL -> *oidc.IDTokenVerifier
}

// New returns a Seed backed by the given store.
func New(s store.Store) *Seed { return &Seed{store: s, issuers: make([]string, 0)} }

func (s *Seed) WithIssuer(iss string) *Seed {
	s.issuers = append(s.issuers, iss)
	return s
}

// WithSelfIssuer makes the seed its own OIDC issuer: it serves /token,
// /.well-known/openid-configuration and the JWKS, and trusts itself when
// verifying (so disco-minted tokens are accepted alongside external ones).
//
// Configured from the environment: DISCO_SIGNING_KEY (PEM; an ephemeral key is
// generated with a warning if unset) and DISCO_ISSUER_URL (defaults to
// https://disco.nuss.io). A bad signing key is fatal — misconfiguration should
// stop startup, not silently disable auth.
func (s *Seed) WithSelfIssuer() *Seed {
	key, ephemeral, err := issuer.KeyFromEnv()
	if err != nil {
		log.Fatalf("disco: signing key: %v", err)
	}
	if ephemeral {
		log.Print("disco: WARNING self-issuer signing key is ephemeral — set DISCO_SIGNING_KEY (PEM) for tokens that survive restarts and scale-out")
	}
	issURL := os.Getenv("DISCO_ISSUER_URL")
	if issURL == "" {
		issURL = DefaultIssuerURL
	}
	iss, err := issuer.New(issURL, key, tokenTTL)
	if err != nil {
		log.Fatalf("disco: issuer: %v", err)
	}
	s.issuer = iss
	// Trust our own issued tokens: ensureVerifiers builds a verifier for this
	// URL like any other (OIDC discovery + remote JWKS over loopback). Deduped
	// against a matching WithIssuer (e.g. WithIssuer(DefaultIssuerURL)).
	s.issuers = append(s.issuers, iss.URL())
	return s
}

// Close releases the seed's resources. Nothing to release for a stateless shim.
func (s *Seed) Close() error { return nil }

// WebService builds the disco API: claim/register/roster behind JWT
// verification, plus an unauthenticated health check.
func (s *Seed) WebService() *restful.WebService {
	ws := new(restful.WebService)
	ws.Path("").Consumes(restful.MIME_JSON).Produces(restful.MIME_JSON)

	ws.Route(ws.GET("/healthz").To(s.handleHealth))

	// Sniff target: an unauthenticated descriptor that identifies this URL as a
	// libetcd discovery seed and advertises its endpoints. A client points From
	// at the bare URL and probes this to decide discovery-vs-plain-peer.
	ws.Route(ws.GET(DiscoveryDescriptorPath).To(s.handleDescriptor))

	// Discovery ops require a verified cluster JWT (the verify filter). /claim
	// takes no body, so it overrides the WebService JSON consume to accept any
	// content type (a bodyless POST otherwise 415s); /register reads a JSON body.
	ws.Route(ws.POST("/claim").Consumes("*/*").Filter(s.verify).To(s.handleClaim))
	ws.Route(ws.POST("/register").Filter(s.verify).To(s.handleRegister))
	ws.Route(ws.GET("/roster").Filter(s.verify).To(s.handleRoster))

	// userinfo verifies a bearer and returns its claims (standard OIDC UserInfo
	// shape). A joining node's /join handler forwards an incoming JWT here to
	// verify it and read its sub, so libetcd itself stays crypto-free.
	ws.Route(ws.GET(userinfoPath).Filter(s.verify).To(s.handleUserinfo))

	// Self-issuer (optional): mint a disco-native identity and publish the
	// discovery + JWKS that verify it. Unauthenticated by design — /token hands
	// out a fresh, isolated cluster namespace. It takes no body, so accept either
	// method and any content type (a bare `curl https://.../token` should work),
	// overriding the WebService-level JSON consume.
	if s.issuer != nil {
		ws.Route(ws.GET("/token").Consumes("*/*").To(s.handleToken))
		ws.Route(ws.POST("/token").Consumes("*/*").To(s.handleToken))
		ws.Route(ws.GET(issuer.DiscoveryPath).To(s.handleDiscovery))
		ws.Route(ws.GET(issuer.JWKSPath).To(s.handleJWKS))
		// Root also serves the JWKS, so a bare GET of the issuer URL returns the
		// verification keys — a convenience alias for the well-known jwks path.
		ws.Route(ws.GET("/").To(s.handleJWKS))
	}
	return ws
}

// claimsAttr is the request attribute the verify filter sets and handlers read:
// the full verified JWT payload. The cluster identity is its sub claim — see
// the sub helper — so there's no separate sub attribute.
const claimsAttr = "claims"

// DiscoveryDescriptorPath is the unauthenticated sniff endpoint; a client probes
// <url>/.well-known/libetcd-discovery to decide whether a From URL is a seed.
const DiscoveryDescriptorPath = "/.well-known/libetcd-discovery"

// discoveryVersion is the descriptor's protocol version.
const discoveryVersion = "v1"

// userinfoPath is the OIDC-style UserInfo endpoint: an authenticated GET that
// returns the verified caller's claims (sub required). Served for every trusted
// issuer's tokens — not just the self-issuer's — and advertised both in the
// discovery descriptor and, when the self-issuer is on, as the OIDC
// userinfo_endpoint.
const userinfoPath = "/userinfo"

var (
	errNoBearer        = errors.New("missing bearer token")
	errNoIssuer        = errors.New("token has no issuer")
	errUntrustedIssuer = errors.New("untrusted issuer")
	errNoSubject       = errors.New("token has no subject")
	errClaimHeld       = errors.New("claim already held")
	errMissingURL      = errors.New("missing url")
	errStore           = errors.New("store error")
	errMint            = errors.New("token minting failed")
)

// verify is the seed-side JWT gate. The node carries its cluster JWT as a
// bearer; the seed verifies signature + iss + exp against the trusted issuers'
// OIDC/JWKS and extracts sub — the cluster identity that namespaces the roster.
// Fail-closed: any failure rejects the request, so the seed never serves an
// unauthenticated discovery op.
//
// TODO: audience is not enforced yet — a valid token from any caller of a
// trusted issuer (e.g. any GitHub Actions workflow) passes, namespaced by its
// own sub, so it can form or join only clusters under that sub, never touch
// another's. Tighten later with an expected audience and/or an allowed-sub
// policy.
func (s *Seed) verify(req *restful.Request, resp *restful.Response, chain *restful.FilterChain) {
	// Pull the token out of an "Authorization: Bearer <token>" header.
	const prefix = "Bearer "
	h := req.HeaderParameter("Authorization")
	var raw string
	if len(h) >= len(prefix) && strings.EqualFold(h[:len(prefix)], prefix) {
		raw = strings.TrimSpace(h[len(prefix):])
	}
	if raw == "" {
		_ = resp.WriteError(http.StatusUnauthorized, errNoBearer)
		return
	}

	// Decode the token *unverified* just to read its issuer, so we can pick the
	// matching verifier. These claims are attacker-controlled — anything
	// malformed or missing an issuer is rejected here, and GetVerifier rejects an
	// untrusted issuer before fetching any keys; the verifier below then checks
	// the signature, issuer, and expiry for real.
	parsed, err := jwt.ParseSigned(raw, []jose.SignatureAlgorithm{jose.RS256})
	if err != nil {
		_ = resp.WriteError(http.StatusUnauthorized, fmt.Errorf("jwt: %w", err))
		return
	}
	var unverified jwt.Claims
	if err := parsed.UnsafeClaimsWithoutVerification(&unverified); err != nil || unverified.Issuer == "" {
		_ = resp.WriteError(http.StatusUnauthorized, errNoIssuer)
		return
	}

	verifier, err := s.GetVerifier(unverified.Issuer)
	if err != nil {
		// Untrusted issuer is the caller's fault (401); a trusted issuer whose
		// discovery is unreachable is transient (503, retry).
		if errors.Is(err, errUntrustedIssuer) {
			_ = resp.WriteError(http.StatusUnauthorized, err)
		} else {
			_ = resp.WriteError(http.StatusServiceUnavailable, err)
		}
		return
	}

	ctx := req.Request.Context()
	tok, err := verifier.Verify(ctx, raw)
	if err != nil {
		_ = resp.WriteError(http.StatusUnauthorized, fmt.Errorf("jwt: %w", err))
		return
	}
	if tok.Subject == "" {
		_ = resp.WriteError(http.StatusUnauthorized, errNoSubject)
		return
	}

	// Stash the full verified claim set; sub is one of its claims (the sub
	// helper reads it), and /userinfo echoes the whole payload — a joining node
	// forwards its token here to learn its identity without re-parsing.
	var cl map[string]any
	if err := tok.Claims(&cl); err != nil {
		_ = resp.WriteError(http.StatusUnauthorized, fmt.Errorf("jwt claims: %w", err))
		return
	}

	req.SetAttribute(claimsAttr, cl)
	chain.ProcessFilter(req, resp)
}

// GetVerifier returns the OIDC verifier for a trusted issuer, building it once
// (OIDC discovery finds the jwks_uri; Provider.Verifier wires a remote KeySet
// that fetches, caches, and rotates keys) and caching it by URL. iss is matched
// against the configured trust list FIRST — callers pass the issuer of an
// as-yet-unverified token, so an unknown one must be rejected (errUntrustedIssuer)
// before any key fetch. Concurrent first-uses may each build; LoadOrStore keeps
// one. A discovery failure isn't cached, so a transient outage retries.
func (s *Seed) GetVerifier(iss string) (*oidc.IDTokenVerifier, error) {
	if !slices.Contains(s.issuers, iss) {
		return nil, fmt.Errorf("%w: %q", errUntrustedIssuer, iss)
	}
	if v, ok := s.verifiers.Load(iss); ok {
		return v.(*oidc.IDTokenVerifier), nil
	}
	// Bound the discovery fetch so a hung issuer can't wedge a request.
	ctx, cancel := context.WithTimeout(context.Background(), discoveryTimeout)
	defer cancel()
	provider, err := oidc.NewProvider(ctx, iss)
	if err != nil {
		return nil, fmt.Errorf("oidc discovery %q: %w", iss, err)
	}
	// SkipClientIDCheck: audience is not enforced yet (see verify).
	v, _ := s.verifiers.LoadOrStore(iss, provider.Verifier(&oidc.Config{SkipClientIDCheck: true}))
	return v.(*oidc.IDTokenVerifier), nil
}

// claims returns the full verified JWT payload the verify filter stashed.
func claims(req *restful.Request) map[string]any {
	c, _ := req.Attribute(claimsAttr).(map[string]any)
	return c
}

// sub returns the verified cluster identity — the sub claim of the stashed
// payload. The verify filter rejects a token without one, so a request that
// reached a handler always has it.
func sub(req *restful.Request) string {
	s, _ := claims(req)["sub"].(string)
	return s
}

// handleClaim runs the atomic bootstrap claim: the first caller for this
// cluster wins (200) and bootstraps; the rest get 409 and fall back to joining
// the roster. See store.Store.Claim.
func (s *Seed) handleClaim(req *restful.Request, resp *restful.Response) {
	won, err := s.store.Claim(req.Request.Context(), sub(req))
	if err != nil {
		writeStoreError(resp, err)
		return
	}
	if !won {
		_ = resp.WriteError(http.StatusConflict, errClaimHeld)
		return
	}
	_ = resp.WriteAsJson(claimResponse{Won: true})
}

// handleRegister advertises the caller as a live join target with a TTL.
// Idempotent — re-calling refreshes the TTL (keepalive-as-re-register).
func (s *Seed) handleRegister(req *restful.Request, resp *restful.Response) {
	var body registerRequest
	if err := req.ReadEntity(&body); err != nil || body.URL == "" {
		_ = resp.WriteError(http.StatusBadRequest, errMissingURL)
		return
	}
	if err := s.store.Register(req.Request.Context(), sub(req), body.ID, body.URL); err != nil {
		writeStoreError(resp, err)
		return
	}
	resp.WriteHeader(http.StatusNoContent)
}

// handleRoster returns the current live join-target URLs for this cluster.
func (s *Seed) handleRoster(req *restful.Request, resp *restful.Response) {
	urls, err := s.store.Roster(req.Request.Context(), sub(req))
	if err != nil {
		writeStoreError(resp, err)
		return
	}
	_ = resp.WriteAsJson(rosterResponse{URLs: urls})
}

// handleHealth is an unauthenticated liveness probe (used by rowdy / the
// platform health check).
func (s *Seed) handleHealth(_ *restful.Request, resp *restful.Response) {
	_ = resp.WriteAsJson(message{Message: "ok"})
}

// handleDescriptor serves the discovery descriptor a client sniffs to recognize
// this URL as a libetcd discovery seed. Always served (discovery works with
// external JWTs too); token is advertised only when the self-issuer is enabled,
// since /token 404s otherwise.
func (s *Seed) handleDescriptor(_ *restful.Request, resp *restful.Response) {
	d := descriptor{
		Discovery: discoveryVersion,
		Userinfo:  userinfoPath,
		Claim:     "/claim",
		Register:  "/register",
		Roster:    "/roster",
	}
	if s.issuer != nil {
		d.Token = "/token"
	}
	_ = resp.WriteAsJson(d)
}

// handleUserinfo is the OIDC UserInfo endpoint: the verify filter has already
// authenticated the bearer, so this returns its verified claim set as a JSON
// object with sub required (the standard UserInfo response). A joining node's
// /join handler forwards an incoming join JWT here to verify it and read back
// sub — the cluster identity it matches against — without doing crypto itself.
func (s *Seed) handleUserinfo(req *restful.Request, resp *restful.Response) {
	_ = resp.WriteAsJson(claims(req))
}

// handleToken mints a fresh disco-native identity and returns a signed token for
// it. Unauthenticated: each call yields a new random sub (an isolated cluster
// namespace), so handing it out freely only lets a caller form/join its own
// cluster. Share the returned token across the nodes of one cluster.
func (s *Seed) handleToken(req *restful.Request, resp *restful.Response) {
	sub, err := issuer.NewRandomSub()
	if err != nil {
		_ = resp.WriteError(http.StatusInternalServerError, errMint)
		return
	}

	// Fold the CloudFront viewer headers into the token as claims (empty for a
	// direct, non-CloudFront caller — Mint then folds nothing). http.Header.Get
	// canonicalizes the lookup key, so the lowercase names match any wire casing.
	claims := make(map[string]any, len(cloudfrontStringClaims)+len(cloudfrontBoolClaims))
	for hdr, name := range cloudfrontStringClaims {
		if v := req.Request.Header.Get(hdr); v != "" {
			claims[name] = v
		}
	}
	for hdr, name := range cloudfrontBoolClaims {
		if v := req.Request.Header.Get(hdr); v != "" {
			claims[name] = strings.EqualFold(v, "true")
		}
	}

	token, expiresIn, err := s.issuer.Mint(sub, claims)
	if err != nil {
		_ = resp.WriteError(http.StatusInternalServerError, errMint)
		return
	}
	_ = resp.WriteAsJson(tokenResponse{Token: token, Sub: sub, ExpiresIn: expiresIn})
}

// cloudfrontStringClaims / cloudfrontBoolClaims map the CloudFront viewer
// request headers disco folds into a minted /token to their top-level claim
// names. CloudFront injects these at the edge — the client can't forge them — so
// only cloudfront-* is trusted; authorization, host, cookie, and other
// client-settable headers are never included. Device-class flags become bools;
// everything else carries through as the header's string value.
var (
	cloudfrontStringClaims = map[string]string{
		"cloudfront-viewer-country":             "country",
		"cloudfront-viewer-country-name":        "country_name",
		"cloudfront-viewer-country-region":      "region",
		"cloudfront-viewer-country-region-name": "region_name",
		"cloudfront-viewer-city":                "city",
		"cloudfront-viewer-postal-code":         "postal_code",
		"cloudfront-viewer-metro-code":          "metro_code",
		"cloudfront-viewer-latitude":            "latitude",
		"cloudfront-viewer-longitude":           "longitude",
		"cloudfront-viewer-time-zone":           "time_zone",
		"cloudfront-viewer-asn":                 "asn",
		"cloudfront-viewer-http-version":        "http_version",
	}
	cloudfrontBoolClaims = map[string]string{
		"cloudfront-is-mobile-viewer":  "is_mobile",
		"cloudfront-is-desktop-viewer": "is_desktop",
		"cloudfront-is-tablet-viewer":  "is_tablet",
		"cloudfront-is-smarttv-viewer": "is_smarttv",
	}
)

// handleDiscovery serves the issuer's OpenID Provider configuration, augmented
// with the seed's userinfo_endpoint so disco advertises a conformant OIDC
// UserInfo endpoint (the path is the seed's, built on the issuer's URL).
func (s *Seed) handleDiscovery(_ *restful.Request, resp *restful.Response) {
	d := s.issuer.Discovery()
	d["userinfo_endpoint"] = s.issuer.URL() + userinfoPath
	_ = resp.WriteAsJson(d)
}

// handleJWKS serves the issuer's public verification keys.
func (s *Seed) handleJWKS(_ *restful.Request, resp *restful.Response) {
	_ = resp.WriteAsJson(s.issuer.JWKS())
}

// writeStoreError maps a Store error to a status: scaffold stubs surface as 501,
// anything else as a 502 (the seed's backing failed).
func writeStoreError(resp *restful.Response, err error) {
	if errors.Is(err, store.ErrNotImplemented) {
		_ = resp.WriteError(http.StatusNotImplemented, err)
		return
	}
	_ = resp.WriteError(http.StatusBadGateway, errStore)
}

// Wire types for the disco API. These will move to a shared package once the
// step-1 client resolver (in the library) needs to speak the same protocol.
type (
	message struct {
		Message string `json:"message"`
	}
	registerRequest struct {
		ID  string `json:"id"`  // stable per-node id; re-register overwrites
		URL string `json:"url"` // advertised peer URL to hand future joiners
	}
	rosterResponse struct {
		URLs []string `json:"urls"`
	}
	claimResponse struct {
		Won bool `json:"won"`
	}
	tokenResponse struct {
		Token     string `json:"token"`
		Sub       string `json:"sub"`
		ExpiresIn int    `json:"expires_in"`
	}
	// descriptor is the sniff document: a discovery marker + endpoint paths.
	descriptor struct {
		Discovery string `json:"discovery"` // protocol version, e.g. "v1"
		Token     string `json:"token,omitempty"`
		Userinfo  string `json:"userinfo"`
		Claim     string `json:"claim"`
		Register  string `json:"register"`
		Roster    string `json:"roster"`
	}
)
