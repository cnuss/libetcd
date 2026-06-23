// Package seed serves the disco discovery API: claim, register, and roster,
// each a thin translation of a verified request onto a store.Store operation
// (issue #108). The seed holds no cluster state of its own.
package seed

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"

	"github.com/coreos/go-oidc/v3/oidc"
	restful "github.com/emicklei/go-restful/v3"

	"github.com/cnuss/libetcd/cmd/disco/internal/store"
)

// Seed is the discovery rendezvous service — a stateless shim over a Store.
type Seed struct {
	store   store.Store
	issuers []string // OIDC issuers to trust for cluster JWTs

	mu        sync.Mutex
	verifiers []*oidc.IDTokenVerifier // one per issuer, built lazily from OIDC discovery
}

// New returns a Seed backed by the given store.
func New(s store.Store) *Seed { return &Seed{store: s, issuers: make([]string, 0)} }

func (s *Seed) WithIssuer(iss string) *Seed {
	s.issuers = append(s.issuers, iss)
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

	// Discovery ops require a verified cluster JWT (the verify filter).
	ws.Route(ws.POST("/claim").Filter(s.verify).To(s.handleClaim))
	ws.Route(ws.POST("/register").Filter(s.verify).To(s.handleRegister))
	ws.Route(ws.GET("/roster").Filter(s.verify).To(s.handleRoster))
	return ws
}

// subAttr is the request attribute the verify filter sets and handlers read:
// the cluster identity extracted from the JWT sub claim.
const subAttr = "sub"

var (
	errNoBearer   = errors.New("missing bearer token")
	errNoIssuers  = errors.New("no trusted issuers configured")
	errNoSubject  = errors.New("token has no subject")
	errClaimHeld  = errors.New("claim already held")
	errMissingURL = errors.New("missing url")
	errStore      = errors.New("store error")
)

// verify is the JWT gate (issue #108: seed-side verification). The node carries
// its cluster JWT as a bearer; the seed verifies signature + iss + exp against
// the trusted issuers' OIDC/JWKS and extracts sub — the cluster identity that
// namespaces the roster. Fail-closed: any failure rejects the request, so the
// seed never serves an unauthenticated discovery op.
//
// Audience is not enforced yet: a valid token from any caller of a trusted
// issuer (e.g. any GitHub Actions workflow) passes, namespaced by its own sub —
// it can form or join only clusters under that sub, never touch another's.
// Tighten later with an expected audience and/or an allowed-sub policy.
// TODO(#108).
func (s *Seed) verify(req *restful.Request, resp *restful.Response, chain *restful.FilterChain) {
	raw := bearerToken(req)
	if raw == "" {
		_ = resp.WriteError(http.StatusUnauthorized, errNoBearer)
		return
	}
	ctx := req.Request.Context()
	verifiers, err := s.ensureVerifiers(ctx)
	if err != nil {
		// Can't reach the issuer(s) to fetch keys — transient; let the caller retry.
		_ = resp.WriteError(http.StatusServiceUnavailable, err)
		return
	}

	// A token is good if any trusted issuer validates it. The wrong verifier
	// fails fast on the iss mismatch before checking the signature.
	var tok *oidc.IDToken
	var verr error
	for _, v := range verifiers {
		if tok, verr = v.Verify(ctx, raw); verr == nil {
			break
		}
	}
	if tok == nil {
		_ = resp.WriteError(http.StatusUnauthorized, fmt.Errorf("jwt: %w", verr))
		return
	}
	if tok.Subject == "" {
		_ = resp.WriteError(http.StatusUnauthorized, errNoSubject)
		return
	}

	req.SetAttribute(subAttr, tok.Subject)
	chain.ProcessFilter(req, resp)
}

// ensureVerifiers lazily builds one OIDC verifier per trusted issuer (each does
// a discovery fetch to find the JWKS endpoint) and caches them. If discovery
// fails it leaves the cache empty and returns the error, so a transient issuer
// outage doesn't permanently wedge the seed — the next request retries.
func (s *Seed) ensureVerifiers(ctx context.Context) ([]*oidc.IDTokenVerifier, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.verifiers != nil {
		return s.verifiers, nil
	}
	if len(s.issuers) == 0 {
		return nil, errNoIssuers
	}
	vs := make([]*oidc.IDTokenVerifier, 0, len(s.issuers))
	for _, iss := range s.issuers {
		provider, err := oidc.NewProvider(ctx, iss)
		if err != nil {
			return nil, fmt.Errorf("oidc discovery %q: %w", iss, err)
		}
		// SkipClientIDCheck: audience is not enforced yet (see verify).
		vs = append(vs, provider.Verifier(&oidc.Config{SkipClientIDCheck: true}))
	}
	s.verifiers = vs
	return vs, nil
}

// bearerToken pulls the token out of an "Authorization: Bearer <token>" header.
func bearerToken(req *restful.Request) string {
	const prefix = "Bearer "
	h := req.HeaderParameter("Authorization")
	if len(h) >= len(prefix) && strings.EqualFold(h[:len(prefix)], prefix) {
		return strings.TrimSpace(h[len(prefix):])
	}
	return ""
}

// sub returns the verified cluster identity the verify filter stashed.
func sub(req *restful.Request) string {
	s, _ := req.Attribute(subAttr).(string)
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
)
