// Package seed serves the disco discovery API: claim, register, and roster,
// each a thin translation of a verified request onto a store.Store operation
// (issue #108). The seed holds no cluster state of its own.
package seed

import (
	"errors"
	"net/http"

	restful "github.com/emicklei/go-restful/v3"

	"github.com/cnuss/libetcd/cmd/disco/internal/store"
)

// Seed is the discovery rendezvous service — a stateless shim over a Store.
type Seed struct {
	store store.Store
}

// New returns a Seed backed by the given store.
func New(s store.Store) *Seed { return &Seed{store: s} }

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
	errVerifyUnimplemented = errors.New("jwt verification not implemented")
	errClaimHeld           = errors.New("claim already held")
	errMissingURL          = errors.New("missing url")
	errStore               = errors.New("store error")
)

// verify is the JWT gate (issue #108 decision: seed-side verification). The
// node carries the cluster JWT as a bearer; the seed checks sig/exp/aud against
// the issuer JWKS and extracts sub — the cluster identity that namespaces the
// roster. Fail-closed: until implemented it rejects every request, so the seed
// never serves an unauthenticated discovery op.
func (s *Seed) verify(req *restful.Request, resp *restful.Response, chain *restful.FilterChain) {
	// TODO(#108): verify the bearer JWT and admit the request:
	//   raw := strings.TrimPrefix(req.HeaderParameter("Authorization"), "Bearer ")
	//   claims, err := verifyJWT(raw)  // golang-jwt/jwt/v5 + issuer JWKS
	//   if err != nil { resp.WriteError(http.StatusUnauthorized, err); return }
	//   req.SetAttribute(subAttr, claims.Subject)
	//   chain.ProcessFilter(req, resp)
	_ = chain
	_ = resp.WriteError(http.StatusNotImplemented, errVerifyUnimplemented)
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
