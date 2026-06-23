package v1alpha1

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// discoveryWellKnown is the unauthenticated sniff endpoint a discovery seed
// serves; a From URL that answers it with a valid descriptor is a seed.
const discoveryWellKnown = "/.well-known/libetcd-discovery"

// discoveryDescriptor mirrors the seed's sniff document: a version marker plus
// the endpoint paths, so the client neither hard-codes them nor mistakes a plain
// OIDC issuer for a seed.
type discoveryDescriptor struct {
	Discovery string `json:"discovery"`
	Token     string `json:"token,omitempty"`
	Claim     string `json:"claim"`
	Register  string `json:"register"`
	Roster    string `json:"roster"`
}

// discoverySeed is a sniffed discovery seed: its base URL, advertised endpoint
// paths, and the bearer (the cluster JWT) the client presents on every op.
type discoverySeed struct {
	base  string
	desc  discoveryDescriptor
	token string
	http  *http.Client
}

// probeSeed GETs raw + discoveryWellKnown and, if it answers with a valid
// descriptor, returns the seed. Any non-200, parse failure, or transport error
// means "not a seed" (ok=false) so the caller falls through to a plain raft
// peer — a transient probe error and a real peer both just proceed as a peer.
func probeSeed(ctx context.Context, raw string, hc *http.Client) (seed *discoverySeed, ok bool) {
	base := strings.TrimRight(raw, "/")
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+discoveryWellKnown, nil)
	if err != nil {
		return nil, false
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Cache-Control", "no-cache") // bypass any edge cache fronting the seed
	resp, err := hc.Do(req)
	if err != nil {
		return nil, false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, false
	}
	var d discoveryDescriptor
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<16)).Decode(&d); err != nil {
		return nil, false
	}
	// A real descriptor names the discovery version and all three core ops;
	// anything else is some other JSON endpoint, not a seed.
	if d.Discovery == "" || d.Claim == "" || d.Register == "" || d.Roster == "" {
		return nil, false
	}
	return &discoverySeed{base: base, desc: d, http: hc}, true
}

// claim attempts the atomic bootstrap claim. won=true to exactly one caller (it
// bootstraps) and the seed returns the freshly minted cluster secret; the rest
// get won=false and an empty secret (they read it from the roster). The secret —
// not the JWT — is the cluster's join credential, so every node with the same
// sub shares it (issue #120).
func (s *discoverySeed) claim(ctx context.Context) (won bool, secret string, err error) {
	resp, err := s.do(ctx, http.MethodPost, s.desc.Claim, nil)
	if err != nil {
		return false, "", err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK:
		var out struct {
			Secret string `json:"secret"`
		}
		if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<16)).Decode(&out); err != nil {
			return false, "", fmt.Errorf("claim: decode: %w", err)
		}
		return true, out.Secret, nil
	case http.StatusConflict:
		return false, "", nil
	default:
		return false, "", s.statusError("claim", resp)
	}
}

// register advertises url as a live join target under a stable id (idempotent —
// re-calling refreshes the TTL).
func (s *discoverySeed) register(ctx context.Context, id, url string) error {
	body, err := json.Marshal(map[string]string{"id": id, "url": url})
	if err != nil {
		return err
	}
	resp, err := s.do(ctx, http.MethodPost, s.desc.Register, body)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		return s.statusError("register", resp)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}

// roster returns the current live join-target URLs and the cluster secret.
func (s *discoverySeed) roster(ctx context.Context) (urls []string, secret string, err error) {
	resp, err := s.do(ctx, http.MethodGet, s.desc.Roster, nil)
	if err != nil {
		return nil, "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, "", s.statusError("roster", resp)
	}
	var out struct {
		URLs   []string `json:"urls"`
		Secret string   `json:"secret"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&out); err != nil {
		return nil, "", fmt.Errorf("roster: decode: %w", err)
	}
	return out.URLs, out.Secret, nil
}

// rosterWait polls roster until it is non-empty or ctx ends, returning the live
// URLs and the cluster secret. A loser that arrives before the bootstrapper has
// registered would otherwise see an empty roster; once it is non-empty the
// winner has minted the secret, so both come back together.
//
// Wait-only by design (issue #108): a winner that dies before registering is
// recovered by an external Join retry once its short-TTL claim frees, not by
// re-claiming in this loop.
func (s *discoverySeed) rosterWait(ctx context.Context) ([]string, string, error) {
	for {
		urls, secret, err := s.roster(ctx)
		if err != nil {
			return nil, "", err
		}
		if len(urls) > 0 {
			return urls, secret, nil
		}
		select {
		case <-ctx.Done():
			return nil, "", fmt.Errorf("roster: empty until deadline: %w", context.Cause(ctx))
		case <-time.After(time.Second):
		}
	}
}

// do issues a seed request with the bearer token and the no-cache header.
func (s *discoverySeed) do(ctx context.Context, method, path string, body []byte) (*http.Response, error) {
	var r io.Reader
	if body != nil {
		r = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, s.base+path, r)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+s.token)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Cache-Control", "no-cache")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	return s.http.Do(req)
}

// statusError reads a bounded error body for a non-success status.
func (s *discoverySeed) statusError(op string, resp *http.Response) error {
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
	return fmt.Errorf("%s: %s: %s", op, resp.Status, strings.TrimSpace(string(b)))
}
