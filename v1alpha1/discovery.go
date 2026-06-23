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
// bootstraps); the rest get won=false (they join the roster).
func (s *discoverySeed) claim(ctx context.Context) (won bool, err error) {
	resp, err := s.do(ctx, http.MethodPost, s.desc.Claim, nil)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK:
		return true, nil
	case http.StatusConflict:
		return false, nil
	default:
		return false, s.statusError("claim", resp)
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

// roster returns the current live join-target URLs.
func (s *discoverySeed) roster(ctx context.Context) ([]string, error) {
	resp, err := s.do(ctx, http.MethodGet, s.desc.Roster, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, s.statusError("roster", resp)
	}
	var out struct {
		URLs []string `json:"urls"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&out); err != nil {
		return nil, fmt.Errorf("roster: decode: %w", err)
	}
	return out.URLs, nil
}

// rosterWait polls roster until it is non-empty or ctx ends. A loser that
// arrives before the bootstrapper has registered would otherwise see an empty
// roster.
//
// TODO(#108): per the contract a loser should re-enter the full resolver loop
// (re-claim, in case the winner died before registering) rather than only
// waiting on the roster. For now it waits.
func (s *discoverySeed) rosterWait(ctx context.Context) ([]string, error) {
	for {
		urls, err := s.roster(ctx)
		if err != nil {
			return nil, err
		}
		if len(urls) > 0 {
			return urls, nil
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("roster: empty until deadline: %w", context.Cause(ctx))
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
