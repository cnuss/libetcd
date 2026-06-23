// Package issuer lets the disco seed act as its own OIDC issuer: it mints short
// JWTs under a random, self-namespaced identity and publishes the discovery +
// JWKS documents needed to verify them. This gives callers without an external
// IdP (e.g. a runner with no GitHub OIDC token) a disco-native cluster identity:
// POST /token once, hand the returned token to every node of that cluster — they
// share the sub, so they find each other; a different /token call is a different
// cluster.
//
// The signing key must be STABLE across process restarts and across Lambda
// instances — a token minted under one key is unverifiable once the published
// JWKS rotates to another. Load it from the environment (DISCO_SIGNING_KEY); the
// ephemeral fallback is for local dev only.
package issuer

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"os"
	"time"

	"github.com/go-jose/go-jose/v4"
	"github.com/go-jose/go-jose/v4/jwt"
)

// Issuer mints and publishes JWTs under its own OIDC identity.
type Issuer struct {
	url    string
	signer jose.Signer
	pubJWK jose.JSONWebKey
	ttl    time.Duration
}

// New builds an Issuer that signs tokens for issuerURL (the value that lands in
// the "iss" claim and the discovery document's "issuer") with the given RSA key.
func New(issuerURL string, key *rsa.PrivateKey, ttl time.Duration) (*Issuer, error) {
	pub := jose.JSONWebKey{Key: &key.PublicKey, Algorithm: "RS256", Use: "sig"}
	tp, err := pub.Thumbprint(crypto.SHA256)
	if err != nil {
		return nil, fmt.Errorf("issuer: key thumbprint: %w", err)
	}
	pub.KeyID = hex.EncodeToString(tp)

	signer, err := jose.NewSigner(
		jose.SigningKey{Algorithm: jose.RS256, Key: jose.JSONWebKey{Key: key, KeyID: pub.KeyID}},
		(&jose.SignerOptions{}).WithType("JWT"),
	)
	if err != nil {
		return nil, fmt.Errorf("issuer: signer: %w", err)
	}
	return &Issuer{url: issuerURL, signer: signer, pubJWK: pub, ttl: ttl}, nil
}

// URL is the issuer identifier (matches the "iss" claim + discovery "issuer").
func (i *Issuer) URL() string { return i.url }

// Mint signs a token for sub and reports its lifetime in seconds.
func (i *Issuer) Mint(sub string) (token string, expiresIn int, err error) {
	now := time.Now()
	raw, err := jwt.Signed(i.signer).Claims(jwt.Claims{
		Issuer:   i.url,
		Subject:  sub,
		Audience: jwt.Audience{"disco"},
		IssuedAt: jwt.NewNumericDate(now),
		Expiry:   jwt.NewNumericDate(now.Add(i.ttl)),
	}).Serialize()
	if err != nil {
		return "", 0, fmt.Errorf("issuer: mint: %w", err)
	}
	return raw, int(i.ttl.Seconds()), nil
}

// JWKS is the public key set served at the jwks_uri.
func (i *Issuer) JWKS() jose.JSONWebKeySet {
	return jose.JSONWebKeySet{Keys: []jose.JSONWebKey{i.pubJWK}}
}

// Discovery is the OpenID Provider configuration served at
// /.well-known/openid-configuration.
func (i *Issuer) Discovery() map[string]any {
	return map[string]any{
		"issuer":                                i.url,
		"jwks_uri":                              i.url + JWKSPath,
		"response_types_supported":              []string{"id_token"},
		"subject_types_supported":               []string{"public"},
		"id_token_signing_alg_values_supported": []string{"RS256"},
	}
}

// Well-known paths the seed serves for this issuer.
const (
	DiscoveryPath = "/.well-known/openid-configuration"
	JWKSPath      = "/.well-known/jwks.json"
)

// NewRandomSub returns a fresh opaque cluster identity (disco:<128-bit random>).
// Callers share the resulting token across the nodes of one cluster; a new sub
// is a new cluster namespace.
func NewRandomSub() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("issuer: random sub: %w", err)
	}
	return "disco:" + hex.EncodeToString(b[:]), nil
}

// KeyFromEnv loads the RSA signing key from DISCO_SIGNING_KEY (PEM, PKCS#1 or
// PKCS#8). If unset it generates an ephemeral key and reports ephemeral=true so
// the caller can warn — ephemeral keys don't survive a restart or a second
// instance, which breaks verification of already-minted tokens.
func KeyFromEnv() (key *rsa.PrivateKey, ephemeral bool, err error) {
	raw := os.Getenv("DISCO_SIGNING_KEY")
	if raw == "" {
		k, err := rsa.GenerateKey(rand.Reader, 2048)
		if err != nil {
			return nil, true, fmt.Errorf("issuer: generate ephemeral key: %w", err)
		}
		return k, true, nil
	}
	block, _ := pem.Decode([]byte(raw))
	if block == nil {
		return nil, false, fmt.Errorf("issuer: DISCO_SIGNING_KEY is not valid PEM")
	}
	if k, err := x509.ParsePKCS1PrivateKey(block.Bytes); err == nil {
		return k, false, nil
	}
	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, false, fmt.Errorf("issuer: parse DISCO_SIGNING_KEY: %w", err)
	}
	k, ok := parsed.(*rsa.PrivateKey)
	if !ok {
		return nil, false, fmt.Errorf("issuer: DISCO_SIGNING_KEY is %T, want RSA", parsed)
	}
	return k, false, nil
}
