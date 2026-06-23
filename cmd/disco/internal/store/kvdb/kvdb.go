// Package kvdb implements store.Store on top of kvdb.io
// (https://kvdb.io/docs/api/).
//
// Key scheme, all in one bucket, namespaced by the verified JWT sub:
//
//	c/<sub>        claim counter — PATCH "+1" ?ttl=, the caller that reads 1 wins
//	r/<sub>/<id>   roster entry  — value=url, ?ttl=, prefix-listed
//
// <sub> and <id> are hashed (see seg) before going into the path: a JWT sub
// contains '/' and ':' (e.g. a GitHub OIDC "repo:owner/name:ref:..."), which
// would otherwise break the path structure and the r/<sub>/ prefix listing. The
// hash is only a namespace — the original value never needs recovering.
//
// The three primitives the protocol needs map directly onto kvdb:
//   - Claim    -> PATCH c/<sub> "+1" ?ttl=N         (atomic incr, creates-as-zero)
//   - Register -> PUT   r/<sub>/<id> ?ttl=N          (TTL'd; re-PUT = keepalive)
//   - Roster   -> GET   ?prefix=r/<sub>/&values=true ([[key,url],...])
//
// Auth: the bucket is protected by a kvdb secret_key, and the seed authenticates
// with it via HTTP Basic auth (env DISCO_KVDB_SECRET; username=secret_key, empty
// password). The secret_key is required because Roster *lists* keys, and per the
// kvdb docs listing is a secret_key operation — scoped access tokens grant
// read/write on sub-keys but cannot list (the only token permissions are read
// and write). So the seed holds the bucket master key. That is acceptable here:
// the seed is the sole trusted client (nodes never touch kvdb — they speak to
// the seed with their cluster JWT), and the bucket holds only ephemeral
// discovery state (claim counters + peer URLs), so the blast radius of a seed
// compromise is bounded to discovery, not application data.
//
// Freshness: every request sets the header "Cache-Control: no-cache". kvdb
// caches responses at the edge, which for discovery is a correctness bug, not a
// latency win — a cached Roster would omit a just-registered member or still
// show an expired one, and a cached claim counter could let two nodes both read
// 1 and both bootstrap (split brain). no-cache forces a fresh read on every op.
package kvdb

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/cnuss/libetcd/cmd/disco/internal/store"
)

// entryTTL is the lifetime stamped on claim and roster keys. Clients refresh
// well within it (issue #108: TTL 10s / keepalive 3s); a member that stops
// refreshing is pruned.
const entryTTL = 10 * time.Second

// client is the kvdb.io-backed store.Store.
type client struct {
	base   string        // https://kvdb.io/<bucket>
	secret string        // kvdb secret_key (HTTP Basic username, empty password)
	ttl    time.Duration // entry lifetime; clients keepalive within it
	http   *http.Client
}

// New reads the bucket + secret_key from the environment and returns a
// store.Store. The secret_key (not a scoped token) is required: Roster lists,
// and listing is a secret_key operation in kvdb.
func New() (store.Store, error) {
	bucket := os.Getenv("DISCO_KVDB_BUCKET")
	secret := os.Getenv("DISCO_KVDB_SECRET")
	if bucket == "" || secret == "" {
		return nil, fmt.Errorf("kvdb: DISCO_KVDB_BUCKET and DISCO_KVDB_SECRET are required")
	}
	return &client{
		base:   "https://kvdb.io/" + bucket,
		secret: secret,
		ttl:    entryTTL,
		http:   http.DefaultClient,
	}, nil
}

// Claim does PATCH c/<sub> with body "+1" and ?ttl, returning won=(value==1).
// The counter is created-as-zero, so the first caller increments it to 1 and
// wins; everyone after reads >1 and joins. The TTL self-heals a bootstrapper
// that dies before the cluster forms (the key expires and a later arrival can
// re-win).
func (c *client) Claim(ctx context.Context, sub string) (bool, error) {
	resp, err := c.do(ctx, http.MethodPatch, "/c/"+seg(sub), c.ttlQuery(), strings.NewReader("+1"))
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	body := snippet(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return false, fmt.Errorf("kvdb claim: %s: %s", resp.Status, body)
	}
	n, err := strconv.Atoi(strings.TrimSpace(body))
	if err != nil {
		return false, fmt.Errorf("kvdb claim: bad counter %q: %w", body, err)
	}
	return n == 1, nil
}

// Register does PUT r/<sub>/<id>=url with ?ttl. Idempotent: re-calling with the
// same id overwrites in place and refreshes the TTL (keepalive-as-re-register).
func (c *client) Register(ctx context.Context, sub, id, url string) error {
	resp, err := c.do(ctx, http.MethodPut, "/r/"+seg(sub)+"/"+seg(id), c.ttlQuery(), strings.NewReader(url))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("kvdb register: %s: %s", resp.Status, snippet(resp.Body))
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}

// Roster does GET ?prefix=r/<sub>/&values=true and returns the urls. kvdb
// answers with [[key, value], ...]; we keep the values (the advertised URLs).
func (c *client) Roster(ctx context.Context, sub string) ([]string, error) {
	q := "prefix=r/" + seg(sub) + "/&values=true&format=json"
	resp, err := c.do(ctx, http.MethodGet, "/", q, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("kvdb roster: %s: %s", resp.Status, snippet(resp.Body))
	}
	var pairs [][]json.RawMessage
	if err := json.NewDecoder(resp.Body).Decode(&pairs); err != nil {
		return nil, fmt.Errorf("kvdb roster: decode: %w", err)
	}
	urls := make([]string, 0, len(pairs))
	for _, p := range pairs {
		if len(p) < 2 {
			continue
		}
		var u string
		if err := json.Unmarshal(p[1], &u); err != nil {
			continue // skip a non-string value rather than failing the whole list
		}
		urls = append(urls, u)
	}
	return urls, nil
}

// do builds and sends a kvdb request with the seed's Basic auth and the
// mandatory no-cache header. The caller closes resp.Body.
func (c *client) do(ctx context.Context, method, path, rawQuery string, body io.Reader) (*http.Response, error) {
	u := c.base + path
	if rawQuery != "" {
		u += "?" + rawQuery
	}
	req, err := http.NewRequestWithContext(ctx, method, u, body)
	if err != nil {
		return nil, err
	}
	req.SetBasicAuth(c.secret, "")
	// kvdb edge-caches responses; a stale roster/claim breaks discovery.
	req.Header.Set("Cache-Control", "no-cache")
	return c.http.Do(req)
}

// ttlQuery is the ?ttl=N stamped on claim and roster writes.
func (c *client) ttlQuery() string {
	return "ttl=" + strconv.Itoa(int(c.ttl.Seconds()))
}

// seg hashes a value into a path-safe key segment (a JWT sub or node id may hold
// '/' and ':'). Truncated to 12 bytes so r/<sub>/<id> stays well under kvdb's
// key-length limit; the hash only namespaces, so 96 bits is ample against
// collision across the handful of clusters/nodes a bucket holds.
func seg(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:12])
}

// snippet reads a bounded prefix of r for counter parsing and error messages.
func snippet(r io.Reader) string {
	b, _ := io.ReadAll(io.LimitReader(r, 256))
	return strings.TrimSpace(string(b))
}
