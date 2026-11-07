// Command go-fixture produces and verifies the Go reference fixtures for the
// P4 session PoC. It uses the same libraries as the baseline frontend:
// gorilla/securecookie v1.1.1 (cookie codec) and encoding/gob (Redis payload).
package main

import (
	"bytes"
	"encoding/base64"
	"encoding/gob"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/gorilla/securecookie"
)

// Mirror of cmd/frontend/internal/auth/session/session.go.
type sessionInfo struct {
	Actor         *Actor        `json:"actor"`
	LastActive    time.Time     `json:"lastActive"`
	ExpiryPeriod  time.Duration `json:"expiryPeriod"`
	UserCreatedAt time.Time     `json:"userCreatedAt"`
}

// Mirror of internal/actor.Actor (FromSessionCookie is json:"-").
type Actor struct {
	UID                 int32  `json:",omitempty"`
	AnonymousUID        string `json:",omitempty"`
	Internal            bool   `json:",omitempty"`
	SourcegraphOperator bool   `json:",omitempty"`
}

const (
	cookieName = "sgs"
	baseID     = "OUYT6S23AHN2ZYTKWKMU7YFJT4DGO22FMINRI7TGSVV63QG2BWGA"
	testKey    = "test-key-123"
	ttl        = 86400 * 30
)

type entry struct {
	Key    string `json:"key"`
	Base64 string `json:"base64"`
	Len    int    `json:"len"`
}

func main() {
	if len(os.Args) < 2 {
		fatal("usage: go-fixture gen|decode-cookie|decode-gob|verify-redis ...")
	}
	switch os.Args[1] {
	case "gen":
		gen(os.Args[2:])
	case "decode-cookie":
		decodeCookie(os.Args[2:])
	case "decode-gob":
		decodeGob(os.Args[2:])
	case "verify-redis":
		verifyRedis(os.Args[2:])
	default:
		fatal("unknown subcommand " + os.Args[1])
	}
}

func gen(args []string) {
	fs := flag.NewFlagSet("gen", flag.ExitOnError)
	out := fs.String("out", "../../../testdata/sessions", "output directory")
	fs.Parse(args)

	if err := os.MkdirAll(*out, 0o755); err != nil {
		fatal(err)
	}

	// 1. Cookie fixtures. The timestamp is the encode time; the Rust side must
	// decode it and re-encode the identical cookie string.
	for _, key := range []struct{ name, key string }{
		{"cookie-empty-key", ""},
		{"cookie-test-key", testKey},
	} {
		codec := securecookie.New([]byte(key.key), nil)
		// securecookie has no public timestamp setter, so the encode time is
		// "now"; recover it from the cookie itself. Rust must decode it and
		// re-encode the identical cookie string.
		cookie, err := codec.Encode(cookieName, baseID)
		if err != nil {
			fatal(err)
		}
		writeJSON(filepath.Join(*out, key.name+".json"), map[string]any{
			"cookie": cookie,
			"name":   cookieName,
			"key":    key.key,
			"id":     baseID,
			"timestamp": cookieTimestamp(cookie),
		})
	}

	// 2. Actor sessionInfo JSON, exactly as SetData("actor", ...) writes it.
	lastActive := time.Date(2026, 11, 7, 15, 19, 15, 703398646, time.UTC)
	userCreated := time.Date(2026, 11, 7, 13, 56, 25, 553432000, time.UTC)
	info := &sessionInfo{
		Actor:         &Actor{UID: 1},
		LastActive:    lastActive,
		ExpiryPeriod:  90 * 24 * time.Hour,
		UserCreatedAt: userCreated,
	}
	infoJSON, err := json.Marshal(info)
	if err != nil {
		fatal(err)
	}
	writeBytes(filepath.Join(*out, "actor.json"), infoJSON)

	// 3. Redis payloads (gob map[interface{}]interface{}).
	single := map[interface{}]interface{}{"actor": infoJSON}
	writeBytes(filepath.Join(*out, "redis-single.gob"), mustGob(single))

	multi := map[interface{}]interface{}{
		"actor":      infoJSON,
		"oauthState": []byte(`"state-123"`),
		"oidcState":  []byte(`"oidc-state-456"`),
		"oidc@0":     []byte(`{"state":"abc","nonce":"def"}`),
		"soap@0":     []byte(`null`),
	}
	writeBytes(filepath.Join(*out, "redis-multi.gob"), mustGob(multi))

	writeBytes(filepath.Join(*out, "redis-empty.gob"), mustGob(map[interface{}]interface{}{}))

	// 4. Expected decodes, in the same JSON shape the Rust CLI prints.
	writeJSON(filepath.Join(*out, "actor-meta.json"), map[string]any{
		"json":             string(infoJSON),
		"len":              len(infoJSON),
		"expiryPeriod_ns":  int64(info.ExpiryPeriod),
		"expiryPeriod_str": info.ExpiryPeriod.String(),
	})
	writeJSON(filepath.Join(*out, "redis-single.expected.json"), expected(mustGob(single)))
	writeJSON(filepath.Join(*out, "redis-multi.expected.json"), expected(mustGob(multi)))
	writeJSON(filepath.Join(*out, "redis-empty.expected.json"), expected(mustGob(map[interface{}]interface{}{})))

	fmt.Printf("wrote fixtures to %s\n", *out)
}

func expected(raw []byte) map[string]any {
	var m map[interface{}]interface{}
	if err := gob.NewDecoder(bytes.NewReader(raw)).Decode(&m); err != nil {
		fatal(err)
	}
	out := make([]entry, 0, len(m))
	for k, v := range m {
		b, ok := v.([]byte)
		if !ok {
			fatal(fmt.Sprintf("value %v is %T, want []byte", k, v))
		}
		out = append(out, entry{Key: fmt.Sprint(k), Base64: base64.StdEncoding.EncodeToString(b), Len: len(b)})
	}
	return map[string]any{"entries": out, "gob_len": len(raw)}
}

func decodeCookie(args []string) {
	fs := flag.NewFlagSet("decode-cookie", flag.ExitOnError)
	cookie := fs.String("cookie", "", "cookie value")
	key := fs.String("key", "", "hash key")
	fs.Parse(args)

	var id string
	// MaxAge(0) disables the 30-day timestamp check so checked-in fixtures stay
	// usable; expiry parity is covered by the Rust tests and verify.sh.
	codec := securecookie.New([]byte(*key), nil).MaxAge(0)
	if err := codec.Decode(cookieName, *cookie, &id); err != nil {
		fatal(err)
	}
	raw, err := base64.URLEncoding.DecodeString(*cookie)
	if err != nil {
		fatal(err)
	}
	parts := bytes.SplitN(raw, []byte("|"), 3)
	ts := atoi(string(parts[0]))
	writeJSONLine(map[string]any{
		"timestamp": ts,
		"id":        id,
		"id_len":    len(id),
		"when":      time.Unix(ts, 0).UTC().Format(time.RFC3339),
	})
}

func decodeGob(args []string) {
	fs := flag.NewFlagSet("decode-gob", flag.ExitOnError)
	hexs := fs.String("hex", "", "gob hex")
	file := fs.String("file", "", "gob file")
	fs.Parse(args)

	var raw []byte
	var err error
	if *hexs != "" {
		raw, err = hex.DecodeString(*hexs)
	} else if *file != "" {
		raw, err = os.ReadFile(*file)
	} else {
		fatal("need -hex or -file")
	}
	if err != nil {
		fatal(err)
	}
	writeJSONLine(expected(raw))
}

func verifyRedis(args []string) {
	fs := flag.NewFlagSet("verify-redis", flag.ExitOnError)
	cookie := fs.String("cookie", "", "cookie value")
	key := fs.String("key", "", "hash key")
	addr := fs.String("redis", "127.0.0.1:6379", "redis address")
	fs.Parse(args)

	var id string
	codec := securecookie.New([]byte(*key), nil).MaxAge(0)
	if err := codec.Decode(cookieName, *cookie, &id); err != nil {
		fatal(err)
	}
	conn, err := redis.Dial("tcp", *addr)
	if err != nil {
		fatal(err)
	}
	defer conn.Close()
	raw, err := redis.Bytes(conn.Do("GET", "session_"+id))
	if err != nil {
		fatal(err)
	}
	var m map[interface{}]interface{}
	if err := gob.NewDecoder(bytes.NewReader(raw)).Decode(&m); err != nil {
		fatal(err)
	}
	out := expected(raw)
	actorRaw, _ := m["actor"].([]byte)
	var actorJSON any
	if actorRaw != nil {
		var v map[string]any
		if err := json.Unmarshal(actorRaw, &v); err != nil {
			fatal(err)
		}
		actorJSON = v
	}
	out["id"] = id
	out["redis_found"] = true
	out["actor"] = actorJSON
	writeJSONLine(out)
}

// cookieTimestamp extracts the unix-seconds prefix of a securecookie value.
func cookieTimestamp(cookie string) int64 {
	raw, err := base64.URLEncoding.DecodeString(cookie)
	if err != nil {
		fatal(err)
	}
	parts := bytes.SplitN(raw, []byte("|"), 3)
	return atoi(string(parts[0]))
}

func mustGob(v any) []byte {
	var b bytes.Buffer
	if err := gob.NewEncoder(&b).Encode(v); err != nil {
		fatal(err)
	}
	return b.Bytes()
}

func writeJSON(path string, v any) {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		fatal(err)
	}
	b = append(b, '\n')
	if err := os.WriteFile(path, b, 0o644); err != nil {
		fatal(err)
	}
}

func writeJSONLine(v any) {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		fatal(err)
	}
}

func writeBytes(path string, b []byte) {
	if err := os.WriteFile(path, b, 0o644); err != nil {
		fatal(err)
	}
}

func atoi(s string) int64 {
	var n int64
	if _, err := fmt.Sscanf(s, "%d", &n); err != nil {
		fatal(err)
	}
	return n
}

func fatal(v any) {
	fmt.Fprintln(os.Stderr, "go-fixture:", v)
	os.Exit(1)
}
