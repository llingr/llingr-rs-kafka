// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-rs-kafka Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

// Unit coverage for GCP IAM, the OAUTHBEARER method=gcp mechanism: the
// GOOG_OAUTH2_TOKEN structure against Google's reference implementation,
// principal derivation order, validation errors, and the offline-reachable
// success and failure paths. A service account key whose token_uri points at
// an httptest server exercises the whole eager-fetch wiring without Google.

package main

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Token structure (the load-bearing format, mirroring Google's reference)
// ---------------------------------------------------------------------------

func decodeSegment(t *testing.T, segment string) []byte {
	t.Helper()
	data, err := base64.RawURLEncoding.DecodeString(segment)
	if err != nil {
		t.Fatalf("segment %q is not unpadded base64url: %v", segment, err)
	}
	return data
}

func TestGCPKafkaTokenStructure(t *testing.T) {
	expiry := time.Unix(1_752_800_000, 0)
	now := time.Unix(1_752_796_400, 0)
	token := gcpKafkaToken("ya29.raw-access-token", expiry, now, "svc@project.iam.gserviceaccount.com")

	if strings.Contains(token, "=") {
		t.Fatalf("segments must be unpadded: %q", token)
	}
	segments := strings.Split(token, ".")
	if len(segments) != 3 {
		t.Fatalf("token must have three dot-separated segments, got %d", len(segments))
	}

	// Header: byte-identical to Google's reference (json.dumps spacing).
	header := string(decodeSegment(t, segments[0]))
	if header != `{"typ": "JWT", "alg": "GOOG_OAUTH2_TOKEN"}` {
		t.Fatalf("header = %q", header)
	}

	// Claims: the reference field set, and machine-parseable.
	claimsRaw := string(decodeSegment(t, segments[1]))
	var claims struct {
		Exp int64  `json:"exp"`
		Iss string `json:"iss"`
		Iat int64  `json:"iat"`
		Sub string `json:"sub"`
	}
	if err := json.Unmarshal([]byte(claimsRaw), &claims); err != nil {
		t.Fatalf("claims do not parse: %v (%q)", err, claimsRaw)
	}
	if claims.Exp != 1_752_800_000 || claims.Iat != 1_752_796_400 {
		t.Fatalf("timestamps wrong: %+v", claims)
	}
	if claims.Iss != "Google" {
		t.Fatalf("iss = %q, want Google", claims.Iss)
	}
	if claims.Sub != "svc@project.iam.gserviceaccount.com" {
		t.Fatalf("sub = %q", claims.Sub)
	}
	// Field order matches the reference (exp, iss, iat, sub).
	if !strings.HasPrefix(claimsRaw, `{"exp": `) || !strings.Contains(claimsRaw, `"iss": "Google", "iat": `) {
		t.Fatalf("claim order diverged from the reference: %q", claimsRaw)
	}

	// Third segment: the RAW access token, verbatim.
	if got := string(decodeSegment(t, segments[2])); got != "ya29.raw-access-token" {
		t.Fatalf("third segment = %q, want the raw access token", got)
	}
}

// A principal needing JSON escaping cannot corrupt the claims document.
func TestGCPKafkaTokenEscapesPrincipal(t *testing.T) {
	token := gcpKafkaToken("tok", time.Unix(1, 0), time.Unix(0, 0), `evil"principal`)
	claimsRaw := decodeSegment(t, strings.Split(token, ".")[1])
	var claims map[string]any
	if err := json.Unmarshal(claimsRaw, &claims); err != nil {
		t.Fatalf("claims corrupted by principal escaping: %v", err)
	}
	if claims["sub"] != `evil"principal` {
		t.Fatalf("sub round-trip failed: %v", claims["sub"])
	}
}

// ---------------------------------------------------------------------------
// Principal derivation order
// ---------------------------------------------------------------------------

func TestGCPPrincipalDerivationOrder(t *testing.T) {
	saJSON := []byte(`{"type":"service_account","client_email":"from-json@x.iam.gserviceaccount.com"}`)

	// Explicit key wins over everything.
	t.Setenv(gcpPrincipalEnv, "from-env@x.iam.gserviceaccount.com")
	principal, berr := gcpPrincipal("from-key@x.iam.gserviceaccount.com", saJSON)
	if berr != nil || principal != "from-key@x.iam.gserviceaccount.com" {
		t.Fatalf("explicit key must win: (%q, %v)", principal, berr)
	}

	// Env override beats the credentials JSON.
	principal, berr = gcpPrincipal("", saJSON)
	if berr != nil || principal != "from-env@x.iam.gserviceaccount.com" {
		t.Fatalf("env must beat JSON: (%q, %v)", principal, berr)
	}

	// Credentials JSON client_email is the fallback.
	t.Setenv(gcpPrincipalEnv, "")
	principal, berr = gcpPrincipal("", saJSON)
	if berr != nil || principal != "from-json@x.iam.gserviceaccount.com" {
		t.Fatalf("JSON client_email fallback: (%q, %v)", principal, berr)
	}

	// Nothing available: the hard error naming the remedies.
	_, berr = gcpPrincipal("", nil)
	if berr == nil || !strings.Contains(berr.msg, "gcp.principal") ||
		!strings.Contains(berr.msg, gcpPrincipalEnv) {
		t.Fatalf("want the principal error naming both remedies, got %v", berr)
	}
}

// ---------------------------------------------------------------------------
// Validation errors
// ---------------------------------------------------------------------------

func TestGCPValidationErrors(t *testing.T) {
	cases := []struct {
		name    string
		pairs   map[string]string
		wantSub string
	}{
		{
			name: "gcp without tls",
			pairs: map[string]string{
				"security.protocol":       "sasl_plaintext",
				"sasl.mechanism":          "OAUTHBEARER",
				"sasl.oauthbearer.method": "gcp",
			},
			wantSub: "requires security.protocol=sasl_ssl",
		},
		{
			name: "gcp rejects static credentials",
			pairs: map[string]string{
				"security.protocol":       "sasl_ssl",
				"sasl.mechanism":          "OAUTHBEARER",
				"sasl.oauthbearer.method": "gcp",
				"sasl.username":           "u",
				"sasl.password":           "p",
			},
			wantSub: "sasl.username/sasl.password are not used with sasl.oauthbearer.method=gcp",
		},
		{
			name: "gcp rejects the oidc key family",
			pairs: map[string]string{
				"security.protocol":                   "sasl_ssl",
				"sasl.mechanism":                      "OAUTHBEARER",
				"sasl.oauthbearer.method":             "gcp",
				"sasl.oauthbearer.token.endpoint.url": "https://idp/token",
			},
			wantSub: "apply to sasl.oauthbearer.method=oidc, not method=gcp",
		},
		{
			name: "gcp keys under the oidc method are orphaned",
			pairs: map[string]string{
				"security.protocol":                   "sasl_ssl",
				"sasl.mechanism":                      "OAUTHBEARER",
				"sasl.oauthbearer.token.endpoint.url": "https://idp/token",
				"sasl.oauthbearer.client.id":          "id",
				"sasl.oauthbearer.client.secret":      "secret",
				"gcp.principal":                       "svc@x.iam.gserviceaccount.com",
			},
			wantSub: "gcp.* options apply only to sasl.mechanism=OAUTHBEARER with sasl.oauthbearer.method=gcp",
		},
		{
			name: "gcp keys under plain are orphaned",
			pairs: map[string]string{
				"security.protocol": "sasl_ssl",
				"sasl.mechanism":    "PLAIN",
				"sasl.username":     "u",
				"sasl.password":     "p",
				"gcp.principal":     "svc@x.iam.gserviceaccount.com",
			},
			wantSub: "gcp.* options apply only to",
		},
		{
			name: "nonexistent credentials file",
			pairs: map[string]string{
				"security.protocol":       "sasl_ssl",
				"sasl.mechanism":          "OAUTHBEARER",
				"sasl.oauthbearer.method": "gcp",
				"gcp.credentials.file":    "/nonexistent/sa-key.json",
			},
			wantSub: "gcp.credentials.file",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, berr := collectSecurity(t, tc.pairs).build()
			if berr == nil {
				t.Fatal("expected an error")
			}
			if !strings.Contains(berr.msg, tc.wantSub) {
				t.Fatalf("error %q does not contain %q", berr.msg, tc.wantSub)
			}
		})
	}
}

func TestGCPKeysAppearInSupportedListing(t *testing.T) {
	_, berr := franzKgoOpts(map[string]string{"no.such.key": "x"})
	if berr == nil {
		t.Fatal("expected unknown-key error")
	}
	for _, want := range []string{"gcp.principal", "gcp.credentials.file"} {
		if !strings.Contains(berr.msg, want) {
			t.Fatalf("supported-keys listing missing %s: %s", want, berr.msg)
		}
	}
}

// ---------------------------------------------------------------------------
// Mechanism paths, offline: a service account key whose token_uri points at
// an httptest server drives the real ADC token flow without Google
// ---------------------------------------------------------------------------

// writeServiceAccountKey writes a syntactically valid service account key
// file whose token_uri is the given endpoint, with a freshly generated RSA
// key so the oauth2 JWT signer accepts it.
func writeServiceAccountKey(t *testing.T, tokenURI string) string {
	t.Helper()
	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate RSA key: %v", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "PRIVATE KEY",
		Bytes: mustMarshalPKCS8(t, rsaKey),
	})
	key := map[string]string{
		"type":         "service_account",
		"project_id":   "test-project",
		"private_key":  string(keyPEM),
		"client_email": "svc@test-project.iam.gserviceaccount.com",
		"token_uri":    tokenURI,
	}
	data, err := json.Marshal(key)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	path := filepath.Join(t.TempDir(), "sa-key.json")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write key file: %v", err)
	}
	return path
}

func mustMarshalPKCS8(t *testing.T, key *rsa.PrivateKey) []byte {
	t.Helper()
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatalf("marshal PKCS8: %v", err)
	}
	return der
}

// The full mechanism assembles against a reachable token endpoint: the
// explicit credentials file resolves, the eager fetch succeeds, the
// principal derives from the key's client_email, and a SASL mechanism is
// returned.
func TestGCPMechanismAssemblesOffline(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"access_token":"ya29.test-token","token_type":"Bearer","expires_in":3600}`)
	}))
	defer server.Close()

	mechanism, berr := collectSecurity(t, map[string]string{
		"security.protocol":       "sasl_ssl",
		"sasl.mechanism":          "OAUTHBEARER",
		"sasl.oauthbearer.method": "gcp",
		"gcp.credentials.file":    writeServiceAccountKey(t, server.URL+"/token"),
	}).build()
	if berr != nil {
		t.Fatalf("mechanism must assemble against a reachable token endpoint: %v", berr)
	}
	if mechanism == nil {
		t.Fatal("expected a SASL mechanism")
	}
}

// An unreachable token endpoint fails the eager fetch with a clean startup
// error naming Application Default Credentials, not a hang.
func TestGCPMechanismUnreachableEndpointIsCleanError(t *testing.T) {
	_, berr := collectSecurity(t, map[string]string{
		"security.protocol":       "sasl_ssl",
		"sasl.mechanism":          "OAUTHBEARER",
		"sasl.oauthbearer.method": "gcp",
		"gcp.credentials.file":    writeServiceAccountKey(t, "http://127.0.0.1:1/token"),
	}).build()
	if berr == nil {
		t.Fatal("unreachable token endpoint must be a startup error")
	}
	if berr.code != errAdapter {
		t.Fatalf("code = %d, want errAdapter (%d): %s", berr.code, errAdapter, berr.msg)
	}
	if !strings.Contains(berr.msg, "fetching initial token from Application Default Credentials") {
		t.Fatalf("error must name the eager fetch: %s", berr.msg)
	}
}

// scrubGoogleEnv leaves the process looking like a GCE machine whose
// metadata service is a dead local port, since GCE_METADATA_HOST makes the
// SDK assume GCE without probing, with no key file and no gcloud user
// credentials. Deterministic offline, on any machine.
func scrubGoogleEnv(t *testing.T) {
	t.Helper()
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "")
	t.Setenv("GCE_METADATA_HOST", "127.0.0.1:1")
	t.Setenv("HOME", t.TempDir()) // no gcloud user credentials
	t.Setenv("CLOUDSDK_CONFIG", t.TempDir())
}

// Metadata-sourced credentials have no client_email JSON, so without
// gcp.principal or Google's env override the principal rule fires as a
// clean startup error naming both remedies, before any network fetch.
func TestGCPMetadataCredentialsNeedPrincipal(t *testing.T) {
	scrubGoogleEnv(t)

	_, berr := collectSecurity(t, map[string]string{
		"security.protocol":       "sasl_ssl",
		"sasl.mechanism":          "OAUTHBEARER",
		"sasl.oauthbearer.method": "gcp",
	}).build()
	if berr == nil {
		t.Fatal("metadata credentials without a principal must be a startup error")
	}
	if berr.code != errBadOption {
		t.Fatalf("code = %d, want errBadOption (%d): %s", berr.code, errBadOption, berr.msg)
	}
	if !strings.Contains(berr.msg, "gcp.principal") ||
		!strings.Contains(berr.msg, gcpPrincipalEnv) {
		t.Fatalf("error must name both principal remedies: %s", berr.msg)
	}
}

// With a principal supplied, the same dead-metadata environment reaches the
// eager token fetch, which fails fast with a clean startup error naming
// ADC; never a hang, because connection refused or the bounded select
// returns first.
func TestGCPUnreachableCredentialSourceIsCleanError(t *testing.T) {
	scrubGoogleEnv(t)

	_, berr := collectSecurity(t, map[string]string{
		"security.protocol":       "sasl_ssl",
		"sasl.mechanism":          "OAUTHBEARER",
		"sasl.oauthbearer.method": "gcp",
		"gcp.principal":           "svc@test-project.iam.gserviceaccount.com",
	}).build()
	if berr == nil {
		t.Fatal("an unreachable credential source must be a startup error")
	}
	if berr.code != errAdapter {
		t.Fatalf("code = %d, want errAdapter (%d): %s", berr.code, errAdapter, berr.msg)
	}
	if !strings.Contains(berr.msg, "Application Default Credentials") {
		t.Fatalf("error must name ADC: %s", berr.msg)
	}
}
