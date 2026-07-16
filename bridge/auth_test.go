// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-rs-kafka Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

// Unit coverage for AWS_MSK_IAM and OAUTHBEARER OIDC: mechanism selection,
// cross-key validation, and the parts of each credential path that resolve
// without a network. The OIDC token fetch cycle runs against httptest; the
// AWS cases cover static environment credentials and missing credentials.

package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Cross-key validation (no network): misconfiguration is a loud startup error
// ---------------------------------------------------------------------------

func TestAuthValidationErrors(t *testing.T) {
	cases := []struct {
		name    string
		pairs   map[string]string
		wantSub string
	}{
		{
			name: "iam without tls",
			pairs: map[string]string{
				"security.protocol": "sasl_plaintext",
				"sasl.mechanism":    "AWS_MSK_IAM",
			},
			wantSub: "AWS_MSK_IAM requires security.protocol=sasl_ssl",
		},
		{
			name: "iam rejects static credentials",
			pairs: map[string]string{
				"security.protocol": "sasl_ssl",
				"sasl.mechanism":    "AWS_MSK_IAM",
				"sasl.username":     "u",
				"sasl.password":     "p",
			},
			wantSub: "sasl.username/sasl.password are not used with AWS_MSK_IAM",
		},
		{
			name: "session name without role arn",
			pairs: map[string]string{
				"security.protocol":     "sasl_ssl",
				"sasl.mechanism":        "AWS_MSK_IAM",
				"aws.role.session.name": "svc",
			},
			wantSub: "aws.role.session.name requires aws.role.arn",
		},
		{
			name: "aws keys without iam mechanism",
			pairs: map[string]string{
				"security.protocol": "sasl_ssl",
				"sasl.mechanism":    "SCRAM-SHA-256",
				"sasl.username":     "u",
				"sasl.password":     "p",
				"aws.region":        "eu-west-2",
			},
			wantSub: "aws.* options apply only to sasl.mechanism=AWS_MSK_IAM",
		},
		{
			name: "aws keys under ssl protocol (not sasl)",
			pairs: map[string]string{
				"security.protocol": "ssl",
				"aws.region":        "eu-west-2",
			},
			wantSub: "sasl.* options require security.protocol=sasl_plaintext or sasl_ssl",
		},
		{
			name: "oauth missing token endpoint",
			pairs: map[string]string{
				"security.protocol":              "sasl_ssl",
				"sasl.mechanism":                 "OAUTHBEARER",
				"sasl.oauthbearer.client.id":     "id",
				"sasl.oauthbearer.client.secret": "secret",
			},
			wantSub: "OAUTHBEARER requires sasl.oauthbearer.token.endpoint.url",
		},
		{
			name: "oauth missing client credentials",
			pairs: map[string]string{
				"security.protocol":                   "sasl_ssl",
				"sasl.mechanism":                      "OAUTHBEARER",
				"sasl.oauthbearer.token.endpoint.url": "https://idp/token",
			},
			wantSub: "sasl.oauthbearer.client.id and sasl.oauthbearer.client.secret",
		},
		{
			name: "oauth unsupported method",
			pairs: map[string]string{
				"security.protocol":                   "sasl_ssl",
				"sasl.mechanism":                      "OAUTHBEARER",
				"sasl.oauthbearer.token.endpoint.url": "https://idp/token",
				"sasl.oauthbearer.client.id":          "id",
				"sasl.oauthbearer.client.secret":      "secret",
				"sasl.oauthbearer.method":             "callback",
			},
			wantSub: `sasl.oauthbearer.method="callback" is not supported`,
		},
		{
			name: "oauth keys without oauthbearer mechanism",
			pairs: map[string]string{
				"security.protocol":          "sasl_ssl",
				"sasl.mechanism":             "PLAIN",
				"sasl.username":              "u",
				"sasl.password":              "p",
				"sasl.oauthbearer.client.id": "id",
			},
			wantSub: "sasl.oauthbearer.* options apply only to sasl.mechanism=OAUTHBEARER",
		},
		{
			name: "unsupported mechanism lists the new ones",
			pairs: map[string]string{
				"security.protocol": "sasl_ssl",
				"sasl.mechanism":    "SCRAM-SHA-1",
			},
			wantSub: "AWS_MSK_IAM, OAUTHBEARER",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, berr := collectSecurity(t, tc.pairs).build()
			if berr == nil {
				t.Fatal("expected an error")
			}
			if berr.code != errBadOption {
				t.Fatalf("code = %d, want errBadOption (%d): %s", berr.code, errBadOption, berr.msg)
			}
			if !strings.Contains(berr.msg, tc.wantSub) {
				t.Fatalf("error %q does not contain %q", berr.msg, tc.wantSub)
			}
		})
	}
}

// The new mechanism keys must appear in the unknown-key supported listing so
// a typo (e.g. sasl.oauthbearer.clientid) lists them.
func TestAuthKeysAppearInSupportedListing(t *testing.T) {
	_, berr := franzKgoOpts(map[string]string{"no.such.key": "x"})
	if berr == nil {
		t.Fatal("expected unknown-key error")
	}
	for _, want := range []string{
		"aws.region", "aws.profile", "aws.role.arn", "aws.role.session.name",
		"sasl.oauthbearer.token.endpoint.url", "sasl.oauthbearer.client.id",
		"sasl.oauthbearer.client.secret", "sasl.oauthbearer.scope",
		"sasl.oauthbearer.extensions", "sasl.oauthbearer.method",
	} {
		if !strings.Contains(berr.msg, want) {
			t.Fatalf("supported-keys listing missing %s: %s", want, berr.msg)
		}
	}
}

// ---------------------------------------------------------------------------
// OAUTHBEARER OIDC token fetcher
// ---------------------------------------------------------------------------

func TestOIDCFetcherHappyPath(t *testing.T) {
	var gotGrant, gotAuth string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		gotGrant = r.Form.Get("grant_type")
		gotAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"access_token":"tok-abc","token_type":"Bearer","expires_in":3600}`)
	}))
	defer server.Close()

	fetcher := newOIDCTokenFetcher(server.URL, "client", "secret", "kafka")
	token, err := fetcher.accessToken(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if token != "tok-abc" {
		t.Fatalf("token = %q, want tok-abc", token)
	}
	if gotGrant != "client_credentials" {
		t.Fatalf("grant_type = %q, want client_credentials", gotGrant)
	}
	if !strings.HasPrefix(gotAuth, "Basic ") {
		t.Fatalf("client credentials must travel as Basic auth, got %q", gotAuth)
	}
}

// A cached, unexpired token is reused: exactly one HTTP call across repeated
// accessToken invocations.
func TestOIDCFetcherCachesUntilExpiry(t *testing.T) {
	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		fmt.Fprintf(w, `{"access_token":"tok-%d","expires_in":3600}`, calls)
	}))
	defer server.Close()

	fetcher := newOIDCTokenFetcher(server.URL, "c", "s", "")
	first, err := fetcher.accessToken(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	second, err := fetcher.accessToken(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if first != second || calls != 1 {
		t.Fatalf("token not cached: first=%q second=%q calls=%d", first, second, calls)
	}
}

// Within the refresh leeway of expiry the fetcher re-fetches, so a request
// never travels with an almost-dead token.
func TestOIDCFetcherRefreshesNearExpiry(t *testing.T) {
	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		fmt.Fprintf(w, `{"access_token":"tok-%d","expires_in":40}`, calls)
	}))
	defer server.Close()

	base := time.Now()
	fetcher := newOIDCTokenFetcher(server.URL, "c", "s", "")
	fetcher.now = func() time.Time { return base }

	if _, err := fetcher.accessToken(context.Background()); err != nil {
		t.Fatal(err)
	}
	// 40s lifetime, 30s leeway: at +15s the token is within the leeway.
	fetcher.now = func() time.Time { return base.Add(15 * time.Second) }
	second, err := fetcher.accessToken(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if calls != 2 || second != "tok-2" {
		t.Fatalf("expected a refresh near expiry: calls=%d token=%q", calls, second)
	}
}

func TestOIDCFetcherErrorPaths(t *testing.T) {
	t.Run("http error status", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusUnauthorized)
			fmt.Fprint(w, `{"error":"invalid_client"}`)
		}))
		defer server.Close()
		_, err := newOIDCTokenFetcher(server.URL, "c", "bad", "").accessToken(context.Background())
		if err == nil || !strings.Contains(err.Error(), "401") {
			t.Fatalf("want a 401 error, got %v", err)
		}
	})

	t.Run("no access token in response", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			fmt.Fprint(w, `{"token_type":"Bearer"}`)
		}))
		defer server.Close()
		_, err := newOIDCTokenFetcher(server.URL, "c", "s", "").accessToken(context.Background())
		if err == nil || !strings.Contains(err.Error(), "no access_token") {
			t.Fatalf("want a missing-token error, got %v", err)
		}
	})

	t.Run("malformed json", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			fmt.Fprint(w, `not json`)
		}))
		defer server.Close()
		_, err := newOIDCTokenFetcher(server.URL, "c", "s", "").accessToken(context.Background())
		if err == nil || !strings.Contains(err.Error(), "parsing token response") {
			t.Fatalf("want a parse error, got %v", err)
		}
	})

	t.Run("endpoint unreachable", func(t *testing.T) {
		// Nothing listening on this port; the request fails fast.
		_, err := newOIDCTokenFetcher("http://127.0.0.1:1/token", "c", "s", "").accessToken(context.Background())
		if err == nil || !strings.Contains(err.Error(), "unreachable") {
			t.Fatalf("want an unreachable error, got %v", err)
		}
	})
}

// expires_in delivered as a JSON string, as some IdPs send it, is accepted.
func TestOIDCFetcherStringExpiresIn(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, `{"access_token":"tok","expires_in":"3600"}`)
	}))
	defer server.Close()
	base := time.Now()
	fetcher := newOIDCTokenFetcher(server.URL, "c", "s", "")
	fetcher.now = func() time.Time { return base }
	if _, err := fetcher.accessToken(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := fetcher.expiresAt.Sub(base); got != 3600*time.Second {
		t.Fatalf("expires_in string not parsed: got %s", got)
	}
}

// The OAUTHBEARER mechanism assembles end to end against a reachable token
// endpoint: validation passes, the eager token fetch succeeds, and a SASL
// mechanism is returned. Offline via httptest, so it covers the success
// wiring of oauthOIDCMechanism without a broker or a real IdP.
func TestOAuthOIDCMechanismAssemblesAgainstTokenEndpoint(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, `{"access_token":"tok","expires_in":3600}`)
	}))
	defer server.Close()

	mechanism, berr := collectSecurity(t, map[string]string{
		"security.protocol":                   "sasl_ssl",
		"sasl.mechanism":                      "OAUTHBEARER",
		"sasl.oauthbearer.token.endpoint.url": server.URL,
		"sasl.oauthbearer.client.id":          "id",
		"sasl.oauthbearer.client.secret":      "secret",
		"sasl.oauthbearer.scope":              "kafka",
		"sasl.oauthbearer.extensions":         "logicalCluster=lkc-1",
		"sasl.oauthbearer.method":             "oidc",
	}).build()
	if berr != nil {
		t.Fatalf("mechanism must assemble against a reachable endpoint: %v", berr)
	}
	if mechanism == nil {
		t.Fatal("expected a SASL mechanism")
	}
}

// An unreachable token endpoint is a clean startup error (errAdapter), not a
// hang: the eager fetch reports it through oauthOIDCMechanism.
func TestOAuthOIDCMechanismUnreachableEndpointIsCleanError(t *testing.T) {
	_, berr := collectSecurity(t, map[string]string{
		"security.protocol":                   "sasl_ssl",
		"sasl.mechanism":                      "OAUTHBEARER",
		"sasl.oauthbearer.token.endpoint.url": "http://127.0.0.1:1/token",
		"sasl.oauthbearer.client.id":          "id",
		"sasl.oauthbearer.client.secret":      "secret",
	}).build()
	if berr == nil {
		t.Fatal("unreachable endpoint must be a startup error")
	}
	if berr.code != errAdapter {
		t.Fatalf("code = %d, want errAdapter (%d): %s", berr.code, errAdapter, berr.msg)
	}
	if !strings.Contains(berr.msg, "fetching initial token") {
		t.Fatalf("error must name the token fetch: %s", berr.msg)
	}
}

func TestParseOAuthExtensions(t *testing.T) {
	ext, berr := parseOAuthExtensions("logicalCluster=lkc-1, identityPoolId=pool-9")
	if berr != nil {
		t.Fatalf("unexpected error: %v", berr)
	}
	if ext["logicalCluster"] != "lkc-1" || ext["identityPoolId"] != "pool-9" {
		t.Fatalf("extensions parsed wrong: %v", ext)
	}

	if ext, berr := parseOAuthExtensions("   "); berr != nil || ext != nil {
		t.Fatalf("empty extensions: got (%v, %v)", ext, berr)
	}

	if _, berr := parseOAuthExtensions("novalue"); berr == nil {
		t.Fatal("a pair without = must be rejected")
	}
	if _, berr := parseOAuthExtensions("=orphan"); berr == nil {
		t.Fatal("a pair with an empty key must be rejected")
	}
}

// ---------------------------------------------------------------------------
// AWS_MSK_IAM credential paths the SDK resolves without a network
// ---------------------------------------------------------------------------

// Static credentials in the environment resolve offline: the eager Retrieve
// succeeds and a mechanism is returned, exercising the LoadDefaultConfig +
// Retrieve + ManagedStreamingIAM wiring without a broker.
func TestAWSIAMStaticEnvCredentialsResolve(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "AKIAEXAMPLE")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "secretexample")
	t.Setenv("AWS_REGION", "eu-west-2")

	mechanism, berr := collectSecurity(t, map[string]string{
		"security.protocol": "sasl_ssl",
		"sasl.mechanism":    "AWS_MSK_IAM",
	}).build()
	if berr != nil {
		t.Fatalf("static env credentials must resolve: %v", berr)
	}
	if mechanism == nil {
		t.Fatal("expected a SASL mechanism")
	}
}

// No credentials: the eager Retrieve fails with a startup error
// which names the provider chain. IMDS is disabled so the chain
// fails offline instead of probing the metadata endpoint.
func TestAWSIAMNoCredentialsIsCleanError(t *testing.T) {
	for _, key := range []string{
		"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN",
		"AWS_PROFILE", "AWS_WEB_IDENTITY_TOKEN_FILE", "AWS_ROLE_ARN",
	} {
		t.Setenv(key, "")
	}
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	t.Setenv("AWS_CONFIG_FILE", "/nonexistent/config")
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", "/nonexistent/credentials")
	t.Setenv("AWS_REGION", "eu-west-2")

	_, berr := collectSecurity(t, map[string]string{
		"security.protocol": "sasl_ssl",
		"sasl.mechanism":    "AWS_MSK_IAM",
	}).build()
	if berr == nil {
		t.Fatal("no credentials must be a startup error")
	}
	if berr.code != errAdapter {
		t.Fatalf("code = %d, want errAdapter (%d): %s", berr.code, errAdapter, berr.msg)
	}
	if !strings.Contains(berr.msg, "provider chain") {
		t.Fatalf("error must name the provider chain: %s", berr.msg)
	}
}
