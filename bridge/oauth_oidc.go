// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-rs-kafka Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/oauth"
)

// oidcRefreshLeeway is how long before a token's stated expiry the fetcher
// refreshes it, so a request never travels with an almost-dead token.
const oidcRefreshLeeway = 30 * time.Second

// oidcTokenFetcher performs the OAuth 2.0 client-credentials grant against a
// token endpoint and caches the access token until shortly before expiry.
// It is the unit-testable core of the OAUTHBEARER OIDC path: point endpoint
// at an httptest server and it exercises the whole fetch/parse/cache cycle
// with no broker.
type oidcTokenFetcher struct {
	endpoint     string
	clientID     string
	clientSecret string
	scope        string
	client       *http.Client
	// now is time.Now in production; overridable in tests for expiry.
	now func() time.Time

	mu        sync.Mutex
	token     string
	expiresAt time.Time
}

// newOIDCTokenFetcher builds a fetcher with a bounded HTTP client. The
// timeout guards a single token request; runtime callers still pass their own
// context for cancellation.
func newOIDCTokenFetcher(endpoint, clientID, clientSecret, scope string) *oidcTokenFetcher {
	return &oidcTokenFetcher{
		endpoint:     endpoint,
		clientID:     clientID,
		clientSecret: clientSecret,
		scope:        scope,
		client:       &http.Client{Timeout: authResolveTimeout},
		now:          time.Now,
	}
}

// token returns a valid access token, fetching a fresh one when the cache is
// empty or within the refresh leeway of expiry. Safe for concurrent use.
func (f *oidcTokenFetcher) accessToken(ctx context.Context) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.token != "" && f.now().Before(f.expiresAt.Add(-oidcRefreshLeeway)) {
		return f.token, nil
	}

	token, expiresIn, err := f.fetch(ctx)
	if err != nil {
		return "", err
	}
	f.token = token
	// A token with no/zero expires_in is treated as immediately refreshable,
	// fetched every handshake, rather than cached forever.
	f.expiresAt = f.now().Add(time.Duration(expiresIn) * time.Second)
	return f.token, nil
}

// fetch performs one client-credentials POST and returns the access token and
// its lifetime in seconds.
func (f *oidcTokenFetcher) fetch(ctx context.Context) (token string, expiresIn int64, err error) {
	form := url.Values{}
	form.Set("grant_type", "client_credentials")
	if f.scope != "" {
		form.Set("scope", f.scope)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, f.endpoint,
		strings.NewReader(form.Encode()))
	if err != nil {
		return "", 0, fmt.Errorf("building token request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	// Client credentials go in the Basic auth header per RFC 6749 section
	// 2.3.1, the form the common IdPs (Keycloak, Okta, Entra) all accept.
	req.SetBasicAuth(url.QueryEscape(f.clientID), url.QueryEscape(f.clientSecret))

	resp, err := f.client.Do(req)
	if err != nil {
		return "", 0, fmt.Errorf("token endpoint %s unreachable: %w", f.endpoint, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", 0, fmt.Errorf("reading token response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return "", 0, fmt.Errorf("token endpoint %s returned %s: %s",
			f.endpoint, resp.Status, strings.TrimSpace(string(body)))
	}

	// expires_in is a JSON number per RFC 6749, but some IdPs emit it as a
	// string; accept both.
	var parsed struct {
		AccessToken string      `json:"access_token"`
		ExpiresIn   json.Number `json:"expires_in"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		return "", 0, fmt.Errorf("parsing token response: %w", err)
	}
	if parsed.AccessToken == "" {
		return "", 0, fmt.Errorf("token endpoint %s returned no access_token", f.endpoint)
	}
	if parsed.ExpiresIn != "" {
		if n, convErr := strconv.ParseInt(parsed.ExpiresIn.String(), 10, 64); convErr == nil && n > 0 {
			expiresIn = n
		}
	}
	return parsed.AccessToken, expiresIn, nil
}

// oauthOIDCMechanism assembles the OAUTHBEARER SASL mechanism backed by the
// client-credentials token fetcher. The token is fetched once eagerly here so
// an unreachable endpoint or a bad client credential is a clean startup error
// rather than a deferred handshake failure; the fetcher then serves and
// refreshes for the life of the consumer under each handshake's context.
func oauthOIDCMechanism(s *franzSecurity) (sasl.Mechanism, *bridgeError) {
	// OAUTHBEARER carries its own bearer token; static SASL credentials would
	// be silently unused.
	if s.username != "" || s.password != "" {
		return nil, bridgeErrorf(errBadOption,
			"sasl.username/sasl.password are not used with OAUTHBEARER "+
				"(the token comes from the OIDC token endpoint); remove them")
	}
	// The gcp method routed away before this point (see saslMechanism);
	// anything else that is not the OIDC token fetcher is unsupported, and
	// an FFI token-callback tier is deliberately out of scope.
	if s.oauthMethod != "" && s.oauthMethod != "oidc" {
		return nil, bridgeErrorf(errBadOption,
			"sasl.oauthbearer.method=%q is not supported (supported: \"oidc\", \"gcp\"; "+
				"application token callbacks are not available)", s.oauthMethod)
	}
	if s.oauthTokenEndpoint == "" {
		return nil, bridgeErrorf(errBadOption,
			"OAUTHBEARER requires sasl.oauthbearer.token.endpoint.url")
	}
	if s.oauthClientID == "" || s.oauthClientSecret == "" {
		return nil, bridgeErrorf(errBadOption,
			"OAUTHBEARER requires sasl.oauthbearer.client.id and sasl.oauthbearer.client.secret")
	}
	extensions, berr := parseOAuthExtensions(s.oauthExtensions)
	if berr != nil {
		return nil, berr
	}

	fetcher := newOIDCTokenFetcher(s.oauthTokenEndpoint, s.oauthClientID, s.oauthClientSecret, s.oauthScope)

	ctx, cancel := context.WithTimeout(context.Background(), authResolveTimeout)
	defer cancel()
	if _, err := fetcher.accessToken(ctx); err != nil {
		return nil, bridgeErrorf(errAdapter, "OAUTHBEARER: fetching initial token: %v", err)
	}

	authFn := func(handshakeCtx context.Context) (oauth.Auth, error) {
		token, err := fetcher.accessToken(handshakeCtx)
		if err != nil {
			return oauth.Auth{}, err
		}
		return oauth.Auth{Token: token, Extensions: extensions}, nil
	}
	return oauth.Oauth(authFn), nil
}

// parseOAuthExtensions parses the "k1=v1,k2=v2" extension string into the map
// franz-go's oauth.Auth carries: the SASL/OAUTHBEARER extensions, e.g.
// Confluent Cloud's logicalCluster and identityPoolId. Empty input is no
// extensions.
func parseOAuthExtensions(raw string) (map[string]string, *bridgeError) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	out := make(map[string]string)
	for _, pair := range strings.Split(raw, ",") {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		key, value, found := strings.Cut(pair, "=")
		key = strings.TrimSpace(key)
		if !found || key == "" {
			return nil, bridgeErrorf(errBadOption,
				"sasl.oauthbearer.extensions must be comma-separated key=value pairs, got %q", raw)
		}
		out[key] = strings.TrimSpace(value)
	}
	if len(out) == 0 {
		return nil, nil
	}
	return out, nil
}
