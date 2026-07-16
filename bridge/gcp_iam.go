// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-rs-kafka Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/oauth"
	"golang.org/x/oauth2/google"
)

// GCP IAM authentication for Google Cloud Managed Service for Apache Kafka.
// The wire mechanism is standard OAUTHBEARER, selected here by
// sasl.oauthbearer.method=gcp, but the bearer token the service expects is
// NOT the raw Application Default Credentials access token: it is a
// three-part structure in JWT form with the algorithm GOOG_OAUTH2_TOKEN and
// the raw access token in the signature slot. This mirrors Google's own
// reference clients byte for byte (googleapis/managedkafka:
// kafka-auth-local-server/kafka_gcp_credentials_server.py, corroborated by
// the Java GcpLoginCallbackHandler); there is no official Go helper.
// Credentials never cross the FFI.

// gcpKafkaScope is the OAuth scope both Google reference implementations
// request for the ADC token. Deliberately not configurable.
const gcpKafkaScope = "https://www.googleapis.com/auth/cloud-platform"

// gcpPrincipalEnv is Google's own override for the token's sub claim, honoured
// by both of their reference clients; the bridge honours it too, below the
// explicit gcp.principal key.
const gcpPrincipalEnv = "GOOGLE_MANAGED_KAFKA_AUTH_PRINCIPAL"

// gcpKafkaHeader is the fixed token header, matching the reference
// implementation's json.dumps output byte for byte.
const gcpKafkaHeader = `{"typ": "JWT", "alg": "GOOG_OAUTH2_TOKEN"}`

// gcpKafkaToken synthesises the bearer token the managed Kafka service
// expects: b64url(header).b64url(claims).b64url(raw access token), each
// segment unpadded. Claims follow the reference implementation's field
// order (exp, iss, iat, sub); timestamps are whole unix seconds, which the
// Java reference uses and the service accepts from either reference.
func gcpKafkaToken(accessToken string, expiry, now time.Time, principal string) string {
	encode := base64.RawURLEncoding.EncodeToString
	claims := fmt.Sprintf(
		`{"exp": %d, "iss": "Google", "iat": %d, "sub": %s}`,
		expiry.Unix(), now.Unix(), jsonString(principal))
	return encode([]byte(gcpKafkaHeader)) + "." +
		encode([]byte(claims)) + "." +
		encode([]byte(accessToken))
}

// jsonString renders s as a JSON string literal: principals are plain
// emails, but an encoding boundary is not the place to assume that.
func jsonString(s string) string {
	data, err := json.Marshal(s)
	if err != nil {
		// Unreachable: marshalling a string cannot fail.
		return `""`
	}
	return string(data)
}

// gcpPrincipal resolves the token's sub claim: the explicit gcp.principal
// key wins, then Google's own environment override, then the client_email
// of the resolved credentials, which service account key files include,
// else the same hard error Google's reference clients raise.
func gcpPrincipal(configured string, credentialsJSON []byte) (string, *bridgeError) {
	if configured != "" {
		return configured, nil
	}
	if fromEnv := os.Getenv(gcpPrincipalEnv); fromEnv != "" {
		return fromEnv, nil
	}
	if len(credentialsJSON) > 0 {
		var parsed struct {
			ClientEmail string `json:"client_email"`
		}
		if err := json.Unmarshal(credentialsJSON, &parsed); err == nil && parsed.ClientEmail != "" {
			return parsed.ClientEmail, nil
		}
	}
	return "", bridgeErrorf(errBadOption,
		"unable to determine the GCP principal for the credentials: set gcp.principal "+
			"(or the %s environment variable) to the authenticating identity's email",
		gcpPrincipalEnv)
}

// gcpIAMMechanism assembles the OAUTHBEARER mechanism for Google Cloud
// Managed Service for Apache Kafka. Credentials resolve ENTIRELY Go-side
// through Application Default Credentials (environment key file, gcloud
// user credentials, workload identity, GCE metadata), or an explicit
// service account key file via gcp.credentials.file.
//
// The oauth2 TokenSource captures its construction context for every later
// refresh HTTP call, so it is built on a long-lived background context; only
// the EAGER first fetch is bounded by authResolveTimeout, so a missing or
// unreachable credential source is a clean startup error, not a deferred
// handshake failure or a hung build. Per-handshake fetches reuse the
// source's cache and auto-refresh.
func gcpIAMMechanism(s *franzSecurity) (sasl.Mechanism, *bridgeError) {
	// Google mandates TLS; plaintext is unsupported by the service.
	if s.protocol != "sasl_ssl" {
		return nil, bridgeErrorf(errBadOption,
			"sasl.oauthbearer.method=gcp requires security.protocol=sasl_ssl "+
				"(Google Cloud Managed Service for Apache Kafka is TLS-only), got %q",
			s.protocol)
	}
	// The GCP method takes no static credentials and none of the OIDC
	// client-credentials keys; both would be silently unused.
	if s.username != "" || s.password != "" {
		return nil, bridgeErrorf(errBadOption,
			"sasl.username/sasl.password are not used with sasl.oauthbearer.method=gcp "+
				"(credentials come from Application Default Credentials); remove them")
	}
	if s.oauthTokenEndpoint != "" || s.oauthClientID != "" || s.oauthClientSecret != "" ||
		s.oauthScope != "" || s.oauthExtensions != "" {
		return nil, bridgeErrorf(errBadOption,
			"the sasl.oauthbearer.{token.endpoint.url,client.id,client.secret,scope,extensions} "+
				"options apply to sasl.oauthbearer.method=oidc, not method=gcp "+
				"(the GCP token comes from Application Default Credentials)")
	}

	// Long-lived context by design: see the function comment.
	credentialsCtx := context.Background()
	var credentials *google.Credentials
	var err error
	if s.gcpCredentialsFile != "" {
		data, readErr := os.ReadFile(s.gcpCredentialsFile)
		if readErr != nil {
			return nil, bridgeErrorf(errBadOption, "gcp.credentials.file: %v", readErr)
		}
		credentials, err = google.CredentialsFromJSON(credentialsCtx, data, gcpKafkaScope)
	} else {
		credentials, err = google.FindDefaultCredentials(credentialsCtx, gcpKafkaScope)
	}
	if err != nil {
		return nil, bridgeErrorf(errAdapter,
			"gcp: resolving Application Default Credentials (env key file, gcloud user "+
				"credentials, workload identity, GCE metadata): %v", err)
	}

	principal, berr := gcpPrincipal(s.gcpPrincipal, credentials.JSON)
	if berr != nil {
		return nil, berr
	}

	// Eager fail-fast fetch, bounded by select: TokenSource.Token takes no
	// context, and a token endpoint that never responds must fail the build,
	// not hang it.
	type fetchResult struct {
		err error
	}
	done := make(chan fetchResult, 1)
	go func() {
		_, tokenErr := credentials.TokenSource.Token()
		done <- fetchResult{err: tokenErr}
	}()
	select {
	case result := <-done:
		if result.err != nil {
			return nil, bridgeErrorf(errAdapter,
				"gcp: fetching initial token from Application Default Credentials: %v", result.err)
		}
	case <-time.After(authResolveTimeout):
		return nil, bridgeErrorf(errAdapter,
			"gcp: fetching initial token from Application Default Credentials timed out after %s",
			authResolveTimeout)
	}

	authFn := func(context.Context) (oauth.Auth, error) {
		token, tokenErr := credentials.TokenSource.Token()
		if tokenErr != nil {
			return oauth.Auth{}, tokenErr
		}
		return oauth.Auth{
			Token: gcpKafkaToken(token.AccessToken, token.Expiry, time.Now(), principal),
		}, nil
	}
	return oauth.Oauth(authFn), nil
}
