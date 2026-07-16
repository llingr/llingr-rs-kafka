// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-rs-kafka Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

package main

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"os"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// franzSecurity collects the librdkafka-style security keys. Unlike the
// independent options in franzOptionKeys, security config is cross-key:
// security.protocol decides whether TLS and SASL apply, the mechanism decides
// how the credentials are used, and certificates arrive in location/PEM
// pairs. Keys are therefore collected first, then validated and assembled as
// one unit, so every inconsistency is a clear startup error and nothing is
// silently ignored.
//
// Capability boundaries are the README security matrix. The cloud mechanisms
// live in aws_iam.go, oauth_oidc.go, and gcp_iam.go; encrypted client keys
// and GSSAPI/Kerberos are rejected with the errors below.
type franzSecurity struct {
	protocol      string // security.protocol (lowercased)
	caLocation    string // ssl.ca.location
	caPEM         string // ssl.ca.pem
	certLocation  string // ssl.certificate.location
	keyLocation   string // ssl.key.location
	certPEM       string // ssl.certificate.pem
	keyPEM        string // ssl.key.pem
	keyPassword   string // ssl.key.password (rejected: unsupported here)
	verifyCerts   string // enable.ssl.certificate.verification ("", "true", "false")
	endpointIdent string // ssl.endpoint.identification.algorithm ("", "https", "none")
	mechanism     string // sasl.mechanism / sasl.mechanisms
	username      string // sasl.username
	password      string // sasl.password
	// AWS_MSK_IAM: credentials resolve Go-side via the AWS provider chain in
	// aws_iam.go; these keys only steer it.
	awsRegion          string // aws.region
	awsProfile         string // aws.profile
	awsRoleARN         string // aws.role.arn
	awsRoleSessionName string // aws.role.session.name
	// OAUTHBEARER (OIDC client-credentials): the token endpoint and client
	// credentials the bridge fetches tokens with (see oauth_oidc.go).
	oauthTokenEndpoint string // sasl.oauthbearer.token.endpoint.url
	oauthClientID      string // sasl.oauthbearer.client.id
	oauthClientSecret  string // sasl.oauthbearer.client.secret
	oauthScope         string // sasl.oauthbearer.scope
	oauthExtensions    string // sasl.oauthbearer.extensions ("k1=v1,k2=v2")
	oauthMethod        string // sasl.oauthbearer.method ("oidc" or "gcp")
	// GCP IAM (OAUTHBEARER method=gcp): Application Default Credentials
	// steering (see gcp_iam.go).
	gcpPrincipal       string // gcp.principal (the token's sub claim)
	gcpCredentialsFile string // gcp.credentials.file (explicit SA key JSON)
	sawKey             bool   // any security key was provided
}

// franzSecurityCollectors recognises the security keys and stores their values.
// "sasl.mechanism" and "sasl.mechanisms" are aliases, as in librdkafka.
var franzSecurityCollectors = map[string]func(*franzSecurity, string){
	"security.protocol":                     func(s *franzSecurity, v string) { s.protocol = strings.ToLower(strings.TrimSpace(v)) },
	"ssl.ca.location":                       func(s *franzSecurity, v string) { s.caLocation = v },
	"ssl.ca.pem":                            func(s *franzSecurity, v string) { s.caPEM = v },
	"ssl.certificate.location":              func(s *franzSecurity, v string) { s.certLocation = v },
	"ssl.key.location":                      func(s *franzSecurity, v string) { s.keyLocation = v },
	"ssl.certificate.pem":                   func(s *franzSecurity, v string) { s.certPEM = v },
	"ssl.key.pem":                           func(s *franzSecurity, v string) { s.keyPEM = v },
	"ssl.key.password":                      func(s *franzSecurity, v string) { s.keyPassword = v },
	"enable.ssl.certificate.verification":   func(s *franzSecurity, v string) { s.verifyCerts = strings.ToLower(strings.TrimSpace(v)) },
	"ssl.endpoint.identification.algorithm": func(s *franzSecurity, v string) { s.endpointIdent = strings.ToLower(strings.TrimSpace(v)) },
	"sasl.mechanism":                        func(s *franzSecurity, v string) { s.mechanism = v },
	"sasl.mechanisms":                       func(s *franzSecurity, v string) { s.mechanism = v },
	"sasl.username":                         func(s *franzSecurity, v string) { s.username = v },
	"sasl.password":                         func(s *franzSecurity, v string) { s.password = v },
	"aws.region":                            func(s *franzSecurity, v string) { s.awsRegion = strings.TrimSpace(v) },
	"aws.profile":                           func(s *franzSecurity, v string) { s.awsProfile = strings.TrimSpace(v) },
	"aws.role.arn":                          func(s *franzSecurity, v string) { s.awsRoleARN = strings.TrimSpace(v) },
	"aws.role.session.name":                 func(s *franzSecurity, v string) { s.awsRoleSessionName = strings.TrimSpace(v) },
	"sasl.oauthbearer.token.endpoint.url":   func(s *franzSecurity, v string) { s.oauthTokenEndpoint = strings.TrimSpace(v) },
	"sasl.oauthbearer.client.id":            func(s *franzSecurity, v string) { s.oauthClientID = v },
	"sasl.oauthbearer.client.secret":        func(s *franzSecurity, v string) { s.oauthClientSecret = v },
	"sasl.oauthbearer.scope":                func(s *franzSecurity, v string) { s.oauthScope = strings.TrimSpace(v) },
	"sasl.oauthbearer.extensions":           func(s *franzSecurity, v string) { s.oauthExtensions = v },
	"sasl.oauthbearer.method":               func(s *franzSecurity, v string) { s.oauthMethod = strings.ToLower(strings.TrimSpace(v)) },
	"gcp.principal":                         func(s *franzSecurity, v string) { s.gcpPrincipal = strings.TrimSpace(v) },
	"gcp.credentials.file":                  func(s *franzSecurity, v string) { s.gcpCredentialsFile = strings.TrimSpace(v) },
}

// collect stores a recognised security key. Returns false when the key is not
// a security key; the caller then tries the independent option table.
func (s *franzSecurity) collect(key, value string) bool {
	collector, ok := franzSecurityCollectors[key]
	if !ok {
		return false
	}
	collector(s, value)
	s.sawKey = true
	return true
}

func (s *franzSecurity) hasSSLKeys() bool {
	return s.caLocation != "" || s.caPEM != "" ||
		s.certLocation != "" || s.keyLocation != "" ||
		s.certPEM != "" || s.keyPEM != "" ||
		s.keyPassword != "" || s.verifyCerts != "" || s.endpointIdent != ""
}

func (s *franzSecurity) hasSASLKeys() bool {
	return s.mechanism != "" || s.username != "" || s.password != "" ||
		s.hasAWSKeys() || s.hasOAuthKeys() || s.hasGCPKeys()
}

// hasGCPKeys reports whether any gcp.* steering key was provided. Like the
// aws.* family, these are SASL-family keys configuring the OAUTHBEARER gcp
// method, gating the same protocol conflict checks.
func (s *franzSecurity) hasGCPKeys() bool {
	return s.gcpPrincipal != "" || s.gcpCredentialsFile != ""
}

// hasAWSKeys reports whether any aws.* steering key was provided. These are
// SASL-family keys configuring AWS_MSK_IAM, so they gate the same protocol
// conflict checks as sasl.username/password.
func (s *franzSecurity) hasAWSKeys() bool {
	return s.awsRegion != "" || s.awsProfile != "" ||
		s.awsRoleARN != "" || s.awsRoleSessionName != ""
}

// hasOAuthKeys reports whether any sasl.oauthbearer.* key was provided.
func (s *franzSecurity) hasOAuthKeys() bool {
	return s.oauthTokenEndpoint != "" || s.oauthClientID != "" ||
		s.oauthClientSecret != "" || s.oauthScope != "" ||
		s.oauthExtensions != "" || s.oauthMethod != ""
}

// build validates the collected keys and assembles the franz-go security
// options: kgo.DialTLSConfig for the ssl protocols, kgo.SASL for the sasl
// protocols.
func (s *franzSecurity) build() ([]kgo.Opt, *bridgeError) {
	if !s.sawKey {
		return nil, nil
	}

	switch s.protocol {
	case "":
		return nil, bridgeErrorf(errBadOption,
			"security options were provided but security.protocol is not set "+
				"(expected one of: plaintext, ssl, sasl_plaintext, sasl_ssl)")
	case "plaintext":
		if s.hasSSLKeys() || s.hasSASLKeys() {
			return nil, bridgeErrorf(errBadOption,
				"security.protocol=plaintext conflicts with the ssl.*/sasl.* options provided")
		}
		return nil, nil
	case "ssl":
		if s.hasSASLKeys() {
			return nil, bridgeErrorf(errBadOption,
				"sasl.* options require security.protocol=sasl_plaintext or sasl_ssl (got %q)", s.protocol)
		}
	case "sasl_plaintext":
		if s.hasSSLKeys() {
			return nil, bridgeErrorf(errBadOption,
				"ssl.* options require security.protocol=ssl or sasl_ssl (got %q)", s.protocol)
		}
	case "sasl_ssl":
		// TLS + SASL below.
	default:
		return nil, bridgeErrorf(errBadOption,
			"unknown security.protocol %q (expected one of: plaintext, ssl, sasl_plaintext, sasl_ssl)", s.protocol)
	}

	var opts []kgo.Opt

	if s.protocol == "ssl" || s.protocol == "sasl_ssl" {
		tlsConfig, berr := s.tlsConfig()
		if berr != nil {
			return nil, berr
		}
		opts = append(opts, kgo.DialTLSConfig(tlsConfig))
	}

	if s.protocol == "sasl_plaintext" || s.protocol == "sasl_ssl" {
		mechanism, berr := s.saslMechanism()
		if berr != nil {
			return nil, berr
		}
		opts = append(opts, kgo.SASL(mechanism))
	}

	return opts, nil
}

// tlsConfig assembles the *tls.Config from the ssl.* keys, mirroring
// librdkafka's semantics:
//   - ssl.ca.location / ssl.ca.pem  -> RootCAs (absent: system roots)
//   - certificate+key pairs         -> client certificate (mTLS)
//   - enable.ssl.certificate.verification=false -> no verification
//   - ssl.endpoint.identification.algorithm=none -> chain verified against the
//     roots, hostname NOT checked (Go bundles hostname checking into standard
//     verification, so this is a custom VerifyConnection)
func (s *franzSecurity) tlsConfig() (*tls.Config, *bridgeError) {
	if s.keyPassword != "" {
		return nil, bridgeErrorf(errBadOption,
			"ssl.key.password is not supported (encrypted client keys need OpenSSL); "+
				"decrypt the key before configuring it")
	}

	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}

	// Roots.
	if s.caLocation != "" && s.caPEM != "" {
		return nil, bridgeErrorf(errBadOption,
			"ssl.ca.location and ssl.ca.pem are mutually exclusive")
	}
	if s.caLocation != "" || s.caPEM != "" {
		pemBytes := []byte(s.caPEM)
		if s.caLocation != "" {
			fileBytes, err := os.ReadFile(s.caLocation)
			if err != nil {
				return nil, bridgeErrorf(errBadOption, "ssl.ca.location: %v", err)
			}
			pemBytes = fileBytes
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pemBytes) {
			return nil, bridgeErrorf(errBadOption,
				"CA source contains no parseable PEM certificates")
		}
		tlsConfig.RootCAs = pool
	}

	// Client certificate (mTLS).
	locationPair := s.certLocation != "" || s.keyLocation != ""
	pemPair := s.certPEM != "" || s.keyPEM != ""
	switch {
	case locationPair && pemPair:
		return nil, bridgeErrorf(errBadOption,
			"ssl.certificate.location/ssl.key.location and ssl.certificate.pem/ssl.key.pem "+
				"are mutually exclusive")
	case locationPair:
		if s.certLocation == "" || s.keyLocation == "" {
			return nil, bridgeErrorf(errBadOption,
				"ssl.certificate.location and ssl.key.location must both be set")
		}
		certificate, err := tls.LoadX509KeyPair(s.certLocation, s.keyLocation)
		if err != nil {
			return nil, bridgeErrorf(errBadOption, "loading client certificate: %v", err)
		}
		tlsConfig.Certificates = []tls.Certificate{certificate}
	case pemPair:
		if s.certPEM == "" || s.keyPEM == "" {
			return nil, bridgeErrorf(errBadOption,
				"ssl.certificate.pem and ssl.key.pem must both be set")
		}
		certificate, err := tls.X509KeyPair([]byte(s.certPEM), []byte(s.keyPEM))
		if err != nil {
			return nil, bridgeErrorf(errBadOption, "parsing client certificate: %v", err)
		}
		tlsConfig.Certificates = []tls.Certificate{certificate}
	}

	// Verification.
	switch s.verifyCerts {
	case "", "true":
		// default: full verification (chain + hostname), unless the endpoint
		// identification algorithm turns hostname checking off below.
	case "false":
		tlsConfig.InsecureSkipVerify = true
		return tlsConfig, nil
	default:
		return nil, bridgeErrorf(errBadOption,
			"enable.ssl.certificate.verification must be \"true\" or \"false\", got %q", s.verifyCerts)
	}

	switch s.endpointIdent {
	case "", "https":
		// default: hostname verification on.
	case "none":
		// librdkafka semantics: still verify the chain against the roots, but
		// do not check the hostname. Go's standard verification bundles both,
		// so replicate with a custom VerifyConnection.
		roots := tlsConfig.RootCAs
		tlsConfig.InsecureSkipVerify = true
		tlsConfig.VerifyConnection = func(cs tls.ConnectionState) error {
			if len(cs.PeerCertificates) == 0 {
				return errors.New("no peer certificate presented")
			}
			verifyOpts := x509.VerifyOptions{
				Roots:         roots, // nil means system roots
				Intermediates: x509.NewCertPool(),
			}
			for _, intermediate := range cs.PeerCertificates[1:] {
				verifyOpts.Intermediates.AddCert(intermediate)
			}
			_, err := cs.PeerCertificates[0].Verify(verifyOpts)
			return err
		}
	default:
		return nil, bridgeErrorf(errBadOption,
			"ssl.endpoint.identification.algorithm must be \"https\" or \"none\", got %q", s.endpointIdent)
	}

	return tlsConfig, nil
}

// saslMechanism assembles the franz-go SASL mechanism from the sasl.* keys.
// PLAIN and SCRAM take a username/password pair; AWS_MSK_IAM and OAUTHBEARER
// take none, their credentials resolving Go-side in aws_iam.go and
// oauth_oidc.go. The per-mechanism key families are cross-checked here so a
// key belonging to a different mechanism than the one selected is a clear
// error, never silently ignored.
func (s *franzSecurity) saslMechanism() (sasl.Mechanism, *bridgeError) {
	mechanism := strings.ToUpper(strings.TrimSpace(s.mechanism))
	if mechanism == "" {
		return nil, bridgeErrorf(errBadOption,
			"sasl.mechanism is required with security.protocol=%s", s.protocol)
	}

	// Orphaned mechanism-family keys: aws.* only apply to AWS_MSK_IAM,
	// sasl.oauthbearer.* only to OAUTHBEARER. Reject a mismatch rather than
	// silently dropping the key.
	if s.hasAWSKeys() && mechanism != "AWS_MSK_IAM" {
		return nil, bridgeErrorf(errBadOption,
			"aws.* options apply only to sasl.mechanism=AWS_MSK_IAM (got %s)", mechanism)
	}
	if s.hasOAuthKeys() && mechanism != "OAUTHBEARER" {
		return nil, bridgeErrorf(errBadOption,
			"sasl.oauthbearer.* options apply only to sasl.mechanism=OAUTHBEARER (got %s)", mechanism)
	}
	if s.hasGCPKeys() && !(mechanism == "OAUTHBEARER" && s.oauthMethod == "gcp") {
		return nil, bridgeErrorf(errBadOption,
			"gcp.* options apply only to sasl.mechanism=OAUTHBEARER with sasl.oauthbearer.method=gcp")
	}

	switch mechanism {
	case "PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512":
		if s.username == "" {
			return nil, bridgeErrorf(errBadOption, "sasl.username is required with sasl.mechanism=%s", mechanism)
		}
		if s.password == "" {
			return nil, bridgeErrorf(errBadOption, "sasl.password is required with sasl.mechanism=%s", mechanism)
		}
		switch mechanism {
		case "PLAIN":
			return plain.Auth{User: s.username, Pass: s.password}.AsMechanism(), nil
		case "SCRAM-SHA-256":
			return scram.Auth{User: s.username, Pass: s.password}.AsSha256Mechanism(), nil
		default:
			return scram.Auth{User: s.username, Pass: s.password}.AsSha512Mechanism(), nil
		}
	case "AWS_MSK_IAM":
		return awsIAMMechanism(s)
	case "OAUTHBEARER":
		// The method selects the token SOURCE; the wire mechanism is the
		// same. "gcp" is Application Default Credentials for Google Cloud
		// Managed Service for Apache Kafka; "" or "oidc" is the OIDC
		// client-credentials fetcher, which rejects any other method.
		if s.oauthMethod == "gcp" {
			return gcpIAMMechanism(s)
		}
		return oauthOIDCMechanism(s)
	case "GSSAPI":
		return nil, bridgeErrorf(errBadOption,
			"GSSAPI/Kerberos is not supported: this bridge does not wire franz-go's Kerberos "+
				"mechanism; it requires a custom engine build")
	default:
		return nil, bridgeErrorf(errBadOption,
			"unsupported sasl.mechanism %q (supported: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, "+
				"AWS_MSK_IAM, OAUTHBEARER)", mechanism)
	}
}
