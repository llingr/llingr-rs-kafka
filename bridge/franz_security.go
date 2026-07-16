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
// ever silently ignored.
//
// Capability boundaries (documented in the README security matrix):
//   - PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, TLS and mTLS: supported here.
//   - ssl.key.password (encrypted client keys): unsupported; decrypt the key.
//   - OAUTHBEARER and AWS_MSK_IAM: not supported yet (the auth phase adds
//     both; see PLAN.md section 4.1).
//   - GSSAPI/Kerberos: unsupported; the bridge does not wire franz-go's
//     (pure Go) Kerberos mechanism.
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
	sawKey        bool   // any security key was provided at all
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
}

// collect stores a recognised security key. Returns false when the key is not
// a security key (the caller then tries the independent option table).
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
	return s.mechanism != "" || s.username != "" || s.password != ""
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
//   - enable.ssl.certificate.verification=false -> no verification at all
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
func (s *franzSecurity) saslMechanism() (sasl.Mechanism, *bridgeError) {
	mechanism := strings.ToUpper(strings.TrimSpace(s.mechanism))
	if mechanism == "" {
		return nil, bridgeErrorf(errBadOption,
			"sasl.mechanism is required with security.protocol=%s", s.protocol)
	}
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
	case "SCRAM-SHA-512":
		return scram.Auth{User: s.username, Pass: s.password}.AsSha512Mechanism(), nil
	case "OAUTHBEARER":
		return nil, bridgeErrorf(errBadOption,
			"OAUTHBEARER is not supported yet")
	case "GSSAPI":
		return nil, bridgeErrorf(errBadOption,
			"GSSAPI/Kerberos is not supported: this bridge does not wire franz-go's Kerberos "+
				"mechanism; it requires a custom engine build")
	default:
		return nil, bridgeErrorf(errBadOption,
			"unsupported sasl.mechanism %q (supported: PLAIN, "+
				"SCRAM-SHA-256, SCRAM-SHA-512)", mechanism)
	}
}
