// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-rs-kafka Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// testPEMs generates a self-signed CA and a client keypair signed by it,
// returning PEM strings (caCert, clientCert, clientKey).
func testPEMs(t *testing.T) (string, string, string) {
	t.Helper()

	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate CA key: %v", err)
	}
	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "llingr-test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create CA cert: %v", err)
	}

	clientKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate client key: %v", err)
	}
	clientTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "llingr-test-client"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatalf("parse CA cert: %v", err)
	}
	clientDER, err := x509.CreateCertificate(rand.Reader, clientTemplate, caCert, &clientKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create client cert: %v", err)
	}
	clientKeyDER, err := x509.MarshalECPrivateKey(clientKey)
	if err != nil {
		t.Fatalf("marshal client key: %v", err)
	}

	encode := func(blockType string, der []byte) string {
		return string(pem.EncodeToMemory(&pem.Block{Type: blockType, Bytes: der}))
	}
	return encode("CERTIFICATE", caDER),
		encode("CERTIFICATE", clientDER),
		encode("EC PRIVATE KEY", clientKeyDER)
}

func writeTempFile(t *testing.T, name, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
	return path
}

// collectSecurity feeds pairs through the same collect path franzKgoOpts uses.
func collectSecurity(t *testing.T, pairs map[string]string) *franzSecurity {
	t.Helper()
	security := &franzSecurity{}
	for key, value := range pairs {
		if !security.collect(key, value) {
			t.Fatalf("key %q was not recognised as a security key", key)
		}
	}
	return security
}

func TestFranzSecurityHappyPaths(t *testing.T) {
	caPEM, certPEM, keyPEM := testPEMs(t)
	caFile := writeTempFile(t, "ca.pem", caPEM)
	certFile := writeTempFile(t, "client.pem", certPEM)
	keyFile := writeTempFile(t, "client.key", keyPEM)

	tests := []struct {
		name     string
		pairs    map[string]string
		wantOpts int
	}{
		{
			name: "sasl_ssl scram256 with ca file",
			pairs: map[string]string{
				"security.protocol": "SASL_SSL", // case-insensitive
				"sasl.mechanism":    "SCRAM-SHA-256",
				"sasl.username":     "user",
				"sasl.password":     "pass",
				"ssl.ca.location":   caFile,
			},
			wantOpts: 2, // DialTLSConfig + SASL
		},
		{
			name: "sasl_plaintext plain",
			pairs: map[string]string{
				"security.protocol": "sasl_plaintext",
				"sasl.mechanisms":   "PLAIN", // plural alias
				"sasl.username":     "user",
				"sasl.password":     "pass",
			},
			wantOpts: 1,
		},
		{
			name: "ssl with mtls file pair",
			pairs: map[string]string{
				"security.protocol":        "ssl",
				"ssl.ca.location":          caFile,
				"ssl.certificate.location": certFile,
				"ssl.key.location":         keyFile,
			},
			wantOpts: 1,
		},
		{
			name: "ssl with inline pems and system roots",
			pairs: map[string]string{
				"security.protocol":   "ssl",
				"ssl.certificate.pem": certPEM,
				"ssl.key.pem":         keyPEM,
			},
			wantOpts: 1,
		},
		{
			name:     "explicit plaintext",
			pairs:    map[string]string{"security.protocol": "plaintext"},
			wantOpts: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts, berr := collectSecurity(t, tt.pairs).build()
			if berr != nil {
				t.Fatalf("unexpected error: %v", berr)
			}
			if len(opts) != tt.wantOpts {
				t.Fatalf("got %d opts, want %d", len(opts), tt.wantOpts)
			}
		})
	}
}

func TestFranzSecurityTLSConfigAssembly(t *testing.T) {
	caPEM, certPEM, keyPEM := testPEMs(t)
	caFile := writeTempFile(t, "ca.pem", caPEM)

	t.Run("roots and client cert set", func(t *testing.T) {
		security := collectSecurity(t, map[string]string{
			"security.protocol":   "ssl",
			"ssl.ca.location":     caFile,
			"ssl.certificate.pem": certPEM,
			"ssl.key.pem":         keyPEM,
		})
		tlsConfig, berr := security.tlsConfig()
		if berr != nil {
			t.Fatalf("unexpected error: %v", berr)
		}
		if tlsConfig.RootCAs == nil {
			t.Error("RootCAs not set from ssl.ca.location")
		}
		if len(tlsConfig.Certificates) != 1 {
			t.Errorf("got %d client certificates, want 1", len(tlsConfig.Certificates))
		}
		if tlsConfig.InsecureSkipVerify {
			t.Error("InsecureSkipVerify must be false by default")
		}
	})

	t.Run("verification disabled", func(t *testing.T) {
		security := collectSecurity(t, map[string]string{
			"security.protocol":                   "ssl",
			"enable.ssl.certificate.verification": "false",
		})
		tlsConfig, berr := security.tlsConfig()
		if berr != nil {
			t.Fatalf("unexpected error: %v", berr)
		}
		if !tlsConfig.InsecureSkipVerify {
			t.Error("InsecureSkipVerify must be true when verification is disabled")
		}
		if tlsConfig.VerifyConnection != nil {
			t.Error("no custom verifier expected when verification is fully disabled")
		}
	})

	t.Run("hostname verification off keeps chain verification", func(t *testing.T) {
		security := collectSecurity(t, map[string]string{
			"security.protocol":                     "ssl",
			"ssl.ca.location":                       caFile,
			"ssl.endpoint.identification.algorithm": "none",
		})
		tlsConfig, berr := security.tlsConfig()
		if berr != nil {
			t.Fatalf("unexpected error: %v", berr)
		}
		if !tlsConfig.InsecureSkipVerify || tlsConfig.VerifyConnection == nil {
			t.Fatal("expected InsecureSkipVerify + custom VerifyConnection for algorithm=none")
		}
	})
}

func TestFranzSecurityValidationErrors(t *testing.T) {
	caPEM, certPEM, keyPEM := testPEMs(t)
	_ = keyPEM

	tests := []struct {
		name    string
		pairs   map[string]string
		wantSub string
	}{
		{
			name:    "missing protocol",
			pairs:   map[string]string{"sasl.username": "user"},
			wantSub: "security.protocol is not set",
		},
		{
			name: "unknown protocol",
			pairs: map[string]string{
				"security.protocol": "quantum",
			},
			wantSub: "unknown security.protocol",
		},
		{
			name: "plaintext with sasl keys",
			pairs: map[string]string{
				"security.protocol": "plaintext",
				"sasl.username":     "user",
			},
			wantSub: "conflicts",
		},
		{
			name: "ssl with sasl keys",
			pairs: map[string]string{
				"security.protocol": "ssl",
				"sasl.mechanism":    "PLAIN",
			},
			wantSub: "sasl.* options require",
		},
		{
			name: "sasl_plaintext with ssl keys",
			pairs: map[string]string{
				"security.protocol": "sasl_plaintext",
				"sasl.mechanism":    "PLAIN",
				"sasl.username":     "u",
				"sasl.password":     "p",
				"ssl.ca.pem":        caPEM,
			},
			wantSub: "ssl.* options require",
		},
		{
			name: "missing mechanism",
			pairs: map[string]string{
				"security.protocol": "sasl_plaintext",
				"sasl.username":     "u",
				"sasl.password":     "p",
			},
			wantSub: "sasl.mechanism is required",
		},
		{
			name: "missing username",
			pairs: map[string]string{
				"security.protocol": "sasl_plaintext",
				"sasl.mechanism":    "PLAIN",
				"sasl.password":     "p",
			},
			wantSub: "sasl.username is required",
		},
		{
			name: "missing password",
			pairs: map[string]string{
				"security.protocol": "sasl_plaintext",
				"sasl.mechanism":    "SCRAM-SHA-512",
				"sasl.username":     "u",
			},
			wantSub: "sasl.password is required",
		},
		{
			name: "gssapi rejected honestly",
			pairs: map[string]string{
				"security.protocol": "sasl_plaintext",
				"sasl.mechanism":    "GSSAPI",
				"sasl.username":     "u",
				"sasl.password":     "p",
			},
			wantSub: "does not wire franz-go's Kerberos mechanism",
		},
		{
			name: "oauthbearer rejects static credentials",
			pairs: map[string]string{
				"security.protocol": "sasl_ssl",
				"sasl.mechanism":    "OAUTHBEARER",
				"sasl.username":     "u",
				"sasl.password":     "p",
			},
			wantSub: "sasl.username/sasl.password are not used with OAUTHBEARER",
		},
		{
			name: "unsupported mechanism lists supported",
			pairs: map[string]string{
				"security.protocol": "sasl_plaintext",
				"sasl.mechanism":    "SCRAM-SHA-1",
				"sasl.username":     "u",
				"sasl.password":     "p",
			},
			wantSub: "supported: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512",
		},
		{
			name: "encrypted key rejected",
			pairs: map[string]string{
				"security.protocol": "ssl",
				"ssl.key.password":  "secret",
			},
			wantSub: "ssl.key.password is not supported",
		},
		{
			name: "ca location and pem exclusive",
			pairs: map[string]string{
				"security.protocol": "ssl",
				"ssl.ca.location":   "/nonexistent",
				"ssl.ca.pem":        caPEM,
			},
			wantSub: "mutually exclusive",
		},
		{
			name: "cert without key",
			pairs: map[string]string{
				"security.protocol":   "ssl",
				"ssl.certificate.pem": certPEM,
			},
			wantSub: "must both be set",
		},
		{
			name: "unreadable ca file",
			pairs: map[string]string{
				"security.protocol": "ssl",
				"ssl.ca.location":   "/nonexistent/ca.pem",
			},
			wantSub: "ssl.ca.location",
		},
		{
			name: "bad verification flag",
			pairs: map[string]string{
				"security.protocol":                   "ssl",
				"enable.ssl.certificate.verification": "maybe",
			},
			wantSub: "must be \"true\" or \"false\"",
		},
		{
			name: "bad endpoint identification",
			pairs: map[string]string{
				"security.protocol":                     "ssl",
				"ssl.endpoint.identification.algorithm": "sha256",
			},
			wantSub: "must be \"https\" or \"none\"",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, berr := collectSecurity(t, tt.pairs).build()
			if berr == nil {
				t.Fatal("expected an error")
			}
			if !strings.Contains(berr.msg, tt.wantSub) {
				t.Fatalf("error %q does not contain %q", berr.msg, tt.wantSub)
			}
		})
	}
}

// TestFranzKgoOptsRoutesSecurityKeys proves security and independent keys
// combine through the public entry point, and that the unknown-key error now
// lists the security keys as supported.
func TestFranzKgoOptsRoutesSecurityKeys(t *testing.T) {
	opts, berr := franzKgoOpts(map[string]string{
		"session.timeout.ms": "9000",
		"security.protocol":  "sasl_plaintext",
		"sasl.mechanism":     "PLAIN",
		"sasl.username":      "u",
		"sasl.password":      "p",
	})
	if berr != nil {
		t.Fatalf("unexpected error: %v", berr)
	}
	if len(opts) != 2 { // SessionTimeout + SASL
		t.Fatalf("got %d opts, want 2", len(opts))
	}

	_, berr = franzKgoOpts(map[string]string{"no.such.key": "x"})
	if berr == nil {
		t.Fatal("expected unknown-key error")
	}
	if !strings.Contains(berr.msg, "security.protocol") || !strings.Contains(berr.msg, "sasl.username") {
		t.Fatalf("supported-keys listing must include security keys, got: %s", berr.msg)
	}
}
