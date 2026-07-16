// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-rs-kafka Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

package main

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/twmb/franz-go/pkg/sasl"
	saslaws "github.com/twmb/franz-go/pkg/sasl/aws"
)

// authResolveTimeout bounds the eager credential/token resolution at init:
// an STS or IMDS endpoint that never responds must fail the build, not hang
// indefinitely. The per-handshake resolutions at runtime use kgo's own context.
const authResolveTimeout = 20 * time.Second

// awsIAMMechanism assembles the AWS_MSK_IAM SASL mechanism. Credentials are
// resolved by the aws-sdk-go-v2's default provider chain (env, shared
// config/profile, STS assume-role, web identity/IRSA, IMDS). The curated
// keys steer the chain:
//
//   - aws.region: the SDK config region (STS endpoint, provider region).
//     NOTE this is NOT the SigV4 signing region, which franz-go derives from
//     the broker hostname (falling back to AWS_REGION/AWS_DEFAULT_REGION);
//     against real MSK hostnames the two agree.
//   - aws.profile: shared-config profile name.
//   - aws.role.arn (+ optional aws.role.session.name): STS assume-role
//     layered on top of the base chain.
//
// A single credential Retrieve runs eagerly, so a missing/invalid
// credential source is a clean startup error rather than a deferred
// failure on the first broker handshake.
//
// The SDK's CredentialsCache then serves and auto-refreshes for the life
// of the consumer, so the per-handshake authFn (runtime re-auth, MSK
// session re-authentication) reuses the warm cache under kgo's context.
func awsIAMMechanism(s *franzSecurity) (sasl.Mechanism, *bridgeError) {
	// AWS_MSK_IAM is TLS-only (SigV4 over an unencrypted transport would
	// leak the signed request); MSK also enforces this.
	if s.protocol != "sasl_ssl" {
		return nil, bridgeErrorf(errBadOption,
			"sasl.mechanism=AWS_MSK_IAM requires security.protocol=sasl_ssl (MSK IAM is TLS-only), got %q",
			s.protocol)
	}
	// AWS_MSK_IAM takes no static credentials; username/password would be
	// silently unused; this bridge rejects an otherwise quiet misconfiguration.
	if s.username != "" || s.password != "" {
		return nil, bridgeErrorf(errBadOption,
			"sasl.username/sasl.password are not used with AWS_MSK_IAM "+
				"(credentials come from the AWS provider chain); remove them")
	}
	// A session name only makes sense with a role to assume.
	if s.awsRoleSessionName != "" && s.awsRoleARN == "" {
		return nil, bridgeErrorf(errBadOption,
			"aws.role.session.name requires aws.role.arn")
	}

	ctx, cancel := context.WithTimeout(context.Background(), authResolveTimeout)
	defer cancel()

	loadOpts := []func(*config.LoadOptions) error{}
	if s.awsRegion != "" {
		loadOpts = append(loadOpts, config.WithRegion(s.awsRegion))
	}
	if s.awsProfile != "" {
		loadOpts = append(loadOpts, config.WithSharedConfigProfile(s.awsProfile))
	}
	cfg, err := config.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return nil, bridgeErrorf(errAdapter, "AWS_MSK_IAM: loading AWS config: %v", err)
	}

	// Optional STS assume-role layered on the base chain. The cache wrapper
	// makes both the eager Retrieve below and the per-handshake authFn reuse
	// one auto-refreshing credential source.
	if s.awsRoleARN != "" {
		provider := stscreds.NewAssumeRoleProvider(sts.NewFromConfig(cfg), s.awsRoleARN,
			func(o *stscreds.AssumeRoleOptions) {
				if s.awsRoleSessionName != "" {
					o.RoleSessionName = s.awsRoleSessionName
				}
			})
		cfg.Credentials = aws.NewCredentialsCache(provider)
	}

	// Eager fail-fast: resolve once so a missing credential source is a clean
	// startup error. Warms the cache for the handshake authFn.
	if _, err := cfg.Credentials.Retrieve(ctx); err != nil {
		return nil, bridgeErrorf(errAdapter,
			"AWS_MSK_IAM: resolving credentials from the AWS provider chain "+
				"(env, shared config/profile, STS, web identity, IMDS): %v", err)
	}

	authFn := func(handshakeCtx context.Context) (saslaws.Auth, error) {
		creds, err := cfg.Credentials.Retrieve(handshakeCtx)
		if err != nil {
			return saslaws.Auth{}, err
		}
		return saslaws.Auth{
			AccessKey:    creds.AccessKeyID,
			SecretKey:    creds.SecretAccessKey,
			SessionToken: creds.SessionToken,
		}, nil
	}
	return saslaws.ManagedStreamingIAM(authFn), nil
}
