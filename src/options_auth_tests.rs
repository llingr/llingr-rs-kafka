// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-rs-kafka Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

//! Coverage for the AWS_MSK_IAM and OAUTHBEARER OIDC typed Options setters:
//! every setter emits its documented key, and credentials are redacted in
//! Debug.

use crate::options::Options;
use llingr_nexus::AdapterOptions;

fn entry<'a>(entries: &'a [(String, String)], key: &str) -> Option<&'a str> {
    entries
        .iter()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.as_str())
}

#[test]
fn aws_msk_iam_forces_tls_and_emits_mechanism() {
    let entries = Options::new().sasl_aws_msk_iam().entries();
    assert_eq!(
        entry(&entries, "security.protocol"),
        Some("sasl_ssl"),
        "MSK IAM is TLS-only, so the setter must select sasl_ssl"
    );
    assert_eq!(entry(&entries, "sasl.mechanism"), Some("AWS_MSK_IAM"));
    // No static credential keys are emitted.
    assert_eq!(entry(&entries, "sasl.username"), None);
    assert_eq!(entry(&entries, "sasl.password"), None);
}

#[test]
fn aws_steering_keys_emit_when_set() {
    let entries = Options::new()
        .sasl_aws_msk_iam()
        .aws_region("eu-west-2")
        .aws_profile("prod")
        .aws_assume_role(
            "arn:aws:iam::123456789012:role/consumer",
            Some("svc-orders"),
        )
        .entries();
    assert_eq!(entry(&entries, "aws.region"), Some("eu-west-2"));
    assert_eq!(entry(&entries, "aws.profile"), Some("prod"));
    assert_eq!(
        entry(&entries, "aws.role.arn"),
        Some("arn:aws:iam::123456789012:role/consumer")
    );
    assert_eq!(entry(&entries, "aws.role.session.name"), Some("svc-orders"));
}

#[test]
fn aws_assume_role_without_session_name_emits_only_the_arn() {
    let entries = Options::new()
        .sasl_aws_msk_iam()
        .aws_assume_role("arn:aws:iam::123456789012:role/consumer", None)
        .entries();
    assert_eq!(
        entry(&entries, "aws.role.arn"),
        Some("arn:aws:iam::123456789012:role/consumer")
    );
    assert_eq!(
        entry(&entries, "aws.role.session.name"),
        None,
        "no session name means the SDK generates one"
    );
}

#[test]
fn aws_steering_setters_before_mechanism_are_inert() {
    // aws_region without a preceding sasl_aws_msk_iam has no SASL to attach
    // to, so it emits nothing.
    let entries = Options::new().aws_region("eu-west-2").entries();
    assert_eq!(entry(&entries, "aws.region"), None);
    assert_eq!(entry(&entries, "sasl.mechanism"), None);
}

#[test]
fn oauthbearer_emits_endpoint_and_credentials() {
    let entries = Options::new()
        .sasl_oauthbearer_oidc("https://idp.example/token", "client-id", "client-secret")
        .entries();
    assert_eq!(entry(&entries, "sasl.mechanism"), Some("OAUTHBEARER"));
    assert_eq!(
        entry(&entries, "sasl.oauthbearer.token.endpoint.url"),
        Some("https://idp.example/token")
    );
    assert_eq!(
        entry(&entries, "sasl.oauthbearer.client.id"),
        Some("client-id")
    );
    assert_eq!(
        entry(&entries, "sasl.oauthbearer.client.secret"),
        Some("client-secret")
    );
    // OAUTHBEARER does not force TLS: sasl_plaintext without a tls_* setter.
    assert_eq!(entry(&entries, "security.protocol"), Some("sasl_plaintext"));
}

#[test]
fn oauthbearer_with_tls_scope_and_extensions() {
    let entries = Options::new()
        .tls()
        .sasl_oauthbearer_oidc("https://idp.example/token", "id", "secret")
        .oauthbearer_scope("kafka")
        .oauthbearer_extensions([("logicalCluster", "lkc-1"), ("identityPoolId", "pool-9")])
        .entries();
    assert_eq!(entry(&entries, "security.protocol"), Some("sasl_ssl"));
    assert_eq!(entry(&entries, "sasl.oauthbearer.scope"), Some("kafka"));
    assert_eq!(
        entry(&entries, "sasl.oauthbearer.extensions"),
        Some("logicalCluster=lkc-1,identityPoolId=pool-9")
    );
}

/// The client secret must never appear in Debug; the endpoint, client id,
/// and scope are not secrets and stay visible.
#[test]
fn oauthbearer_debug_redacts_client_secret() {
    let options = Options::new().sasl_oauthbearer_oidc(
        "https://idp.example/token",
        "public-client-id",
        "s3cr3t-value",
    );
    let debug = format!("{options:?}");
    assert!(
        !debug.contains("s3cr3t-value"),
        "client secret leaked: {debug}"
    );
    assert!(debug.contains("<redacted>"), "{debug}");
    assert!(
        debug.contains("public-client-id"),
        "client id is not a secret"
    );
    assert!(debug.contains("idp.example"), "endpoint is not a secret");
}

/// A later mechanism setter replaces an earlier one: the AWS keys from a
/// discarded sasl_aws_msk_iam do not bleed into an OAUTHBEARER
/// configuration.
#[test]
fn last_mechanism_setter_wins() {
    let entries = Options::new()
        .sasl_aws_msk_iam()
        .aws_region("eu-west-2")
        .sasl_oauthbearer_oidc("https://idp/token", "id", "secret")
        .entries();
    assert_eq!(entry(&entries, "sasl.mechanism"), Some("OAUTHBEARER"));
    assert_eq!(
        entry(&entries, "aws.region"),
        None,
        "discarded AWS config must not emit"
    );
}

/// The typed-vs-string conflict guard covers the aws.* key family: an aws.*
/// string pair alongside the typed AWS setter is rejected.
#[test]
fn aws_typed_and_string_conflict_is_rejected() {
    let options = Options::new()
        .sasl_aws_msk_iam()
        .aws_region("eu-west-2")
        .kafka_option("aws.region", "us-east-1");
    let message = options.validate().expect_err("conflict must be rejected");
    assert!(message.contains("aws.region"), "{message}");
}

#[test]
fn oauthbearer_typed_and_string_conflict_is_rejected() {
    let options = Options::new()
        .sasl_oauthbearer_oidc("https://idp/token", "id", "secret")
        .kafka_option("sasl.oauthbearer.client.id", "other");
    let message = options.validate().expect_err("conflict must be rejected");
    assert!(message.contains("sasl.oauthbearer.client.id"), "{message}");
}
