// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-rs-kafka Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

//! Coverage for the GCP IAM typed Options setters, OAUTHBEARER with
//! method=gcp: every setter emits its documented key, TLS is forced, and
//! the typed-vs-string conflict guard covers the gcp.* family.

use crate::options::Options;
use llingr_nexus::AdapterOptions;

fn entry<'a>(entries: &'a [(String, String)], key: &str) -> Option<&'a str> {
    entries
        .iter()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.as_str())
}

#[test]
fn gcp_iam_forces_tls_and_emits_mechanism_and_method() {
    let entries = Options::new().sasl_gcp_iam().entries();
    assert_eq!(
        entry(&entries, "security.protocol"),
        Some("sasl_ssl"),
        "Google's managed Kafka is TLS-only, so the setter must select sasl_ssl"
    );
    assert_eq!(entry(&entries, "sasl.mechanism"), Some("OAUTHBEARER"));
    assert_eq!(entry(&entries, "sasl.oauthbearer.method"), Some("gcp"));
    // No static credentials, no OIDC keys.
    assert_eq!(entry(&entries, "sasl.username"), None);
    assert_eq!(entry(&entries, "sasl.oauthbearer.token.endpoint.url"), None);
}

#[test]
fn gcp_steering_keys_emit_when_set() {
    let entries = Options::new()
        .sasl_gcp_iam()
        .gcp_principal("svc@project.iam.gserviceaccount.com")
        .gcp_credentials_file("/secrets/sa-key.json")
        .entries();
    assert_eq!(
        entry(&entries, "gcp.principal"),
        Some("svc@project.iam.gserviceaccount.com")
    );
    assert_eq!(
        entry(&entries, "gcp.credentials.file"),
        Some("/secrets/sa-key.json")
    );
}

#[test]
fn gcp_steering_setters_before_mechanism_are_inert() {
    // gcp_principal without a preceding sasl_gcp_iam has no SASL to attach
    // to, so it emits nothing.
    let entries = Options::new()
        .gcp_principal("svc@project.iam.gserviceaccount.com")
        .entries();
    assert_eq!(entry(&entries, "gcp.principal"), None);
    assert_eq!(entry(&entries, "sasl.mechanism"), None);
}

/// A later mechanism setter replaces an earlier one: discarded GCP config
/// does not bleed into an OIDC configuration and vice versa.
#[test]
fn last_mechanism_setter_wins_across_oauthbearer_methods() {
    let entries = Options::new()
        .sasl_gcp_iam()
        .gcp_principal("svc@project.iam.gserviceaccount.com")
        .sasl_oauthbearer_oidc("https://idp/token", "id", "secret")
        .entries();
    assert_eq!(entry(&entries, "sasl.mechanism"), Some("OAUTHBEARER"));
    assert_eq!(
        entry(&entries, "sasl.oauthbearer.method"),
        None,
        "the oidc setter does not emit a method key"
    );
    assert_eq!(entry(&entries, "gcp.principal"), None);

    let reversed = Options::new()
        .sasl_oauthbearer_oidc("https://idp/token", "id", "secret")
        .sasl_gcp_iam()
        .entries();
    assert_eq!(
        reversed
            .iter()
            .filter(|(k, _)| k == "sasl.mechanism")
            .count(),
        1
    );
    assert_eq!(
        entry(&reversed, "sasl.oauthbearer.method"),
        Some("gcp"),
        "the gcp method wins after replacement"
    );
    assert_eq!(
        entry(&reversed, "sasl.oauthbearer.token.endpoint.url"),
        None
    );
}

/// The typed-vs-string conflict guard covers the gcp.* family and the method
/// key the setter emits.
#[test]
fn gcp_typed_and_string_conflicts_are_rejected() {
    let options = Options::new()
        .sasl_gcp_iam()
        .gcp_principal("svc@x.iam.gserviceaccount.com")
        .kafka_option("gcp.principal", "other@x.iam.gserviceaccount.com");
    let message = options.validate().expect_err("conflict must be rejected");
    assert!(message.contains("gcp.principal"), "{message}");

    let method_clash = Options::new()
        .sasl_gcp_iam()
        .kafka_option("sasl.oauthbearer.method", "oidc");
    let message = method_clash
        .validate()
        .expect_err("method conflict must be rejected");
    assert!(message.contains("sasl.oauthbearer.method"), "{message}");
}

/// No GCP option is a secret: the principal is an email and the file value
/// is a path, so Debug shows it all. The guard is that no spurious
/// redaction hides operational config.
#[test]
fn gcp_debug_shows_steering_keys() {
    let options = Options::new()
        .sasl_gcp_iam()
        .gcp_principal("svc@project.iam.gserviceaccount.com")
        .gcp_credentials_file("/secrets/sa-key.json");
    let debug = format!("{options:?}");
    assert!(
        debug.contains("svc@project.iam.gserviceaccount.com"),
        "{debug}"
    );
    assert!(debug.contains("/secrets/sa-key.json"), "{debug}");
}
