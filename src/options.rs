// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-rs-kafka Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

//! Typed Kafka client options ([`Options`]).
//!
//! The engine's pure-Go broker layer, franz-go, accepts a curated set of
//! Kafka client options, each translated by the bridge into a typed franz-go
//! option, plus the full TLS/SASL security options: PLAIN, SCRAM-SHA-256/512,
//! TLS and mTLS. This builder makes that set discoverable at compile time.
//! An option outside the curated set fails engine initialisation with the
//! full supported-key list, never a silent no-op.
//!
//! ```ignore
//! use llingr_kafka::{AutoOffsetReset, Builder, Options};
//! use std::time::Duration;
//!
//! let engine = Builder::new("orders", MyProcessor, MyDeadLetters)
//!     .brokers("broker:9093")
//!     .consumer_group("order-processor")
//!     .options(
//!         Options::new()
//!             .auto_offset_reset(AutoOffsetReset::Earliest)
//!             .session_timeout(Duration::from_secs(30))
//!             .tls_ca_location("/etc/ssl/certs/cluster-ca.pem")
//!             .sasl_scram_sha256("svc-orders", &password),
//!     )
//!     .build()?;
//! ```

use std::fmt;
use std::time::Duration;

use llingr_nexus::{duration_ms_ceil, Adapter, AdapterOptions, AutoOffsetReset};

/// SASL mechanism selected on an [`Options`] builder.
#[derive(Clone, Copy, PartialEq, Eq)]
enum SaslMechanism {
    Plain,
    ScramSha256,
    ScramSha512,
}

impl SaslMechanism {
    fn as_str(&self) -> &'static str {
        match self {
            SaslMechanism::Plain => "PLAIN",
            SaslMechanism::ScramSha256 => "SCRAM-SHA-256",
            SaslMechanism::ScramSha512 => "SCRAM-SHA-512",
        }
    }
}

/// The SASL configuration selected on an [`Options`] builder. PLAIN and SCRAM
/// hold a username/password; AWS_MSK_IAM, OAUTHBEARER, and GCP IAM hold no
/// static credentials, resolving through the AWS provider chain, the OIDC
/// token endpoint, and Application Default Credentials respectively.
#[derive(Clone)]
enum Sasl {
    UserPass {
        mechanism: SaslMechanism,
        username: String,
        password: String,
    },
    AwsMskIam {
        region: Option<String>,
        profile: Option<String>,
        role_arn: Option<String>,
        role_session_name: Option<String>,
    },
    OauthbearerOidc {
        token_endpoint: String,
        client_id: String,
        client_secret: String,
        scope: Option<String>,
        extensions: Vec<(String, String)>,
    },
    GcpIam {
        principal: Option<String>,
        credentials_file: Option<String>,
    },
}

/// Consumer-group partition assignment strategy
/// (`partition.assignment.strategy`).
///
/// [`CooperativeSticky`](Self::CooperativeSticky) is the default and the
/// recommended choice for new deployments: rebalances revoke only the
/// partitions that actually move. The other three are eager protocols, where
/// every rebalance revokes the whole assignment, provided for joining
/// consumer groups whose members do not all speak cooperative-sticky yet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BalanceStrategy {
    /// Incremental (cooperative) rebalancing; only affected partitions are
    /// revoked. The default when no strategy is set.
    CooperativeSticky,
    /// Eager protocol that minimises partition movement between assignments.
    Sticky,
    /// Eager protocol; simple and widely supported.
    RoundRobin,
    /// Eager protocol; Kafka's classic default, maximum compatibility.
    Range,
}

impl BalanceStrategy {
    fn wire_name(self) -> &'static str {
        match self {
            BalanceStrategy::CooperativeSticky => "cooperative-sticky",
            BalanceStrategy::Sticky => "sticky",
            BalanceStrategy::RoundRobin => "roundrobin",
            BalanceStrategy::Range => "range",
        }
    }
}

/// Verbosity of the Kafka client's internal diagnostics
/// (`llingr.client.log.level`), which the engine bridges into the `log`
/// facade alongside its own lines.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientLogLevel {
    /// No client diagnostics.
    None,
    /// Client errors only.
    Error,
    /// Errors and warnings.
    Warn,
    /// Group lifecycle: joins, leaves and their failures, rebalance rounds.
    /// The default when no level is set.
    Info,
    /// Per-request client internals. Very chatty; for diagnostic runs.
    Debug,
}

impl ClientLogLevel {
    fn wire_name(self) -> &'static str {
        match self {
            ClientLogLevel::None => "none",
            ClientLogLevel::Error => "error",
            ClientLogLevel::Warn => "warn",
            ClientLogLevel::Info => "info",
            ClientLogLevel::Debug => "debug",
        }
    }
}

/// Typed option builder for the Kafka client.
///
/// The security setters compute `security.protocol` automatically: any `tls_*`
/// setter or [`tls`](Self::tls) enables TLS, any `sasl_*` setter enables
/// SASL, and the emitted protocol is the resulting combination: `ssl`,
/// `sasl_plaintext`, or `sasl_ssl`.
///
/// `Duration`-valued setters emit whole milliseconds, the granularity of
/// every Kafka client timing option; a sub-millisecond remainder rounds up,
/// so a non-zero `Duration` never silently becomes zero.
#[derive(Clone, Default)]
pub struct Options {
    entries: Vec<(String, String)>,
    tls: bool,
    ssl_entries: Vec<(String, String)>,
    sasl: Option<Sasl>,
}

impl Options {
    /// An empty builder: no options, no security.
    pub fn new() -> Self {
        Self::default()
    }

    fn push(mut self, key: &str, value: String) -> Self {
        self.entries.push((key.to_string(), value));
        self
    }

    fn push_ssl(mut self, key: &str, value: &str) -> Self {
        self.tls = true;
        self.ssl_entries.push((key.to_string(), value.to_string()));
        self
    }

    // -- client options -----------------------------------------------------

    /// Where consumption starts when no committed offset exists.
    pub fn auto_offset_reset(self, reset: AutoOffsetReset) -> Self {
        self.push("auto.offset.reset", reset.as_str().to_string())
    }

    /// Client identifier presented to the broker.
    pub fn client_id(self, id: &str) -> Self {
        self.push("client.id", id.to_string())
    }

    /// Static group membership identifier (`group.instance.id`, translated to
    /// `kgo.InstanceID`): pod restarts within the session timeout rejoin
    /// without a rebalance.
    pub fn group_instance_id(self, id: &str) -> Self {
        self.push("group.instance.id", id.to_string())
    }

    /// Consumer group session timeout.
    pub fn session_timeout(self, d: Duration) -> Self {
        self.push("session.timeout.ms", duration_ms_ceil(d).to_string())
    }

    /// Consumer group heartbeat interval.
    pub fn heartbeat_interval(self, d: Duration) -> Self {
        self.push("heartbeat.interval.ms", duration_ms_ceil(d).to_string())
    }

    /// How long a rebalance may take before the member is evicted. Must
    /// exceed the engine's drain timeout; the engine drains in-flight work
    /// during a rebalance before acknowledging the revoke, and a rebalance
    /// timeout at or below the drain budget is rejected at initialisation.
    pub fn rebalance_timeout(self, d: Duration) -> Self {
        self.push("rebalance.timeout.ms", duration_ms_ceil(d).to_string())
    }

    /// Verbosity of the Kafka client's internal diagnostics, bridged into the
    /// `log` facade (`llingr.client.log.level`).
    ///
    /// Without this setter the client logs at [`Info`](ClientLogLevel::Info):
    /// group joins, leaves and their failures, rebalance rounds.
    pub fn client_log_level(self, level: ClientLogLevel) -> Self {
        self.push("llingr.client.log.level", level.wire_name().to_string())
    }

    /// Consumer-group partition assignment strategies, in preference order
    /// (`partition.assignment.strategy`, translated to `kgo.Balancers`).
    ///
    /// Without this setter the engine uses cooperative-sticky. The group
    /// coordinator picks the first protocol that EVERY group member supports,
    /// so listing fallbacks lets this consumer join a group whose other
    /// members still run legacy eager protocols, for example mid-migration:
    ///
    /// ```ignore
    /// use llingr_kafka::{BalanceStrategy, Options};
    ///
    /// Options::new().partition_assignment_strategy(&[
    ///     BalanceStrategy::CooperativeSticky,
    ///     BalanceStrategy::Sticky,
    ///     BalanceStrategy::RoundRobin,
    ///     BalanceStrategy::Range,
    /// ]);
    /// ```
    ///
    /// With an eager strategy every rebalance revokes the whole assignment;
    /// the engine still drains in-flight work and commits before the handoff,
    /// so the delivery guarantees are unchanged. An empty slice is rejected
    /// at engine build time, never silently ignored.
    pub fn partition_assignment_strategy(self, strategies: &[BalanceStrategy]) -> Self {
        let value = strategies
            .iter()
            .map(|s| s.wire_name())
            .collect::<Vec<_>>()
            .join(",");
        self.push("partition.assignment.strategy", value)
    }

    /// Minimum bytes the broker accumulates before answering a fetch.
    pub fn fetch_min_bytes(self, n: u32) -> Self {
        self.push("fetch.min.bytes", n.to_string())
    }

    /// Maximum bytes returned per fetch across all partitions.
    pub fn fetch_max_bytes(self, n: u32) -> Self {
        self.push("fetch.max.bytes", n.to_string())
    }

    /// Maximum bytes returned per partition per fetch.
    pub fn max_partition_fetch_bytes(self, n: u32) -> Self {
        self.push("max.partition.fetch.bytes", n.to_string())
    }

    /// Maximum time the broker holds a fetch before answering.
    pub fn fetch_max_wait(self, d: Duration) -> Self {
        self.push("fetch.max.wait.ms", duration_ms_ceil(d).to_string())
    }

    /// Rack identifier (`client.rack`, translated to `kgo.Rack`): enables
    /// rack-aware fetch-from-follower on brokers that support it.
    pub fn rack(self, rack: &str) -> Self {
        self.push("client.rack", rack.to_string())
    }

    /// Maximum age of cached broker metadata (`metadata.max.age.ms`,
    /// translated to `kgo.MetadataMaxAge`). Lower it for faster reaction to
    /// leadership moves during rolling broker restarts.
    pub fn metadata_max_age(self, d: Duration) -> Self {
        self.push("metadata.max.age.ms", duration_ms_ceil(d).to_string())
    }

    /// Connection establishment timeout: TCP dial plus the TLS handshake
    /// when TLS is configured (`socket.connection.setup.timeout.ms`,
    /// translated to `kgo.DialTimeout`; client default 10s).
    pub fn dial_timeout(self, d: Duration) -> Self {
        self.push(
            "socket.connection.setup.timeout.ms",
            duration_ms_ceil(d).to_string(),
        )
    }

    /// How long an idle broker connection may live before being reaped
    /// (`connections.max.idle.ms`, translated to `kgo.ConnIdleTimeout`;
    /// client default 30s).
    pub fn connection_idle_timeout(self, d: Duration) -> Self {
        self.push("connections.max.idle.ms", duration_ms_ceil(d).to_string())
    }

    /// Retry budget for retryable non-fetch requests: group joins, offset
    /// commits, metadata (`llingr.request.retries`, translated to
    /// `kgo.RequestRetries`; client default 20).
    pub fn request_retries(self, n: u32) -> Self {
        self.push("llingr.request.retries", n.to_string())
    }

    /// Upper bound on how long one retryable non-fetch request keeps being
    /// retried (`llingr.retry.timeout.ms`, translated to `kgo.RetryTimeout`;
    /// client default: the session timeout for group requests, 30s for the
    /// rest).
    pub fn retry_timeout(self, d: Duration) -> Self {
        self.push("llingr.retry.timeout.ms", duration_ms_ceil(d).to_string())
    }

    /// Maximum in-flight fetch requests across all brokers
    /// (`llingr.max.concurrent.fetches`, translated to
    /// `kgo.MaxConcurrentFetches`; client default: unlimited, bounded by the
    /// broker count). Caps fetch memory alongside
    /// [`fetch_max_bytes`](Self::fetch_max_bytes). Must be at least 1;
    /// zero is rejected at engine initialisation.
    pub fn max_concurrent_fetches(self, n: u32) -> Self {
        self.push("llingr.max.concurrent.fetches", n.to_string())
    }

    /// Auto-create the consumed topic on first metadata fetch
    /// (`allow.auto.create.topics=true`; client default off, matching the
    /// Java consumer). The broker must also permit auto-creation.
    pub fn allow_auto_topic_creation(self) -> Self {
        self.push("allow.auto.create.topics", "true".to_string())
    }

    /// Disable CRC32 validation of fetched record batches
    /// (`check.crcs=false`; validation is on by default). Only for brokers
    /// that do not produce proper CRCs in record batches.
    pub fn disable_fetch_crc_validation(self) -> Self {
        self.push("check.crcs", "false".to_string())
    }

    /// Opt out of KIP-714 client metrics push to the broker
    /// (`enable.metrics.push=false`; on by default where the broker
    /// supports receiving them).
    pub fn disable_client_metrics_push(self) -> Self {
        self.push("enable.metrics.push", "false".to_string())
    }

    // -- poll-error resilience ------------------------------------------------

    /// How long a partition may fail continuously before the engine stops
    /// itself with an emergency shutdown reporting the reason
    /// (`llingr.poll.error.bail.after.ms`; default 10 minutes). Zero
    /// disables the bail entirely; a non-zero value must be between
    /// 1 minute and 1 hour or engine initialisation fails.
    pub fn poll_error_bail_after(self, d: Duration) -> Self {
        self.push(
            "llingr.poll.error.bail.after.ms",
            duration_ms_ceil(d).to_string(),
        )
    }

    /// Minimum interval between repeat logs of the same partition poll
    /// error (`llingr.poll.error.log.interval.ms`; default 1s). Must be
    /// positive.
    pub fn poll_error_log_interval(self, d: Duration) -> Self {
        self.push(
            "llingr.poll.error.log.interval.ms",
            duration_ms_ceil(d).to_string(),
        )
    }

    /// Pause after a broker error that returned no record, so a failing
    /// partition does not spin the poll loop
    /// (`llingr.poll.error.backoff.ms`; default 25ms). Zero disables the
    /// backoff; the value must be at most 5 seconds or engine
    /// initialisation fails.
    pub fn poll_error_backoff(self, d: Duration) -> Self {
        self.push(
            "llingr.poll.error.backoff.ms",
            duration_ms_ceil(d).to_string(),
        )
    }

    // -- TLS ------------------------------------------------------------------

    /// Enable TLS with the system trust roots and no client certificate.
    /// Implied by every other `tls_*` setter; call this alone when the broker
    /// certificate chains to a public CA.
    pub fn tls(mut self) -> Self {
        self.tls = true;
        self
    }

    /// Trust the CA certificate(s) in the given PEM file (`ssl.ca.location`).
    pub fn tls_ca_location(self, path: &str) -> Self {
        self.push_ssl("ssl.ca.location", path)
    }

    /// Trust the CA certificate(s) in the given PEM string (`ssl.ca.pem`).
    pub fn tls_ca_pem(self, pem: &str) -> Self {
        self.push_ssl("ssl.ca.pem", pem)
    }

    /// Present a client certificate (mTLS) from PEM files
    /// (`ssl.certificate.location` + `ssl.key.location`). The key must be
    /// unencrypted: encrypted client keys are not supported, decrypt the key
    /// before configuring it.
    pub fn tls_client_certificate(self, certificate_path: &str, key_path: &str) -> Self {
        self.push_ssl("ssl.certificate.location", certificate_path)
            .push_ssl("ssl.key.location", key_path)
    }

    /// Present a client certificate (mTLS) from PEM strings
    /// (`ssl.certificate.pem` + `ssl.key.pem`).
    pub fn tls_client_certificate_pem(self, certificate_pem: &str, key_pem: &str) -> Self {
        self.push_ssl("ssl.certificate.pem", certificate_pem)
            .push_ssl("ssl.key.pem", key_pem)
    }

    /// Disable ALL server certificate verification
    /// (`enable.ssl.certificate.verification=false`). The connection is still
    /// encrypted but the peer is not authenticated: development use only.
    pub fn disable_certificate_verification(self) -> Self {
        self.push_ssl("enable.ssl.certificate.verification", "false")
    }

    /// Verify the certificate chain but not the hostname
    /// (`ssl.endpoint.identification.algorithm=none`). For brokers reached by
    /// IP address or through tunnels where the certificate name cannot match.
    pub fn disable_hostname_verification(self) -> Self {
        self.push_ssl("ssl.endpoint.identification.algorithm", "none")
    }

    // -- SASL -----------------------------------------------------------------

    /// Authenticate with SASL/PLAIN. Combine with a `tls_*` setter or
    /// [`tls`](Self::tls) so credentials never travel unencrypted.
    pub fn sasl_plain(mut self, username: &str, password: &str) -> Self {
        self.sasl = Some(Sasl::UserPass {
            mechanism: SaslMechanism::Plain,
            username: username.to_string(),
            password: password.to_string(),
        });
        self
    }

    /// Authenticate with SASL/SCRAM-SHA-256.
    pub fn sasl_scram_sha256(mut self, username: &str, password: &str) -> Self {
        self.sasl = Some(Sasl::UserPass {
            mechanism: SaslMechanism::ScramSha256,
            username: username.to_string(),
            password: password.to_string(),
        });
        self
    }

    /// Authenticate with SASL/SCRAM-SHA-512.
    pub fn sasl_scram_sha512(mut self, username: &str, password: &str) -> Self {
        self.sasl = Some(Sasl::UserPass {
            mechanism: SaslMechanism::ScramSha512,
            username: username.to_string(),
            password: password.to_string(),
        });
        self
    }

    // -- AWS_MSK_IAM ------------------------------------------------------------

    /// Authenticate to Amazon MSK with IAM (`sasl.mechanism=AWS_MSK_IAM`).
    ///
    /// No static credentials are configured here: the engine resolves them
    /// Go-side with the AWS SDK's default provider chain of environment
    /// variables, shared config/profile, STS assume-role, web identity/IRSA,
    /// and EC2 instance metadata. This setter enables TLS automatically, because
    /// MSK IAM is TLS-only; layer a `tls_*` setter on top only to pin a
    /// specific CA. Refine the chain with the `aws_*` setters below.
    pub fn sasl_aws_msk_iam(mut self) -> Self {
        self.tls = true; // MSK IAM is TLS-only; force sasl_ssl.
        self.sasl = Some(Sasl::AwsMskIam {
            region: None,
            profile: None,
            role_arn: None,
            role_session_name: None,
        });
        self
    }

    /// Set the AWS region for the credential provider chain (`aws.region`).
    /// Optional; without it the chain uses `AWS_REGION` / the shared config.
    /// This steers the credential/STS region, not the SigV4 signing region,
    /// which franz-go derives from the broker hostname.
    /// Only meaningful after [`sasl_aws_msk_iam`](Self::sasl_aws_msk_iam).
    pub fn aws_region(mut self, region: &str) -> Self {
        if let Some(Sasl::AwsMskIam { region: r, .. }) = &mut self.sasl {
            *r = Some(region.to_string());
        }
        self
    }

    /// Select a shared-config profile for the credential provider chain
    /// (`aws.profile`). Optional. Only meaningful after
    /// [`sasl_aws_msk_iam`](Self::sasl_aws_msk_iam).
    pub fn aws_profile(mut self, profile: &str) -> Self {
        if let Some(Sasl::AwsMskIam { profile: p, .. }) = &mut self.sasl {
            *p = Some(profile.to_string());
        }
        self
    }

    /// Assume an IAM role on top of the base credential chain
    /// (`aws.role.arn`, with an optional `aws.role.session.name`). Optional.
    /// Only meaningful after [`sasl_aws_msk_iam`](Self::sasl_aws_msk_iam).
    pub fn aws_assume_role(mut self, role_arn: &str, session_name: Option<&str>) -> Self {
        if let Some(Sasl::AwsMskIam {
            role_arn: arn,
            role_session_name: name,
            ..
        }) = &mut self.sasl
        {
            *arn = Some(role_arn.to_string());
            *name = session_name.map(str::to_string);
        }
        self
    }

    // -- OAUTHBEARER (OIDC client-credentials) ----------------------------------

    /// Authenticate with SASL/OAUTHBEARER using the OAuth 2.0 client-
    /// credentials grant (`sasl.mechanism=OAUTHBEARER`). The engine fetches a
    /// token from `token_endpoint_url` with `client_id`/`client_secret`,
    /// caches it, and refreshes before expiry.
    ///
    /// This does NOT enable TLS on its own: OAUTHBEARER is permitted over
    /// `sasl_plaintext` for test clusters. Production deployments should
    /// add a `tls_*` setter so the bearer token never travels unencrypted.
    /// Add an audience/scope with [`oauthbearer_scope`](Self::oauthbearer_scope)
    /// and IdP extensions with
    /// [`oauthbearer_extensions`](Self::oauthbearer_extensions).
    pub fn sasl_oauthbearer_oidc(
        mut self,
        token_endpoint_url: &str,
        client_id: &str,
        client_secret: &str,
    ) -> Self {
        self.sasl = Some(Sasl::OauthbearerOidc {
            token_endpoint: token_endpoint_url.to_string(),
            client_id: client_id.to_string(),
            client_secret: client_secret.to_string(),
            scope: None,
            extensions: Vec::new(),
        });
        self
    }

    /// Set the OAuth scope requested from the token endpoint
    /// (`sasl.oauthbearer.scope`). Optional. Only meaningful after
    /// [`sasl_oauthbearer_oidc`](Self::sasl_oauthbearer_oidc).
    pub fn oauthbearer_scope(mut self, scope: &str) -> Self {
        if let Some(Sasl::OauthbearerOidc { scope: s, .. }) = &mut self.sasl {
            *s = Some(scope.to_string());
        }
        self
    }

    /// Set SASL/OAUTHBEARER extensions (`sasl.oauthbearer.extensions`), the
    /// key/value pairs some brokers require, for example Confluent Cloud's
    /// `logicalCluster` and `identityPoolId`. Optional. Only meaningful
    /// after [`sasl_oauthbearer_oidc`](Self::sasl_oauthbearer_oidc).
    pub fn oauthbearer_extensions<K, V>(mut self, pairs: impl IntoIterator<Item = (K, V)>) -> Self
    where
        K: Into<String>,
        V: Into<String>,
    {
        if let Some(Sasl::OauthbearerOidc { extensions, .. }) = &mut self.sasl {
            *extensions = pairs
                .into_iter()
                .map(|(k, v)| (k.into(), v.into()))
                .collect();
        }
        self
    }

    // -- GCP IAM (Google Cloud Managed Service for Apache Kafka) -----------------

    /// Authenticate to Google Cloud Managed Service for Apache Kafka with IAM
    /// (`sasl.mechanism=OAUTHBEARER`, `sasl.oauthbearer.method=gcp`).
    ///
    /// The wire mechanism is standard OAUTHBEARER; the engine synthesises the
    /// bearer token Google's service expects from Application Default
    /// Credentials, whether an environment key file, gcloud user credentials,
    /// workload identity, or GCE metadata, matching Google's own reference
    /// clients. No
    /// credentials are configured here and none cross the FFI. This setter
    /// enables TLS automatically, because Google's service is TLS-only.
    ///
    /// The token's principal, the authenticating identity's email, is derived
    /// from the credentials where possible; set it explicitly with
    /// [`gcp_principal`](Self::gcp_principal) when the credential source does
    /// not include it, as GCE metadata credentials do not, or use Google's
    /// `GOOGLE_MANAGED_KAFKA_AUTH_PRINCIPAL` environment variable.
    pub fn sasl_gcp_iam(mut self) -> Self {
        self.tls = true; // Google's managed Kafka is TLS-only; force sasl_ssl.
        self.sasl = Some(Sasl::GcpIam {
            principal: None,
            credentials_file: None,
        });
        self
    }

    /// Set the GCP principal for the token's subject claim (`gcp.principal`):
    /// the authenticating identity's email. Optional when the credential
    /// source includes it, as service account key files do. Only meaningful
    /// after [`sasl_gcp_iam`](Self::sasl_gcp_iam).
    pub fn gcp_principal(mut self, principal: &str) -> Self {
        if let Some(Sasl::GcpIam { principal: p, .. }) = &mut self.sasl {
            *p = Some(principal.to_string());
        }
        self
    }

    /// Use an explicit service account key JSON file instead of Application
    /// Default Credentials (`gcp.credentials.file`). Optional. Only
    /// meaningful after [`sasl_gcp_iam`](Self::sasl_gcp_iam).
    pub fn gcp_credentials_file(mut self, path: &str) -> Self {
        if let Some(Sasl::GcpIam {
            credentials_file: f,
            ..
        }) = &mut self.sasl
        {
            *f = Some(path.to_string());
        }
        self
    }

    // -- string escape hatch ----------------------------------------------------

    /// Add a Kafka client option as an librdkafka-style key/value pair: the
    /// escape hatch for curated string keys the typed setters do not cover,
    /// for example `isolation.level`, `security.protocol`, and
    /// `ssl.endpoint.identification.algorithm`.
    ///
    /// Keys are translated by the engine into typed franz-go options at
    /// initialisation. An unknown key FAILS initialisation with the full
    /// supported-key list, never a silent no-op, and the security keys
    /// (`security.protocol`, `ssl.*`, `sasl.*`) are collected and validated
    /// as one unit, so conflicting protocol/credential combinations are
    /// initialisation errors with specific messages.
    ///
    /// Repeating a key is allowed and deterministic: the LAST write wins,
    /// as in layered configuration, applied across typed setters and string
    /// pairs in call order. Setting the same security key both through a
    /// typed setter and a string pair on one builder is ambiguous and
    /// rejected when the engine is built; configure each key in one place.
    pub fn kafka_option(mut self, key: impl Into<String>, value: impl ToString) -> Self {
        self.entries.push((key.into(), value.to_string()));
        self
    }

    /// Add many Kafka client options at once, e.g. from a `HashMap` or a
    /// properties file. Same semantics as repeated
    /// [`kafka_option`](Self::kafka_option) calls: last write wins.
    pub fn kafka_options<K, V>(mut self, pairs: impl IntoIterator<Item = (K, V)>) -> Self
    where
        K: Into<String>,
        V: ToString,
    {
        for (key, value) in pairs {
            self.entries.push((key.into(), value.to_string()));
        }
        self
    }

    /// The keys the active typed security setters emit; string pairs for any
    /// of these are ambiguous and rejected by
    /// [`validate`](AdapterOptions::validate).
    fn typed_security_keys(&self) -> Vec<&str> {
        if !self.tls && self.sasl.is_none() {
            return Vec::new();
        }
        let mut keys = vec!["security.protocol"];
        keys.extend(self.ssl_entries.iter().map(|(k, _)| k.as_str()));
        // "sasl.mechanisms" is librdkafka's alias for "sasl.mechanism"; a
        // string pair under either name conflicts with any typed SASL setter.
        match &self.sasl {
            Some(Sasl::UserPass { .. }) => keys.extend([
                "sasl.mechanism",
                "sasl.mechanisms",
                "sasl.username",
                "sasl.password",
            ]),
            Some(Sasl::AwsMskIam { .. }) => keys.extend([
                "sasl.mechanism",
                "sasl.mechanisms",
                "aws.region",
                "aws.profile",
                "aws.role.arn",
                "aws.role.session.name",
            ]),
            Some(Sasl::OauthbearerOidc { .. }) => keys.extend([
                "sasl.mechanism",
                "sasl.mechanisms",
                "sasl.oauthbearer.token.endpoint.url",
                "sasl.oauthbearer.client.id",
                "sasl.oauthbearer.client.secret",
                "sasl.oauthbearer.scope",
                "sasl.oauthbearer.extensions",
            ]),
            Some(Sasl::GcpIam { .. }) => keys.extend([
                "sasl.mechanism",
                "sasl.mechanisms",
                "sasl.oauthbearer.method",
                "gcp.principal",
                "gcp.credentials.file",
            ]),
            None => {}
        }
        keys
    }
}

impl AdapterOptions for Options {
    fn adapter(&self) -> Adapter {
        Adapter::Franz
    }

    fn entries(&self) -> Vec<(String, String)> {
        let mut out = self.entries.clone();
        if self.tls || self.sasl.is_some() {
            let protocol = match (self.tls, self.sasl.is_some()) {
                (true, true) => "sasl_ssl",
                (true, false) => "ssl",
                (false, true) => "sasl_plaintext",
                (false, false) => unreachable!(),
            };
            out.push(("security.protocol".to_string(), protocol.to_string()));
            out.extend(self.ssl_entries.iter().cloned());
            match &self.sasl {
                Some(Sasl::UserPass {
                    mechanism,
                    username,
                    password,
                }) => {
                    out.push(("sasl.mechanism".to_string(), mechanism.as_str().to_string()));
                    out.push(("sasl.username".to_string(), username.clone()));
                    out.push(("sasl.password".to_string(), password.clone()));
                }
                Some(Sasl::AwsMskIam {
                    region,
                    profile,
                    role_arn,
                    role_session_name,
                }) => {
                    out.push(("sasl.mechanism".to_string(), "AWS_MSK_IAM".to_string()));
                    if let Some(region) = region {
                        out.push(("aws.region".to_string(), region.clone()));
                    }
                    if let Some(profile) = profile {
                        out.push(("aws.profile".to_string(), profile.clone()));
                    }
                    if let Some(role_arn) = role_arn {
                        out.push(("aws.role.arn".to_string(), role_arn.clone()));
                    }
                    if let Some(name) = role_session_name {
                        out.push(("aws.role.session.name".to_string(), name.clone()));
                    }
                }
                Some(Sasl::OauthbearerOidc {
                    token_endpoint,
                    client_id,
                    client_secret,
                    scope,
                    extensions,
                }) => {
                    out.push(("sasl.mechanism".to_string(), "OAUTHBEARER".to_string()));
                    out.push((
                        "sasl.oauthbearer.token.endpoint.url".to_string(),
                        token_endpoint.clone(),
                    ));
                    out.push(("sasl.oauthbearer.client.id".to_string(), client_id.clone()));
                    out.push((
                        "sasl.oauthbearer.client.secret".to_string(),
                        client_secret.clone(),
                    ));
                    if let Some(scope) = scope {
                        out.push(("sasl.oauthbearer.scope".to_string(), scope.clone()));
                    }
                    if !extensions.is_empty() {
                        let joined = extensions
                            .iter()
                            .map(|(k, v)| format!("{k}={v}"))
                            .collect::<Vec<_>>()
                            .join(",");
                        out.push(("sasl.oauthbearer.extensions".to_string(), joined));
                    }
                }
                Some(Sasl::GcpIam {
                    principal,
                    credentials_file,
                }) => {
                    out.push(("sasl.mechanism".to_string(), "OAUTHBEARER".to_string()));
                    out.push(("sasl.oauthbearer.method".to_string(), "gcp".to_string()));
                    if let Some(principal) = principal {
                        out.push(("gcp.principal".to_string(), principal.clone()));
                    }
                    if let Some(file) = credentials_file {
                        out.push(("gcp.credentials.file".to_string(), file.clone()));
                    }
                }
                None => {}
            }
        }
        out
    }

    /// Rejects the ambiguous mix: the same security key arriving from both a
    /// typed setter and a string [`kafka_option`](Options::kafka_option) pair
    /// on this builder. String security pairs WITHOUT typed setters are fine,
    /// because the engine's cross-key validation handles them as one unit,
    /// and string ssl/sasl keys the typed setters do not emit compose fine.
    fn validate(&self) -> Result<(), String> {
        let typed_keys = self.typed_security_keys();
        if typed_keys.is_empty() {
            return Ok(());
        }
        let mut conflicts: Vec<&str> = self
            .entries
            .iter()
            .map(|(key, _)| key.as_str())
            .filter(|key| typed_keys.contains(key))
            .collect();
        conflicts.sort_unstable();
        conflicts.dedup();
        if conflicts.is_empty() {
            Ok(())
        } else {
            Err(format!(
                "Options: security key(s) set both via typed setters and kafka_option(): {}; \
                 configure each key with one style only",
                conflicts.join(", ")
            ))
        }
    }
}

/// Manual Debug: never print credentials. Values of keys containing
/// "password", "secret" or "key.pem" are redacted. The "key.pem" match covers
/// `ssl.key.pem`, whose value is the client PRIVATE key; `ssl.certificate.pem`
/// and `ssl.ca.pem` are public material and stay visible.
impl fmt::Debug for Options {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut redacted: Vec<(String, String)> = Vec::new();
        for (key, value) in self.entries() {
            if key.contains("password") || key.contains("secret") || key.contains("key.pem") {
                redacted.push((key, "<redacted>".to_string()));
            } else {
                redacted.push((key, value));
            }
        }
        f.debug_struct("Options")
            .field("entries", &redacted)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry<'a>(entries: &'a [(String, String)], key: &str) -> Option<&'a str> {
        entries
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }

    #[test]
    fn options_render_kafka_style_entries() {
        let options = Options::new()
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .client_id("app-7")
            .group_instance_id("orders-0")
            .session_timeout(Duration::from_secs(30))
            .fetch_max_wait(Duration::from_millis(500))
            .fetch_min_bytes(1024);

        assert_eq!(options.adapter(), Adapter::Franz);
        let entries = options.entries();
        assert_eq!(entry(&entries, "auto.offset.reset"), Some("earliest"));
        assert_eq!(entry(&entries, "client.id"), Some("app-7"));
        assert_eq!(entry(&entries, "group.instance.id"), Some("orders-0"));
        assert_eq!(entry(&entries, "session.timeout.ms"), Some("30000"));
        assert_eq!(entry(&entries, "fetch.max.wait.ms"), Some("500"));
        assert_eq!(entry(&entries, "fetch.min.bytes"), Some("1024"));
        assert_eq!(
            entry(&entries, "security.protocol"),
            None,
            "no security requested"
        );
    }

    #[test]
    fn validate_defaults_to_ok() {
        // Typed setters alone: the typed-vs-string ambiguity cannot
        // arise on this builder; validate() is the trait default.
        let options = Options::new()
            .tls_ca_location("/ca.pem")
            .sasl_scram_sha256("u", "p");
        assert!(options.validate().is_ok());
    }

    #[test]
    fn sub_millisecond_duration_rounds_up() {
        let entries = Options::new()
            .session_timeout(Duration::from_micros(500))
            .entries();
        assert_eq!(
            entry(&entries, "session.timeout.ms"),
            Some("1"),
            "never silently zero"
        );
    }

    #[test]
    fn security_protocol_matrix() {
        let tls_only = Options::new().tls().entries();
        assert_eq!(entry(&tls_only, "security.protocol"), Some("ssl"));

        let sasl_only = Options::new().sasl_plain("u", "p").entries();
        assert_eq!(
            entry(&sasl_only, "security.protocol"),
            Some("sasl_plaintext")
        );

        let both = Options::new()
            .tls_ca_location("/ca.pem")
            .sasl_scram_sha256("u", "p")
            .entries();
        assert_eq!(entry(&both, "security.protocol"), Some("sasl_ssl"));
        assert_eq!(entry(&both, "ssl.ca.location"), Some("/ca.pem"));
        assert_eq!(entry(&both, "sasl.mechanism"), Some("SCRAM-SHA-256"));
        assert_eq!(entry(&both, "sasl.username"), Some("u"));
        assert_eq!(entry(&both, "sasl.password"), Some("p"));
    }

    #[test]
    fn tls_setters_emit_ssl_keys_and_imply_tls() {
        let entries = Options::new()
            .tls_client_certificate("/cert.pem", "/key.pem")
            .disable_hostname_verification()
            .entries();
        assert_eq!(entry(&entries, "security.protocol"), Some("ssl"));
        assert_eq!(
            entry(&entries, "ssl.certificate.location"),
            Some("/cert.pem")
        );
        assert_eq!(entry(&entries, "ssl.key.location"), Some("/key.pem"));
        assert_eq!(
            entry(&entries, "ssl.endpoint.identification.algorithm"),
            Some("none")
        );
    }

    #[test]
    fn scram_512_and_verification_toggle() {
        let entries = Options::new()
            .sasl_scram_sha512("u", "p")
            .disable_certificate_verification()
            .entries();
        assert_eq!(entry(&entries, "security.protocol"), Some("sasl_ssl"));
        assert_eq!(entry(&entries, "sasl.mechanism"), Some("SCRAM-SHA-512"));
        assert_eq!(
            entry(&entries, "enable.ssl.certificate.verification"),
            Some("false")
        );
    }

    #[test]
    fn debug_redacts_passwords() {
        let options = Options::new().sasl_plain("user", "hunter2");
        let debug = format!("{options:?}");
        assert!(
            !debug.contains("hunter2"),
            "password leaked into Debug: {debug}"
        );
        assert!(debug.contains("<redacted>"));
        assert!(debug.contains("user"), "username is not a secret");
    }

    #[test]
    fn partition_assignment_strategy_emits_names_in_preference_order() {
        let options = Options::new().partition_assignment_strategy(&[
            BalanceStrategy::CooperativeSticky,
            BalanceStrategy::Sticky,
            BalanceStrategy::RoundRobin,
            BalanceStrategy::Range,
        ]);
        let entries = options.entries();
        let value = entries
            .iter()
            .find(|(k, _)| k == "partition.assignment.strategy")
            .map(|(_, v)| v.as_str());
        assert_eq!(value, Some("cooperative-sticky,sticky,roundrobin,range"));
    }

    #[test]
    fn debug_redacts_inline_private_key_but_not_public_material() {
        let options = Options::new()
            .tls_ca_pem("PUBLIC CA PEM")
            .tls_client_certificate_pem("PUBLIC CERT PEM", "PRIVATE KEY PEM");
        let debug = format!("{options:?}");
        assert!(
            !debug.contains("PRIVATE KEY PEM"),
            "ssl.key.pem private key leaked into Debug: {debug}"
        );
        assert!(
            debug.contains("PUBLIC CERT PEM") && debug.contains("PUBLIC CA PEM"),
            "certificate and CA are public material and must stay visible: {debug}"
        );
    }

    /// Every ClientLogLevel variant emits its documented wire name under the
    /// `llingr.client.log.level` key. A mis-mapped variant would silently
    /// change client diagnostics verbosity.
    #[test]
    fn client_log_level_wire_names() {
        let cases = [
            (ClientLogLevel::None, "none"),
            (ClientLogLevel::Error, "error"),
            (ClientLogLevel::Warn, "warn"),
            (ClientLogLevel::Info, "info"),
            (ClientLogLevel::Debug, "debug"),
        ];
        for (level, wire) in cases {
            let entries = Options::new().client_log_level(level).entries();
            assert_eq!(
                entry(&entries, "llingr.client.log.level"),
                Some(wire),
                "{level:?}"
            );
        }
    }

    /// Key-name correctness for the scalar setters not covered elsewhere:
    /// a renamed key is caught by the bridge's curated set check only at
    /// init, so pin each literal here. Also covers Latest.
    #[test]
    fn remaining_scalar_setters_emit_documented_keys() {
        let entries = Options::new()
            .auto_offset_reset(AutoOffsetReset::Latest)
            .heartbeat_interval(Duration::from_secs(3))
            .rebalance_timeout(Duration::from_secs(60))
            .fetch_max_bytes(52_428_800)
            .max_partition_fetch_bytes(1_048_576)
            .rack("eu-west-1a")
            .metadata_max_age(Duration::from_secs(120))
            .entries();
        assert_eq!(entry(&entries, "auto.offset.reset"), Some("latest"));
        assert_eq!(entry(&entries, "heartbeat.interval.ms"), Some("3000"));
        assert_eq!(entry(&entries, "rebalance.timeout.ms"), Some("60000"));
        assert_eq!(entry(&entries, "fetch.max.bytes"), Some("52428800"));
        assert_eq!(
            entry(&entries, "max.partition.fetch.bytes"),
            Some("1048576")
        );
        assert_eq!(entry(&entries, "client.rack"), Some("eu-west-1a"));
        assert_eq!(entry(&entries, "metadata.max.age.ms"), Some("120000"));
    }

    /// SASL/PLAIN's mechanism string: a typo here breaks PLAIN auth while
    /// the SCRAM tests stay green.
    #[test]
    fn sasl_plain_emits_plain_mechanism() {
        let entries = Options::new().sasl_plain("u", "p").entries();
        assert_eq!(entry(&entries, "sasl.mechanism"), Some("PLAIN"));
        assert_eq!(entry(&entries, "sasl.username"), Some("u"));
        assert_eq!(entry(&entries, "sasl.password"), Some("p"));
    }

    /// Calling a disable_* setter ALONE implies TLS, because push_ssl flips
    /// the tls flag: security.protocol must come out as ssl, not stay absent
    /// with a dangling ssl.* property on a plaintext connection.
    #[test]
    fn disable_setters_alone_imply_tls() {
        let entries = Options::new().disable_certificate_verification().entries();
        assert_eq!(entry(&entries, "security.protocol"), Some("ssl"));

        let entries = Options::new().disable_hostname_verification().entries();
        assert_eq!(entry(&entries, "security.protocol"), Some("ssl"));
    }

    /// The PEM setters emit the documented inline keys: a key rename would
    /// silently drop the CA / certificate material.
    #[test]
    fn pem_setters_emit_inline_keys() {
        let entries = Options::new()
            .tls_ca_pem("CA")
            .tls_client_certificate_pem("CERT", "KEY")
            .entries();
        assert_eq!(entry(&entries, "ssl.ca.pem"), Some("CA"));
        assert_eq!(entry(&entries, "ssl.certificate.pem"), Some("CERT"));
        assert_eq!(entry(&entries, "ssl.key.pem"), Some("KEY"));
    }

    /// Repeated sasl_* calls replace the credentials, last write wins:
    /// exactly one mechanism/username/password triple is emitted, with the
    /// final values.
    #[test]
    fn repeated_sasl_calls_last_write_wins() {
        let entries = Options::new()
            .sasl_plain("old-user", "old-pass")
            .sasl_scram_sha512("new-user", "new-pass")
            .entries();
        assert_eq!(entry(&entries, "sasl.mechanism"), Some("SCRAM-SHA-512"));
        assert_eq!(entry(&entries, "sasl.username"), Some("new-user"));
        assert_eq!(entry(&entries, "sasl.password"), Some("new-pass"));
        assert_eq!(
            entries
                .iter()
                .filter(|(k, _)| k == "sasl.mechanism")
                .count(),
            1,
            "exactly one mechanism entry"
        );
        assert!(!entries.iter().any(|(_, v)| v == "old-pass"));
    }

    /// Security entries are appended AFTER the base client entries, in a
    /// fixed order: protocol, then ssl.*, then the sasl triple. The bridge
    /// consumes them positionally-independently today, but the emitted order
    /// is the crate's contract and silent reordering should not ship.
    #[test]
    fn security_entries_append_after_base_in_fixed_order() {
        let entries = Options::new()
            .client_id("app")
            .tls_ca_location("/ca.pem")
            .sasl_plain("u", "p")
            .entries();
        let keys: Vec<&str> = entries.iter().map(|(k, _)| k.as_str()).collect();
        let position = |key: &str| keys.iter().position(|k| *k == key).unwrap();
        assert!(position("client.id") < position("security.protocol"));
        assert!(position("security.protocol") < position("ssl.ca.location"));
        assert!(position("ssl.ca.location") < position("sasl.mechanism"));
        assert!(position("sasl.mechanism") < position("sasl.username"));
        assert!(position("sasl.username") < position("sasl.password"));
    }

    /// An empty strategy slice currently emits an empty value, rejected at
    /// engine build time per the doc; pin that it does not panic and does
    /// emit the key, so the build-time rejection stays reachable.
    #[test]
    fn empty_assignment_strategy_emits_empty_value() {
        let entries = Options::new().partition_assignment_strategy(&[]).entries();
        assert_eq!(entry(&entries, "partition.assignment.strategy"), Some(""));
    }

    // -- string escape hatch (kafka_option / kafka_options) ------------------

    #[test]
    fn kafka_option_records_pairs_in_call_order() {
        let entries = Options::new()
            .client_id("app")
            .kafka_option("isolation.level", "read_committed")
            .kafka_option("fetch.min.bytes", 1024)
            .entries();
        assert_eq!(entry(&entries, "isolation.level"), Some("read_committed"));
        assert_eq!(entry(&entries, "fetch.min.bytes"), Some("1024"));
        let keys: Vec<&str> = entries.iter().map(|(k, _)| k.as_str()).collect();
        let position = |key: &str| keys.iter().position(|k| *k == key).unwrap();
        assert!(
            position("client.id") < position("isolation.level"),
            "typed and string pairs interleave in call order"
        );
    }

    #[test]
    fn kafka_options_plural_ingests_iterators() {
        let pairs = [("metadata.max.age.ms", "10000"), ("client.rack", "eu-1a")];
        let entries = Options::new().kafka_options(pairs).entries();
        assert_eq!(entry(&entries, "metadata.max.age.ms"), Some("10000"));
        assert_eq!(entry(&entries, "client.rack"), Some("eu-1a"));
    }

    /// The same security key from a typed setter AND a string pair is
    /// ambiguous: validate() rejects it, naming the key.
    #[test]
    fn typed_and_string_security_key_conflict_is_rejected() {
        let options = Options::new()
            .sasl_scram_sha256("u", "p")
            .kafka_option("sasl.password", "other");
        let message = options.validate().expect_err("conflict must be rejected");
        assert!(message.contains("sasl.password"), "{message}");
        assert!(message.contains("kafka_option"), "{message}");
    }

    /// librdkafka's "sasl.mechanisms" alias conflicts with the typed SASL
    /// setters exactly like the canonical spelling.
    #[test]
    fn sasl_mechanisms_alias_conflicts_with_typed_sasl() {
        let options = Options::new()
            .sasl_plain("u", "p")
            .kafka_option("sasl.mechanisms", "PLAIN");
        assert!(options.validate().is_err());
    }

    /// Security keys arriving ONLY as string pairs are the pass-through
    /// model: no client-side conflict, the engine validates them as one
    /// unit at initialisation.
    #[test]
    fn string_only_security_keys_pass_validation() {
        let options = Options::new()
            .kafka_option("security.protocol", "sasl_ssl")
            .kafka_option("sasl.mechanism", "SCRAM-SHA-256")
            .kafka_option("sasl.username", "u")
            .kafka_option("sasl.password", "p");
        assert!(options.validate().is_ok());
    }

    /// A string security key the typed setters did not emit composes with
    /// them: tls() alone emits only security.protocol, so a string
    /// hostname-verification pair is not a conflict.
    #[test]
    fn non_overlapping_string_security_key_composes_with_typed_tls() {
        let options = Options::new()
            .tls()
            .kafka_option("ssl.endpoint.identification.algorithm", "none");
        assert!(options.validate().is_ok());
    }

    /// Credentials supplied through the string escape hatch are redacted in
    /// Debug exactly like typed ones.
    #[test]
    fn debug_redacts_string_pair_passwords() {
        let options = Options::new().kafka_option("sasl.password", "hunter2");
        let debug = format!("{options:?}");
        assert!(!debug.contains("hunter2"), "{debug}");
        assert!(debug.contains("<redacted>"), "{debug}");
    }

    // -- connection, retry, and fetch tuning ----------------------------------

    /// Every setter here emits its documented key with millisecond or
    /// literal values: a renamed key here would silently miss the bridge's
    /// curated table and fail init, so pin each literal.
    #[test]
    fn audit_setters_emit_documented_keys() {
        let entries = Options::new()
            .dial_timeout(Duration::from_secs(5))
            .connection_idle_timeout(Duration::from_secs(60))
            .request_retries(10)
            .retry_timeout(Duration::from_secs(45))
            .max_concurrent_fetches(4)
            .allow_auto_topic_creation()
            .disable_fetch_crc_validation()
            .disable_client_metrics_push()
            .entries();
        assert_eq!(
            entry(&entries, "socket.connection.setup.timeout.ms"),
            Some("5000")
        );
        assert_eq!(entry(&entries, "connections.max.idle.ms"), Some("60000"));
        assert_eq!(entry(&entries, "llingr.request.retries"), Some("10"));
        assert_eq!(entry(&entries, "llingr.retry.timeout.ms"), Some("45000"));
        assert_eq!(entry(&entries, "llingr.max.concurrent.fetches"), Some("4"));
        assert_eq!(entry(&entries, "allow.auto.create.topics"), Some("true"));
        assert_eq!(entry(&entries, "check.crcs"), Some("false"));
        assert_eq!(entry(&entries, "enable.metrics.push"), Some("false"));
    }

    /// The poll-error resilience setters emit the llingr.* adapter keys in
    /// milliseconds, including the documented zero-disables values.
    #[test]
    fn poll_error_setters_emit_adapter_keys() {
        let entries = Options::new()
            .poll_error_bail_after(Duration::from_secs(300))
            .poll_error_log_interval(Duration::from_secs(2))
            .poll_error_backoff(Duration::from_millis(100))
            .entries();
        assert_eq!(
            entry(&entries, "llingr.poll.error.bail.after.ms"),
            Some("300000")
        );
        assert_eq!(
            entry(&entries, "llingr.poll.error.log.interval.ms"),
            Some("2000")
        );
        assert_eq!(entry(&entries, "llingr.poll.error.backoff.ms"), Some("100"));

        let disabled = Options::new()
            .poll_error_bail_after(Duration::ZERO)
            .poll_error_backoff(Duration::ZERO)
            .entries();
        assert_eq!(
            entry(&disabled, "llingr.poll.error.bail.after.ms"),
            Some("0"),
            "zero must survive as the documented disable value"
        );
        assert_eq!(entry(&disabled, "llingr.poll.error.backoff.ms"), Some("0"));
    }
}
