# Security

You secure the connection to your cluster with the `Options` builder: call the
`tls_*` and `sasl_*` setters for what your brokers require, and the crate works
out the wire protocol and hands the assembled configuration to the engine. The
guiding principle is that nothing is silently ignored or silently insecure: the
security keys are validated together as one unit at startup, so a
half-configured or contradictory setup fails at `build()` with a specific
message rather than connecting in a weaker mode than you intended.

This page covers the mechanisms that work today, one worked example each, the
string escape hatch for keys the typed setters do not cover, credential hygiene,
the full misconfiguration error catalogue, and the mechanisms that are scheduled
or unsupported. You attach an `Options` value with the builder's `.options(...)`
hook.

Where a mechanism has a standalone program at the crate root, its section links
it, and you run it with `cargo run --example auth_<name>`; `examples/README.md`
indexes all six. The `examples/e2e/` compose stack runs the SCRAM configuration
end to end (`make example-verify`); the other five are standalone programs, each
configured for the broker its mechanism needs, and every file's header comment
lists the environment variables it reads and the infrastructure it requires.

## How the protocol is decided

You do not set `security.protocol` yourself when you use the typed setters; the
crate computes it. Any `tls_*` setter (or `tls()` alone) enables TLS, any
`sasl_*` setter enables SASL, and the emitted protocol is the combination:

- TLS only becomes `ssl`.
- SASL only becomes `sasl_plaintext`.
- TLS and SASL together become `sasl_ssl` (the usual production posture).

A few mechanisms have their own protocol rule, covered in their sections below.
`sasl_aws_msk_iam()` enables TLS itself and requires `sasl_ssl` (MSK IAM is
TLS-only). OAUTHBEARER has two methods whose rules differ deliberately, and they
do not contradict each other, they follow each provider's policy: the OIDC
client-credentials method (`sasl_oauthbearer_oidc`) is permitted over
`sasl_plaintext` as well as `sasl_ssl`, though TLS is strongly advised in
production, while the GCP method (`sasl_gcp_iam`) enables TLS itself and
requires `sasl_ssl`, because Google mandates TLS for Managed Service for Apache
Kafka.

TLS connections negotiate a minimum of TLS 1.2. The assembled TLS and SASL
configuration is built by the engine into a real `crypto/tls` config and a
franz-go SASL mechanism; you configure intent through the setters, not the
franz-go types.

## TLS: server authentication

When the broker's certificate chains to a public CA (Confluent Cloud, many
managed services), the system trust roots are enough:

```rust
# fn demo() -> Result<(), Box<dyn std::error::Error>> {
use llingr_kafka::Options;
let opts = Options::new().tls();
# let _ = opts; Ok(())
# }
```

For a private CA, trust it from a PEM file with `tls_ca_location`, or inline with
`tls_ca_pem`:

```rust
# fn demo() -> Result<(), Box<dyn std::error::Error>> {
use llingr_kafka::Options;
let opts = Options::new().tls_ca_location("/etc/ssl/certs/cluster-ca.pem");
# let _ = opts; Ok(())
# }
```

## mTLS: client certificate

For mutual TLS, present a client certificate alongside the CA. From PEM files
with `tls_client_certificate` (certificate path, then key path):

```rust
# fn demo() -> Result<(), Box<dyn std::error::Error>> {
use llingr_kafka::Options;
let opts = Options::new()
    .tls_ca_location("/etc/ssl/certs/cluster-ca.pem")
    .tls_client_certificate("/etc/ssl/client.pem", "/etc/ssl/client.key");
# let _ = opts; Ok(())
# }
```

Or from inline PEM strings with `tls_client_certificate_pem` (certificate PEM,
then key PEM), which suits secrets delivered through the environment rather than
mounted files:

```rust
# fn demo() -> Result<(), Box<dyn std::error::Error>> {
use llingr_kafka::Options;
let cert = std::env::var("CLIENT_CERT_PEM")?;
let key = std::env::var("CLIENT_KEY_PEM")?;
let opts = Options::new()
    .tls_ca_location("/etc/ssl/certs/cluster-ca.pem")
    .tls_client_certificate_pem(&cert, &key);
# let _ = opts; Ok(())
# }
```

The client key must be an unencrypted PEM. Encrypted client keys (the
`ssl.key.password` capability) are not supported here; decrypt the key before
configuring it.

Runnable example: `examples/auth/mtls.rs` (`cargo run --example auth_mtls`). It reads the CA
and client certificate/key paths from the environment; the file's header comment
lists them.

## SASL PLAIN over TLS: the Confluent Cloud configuration

SASL PLAIN carries a username and password and must only travel over TLS.
Confluent Cloud is exactly this configuration: the API key is the username and the API
secret is the password, over TLS to a public-CA broker, so `tls()` with system
roots plus `sasl_plain` is the whole configuration:

```rust
# fn demo() -> Result<(), Box<dyn std::error::Error>> {
use llingr_kafka::Options;
let key = std::env::var("KAFKA_API_KEY")?;
let secret = std::env::var("KAFKA_API_SECRET")?;
let opts = Options::new().tls().sasl_plain(&key, &secret);
# let _ = opts; Ok(())
# }
```

Runnable example: `examples/auth/sasl_plain.rs` (`cargo run --example auth_sasl_plain`),
which reads the API key and secret from `KAFKA_API_KEY` and `KAFKA_API_SECRET` and
points at a public-CA broker such as Confluent Cloud.

## SASL SCRAM over TLS

SCRAM-SHA-256 and SCRAM-SHA-512 are the usual choice for self-hosted clusters.
Combine the SASL setter with TLS (a private CA here):

```rust
# fn demo() -> Result<(), Box<dyn std::error::Error>> {
use llingr_kafka::Options;
let password = std::env::var("KAFKA_SASL_PASSWORD")?;
let opts = Options::new()
    .tls_ca_location("/etc/ssl/certs/cluster-ca.pem")
    .sasl_scram_sha256("svc-orders", &password); // or sasl_scram_sha512
# let _ = opts; Ok(())
# }
```

Between them the setters cover Confluent Cloud (SASL PLAIN over TLS), RedPanda,
self-hosted SASL/SCRAM clusters, and mTLS shops (file-path or inline-PEM client
certificates).

The `examples/e2e/` compose stack runs this exact SCRAM-over-TLS configuration
end to end: it authenticates its consumer with `sasl_scram_sha256` and
`tls_ca_location` against a real RedPanda broker on a `sasl_ssl` listener, and
`make example-verify` drives the full handshake on every run. See
`docs/example.md`. The standalone form is `examples/auth/scram_sha256.rs`
(`cargo run --example auth_scram_sha256`), configured for a SCRAM `sasl_ssl`
broker and its CA.

## AWS_MSK_IAM (Amazon MSK)

For Amazon MSK you authenticate with IAM rather than a static credential. Call
`sasl_aws_msk_iam()`: it selects the mechanism and enables TLS for you, because
MSK IAM is TLS-only and always negotiates `sasl_ssl`. The credentials are
resolved on the Go side through the AWS SDK's default provider chain in the
usual order, from environment variables through the shared config and
credentials files and their profile, STS assume-role, web identity / IRSA, and
the EC2 instance metadata endpoint, and they never cross the FFI boundary.

```rust
# fn demo() -> Result<(), Box<dyn std::error::Error>> {
use llingr_kafka::Options;
// The mechanism enables TLS on its own; credentials come from the provider chain.
let opts = Options::new()
    .sasl_aws_msk_iam()
    .aws_region("eu-west-1");
# let _ = opts; Ok(())
# }
```

Refine the chain with the optional `aws_*` setters: `aws_profile("...")` selects a
shared-config profile, and `aws_assume_role(role_arn, session_name)` layers an STS
assume-role on top of the base chain (the role ARN is usable alone; the session
name is optional):

```rust
# fn demo() -> Result<(), Box<dyn std::error::Error>> {
use llingr_kafka::Options;
let opts = Options::new()
    .sasl_aws_msk_iam()
    .aws_assume_role("arn:aws:iam::123456789012:role/orders-consumer", Some("orders-1"));
# let _ = opts; Ok(())
# }
```

**The `aws_region` nuance is worth understanding, because it bites on mock
stacks.** franz-go derives the SigV4 *signing* region from the broker hostname,
falling back to the `AWS_REGION` environment variable; the `aws_region` setter
(the `aws.region` key) steers only the credential and STS chain. Against a real
MSK cluster the two regions agree by construction, so you set `aws_region` and
nothing else. But if you run against a mock or any broker whose hostname is not
an MSK endpoint, franz-go cannot derive a signing region from the hostname, so
you must set `AWS_REGION` in the consumer's environment as well.

Credentials are resolved eagerly, at `build()`: the crate does one credential
retrieval, bounded to 20 seconds, as the engine initialises, so a missing or
invalid credential source is a clean startup error naming the provider chain,
not a stall on the first broker handshake. This matches the crate's eager
connect posture, where `build()` fails fast rather than deferring problems to
run time. After that, the SDK's credential cache serves and auto-refreshes for
the life of the consumer.

Runnable example: `examples/auth/aws_msk_iam.rs` (`cargo run --example
auth_aws_msk_iam`). It runs against a real Amazon MSK cluster with IAM
authentication and credentials available on the AWS provider chain, as the file's
header explains.

## OAUTHBEARER (OIDC client-credentials)

For OIDC-issued bearer tokens, use `sasl_oauthbearer_oidc(token_endpoint_url,
client_id, client_secret)`. The engine runs the OAuth 2.0 client-credentials
grant on the Go side: it fetches a token from the endpoint, caches it, and
refreshes it before expiry with 30 seconds of leeway, so a request never travels
with an almost-dead token. Add an audience or scope with `oauthbearer_scope`,
and the IdP extensions some brokers require, for example Confluent Cloud's
`logicalCluster` and `identityPoolId`, with `oauthbearer_extensions`:

```rust
# fn demo() -> Result<(), Box<dyn std::error::Error>> {
use llingr_kafka::Options;
let secret = std::env::var("OIDC_CLIENT_SECRET")?;
let opts = Options::new()
    .tls() // production: keep the bearer token off the wire in the clear
    .sasl_oauthbearer_oidc("https://idp.example.com/oauth2/token", "svc-orders", &secret)
    .oauthbearer_scope("kafka")
    .oauthbearer_extensions([("logicalCluster", "lkc-abc123"), ("identityPoolId", "pool-42")]);
# let _ = opts; Ok(())
# }
```

Unlike AWS_MSK_IAM, OAUTHBEARER does not enable TLS on its own: it is permitted
over `sasl_plaintext` so it works against a plaintext test cluster. In
production, add a `tls_*` setter (or `tls()`) so the bearer token never travels
unencrypted, exactly as the example above does.

The first token is fetched eagerly at `build()`, bounded to 20 seconds, so a
wrong endpoint, wrong credentials, or an IdP that never responds fails the
build rather than hanging or stalling the first handshake: the same fail-fast
behaviour as AWS_MSK_IAM.

Runnable example: `examples/auth/oauthbearer_oidc.rs` (`cargo run --example
auth_oauthbearer_oidc`), which reads the token endpoint and client credentials from
the environment and points at an OIDC IdP plus an OAUTHBEARER broker.

## GCP IAM (Google Cloud Managed Service for Apache Kafka)

Google Cloud's Managed Service for Apache Kafka authenticates with IAM, and it
does so over standard SASL/OAUTHBEARER. This is the more conventional of the two
hyperscaler mechanisms: AWS_MSK_IAM is a Kafka mechanism AWS invented, whereas
GCP puts an ordinary OAUTHBEARER token on the wire. Call `sasl_gcp_iam()`: it
selects OAUTHBEARER with the GCP token method (`sasl.oauthbearer.method=gcp`) and
enables TLS for you, because Google's service is TLS-only and always negotiates
`sasl_ssl`. Credentials are resolved on the Go side and never cross the FFI
boundary.

```rust
# fn demo() -> Result<(), Box<dyn std::error::Error>> {
use llingr_kafka::Options;
// Application Default Credentials; the mechanism enables TLS on its own.
let opts = Options::new().sasl_gcp_iam();
# let _ = opts; Ok(())
# }
```

Credentials come from Application Default Credentials, Google's standard
resolution order, which the engine uses unchanged: the
`GOOGLE_APPLICATION_CREDENTIALS` key file, gcloud user credentials, workload
identity, and finally the GCE / GKE metadata server. On Google compute that means
no credential configuration. To pin an explicit service account key file
instead of the chain, use `gcp_credentials_file` (the value is the file path, not
a secret):

```rust
# fn demo() -> Result<(), Box<dyn std::error::Error>> {
use llingr_kafka::Options;
let opts = Options::new()
    .sasl_gcp_iam()
    .gcp_credentials_file("/etc/gcp/orders-consumer-key.json");
# let _ = opts; Ok(())
# }
```

**The token carries a principal (the authenticating identity's email), and the
engine resolves it in a fixed order:** an explicit `gcp_principal(...)` wins; then
the `GOOGLE_MANAGED_KAFKA_AUTH_PRINCIPAL` environment variable (the same override
Google's own reference clients honour); then the `client_email` from the resolved
credentials, which service account key files contain. If none of those yields a
principal (for example under GCE metadata credentials, which do not include an
email), `build()` fails asking you to set it. Set it explicitly for those
sources:

```rust
# fn demo() -> Result<(), Box<dyn std::error::Error>> {
use llingr_kafka::Options;
let opts = Options::new()
    .sasl_gcp_iam()
    .gcp_principal("orders-consumer@my-project.iam.gserviceaccount.com");
# let _ = opts; Ok(())
# }
```

The OAuth scope is fixed to `https://www.googleapis.com/auth/cloud-platform` (the
scope both of Google's reference implementations request) and is deliberately not
configurable. The bearer token the service expects is a `GOOG_OAUTH2_TOKEN`
structure synthesised from the access token for you; you never construct or see
it.

Like the other resolved-credential mechanisms, the first token is fetched
eagerly, at `build()`, bounded to 20 seconds, so a missing credential source, a
principal that cannot be determined, or an unreachable metadata server fails the
build rather than stalling the first handshake. After that the token source
caches and refreshes the token for the life of the consumer.

The zero-configuration alternative is SASL/PLAIN. Google's managed Kafka also
accepts a service account over PLAIN, with the service account email as the
username and its base64-encoded key JSON as the password, over TLS: that is the
existing `tls().sasl_plain(email, base64_key_json)` configuration from the SASL
PLAIN section above, with no GCP-specific code. The trade-off is lifetime: a service
account key does not expire, whereas `sasl_gcp_iam()` mints short-lived access
tokens and refreshes them, so prefer the IAM path unless a static key is
specifically what you want.

Runnable example: `examples/auth/gcp_iam.rs` (`cargo run --example auth_gcp_iam`),
which takes an optional key file (`GCP_CREDENTIALS_FILE`) and principal
(`GCP_PRINCIPAL`) from the environment. It runs against a real Google Cloud
Managed Service for Apache Kafka cluster with Application Default Credentials, as
the file's header explains.

## Development verification toggles

Two setters relax verification for development against brokers reached by IP or
through tunnels. They are documented so you never reach for them in production by
accident:

```rust
# fn demo() -> Result<(), Box<dyn std::error::Error>> {
use llingr_kafka::Options;
// Verify the certificate chain but not the hostname (broker reached by IP).
let a = Options::new().tls_ca_location("/etc/ssl/ca.pem").disable_hostname_verification();

// No verification: encrypted but unauthenticated. Development only.
let b = Options::new().tls().disable_certificate_verification();
# let _ = (a, b); Ok(())
# }
```

`disable_hostname_verification` still verifies the certificate chain against the
trust roots and only skips the hostname check; `disable_certificate_verification`
turns verification off entirely, so the connection is encrypted but the peer is
not authenticated, which is why it belongs in development only.

## The string escape hatch

A few security keys have no typed setter because they are rarely needed. Set them
as librdkafka-style string pairs on the same `Options` builder with
`kafka_option`, and the engine validates them alongside the typed ones as one
unit. The keys reachable this way include `security.protocol` (to set the
protocol explicitly), `ssl.endpoint.identification.algorithm`, and
`isolation.level`. Configuring the same security key both through a typed setter
and a string pair on one builder is rejected (see the catalogue below), so use
one style per key.

```rust
# fn demo() -> Result<(), Box<dyn std::error::Error>> {
use llingr_kafka::Options;
// SCRAM over TLS with the read-committed isolation level set as a string pair.
let password = std::env::var("KAFKA_SASL_PASSWORD")?;
let opts = Options::new()
    .tls_ca_location("/etc/ssl/certs/cluster-ca.pem")
    .sasl_scram_sha256("svc-orders", &password)
    .kafka_option("isolation.level", "read_committed");
# let _ = opts; Ok(())
# }
```

## Credential hygiene

Pass secrets in from the environment or mounted files; never hard-code them, as
the examples above do with `std::env::var`. `Options` implements `Debug` with
credential values redacted: any value whose key contains `password`, `secret`, or
`key.pem`, which covers the inline client private key `ssl.key.pem` and the OIDC
`client.secret`, is hidden, while public material such as certificates and CA
bundles stays visible. The assembled configuration travels in memory to the
engine and is never logged by the crate. For AWS_MSK_IAM there is no secret in
`Options` to redact: the credentials come from the AWS provider chain, whether
environment, files, or instance metadata, never from the builder.

## Misconfiguration error catalogue

Security misconfiguration fails at `build()`, never at connect time in a weaker
mode. The cases, and what each means:

- **The same key set two ways (caught on the Rust side).** Setting a security key
  through both a typed setter and a string `kafka_option` pair on one `Options`
  builder is ambiguous and rejected with:

  `Options: security key(s) set both via typed setters and kafka_option(): <keys>; configure each key with one style only`

  Configure each key in one place.

- **Security keys without a protocol.** Providing `ssl.*`/`sasl.*` keys but no
  `security.protocol` fails, naming the expected values (`plaintext`, `ssl`,
  `sasl_plaintext`, `sasl_ssl`). When you use the typed setters this cannot
  happen, because they compute the protocol for you.

- **Protocol and credentials that contradict.** `sasl.*` keys under a non-SASL
  protocol, `ssl.*` keys under `plaintext`, or any keys under `plaintext` fail
  with a message naming the mismatch, for example `sasl.* options require
  security.protocol=sasl_plaintext or sasl_ssl (got "ssl")`, `ssl.* options
  require security.protocol=ssl or sasl_ssl (got "sasl_plaintext")`, or
  `security.protocol=plaintext conflicts with the ssl.*/sasl.* options provided`.
  An unrecognised protocol fails with `unknown security.protocol "quantum"
  (expected one of: plaintext, ssl, sasl_plaintext, sasl_ssl)`.

- **Certificate keys that half-match.** `ssl.ca.location` and `ssl.ca.pem` are
  mutually exclusive; the file-path and inline-PEM client-certificate forms are
  mutually exclusive; and a certificate without its key (or a key without its
  certificate) fails. The typed `tls_client_certificate*` setters always set both
  halves, so this arises only when mixing in string pairs.

- **Bad toggle values.** `enable.ssl.certificate.verification` must be `true` or
  `false`, and `ssl.endpoint.identification.algorithm` must be `https` or `none`;
  any other value fails.

- **Incomplete SASL (PLAIN and SCRAM).** A PLAIN or SCRAM protocol requires a
  mechanism, a username, and a password; a missing one fails naming what is
  missing (`sasl.mechanism is required with security.protocol=sasl_plaintext`,
  `sasl.username is required with sasl.mechanism=PLAIN`, `sasl.password is
  required with sasl.mechanism=PLAIN`). The supported mechanisms are PLAIN,
  SCRAM-SHA-256, SCRAM-SHA-512, AWS_MSK_IAM, and OAUTHBEARER; anything else fails,
  for example `unsupported sasl.mechanism "SCRAM-SHA-1" (supported: PLAIN,
  SCRAM-SHA-256, SCRAM-SHA-512, AWS_MSK_IAM, OAUTHBEARER)`.

- **AWS_MSK_IAM and OAUTHBEARER misconfiguration.** These mechanisms take no
  static username or password (their credentials are resolved for you), and each
  rejects keys belonging to the other. The exact messages:
  - `sasl.mechanism=AWS_MSK_IAM requires security.protocol=sasl_ssl (MSK IAM is TLS-only), got "..."` (use `sasl_aws_msk_iam()`, which enables TLS for you).
  - `sasl.username/sasl.password are not used with AWS_MSK_IAM (credentials come from the AWS provider chain); remove them`, and correspondingly `sasl.username/sasl.password are not used with OAUTHBEARER (the token comes from the OIDC token endpoint); remove them`.
  - `aws.role.session.name requires aws.role.arn`.
  - `aws.* options apply only to sasl.mechanism=AWS_MSK_IAM (got ...)` and `sasl.oauthbearer.* options apply only to sasl.mechanism=OAUTHBEARER (got ...)`.
  - `OAUTHBEARER requires sasl.oauthbearer.token.endpoint.url`, `OAUTHBEARER requires sasl.oauthbearer.client.id and sasl.oauthbearer.client.secret`, `sasl.oauthbearer.method="..." is not supported (supported: "oidc", "gcp"; application token callbacks are not available)`, and `sasl.oauthbearer.extensions must be comma-separated key=value pairs, got "..."`.
  Because credentials are resolved eagerly at `build()`, an unresolvable
  credential source or an unreachable token endpoint is a startup error too:
  `AWS_MSK_IAM: resolving credentials from the AWS provider chain (env, shared
  config/profile, STS, web identity, IMDS): ...` and `OAUTHBEARER: fetching
  initial token: ...`.

- **GCP IAM (OAUTHBEARER `method=gcp`) misconfiguration.** The GCP method takes no
  static credentials and none of the OIDC client-credentials keys, resolves its
  credentials for you, and is TLS-only. The exact messages:
  - `sasl.oauthbearer.method=gcp requires security.protocol=sasl_ssl (Google Cloud Managed Service for Apache Kafka is TLS-only), got "..."` (use `sasl_gcp_iam()`, which enables TLS for you).
  - `sasl.username/sasl.password are not used with sasl.oauthbearer.method=gcp (credentials come from Application Default Credentials); remove them`.
  - `the sasl.oauthbearer.{token.endpoint.url,client.id,client.secret,scope,extensions} options apply to sasl.oauthbearer.method=oidc, not method=gcp (the GCP token comes from Application Default Credentials)`.
  - `gcp.* options apply only to sasl.mechanism=OAUTHBEARER with sasl.oauthbearer.method=gcp`.
  - `unable to determine the GCP principal for the credentials: set gcp.principal (or the GOOGLE_MANAGED_KAFKA_AUTH_PRINCIPAL environment variable) to the authenticating identity's email`.
  - `gcp.credentials.file: ...` when the named service account key file cannot be read.
  Because the token is fetched eagerly at `build()`, an unresolvable credential
  source or an unreachable metadata server is a startup error too: `gcp: resolving
  Application Default Credentials (env key file, gcloud user credentials, workload
  identity, GCE metadata): ...`, `gcp: fetching initial token from Application
  Default Credentials: ...`, and the timeout variant `gcp: fetching initial token
  from Application Default Credentials timed out after 20s`.

- **Encrypted client key.** `ssl.key.password` fails with `ssl.key.password is
  not supported (encrypted client keys need OpenSSL); decrypt the key before
  configuring it`.

The Rust-side conflict (the first case) and every engine-side message above are
the exact strings the crate emits today, and they match the catalogue in
`docs/troubleshooting.md`.

## Unsupported mechanisms

AWS_MSK_IAM, OAUTHBEARER, and GCP IAM are supported, with their own sections
above. Two mechanisms remain unsupported and will not be built without a concrete
need:

- **SASL GSSAPI (Kerberos)** is not supported: selecting it fails with
  `GSSAPI/Kerberos is not supported: this bridge does not wire franz-go's
  Kerberos mechanism; it requires a custom engine build`.
- **Custom token-callback identity-provider flows** (an application-supplied
  token provider crossing the FFI) are out of scope; the OIDC client-credentials
  flow above covers the common case.

This coverage matches the security matrix in the repository README; treat that
matrix and this page as saying the same thing, and `docs/kafka-options.md` for
the non-security client options.
