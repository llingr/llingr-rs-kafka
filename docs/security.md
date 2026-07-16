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

## How the protocol is decided

You do not set `security.protocol` yourself when you use the typed setters; the
crate computes it. Any `tls_*` setter (or `tls()` alone) enables TLS, any
`sasl_*` setter enables SASL, and the emitted protocol is the combination:

- TLS only becomes `ssl`.
- SASL only becomes `sasl_plaintext`.
- TLS and SASL together become `sasl_ssl` (the usual production shape).

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

## SASL PLAIN over TLS (the Confluent Cloud shape)

SASL PLAIN carries a username and password and must only ever travel over TLS.
Confluent Cloud is exactly this shape: the API key is the username and the API
secret is the password, over TLS to a public-CA broker, so `tls()` (system
roots) plus `sasl_plain` is the whole configuration:

```rust
# fn demo() -> Result<(), Box<dyn std::error::Error>> {
use llingr_kafka::Options;
let key = std::env::var("KAFKA_API_KEY")?;
let secret = std::env::var("KAFKA_API_SECRET")?;
let opts = Options::new().tls().sasl_plain(&key, &secret);
# let _ = opts; Ok(())
# }
```

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

## Development verification toggles

Two setters relax verification for development against brokers reached by IP or
through tunnels. They are documented so you never reach for them in production by
accident:

```rust
# fn demo() -> Result<(), Box<dyn std::error::Error>> {
use llingr_kafka::Options;
// Verify the certificate chain but not the hostname (broker reached by IP).
let a = Options::new().tls_ca_location("/etc/ssl/ca.pem").disable_hostname_verification();

// No verification at all: encrypted but unauthenticated. Development only.
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
`key.pem` (which covers the inline client private key `ssl.key.pem`) is hidden,
while public material such as certificates and CA bundles stays visible. The
assembled configuration travels in memory to the engine and is never logged by
the crate.

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

- **Incomplete SASL.** A SASL protocol requires a mechanism, a username, and a
  password; a missing one fails naming what is missing (`sasl.mechanism is
  required with security.protocol=sasl_plaintext`, `sasl.username is required
  with sasl.mechanism=PLAIN`, `sasl.password is required with
  sasl.mechanism=PLAIN`). The supported mechanisms are PLAIN, SCRAM-SHA-256, and
  SCRAM-SHA-512; anything else fails, for example `unsupported sasl.mechanism
  "SCRAM-SHA-1" (supported: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512)`.

- **Encrypted client key.** `ssl.key.password` fails with `ssl.key.password is
  not supported (encrypted client keys need OpenSSL); decrypt the key before
  configuring it`.

The Rust-side conflict (the first case) and every engine-side message above are
the exact strings the crate emits today, and they match the catalogue in
`docs/troubleshooting.md`.

## Scheduled and unsupported mechanisms

Two more mechanisms are designed and scheduled, to be built in this crate as a
phase after the core engine and example are green (no dates are promised here):

- **AWS_MSK_IAM.** IAM authentication to Amazon MSK, fed by the AWS SDK's default
  credential provider chain (environment, shared config and credentials files,
  the container and instance metadata endpoints, and an optional assumed role),
  resolved entirely on the Go side so credentials never cross the FFI. It will be
  reachable through curated keys (region, profile, role ARN and session name) and
  matching typed `Options` setters, validated as one unit like the mechanisms
  above (for example, IAM without a TLS protocol will be a startup error).
- **OAUTHBEARER (OIDC client-credentials).** A token fetcher, given a token
  endpoint, a client id and secret, and a scope, refreshing before expiry, again
  resolved on the Go side. Curated keys and typed setters, with the same
  validation discipline. Until it ships, selecting `sasl.mechanism=OAUTHBEARER`
  fails at startup with `OAUTHBEARER is not supported yet`.

Two mechanisms are explicitly unsupported and will not be built without a
concrete need:

- **SASL GSSAPI (Kerberos)** is not supported: selecting it fails with
  `GSSAPI/Kerberos is not supported: this bridge does not wire franz-go's
  Kerberos mechanism; it requires a custom engine build`.
- **Custom token-callback identity-provider flows** (an application-supplied
  token provider crossing the FFI) are out of scope; the OIDC client-credentials
  flow above covers the common case.

This coverage matches the security matrix in the repository README; treat that
matrix and this page as saying the same thing, and `docs/kafka-options.md` for
the non-security client options.
