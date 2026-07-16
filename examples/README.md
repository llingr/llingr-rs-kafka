# Examples

Two kinds of example live here, and they are deliberately separate.

## `auth/` - standalone auth programs

`auth/` holds one small, runnable consumer per Kafka authentication mechanism,
each showing how to wire that mechanism through `llingr_kafka::Options`. Run one
with `cargo run --example auth_<name>`. The cargo target names have the `auth_`
prefix so they sort together, and the files are grouped under the `auth/`
directory. Each file's header lists the environment variables it reads and the
infrastructure it requires.

These programs are standalone by design and not part of the `e2e/` stack. The
`e2e/` consumer uses the same `Options` wiring; SCRAM-SHA-256 over TLS is the
configuration the repository itself runs end to end, and the other mechanisms
require infrastructure the repository does not stand up.

| Target | Mechanism | Requires |
|---|---|---|
| `auth_sasl_plain` | SASL/PLAIN over TLS (API key and secret) | a PLAIN-over-TLS broker with a publicly trusted certificate, for example Confluent Cloud |
| `auth_scram_sha256` | SASL/SCRAM-SHA-256 over TLS | a SASL/SCRAM `sasl_ssl` broker; the `e2e/` stack runs this configuration end to end |
| `auth_mtls` | Mutual TLS (client certificate), no SASL | an ssl listener that requires client certificates |
| `auth_aws_msk_iam` | AWS_MSK_IAM | an Amazon MSK cluster with IAM authentication |
| `auth_oauthbearer_oidc` | SASL/OAUTHBEARER (OIDC client credentials) | an OIDC identity provider and a broker configured for OAUTHBEARER |
| `auth_gcp_iam` | Google Cloud Managed Service for Apache Kafka IAM | a Managed Kafka cluster and ADC credentials |

The full prose reference for every mechanism, including the misconfiguration
error catalogue, is in `docs/security.md`.

## `e2e/` - the end-to-end proof

`e2e/` is the whole compose stack: a franz-go producer, a `llingr-kafka`
consumer, and a RedPanda broker with a SASL/SCRAM-SHA-256-over-TLS (`sasl_ssl`)
listener, plus cert generation and topic/user bootstrap. It is driven by
`make example-verify`, and a clean exit 0 means the entire demux + franz + FFI +
log + metrics chain worked against a real, authenticated broker. This is the
authoritative end-to-end proof; the `auth/` programs are illustrative wiring.
