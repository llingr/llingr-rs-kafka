# End-to-end example

A RedPanda broker with SASL/SCRAM-SHA-256 over TLS auth, the `llingr-kafka`
consumer and a producer to publish some example messages.

## Requirements: Docker

No local Rust or Go toolchain: the consumer and its Go engine build inside the image.

From the repo root:

```sh
make example-verify
```

Exit 0 means the producer's 1000 messages flowed through the full poll -> route -> FFI -> handler -> commit path and the consumer logged them.

Without `make`:

```sh
docker compose -f examples/e2e/docker-compose.yml up --build --abort-on-container-exit
```

Tear down with `make example-down`.
