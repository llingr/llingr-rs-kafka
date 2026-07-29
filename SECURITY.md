# Security policy

To report a security vulnerability, email <security@llingr.dev>.
Please do not open a public issue for vulnerabilities.

## Release integrity

Release assets are built by a trusted-builder workflow that holds signing
permissions and no write access to the repository. Each has a build provenance
attestation naming the workflow, commit and runner behind it:

```sh
gh attestation verify <asset> --repo llingr/llingr-rs-kafka \
  --signer-workflow llingr/llingr-rs-kafka/.github/workflows/release-build.yml
```

Two SBOMs are published and signed:

- The `.crate`, scanned from the packaged crate rather than the repository, so
  it covers the Rust graph and the Go modules the engine compiles in. Attached
  to the release as `.cdx.json` and `.spdx.json`.
- Each engine archive, read from the built library's Go build info, so it lists
  the modules linked into the binary. Ships inside the archive as
  `llingr-engine.cdx.json`.

`--predicate-type https://cyclonedx.org/bom` selects the SBOM attestation rather
than the provenance one.

Dependencies are pinned: `Cargo.lock` and `bridge/go.sum` for the build, action
SHAs for the workflows. Publication to crates.io uses OIDC Trusted Publishing.
