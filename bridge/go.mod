module github.com/llingr/llingr-rs-kafka/bridge

go 1.25.0

// Pinned to PUBLISHED engine releases, fetched from the Go module proxy.
// No `replace` to local working copies: this crate builds reproducibly from
// pinned versions, independent of any sibling checkout on the build machine.

require (
	github.com/llingr/llingr-adapter-franz v0.14.0
	github.com/llingr/llingr-demux v0.12.0
	github.com/llingr/llingr-nexus v0.11.0
	github.com/twmb/franz-go v1.21.5
)

require (
	github.com/klauspost/compress v1.18.6 // indirect
	github.com/pierrec/lz4/v4 v4.1.26 // indirect
	github.com/twmb/franz-go/pkg/kadm v1.18.0 // indirect
	github.com/twmb/franz-go/pkg/kmsg v1.13.1 // indirect
	golang.org/x/crypto v0.51.0 // indirect
)
