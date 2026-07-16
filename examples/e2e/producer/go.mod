// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-rs-kafka Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial
//
// The example producer is a plain, standalone franz-go producer in its own Go
// module, deliberately separate from bridge/go.mod: it is a vanilla ecosystem
// Kafka client, not part of the engine. Pure Go, so its image is a fully
// static scratch binary.
module github.com/llingr/llingr-kafka-example-producer

go 1.25.0

require (
	github.com/google/uuid v1.6.0
	github.com/twmb/franz-go v1.21.5
)

require (
	github.com/klauspost/compress v1.18.6 // indirect
	github.com/pierrec/lz4/v4 v4.1.26 // indirect
	github.com/twmb/franz-go/pkg/kmsg v1.13.1 // indirect
	golang.org/x/crypto v0.51.0 // indirect
)
