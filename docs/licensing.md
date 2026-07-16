# Licensing

llingr-kafka is dual-licensed, so you choose the licence that fits how you
ship. Under the open-source licence you may use it freely provided you meet the
copyleft obligations; under the commercial licence those obligations are lifted
for proprietary and closed-source use. This page explains, in plain terms, what
each choice means for a binary that embeds this crate, so you can decide before
you build. It is a summary to orient you, not legal advice: the `LICENSE` file
in the repository is the governing text, and your own legal counsel is the
right source for how it applies to you.

## The two licences

Every source file in this repository, and any binary you build from it, is
offered under this dual choice, expressed as the SPDX licence expression:

```
AGPL-3.0-only OR LicenseRef-Llingr-Commercial
```

The `OR` is a genuine choice you make as the licensee. You may take the crate
under the GNU Affero General Public License, version 3 only
(`AGPL-3.0-only`), and accept its terms, or you may hold a commercial licence
from Llingr Software Ltd (`LicenseRef-Llingr-Commercial`) and use it under
those terms instead. The expression matches the underlying llingr-demux engine
exactly, because that engine is what this crate statically links.

## Why the whole crate is AGPL, not permissive

llingr-kafka compiles the llingr-demux engine (Go, AGPL dual-licensed) into a
static C archive and links it directly into your binary. Static linking makes
the engine part of the same combined work as your application. That is why the
copyleft reaches the whole binary and why there is no permissive-licensed
subset of this crate you could carve out: the engine is present in every build.

The contract vocabulary (the `Message`, `Traits`, and handler-trait types) is
defined in the separate `llingr-nexus` crate, which is permissively licensed
(Apache-2.0) so it can be shared across the ecosystem. llingr-kafka re-exports
those types for convenience, but re-exporting a permissive type does not make
the binary permissive: the moment the engine is linked in, the combined work is
AGPL unless you hold a commercial licence.

## What AGPL-3.0-only requires of you

The AGPL is a strong copyleft licence. Its defining feature, beyond the ordinary
GPL obligations, is the network clause. In plain terms, if you take llingr-kafka
under `AGPL-3.0-only`, then:

- **If you distribute a binary that embeds this crate**, you must make the
  complete corresponding source code of that combined work available to the
  people you distribute it to, under `AGPL-3.0-only`. "Complete corresponding
  source" means enough to build and modify the whole thing, including your
  application code that is linked with the engine.
- **If you run the binary as a network service** that users interact with
  remotely, the case the AGPL exists to cover, you must offer those users the
  same complete corresponding source, under `AGPL-3.0-only`, even though you
  never hand them a binary. Merely keeping the service private does not avoid
  the obligation once remote users can use its functionality.

These obligations attach to your application as a whole, not just to the parts
you changed, because the linked engine makes it one combined work. If your
application is itself open source under a compatible licence, this is usually
exactly what you want and costs you nothing extra. If your application is
proprietary or closed-source, or you offer it as a hosted service and do not
want to publish your source, the AGPL is not a fit, and the commercial licence
is the route.

## When the commercial licence applies

The commercial licence (`LicenseRef-Llingr-Commercial`) removes the copyleft
and network-clause obligations, so you can embed llingr-kafka in a proprietary
binary or a closed-source SaaS product without publishing your source. You need
it when either of these is true:

- You ship a **proprietary or closed-source application** that embeds the crate
  and you do not want to release its source under `AGPL-3.0-only`.
- You run a **hosted or SaaS service** built on the crate and do not want to
  offer your users the service's source under `AGPL-3.0-only`.

Llingr Software Ltd is the sole licensing channel for the commercial option.
Contact `license@llingr.io` to arrange it. The commercial licence is a separate
agreement; holding it means you use the crate under that agreement's terms
rather than the AGPL, and the `OR` in the SPDX expression is how the licence
text records that this is your choice to make.

## The crates.io manifest and the dual choice

The Cargo package manifest declares the full dual expression in its `license`
field, `AGPL-3.0-only OR LicenseRef-Llingr-Commercial`, the same expression as
every source file. Should a future crates.io publish reject the `LicenseRef-`
identifier in that single field, the fallback is the plain `AGPL-3.0-only`
identifier in the manifest, with the commercial option then carried by the
`LICENSE` file, this documentation, and the REUSE metadata; crates.io would in
that case advertise the copyleft licence alone. Either way the dual choice is
real: the `LICENSE` file is the authority, and it offers both.

## Third-party notices in distributed binaries

Because the engine links statically, your binary embeds third-party Go
components whose licences require attribution when you distribute the binary.
These components are invisible to Rust-side tooling, since cargo, cargo-deny, and
the crates.io metadata never see them, so the obligation is easy to miss. The
one you must carry is the Kafka client:

- **franz-go** (`github.com/twmb/franz-go`) is BSD-3-Clause. The BSD-3-Clause
  licence requires that its copyright notice and licence text accompany binary
  distributions. Other transitive Go dependencies (for example
  `klauspost/compress`) carry their own permissive notices.

The repository ships a `THIRD-PARTY-NOTICES` file listing these components and
their licences, generated from the exact pinned Go modules the engine is built
against, and a script to regenerate it whenever the pinned engine version
moves. When you distribute a binary built from llingr-kafka, include
`THIRD-PARTY-NOTICES` alongside it, in the image, in the release archive, or
wherever your users can find it, so the embedded components' attribution
requirements are met. This obligation is independent of which of the two
licences you took the crate under: it comes from the bundled BSD-3-Clause and
other permissive components, not from the AGPL/commercial choice.
