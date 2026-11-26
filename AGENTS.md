# go-hamt-ipld agent guide

go-hamt-ipld is the Go HAMT used for Filecoin actor state. Its blocks must match [ref-fvm](https://github.com/filecoin-project/ref-fvm)'s `ipld/hamt` byte for byte: Lotus and Forest compute the same state roots from them, so an encoding change is a consensus change.

`doc.go` and the schema comments in `hamt.go` describe the format. Read them before changing encoding, traversal or load validation, and update them in the same PR.

## Format rules

- Blocks don't record bitwidth or hash function; callers pass both as options. Bucket size is fixed at 3. Filecoin uses sha2-256 and a bitwidth per structure, never the library defaults.
- Pointers are a kinded union (a bare CID link, or a bucket array) with a hand-written codec in `pointer_cbor.go`. `cbor_gen.go` is cbor-gen output with hand edits for the bitfield; `make gen` overwrites them, so diff and restore after regenerating.
- Sorted buckets, splitting at the bucket size and cleanup on delete make the root depend only on the contents; a change to them is a format change even when every block still decodes.
- A new load rejection rule must not reject blocks Rust writes, and loosening validation must not admit blocks Rust rejects.
- A format change lands in Rust and Go together.

## Versions and consumers

`master` is v3, which [go-state-types](https://github.com/filecoin-project/go-state-types) wraps per actors version for Lotus. Older majors stay on release branches for historical state. A fix on any branch must leave every root unchanged.

## Testing

- `go test ./...`. `TestMalformedHamt` and the canonical-structure tests cover load validation; a new rejection rule gets a hand-built malformed block.
- Go tests show Go agrees with itself. For a behaviour change, compare roots and blocks with ref-fvm's `ipld/hamt` on the same operations.

## Comments, commits and pull requests

- Write comments and docs year-zero, describing the code as it is; history goes in the commit message and PR description. A comment states what a maintainer can't read from the code: an invariant, a format constraint, a surprising cost.
- PR titles use Conventional Commits, with `!` before the colon for a change that breaks importers.
- Report suspected vulnerabilities as the [Filecoin security policy](https://github.com/filecoin-project/community/blob/master/SECURITY.md) describes, never in a public issue or PR.

## Maintaining agent context

Fix this file in the same PR as a change that makes it wrong. `CLAUDE.md` contains only `@AGENTS.md`, so Claude Code loads this file.

