# fsstore

`fsstore` implements storage's five storage primitives — **Ledger**, **Leaser**, **KV**,
**Blobs**, and **OrderedIndex** — over the local filesystem, giving a durable, single-host backend for session
and workspace persistence. The concrete on-disk formats live here, behind storage's neutral
contracts: the ledger is an append-only frame log (length-prefixed, CRC-32C-checked frames)
with automatic torn-tail recovery on reopen; leases are `flock`-fenced lock files carrying a
durable, strictly-increasing epoch counter; KV is one revision-CAS'd file per key (atomic
temp-file + rename writes); Blobs are content-addressed immutable byte objects; and OrderedIndex
uses one checksummed append-only mutation log plus warm in-memory order, rank, and due views per
namespace. It depends only on the Go standard library and `github.com/looprig/storage`.

## Status

- Implements all five storage primitives and conforms to the `github.com/looprig/storage` v0.7.0
  contract, including its nested-name cases (a name and a name extending it with `/…` coexist).
  The storage conformance suite runs in `conformance_test.go`.
- **v0.6.0 changed the on-disk layout with no migration, one-way**: `Open` refuses a pre-v0.6.0
  root with `ErrLegacyLayout` (see below).
- **Does not implement `storage.BlobReaderLifecycle`** (bounded Blob reader shutdown). Consumers
  that require it, such as `sessionstore.Open`, reject an fsstore Blobs provider (see
  "Blob reader close behavior").

## Install

```sh
go get github.com/looprig/fsstore@latest
```

It sits in tier 1 of the Looprig graph (foundation adapters); its only Looprig dependency is
`github.com/looprig/storage`.

Ledger and KV assume **single-writer-per-name**: their reads take no advisory lock and trust that
no one rewrites committed bytes concurrently. Enforcing that discipline is the **caller's
responsibility** — acquire the name's lease via the `Leaser` before writing those entries.
OrderedIndex instead serializes each operation with both an in-process namespace mutex and a
cross-process advisory lock because a read may recover a torn tail. Path containment is lexical
(validated names, `filepath.Clean`, a root-prefix check); it does not resolve symlinks below the
store root, so owning that directory is the deployment's responsibility.

## Usage

`Open` wires the five backends under one root directory and returns them bundled as a complete
`*storage.Composite` (the primitives collide on method names, so they are composed as a
field-bundle, not a single all-five type — reach each as `store.Ledger`, `store.Leaser`,
`store.KV`, `store.Blobs`, `store.OrderedIndex`). Hand the bundle to a consumer as
`store.Backend()` (or `store.Composite`):

```go
store, err := fsstore.Open(fsstore.Options{Root: "/var/lib/looprig"})
if errors.Is(err, fsstore.ErrLegacyLayout) {
    return fmt.Errorf("pre-v0.6.0 store root; move or delete it: %w", err) // never retry
}
if err != nil {
    return err
}
defer store.Close() // releases in-process state; idempotent

backend := store.Backend() // *storage.Composite
if err := backend.Ledger.Append(ctx, "sessions/example", 0, []byte("created")); err != nil {
    return err
}
rev, err := backend.KV.Put(ctx, "catalog/example", 0, []byte("ready"))
```

`examples/durable` has a complete runnable example covering the ledger, KV and Blobs.

`Root` is required (an empty `Root` is rejected with an `*OptionsError`) and is created at
`0700` if absent. Its layout is `streams/`, `leases/`, `kv/`, `blobs/`, and `ordered/`.

Each primitive maps a name's `/`-separated segments onto directories and stores the name's
leaf as a file carrying a suffix: `@log` (ledger), `@lock` (leaser), `@kv` (KV), `@blob`
(Blobs) and `@olog` (ordered index). Every suffix starts with `@`, a byte the storage name
grammar forbids, so a name's own file never shares a directory entry with the directory
holding the names that extend it: `sessions/a` (`kv/sessions/a@kv`) and `sessions/a/b`
(`kv/sessions/a/b@kv`) coexist in either creation order, as do `a` and `a.log/b`, and a name
that itself ends in a dotted extension round-trips (`a.kv` → `kv/a.kv@kv`).

### v0.6.0: BREAKING on-disk layout, no migration, one-way

fsstore v0.5.x and earlier stored KV and Blobs leaves with **no** suffix and ledger, lease and
ordered-index files with **dotted** suffixes (`.log`, `.lock`, `.olog`). Neither kept a leaf
apart from a longer name's directory (`sessions/a` vs `sessions/a/b`; `a` vs `a.log/b`).
v0.6.0 changed every primitive's suffix and ships **no migration**. `Open` scans `kv/`,
`blobs/`, `streams/`, `leases/` and `ordered/` and **fails closed** with
`*fsstore.LegacyLayoutError` (`errors.Is(err, fsstore.ErrLegacyLayout)`; it carries `Root`,
`Primitive` and the first legacy `Path`) when it finds a pre-v0.6.0 leaf, so old data is
never silently invisible or half-read. A leftover v0.5.x lock file refuses too: it is that
lease's persisted epoch counter, and ignoring it would restart the name's epochs. To proceed,
**move or delete the old store root** and open a fresh one. A root written by v0.6.0 is
misread by v0.5.x (it sees no data under the new names), so never roll a data directory back;
a v0.5.x write into a v0.6.0 root makes the next v0.6.0 `Open` refuse it. A refused `Open`
writes no data and removes nothing, though it may first create any of the five primitive
directories that were missing (empty).

The `@` suffix counts against the filesystem's per-name limit (255 bytes on common
filesystems). A KV key whose final segment is 253 bytes or longer, or a Blobs key whose final
segment is 251 bytes or longer, now fails with a typed IO error (`ENAMETOOLONG`); v0.5.2
accepted those. Ledger, lease and ordered-index names are unaffected (their suffixes kept
the same length). Hash- and UUID-shaped keys are far shorter.

`Close` releases the ledger cache and OrderedIndex namespace views and is idempotent; after
`Close` the `Store` must not be reused.

OrderedIndex warm pages use their maintained views without walking the filesystem or replaying
already-applied frames. If another handle appends to a namespace, the next operation reads and
applies only the new complete tail. Recovery truncates an all-zero unwritten tail, including the
post-crash case where the filesystem persisted the journal size before its data blocks. A zero
region that is not at the tail fails closed with `*OrderedLogCorruptError`; no acknowledged record
is discarded.

An OrderedIndex operation that returns cancellation before filesystem setup leaves no path. If it
returns cancellation after a fresh namespace directory or log is created but before advisory-lock
acquisition, it may leave that inert empty artifact, but never a committed or partial frame and
never a truncation. Once the exclusive advisory lock is held, recovery or mutation completes
without abandoning durability-critical work midway.

## Blob reader close behavior

Each successful `Blobs.Get` returns a concrete, independent close-aware reader
(since v0.5.1). `Read` and `Close` are race-safe; the first Close
publishes closed state before closing its file, repeated Close calls return the
same result, and every later Read returns zero bytes with `fs.ErrClosed` rather
than ordinary EOF. Closing one reader never changes stored bytes or another
reader.

fsstore intentionally does not implement Storage's optional bounded Blob reader
lifecycle capability. Portable ordinary-file I/O generally has no deadline
support, so a slow kernel, network-filesystem, or FUSE Read cannot be
force-cancelled within a finite bound. Close may therefore wait for an active
Read to return; callers requiring strictly bounded reader shutdown must select a
provider that advertises that capability.

Because of this, a consumer that requires the capability refuses an fsstore Blobs
provider: `sessionstore.Open` fails before any I/O with
`InvalidBackendError{Component: "BlobReaderLifecycle"}`.

## Development

The baseline is Go 1.26.8. Verify the module standalone, never against a workspace:

```sh
GOWORK=off go test ./...
make check   # fmt-check, vet, staticcheck, gosec, govulncheck, race tests, build
```

`make test` runs `GOWORK=off go test -race ./...`; `make secure` runs the lint and security
tools from `PATH` and skips any that are not installed. The module has no integration tags or
external infrastructure. See [CONTRIBUTING.md](CONTRIBUTING.md) for the dependency and design
rules (the Go standard library and `github.com/looprig/storage` only).

## License

Apache License 2.0; see [LICENSE](LICENSE).
