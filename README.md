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
`store.KV`, `store.Blobs`, `store.OrderedIndex`). Hand the bundle to a consumer such as
`sessionstore.Open`:

```go
store, err := fsstore.Open(fsstore.Options{Root: "/var/lib/looprig"})
if err != nil {
    return err
}
defer store.Close() // releases in-process state; idempotent

sess, err := sessionstore.Open(store.Backend()) // or store.Composite
```

`Root` is required (an empty `Root` is rejected with an `*OptionsError`) and is created at
`0700` if absent. Its layout is `streams/`, `leases/`, `kv/`, `blobs/`, and `ordered/`.

Each primitive maps a name's `/`-separated segments onto directories and stores the name's
leaf as a file carrying a suffix: `.log` (ledger), `.lock` (leaser), `.olog` (ordered index),
`@kv` (KV) and `@blob` (Blobs). The KV and Blobs suffixes start with `@`, a byte the storage
name grammar forbids, so a key's own file never shares a directory entry with the directory
holding the keys that extend it: `sessions/a` (`kv/sessions/a@kv`) and `sessions/a/b`
(`kv/sessions/a/b@kv`) coexist in either creation order, and a key that itself ends in `.kv`
round-trips (`a.kv` → `kv/a.kv@kv`).

### Breaking on-disk change in v0.6.0 (no migration)

fsstore v0.5.x and earlier stored KV and Blobs leaves **without** a suffix, which made a key
and its `/` extension unable to coexist. v0.6.0 changed the layout and ships **no migration**.
`Open` scans `kv/` and `blobs/` and **fails closed** with `*fsstore.LegacyLayoutError`
(`errors.Is(err, fsstore.ErrLegacyLayout)`; it carries `Root`, `Primitive` and the first
legacy `Path`) when it finds a pre-v0.6.0 leaf, so old data is never silently invisible or
half-read. To proceed, **move or delete the old store root** (or its `kv/` and `blobs/` trees)
and open a fresh one. Ledger, lease and ordered-index files did not change layout. A root
written by v0.6.0 is misread by v0.5.x (`Get` finds nothing; `Keys`/`List` report suffixed
names that are not valid keys), so never roll a data directory back; a v0.5.x write into a
v0.6.0 root makes the next v0.6.0 `Open` refuse it.

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

The planned v0.5.1 release makes each successful `Blobs.Get` return a concrete,
independent close-aware reader. `Read` and `Close` are race-safe; the first Close
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
