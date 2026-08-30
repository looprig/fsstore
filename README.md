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

## Blob reader lifecycle

The planned v0.5.1 release makes each successful `Blobs.Get` return a concrete,
independent close-aware reader. `Read` and `Close` are safe concurrently; the first
Close publishes closed state before closing its file, repeated Close calls return the
same result, and every later Read returns zero bytes with `fs.ErrClosed` rather than
ordinary EOF. Closing one reader never changes stored bytes or another reader.

The Blobs provider advertises `BlobReaderCloseBound() time.Duration` with a
conservative one-second bound so the optional lifecycle capability in the next
published Storage release can discover it. fsstore v0.5.1 remains pinned to Storage
v0.5.0 until that release exists. The bound covers fsstore-controlled synchronization;
ordinary local-file syscalls have no provider queue or network wait but may still be
delayed by the kernel or underlying filesystem outside fsstore's control.
