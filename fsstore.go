package fsstore

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/looprig/storage"
)

// This file is fsstore's composition root: it wires the five filesystem backends
// (ledger, leaser, KV, blobs, ordered index) under one store root into a single storage
// field-bundle and exposes lifecycle (Open/Close).
//
// # Why a field-bundle, not an all-five interface
//
// storage's primitives collide on method names — each of Ledger, KV, and
// Blobs has its own Delete, for instance — so NO single Go type can implement all
// five at once. storage solves this with Composite, a struct that EMBEDS one
// provider per primitive; a caller reaches each as a field (c.Ledger, c.KV, …).
// Store follows the same shape: it embeds *storage.Composite rather than
// pretending to satisfy Ledger+Leaser+KV+Blobs+OrderedIndex itself. A consumer that wants the
// whole bundle (sessionstore.Open, say) is handed store.Composite (or Backend()).

// errRootNotDir is the leaf cause recorded in an *OptionsError when Root already
// exists on disk but is not a directory, so the store cannot be laid out under it.
var errRootNotDir = errors.New("fsstore: store root exists but is not a directory")

// Options configures Open. Root is the single directory under which the store
// lays out its five backends (streams/, leases/, kv/, blobs/, ordered/); it is required.
type Options struct {
	// Root is the store root directory. It is created at 0700 if absent. Required:
	// an empty Root is rejected with an *OptionsError.
	Root string
}

// OptionsError reports an invalid or unusable Open option. Field names the option
// (currently always "Root") and Reason explains the fault; Cause carries the
// underlying filesystem error (or errRootNotDir) when the fault was not a plain
// validation failure, and is exposed via Unwrap for errors.Is/As.
type OptionsError struct {
	Field  string
	Reason string
	Cause  error
}

func (e *OptionsError) Error() string {
	msg := "fsstore: invalid option " + strconv.Quote(e.Field) + ": " + e.Reason
	if e.Cause != nil {
		return msg + ": " + e.Cause.Error()
	}
	return msg
}

// Unwrap exposes the underlying cause (a filesystem error or errRootNotDir), or
// nil for a pure validation failure such as an empty Root.
func (e *OptionsError) Unwrap() error { return e.Cause }

// ErrLegacyLayout is the sentinel every *LegacyLayoutError matches under
// errors.Is. It means the store root holds KV or Blobs data written by fsstore
// v0.5.x or earlier, whose on-disk layout v0.6.0 does not read.
var ErrLegacyLayout = errors.New("fsstore: store root holds pre-v0.6.0 KV/Blobs data")

// LegacyLayoutError reports that Open refused a store root because its kv/ or
// blobs/ tree holds a leaf file in the pre-v0.6.0 layout. fsstore v0.6.0 changed
// the KV and Blobs on-disk layout (every leaf file now carries an '@kv'/'@blob'
// suffix so a key and a key extending it with "/..." can coexist) and ships NO
// migration: old-layout data would otherwise be silently invisible (Get reports
// not-found, Keys/List omit it) while new writes landed beside it. Open fails
// closed instead, before any caller can read or write the root.
//
// Remedy: move or delete the old root (or at least its kv/ and blobs/ trees) and
// open a fresh one; the data is not convertible in place by fsstore. Root is the
// resolved store root, Primitive is "kv" or "blobs", and Path is the first legacy
// leaf file found. It matches ErrLegacyLayout under errors.Is.
type LegacyLayoutError struct {
	Root      string
	Primitive string
	Path      string
}

func (e *LegacyLayoutError) Error() string {
	return "fsstore: store root " + strconv.Quote(e.Root) + " holds pre-v0.6.0 " + e.Primitive +
		" data (" + strconv.Quote(e.Path) + "); v0.6.0 changed the on-disk layout with no migration: " +
		"move or delete the old root and open a fresh one"
}

// Unwrap returns ErrLegacyLayout so callers can classify with errors.Is.
func (e *LegacyLayoutError) Unwrap() error { return ErrLegacyLayout }

// Store is an open fsstore rooted at a single directory on the local filesystem.
// It embeds *storage.Composite, so a caller reaches each primitive as a promoted
// field — store.Ledger, store.Leaser, store.KV, store.Blobs, store.OrderedIndex — and hands the whole
// bundle to a consumer as store.Composite or via Backend. The five primitives
// collide on method names, so no single type can implement all five (see the
// file-level comment); embedding the field-bundle is how Store sidesteps that.
type Store struct {
	*storage.Composite
	root string

	// ledger is retained as its concrete type solely so Close can release the
	// backend's in-process state (its cached name->file registry). ordered retains
	// the OrderedIndex namespace views for the same lifecycle purpose.
	ledger  *ledgerStore
	ordered *orderedIndex

	mu     sync.Mutex
	closed bool
}

// Open assembles a filesystem-backed storage bundle under opts.Root. It requires
// a non-empty Root (empty -> *OptionsError), creates it at 0700, and verifies it
// resolves to a directory. It then wires the five backends under that root —
// <root>/streams, <root>/leases, <root>/kv, <root>/blobs, <root>/ordered, each
// created at 0700 by its constructor — and bundles them with
// storage.NewCompositeWithOrderedIndex.
//
// Before bundling, Open scans kv/ and blobs/ and refuses a root that holds
// pre-v0.6.0 KV/Blobs data with *LegacyLayoutError (errors.Is ErrLegacyLayout);
// v0.6.0 changed that layout and does not migrate it.
//
// On any backend-construction failure Open returns that backend's typed error and
// no Store; because every fs backend only creates directories (none holds an open
// file descriptor until first use), a partial failure leaks no handles and needs
// no unwind.
func Open(opts Options) (*Store, error) {
	if opts.Root == "" {
		return nil, &OptionsError{Field: "Root", Reason: "must not be empty"}
	}
	root := filepath.Clean(opts.Root)
	root, err := filepath.Abs(root)
	if err != nil {
		return nil, &OptionsError{Field: "Root", Reason: "resolve absolute store root", Cause: err}
	}
	switch info, serr := os.Stat(root); {
	case serr == nil:
		// Root already exists: it must be a directory to hold the backend layout.
		if !info.IsDir() {
			return nil, &OptionsError{Field: "Root", Reason: "store root is not a directory", Cause: errRootNotDir}
		}
	case errors.Is(serr, fs.ErrNotExist):
		// Root absent: create it (and any missing parents) at 0700.
		if mkErr := os.MkdirAll(root, 0o700); mkErr != nil {
			return nil, &OptionsError{Field: "Root", Reason: "create store root", Cause: mkErr}
		}
	default:
		return nil, &OptionsError{Field: "Root", Reason: "stat store root", Cause: serr}
	}
	root, err = filepath.EvalSymlinks(root)
	if err != nil {
		return nil, &OptionsError{Field: "Root", Reason: "resolve store root symlinks", Cause: err}
	}

	ledger, err := newLedgerStore(root)
	if err != nil {
		return nil, err
	}
	leaser, err := newLeaserStore(root)
	if err != nil {
		return nil, err
	}
	kv, err := newKVStore(root)
	if err != nil {
		return nil, err
	}
	blobs, err := newBlobStore(root)
	if err != nil {
		return nil, err
	}
	if err := refuseLegacyLayout(root, kv, blobs); err != nil {
		return nil, err
	}
	ordered, err := newOrderedIndex(root)
	if err != nil {
		return nil, err
	}

	composite, err := storage.NewCompositeWithOrderedIndex(ledger, leaser, kv, blobs, ordered)
	if err != nil {
		return nil, err
	}
	return &Store{Composite: composite, root: root, ledger: ledger, ordered: ordered}, nil
}

// refuseLegacyLayout fails Open closed when the kv/ or blobs/ tree holds a
// pre-v0.6.0 (unsuffixed) leaf file, returning *LegacyLayoutError; see
// findLegacyLeaf for why that test is exact. A walk fault is returned as the
// primitive's typed IO error (Op "layout-scan") rather than treated as clean.
// Only kv/ and blobs/ are scanned: the ledger, leaser and ordered backends
// always suffixed their files and did not change layout.
func refuseLegacyLayout(root string, kv *kvStore, blobs *blobStore) error {
	path, found, err := findLegacyLeaf(kv.kv)
	if err != nil {
		return &KVIOError{Op: "layout-scan", Path: kv.kv, Cause: err}
	}
	if found {
		return &LegacyLayoutError{Root: root, Primitive: "kv", Path: path}
	}
	path, found, err = findLegacyLeaf(blobs.blobs)
	if err != nil {
		return &BlobIOError{Op: "layout-scan", Path: blobs.blobs, Cause: err}
	}
	if found {
		return &LegacyLayoutError{Root: root, Primitive: "blobs", Path: path}
	}
	return nil
}

// Backend returns the assembled five-primitive bundle to hand to a consumer such
// as sessionstore.Open. It is the embedded *storage.Composite; callers may read
// store.Composite directly instead.
func (s *Store) Backend() *storage.Composite { return s.Composite }

// StoragePaths reports the canonical local root that contains every primitive.
// The result is newly allocated so callers cannot mutate subsequent reports.
func (s *Store) StoragePaths() []string { return providerPaths(s.root) }

func (s *ledgerStore) StoragePaths() []string { return providerPaths(s.root) }

func (s *leaserStore) StoragePaths() []string { return providerPaths(s.root) }

func (s *kvStore) StoragePaths() []string { return providerPaths(s.root) }

func (s *blobStore) StoragePaths() []string { return providerPaths(s.root) }

func (s *orderedIndex) StoragePaths() []string { return providerPaths(s.store.root) }

func providerPaths(root string) []string { return []string{root} }

// Close releases the store's in-process state. Concretely it drops the ledger
// backend's cached name->file registry and the ordered index's namespace views.
// The fs backends hold no long-lived file descriptors of their own —
// the ledger opens and closes its fd within each Append/Delete (the per-append
// advisory-lock model), the KV and Blobs writers do the same, and a Leaser grant's
// fd is owned and released by the caller via Lease.Release — so there are no open
// handles for Close to reclaim beyond that in-memory registry.
//
// Close is idempotent: a second call is a no-op returning nil. After Close the
// Store must not be reused; open a fresh Store on the same root to use it again.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	return errors.Join(s.ledger.Close(), s.ordered.Close())
}
