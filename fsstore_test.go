package fsstore

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/looprig/storage"
)

// TestOpenRootValidation covers Open's Root handling: an empty Root is rejected
// with a typed *OptionsError; a fresh (and a nested, not-yet-existing) Root is
// created and accepted; and a Root that already exists as a plain file is rejected
// with an *OptionsError carrying errRootNotDir.
func TestOpenRootValidation(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name     string
		root     func(t *testing.T) string
		wantErr  bool
		wantRoot error // sentinel expected via errors.Is on the OptionsError cause, or nil
	}{
		{
			name:    "empty root rejected",
			root:    func(t *testing.T) string { return "" },
			wantErr: true,
		},
		{
			name:    "fresh temp dir accepted",
			root:    func(t *testing.T) string { return t.TempDir() },
			wantErr: false,
		},
		{
			name: "nested missing root created",
			root: func(t *testing.T) string {
				return filepath.Join(t.TempDir(), "a", "b", "store")
			},
			wantErr: false,
		},
		{
			name: "root that is a file rejected",
			root: func(t *testing.T) string {
				f := filepath.Join(t.TempDir(), "not-a-dir")
				if err := os.WriteFile(f, []byte("x"), 0o600); err != nil {
					t.Fatalf("seed file: %v", err)
				}
				return f
			},
			wantErr:  true,
			wantRoot: errRootNotDir,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s, err := Open(Options{Root: tc.root(t)})
			if (err != nil) != tc.wantErr {
				t.Fatalf("Open err = %v, wantErr %v", err, tc.wantErr)
			}
			if tc.wantErr {
				var oe *OptionsError
				if !errors.As(err, &oe) {
					t.Fatalf("Open error = %T %v, want *OptionsError", err, err)
				}
				if oe.Field != "Root" {
					t.Errorf("OptionsError.Field = %q, want %q", oe.Field, "Root")
				}
				if tc.wantRoot != nil && !errors.Is(err, tc.wantRoot) {
					t.Errorf("errors.Is(err, %v) = false, want true (err = %v)", tc.wantRoot, err)
				}
				if s != nil {
					t.Errorf("Open returned a non-nil Store alongside an error")
				}
				return
			}
			if s == nil {
				t.Fatal("Open returned a nil Store with no error")
			}
			if cerr := s.Close(); cerr != nil {
				t.Errorf("Close: %v", cerr)
			}
		})
	}
}

// TestOpenLaysOutBackends verifies Open creates the four backend subdirectories
// under Root, each at 0700, and that Backend()/the embedded Composite expose a
// fully-wired bundle.
func TestOpenLaysOutBackends(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	s, err := Open(Options{Root: root})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		if cerr := s.Close(); cerr != nil {
			t.Errorf("Close: %v", cerr)
		}
	})

	for _, sub := range []string{"streams", "leases", "kv", "blobs"} {
		info, serr := os.Stat(filepath.Join(root, sub))
		if serr != nil {
			t.Fatalf("stat %s: %v", sub, serr)
		}
		if !info.IsDir() {
			t.Errorf("%s is not a directory", sub)
		}
		if perm := info.Mode().Perm(); perm != 0o700 {
			t.Errorf("%s perm = %o, want 0700", sub, perm)
		}
	}

	// Backend() is the embedded Composite, fully wired (no nil primitive).
	b := s.Backend()
	if b == nil {
		t.Fatal("Backend() = nil")
	}
	if b != s.Composite {
		t.Error("Backend() != embedded Composite")
	}
	if b.Ledger == nil || b.Leaser == nil || b.KV == nil || b.Blobs == nil {
		t.Errorf("composite has a nil primitive: ledger=%v leaser=%v kv=%v blobs=%v",
			b.Ledger == nil, b.Leaser == nil, b.KV == nil, b.Blobs == nil)
	}
}

// TestStoreRoundTrip exercises all four primitives through the assembled Store: a
// ledger append+read, a lease acquire+release, a KV put+get, and a blob put+get,
// each reached as a promoted field on the embedded Composite.
func TestStoreRoundTrip(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, err := Open(Options{Root: t.TempDir()})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		if cerr := s.Close(); cerr != nil {
			t.Errorf("Close: %v", cerr)
		}
	})

	// Ledger: append a record at the empty tip, then read it back.
	const ledgerName = "sessions/roundtrip"
	payload := []byte("ledger-record")
	if err := s.Ledger.Append(ctx, ledgerName, 0, payload); err != nil {
		t.Fatalf("Ledger.Append: %v", err)
	}
	cur, err := s.Ledger.Read(ctx, ledgerName, 1)
	if err != nil {
		t.Fatalf("Ledger.Read: %v", err)
	}
	rec, err := cur.Next(ctx)
	if err != nil {
		t.Fatalf("cursor.Next: %v", err)
	}
	if rec.Seq != 1 || !bytes.Equal(rec.Payload, payload) {
		t.Errorf("ledger record = {Seq:%d Payload:%q}, want {1 %q}", rec.Seq, rec.Payload, payload)
	}
	if _, eof := cur.Next(ctx); !errors.Is(eof, io.EOF) {
		t.Errorf("second cursor.Next = %v, want io.EOF", eof)
	}
	if cerr := cur.Close(); cerr != nil {
		t.Errorf("cursor.Close: %v", cerr)
	}

	// Leaser: acquire exclusive ownership, then release it.
	const leaseName = "sessions/roundtrip"
	lease, err := s.Leaser.Acquire(ctx, leaseName)
	if err != nil {
		t.Fatalf("Leaser.Acquire: %v", err)
	}
	if lease.Epoch() != 1 {
		t.Errorf("first lease Epoch = %d, want 1", lease.Epoch())
	}
	if rerr := lease.Release(ctx); rerr != nil {
		t.Errorf("lease.Release: %v", rerr)
	}

	// KV: create a key (expectedRev 0), then read the value and revision back.
	const kvKey = "catalog/entry"
	kvVal := []byte("kv-value")
	rev, err := s.KV.Put(ctx, kvKey, 0, kvVal)
	if err != nil {
		t.Fatalf("KV.Put: %v", err)
	}
	if rev != 1 {
		t.Errorf("KV.Put rev = %d, want 1", rev)
	}
	gotVal, gotRev, err := s.KV.Get(ctx, kvKey)
	if err != nil {
		t.Fatalf("KV.Get: %v", err)
	}
	if gotRev != 1 || !bytes.Equal(gotVal, kvVal) {
		t.Errorf("KV.Get = (%q, %d), want (%q, 1)", gotVal, gotRev, kvVal)
	}

	// Blobs: store bytes, then stream them back.
	const blobKey = "snaps/object"
	blobData := []byte("blob-bytes")
	if err := s.Blobs.Put(ctx, blobKey, bytes.NewReader(blobData)); err != nil {
		t.Fatalf("Blobs.Put: %v", err)
	}
	rc, err := s.Blobs.Get(ctx, blobKey)
	if err != nil {
		t.Fatalf("Blobs.Get: %v", err)
	}
	gotBlob, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("blob ReadAll: %v", err)
	}
	if cerr := rc.Close(); cerr != nil {
		t.Errorf("blob Close: %v", cerr)
	}
	if !bytes.Equal(gotBlob, blobData) {
		t.Errorf("Blobs.Get = %q, want %q", gotBlob, blobData)
	}
}

// TestStoreCloseReleasesAndIsIdempotent verifies Close drops the ledger backend's
// in-process registry (its only releasable state) and that a second Close is a
// no-op returning nil.
//
// Contract note: after Close the Store must not be reused. Close is not a hard
// gate (the fs backends re-derive their on-disk state on demand), so this is a
// caller discipline — the callee re-initialises the registry to stay panic-safe
// rather than failing later operations closed.
func TestStoreCloseReleasesAndIsIdempotent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, err := Open(Options{Root: t.TempDir()})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	// Touch the ledger so its name->file registry is populated.
	if err := s.Ledger.Append(ctx, "sessions/close", 0, []byte("x")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if len(s.ledger.files) == 0 {
		t.Fatal("ledger registry empty after Append, want a cached entry")
	}

	if cerr := s.Close(); cerr != nil {
		t.Fatalf("first Close: %v", cerr)
	}
	if n := len(s.ledger.files); n != 0 {
		t.Errorf("ledger registry has %d entries after Close, want 0", n)
	}
	if !s.closed {
		t.Error("Store.closed = false after Close, want true")
	}

	// Idempotent: a second Close is a no-op returning nil.
	if cerr := s.Close(); cerr != nil {
		t.Errorf("second Close = %v, want nil", cerr)
	}
}

// TestOpenComposesConcreteBackends checks the promoted primitives are the concrete
// fsstore backends (defense against a mis-wired Composite) and share the store
// root's subdirectories.
func TestOpenComposesConcreteBackends(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	s, err := Open(Options{Root: root})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		if cerr := s.Close(); cerr != nil {
			t.Errorf("Close: %v", cerr)
		}
	})

	if _, ok := s.Ledger.(*ledgerStore); !ok {
		t.Errorf("Ledger is %T, want *ledgerStore", s.Ledger)
	}
	if _, ok := s.Leaser.(*leaserStore); !ok {
		t.Errorf("Leaser is %T, want *leaserStore", s.Leaser)
	}
	if _, ok := s.KV.(*kvStore); !ok {
		t.Errorf("KV is %T, want *kvStore", s.KV)
	}
	if _, ok := s.Blobs.(*blobStore); !ok {
		t.Errorf("Blobs is %T, want *blobStore", s.Blobs)
	}
	// The Store's retained ledger is the same instance wired into the Composite.
	if s.ledger != s.Ledger.(*ledgerStore) {
		t.Error("retained ledger differs from the composed Ledger")
	}
}

// Compile-time proof that the four fsstore backends honor their storage contracts.
// (Per-backend assertions also live beside each implementation; these gather them
// at the composition root so a break surfaces here too.)
var (
	_ storage.Ledger = (*ledgerStore)(nil)
	_ storage.Leaser = (*leaserStore)(nil)
	_ storage.KV     = (*kvStore)(nil)
	_ storage.Blobs  = (*blobStore)(nil)
)
