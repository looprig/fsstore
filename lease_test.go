package fsstore

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/looprig/storekit"
)

// leaseLockPath returns the on-disk path the leaser maps name to, so a test can
// inspect the persisted epoch counter or assert the file's location directly.
func leaseLockPath(root, name string) string {
	return filepath.Join(root, "leases", filepath.FromSlash(name)+".lock")
}

// TestLeaseLockFileLocation proves a granted lease materializes its lock file at
// exactly <root>/leases/<name>.lock, with the name's '/'-segments becoming
// directory components, and that pathFor maps to the same location.
func TestLeaseLockFileLocation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cases := []struct {
		label string
		name  string
	}{
		{label: "single segment", name: "solo"},
		{label: "two segments", name: "sessions/one"},
		{label: "deep nesting", name: "a/b/c/held"},
	}
	for _, tc := range cases {
		t.Run(tc.label, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			s, err := newLeaserStore(root)
			if err != nil {
				t.Fatalf("newLeaserStore: %v", err)
			}
			lease, err := s.Acquire(ctx, tc.name)
			if err != nil {
				t.Fatalf("Acquire(%q): %v", tc.name, err)
			}
			t.Cleanup(func() { _ = lease.Release(ctx) })

			want := leaseLockPath(root, tc.name)
			if _, err := os.Stat(want); err != nil {
				t.Fatalf("expected lock file at %q: %v", want, err)
			}
			got, perr := s.pathFor(tc.name)
			if perr != nil {
				t.Fatalf("pathFor(%q): %v", tc.name, perr)
			}
			if got != want {
				t.Errorf("pathFor(%q) = %q, want %q", tc.name, got, want)
			}
		})
	}
}

// TestLeaserEpochPersistsAcrossRestart proves the epoch counter is durable on
// disk: after acquire+release, a BRAND-NEW leaserStore over the same root grants a
// strictly greater epoch, because the counter is read from and written back to the
// lock file (not merely held in memory). This exercises the fs-specific guarantee
// the in-memory oracle cannot.
func TestLeaserEpochPersistsAcrossRestart(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	const name = "sessions/persist"
	root := t.TempDir()

	var last uint64
	for i := 0; i < 4; i++ {
		s, err := newLeaserStore(root) // a fresh store each iteration == a process restart
		if err != nil {
			t.Fatalf("newLeaserStore iteration %d: %v", i, err)
		}
		lease, err := s.Acquire(ctx, name)
		if err != nil {
			t.Fatalf("Acquire iteration %d: %v", i, err)
		}
		if lease.Epoch() <= last {
			t.Fatalf("restart %d Epoch() = %d, want strictly greater than %d (persisted counter must survive restart)", i, lease.Epoch(), last)
		}
		last = lease.Epoch()
		if err := lease.Release(ctx); err != nil {
			t.Fatalf("Release iteration %d: %v", i, err)
		}
	}
	if last != 4 {
		t.Errorf("final epoch after 4 restarts = %d, want 4 (strictly +1 each grant from an empty counter)", last)
	}
}

// TestLeaseHeldReportsPersistedEpoch proves that when a name is already held, a
// second Acquire returns a *LeaseHeldError whose HolderEpoch is read back from the
// persisted lock file (the advisory read needs no lock).
func TestLeaseHeldReportsPersistedEpoch(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	const name = "sessions/held"
	root := t.TempDir()
	s, err := newLeaserStore(root)
	if err != nil {
		t.Fatalf("newLeaserStore: %v", err)
	}
	first, err := s.Acquire(ctx, name)
	if err != nil {
		t.Fatalf("first Acquire: %v", err)
	}
	t.Cleanup(func() { _ = first.Release(ctx) })

	_, err = s.Acquire(ctx, name)
	var held *storekit.LeaseHeldError
	if !errors.As(err, &held) {
		t.Fatalf("second Acquire = %v, want *LeaseHeldError", err)
	}
	if held.Name != name {
		t.Errorf("LeaseHeldError.Name = %q, want %q", held.Name, name)
	}
	if held.HolderEpoch != first.Epoch() {
		t.Errorf("LeaseHeldError.HolderEpoch = %d, want %d (holder's persisted epoch)", held.HolderEpoch, first.Epoch())
	}
}

// TestLeasePathContainment proves names that would escape <root>/leases are
// rejected by the containment guard with a typed *LeasePathError (defense in
// depth: ValidateName already forbids these at the public boundary).
func TestLeasePathContainment(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	s, err := newLeaserStore(root)
	if err != nil {
		t.Fatalf("newLeaserStore: %v", err)
	}
	cases := []struct {
		label string
		name  string
	}{
		{label: "empty maps to leases root", name: ""},
		{label: "parent traversal", name: ".."},
		{label: "escaping traversal", name: "../escape"},
		{label: "deep escape", name: "../../etc/passwd"},
		{label: "interior traversal", name: "a/../../escape"},
	}
	for _, tc := range cases {
		t.Run(tc.label, func(t *testing.T) {
			t.Parallel()
			got, err := s.pathFor(tc.name)
			var lpe *LeasePathError
			if !errors.As(err, &lpe) {
				t.Fatalf("pathFor(%q) = (%q, %v), want *LeasePathError", tc.name, got, err)
			}
			if got != "" {
				t.Errorf("pathFor(%q) path = %q, want empty on rejection", tc.name, got)
			}
		})
	}
}

// TestNewLeaserStoreValidation covers root validation and the 0700 leases
// directory the constructor must create.
func TestNewLeaserStoreValidation(t *testing.T) {
	t.Parallel()
	cases := []struct {
		label   string
		root    func(t *testing.T) string
		wantErr bool
	}{
		{label: "valid temp dir", root: func(t *testing.T) string { return t.TempDir() }, wantErr: false},
		{label: "empty root", root: func(t *testing.T) string { return "" }, wantErr: true},
		{label: "creates nested root", root: func(t *testing.T) string {
			return filepath.Join(t.TempDir(), "a", "b", "store")
		}, wantErr: false},
	}
	for _, tc := range cases {
		t.Run(tc.label, func(t *testing.T) {
			t.Parallel()
			s, err := newLeaserStore(tc.root(t))
			if (err != nil) != tc.wantErr {
				t.Fatalf("newLeaserStore err = %v, wantErr %v", err, tc.wantErr)
			}
			if tc.wantErr {
				var lre *LeaseRootError
				if !errors.As(err, &lre) {
					t.Errorf("want *LeaseRootError, got %T: %v", err, err)
				}
				return
			}
			info, serr := os.Stat(filepath.Join(s.root, "leases"))
			if serr != nil {
				t.Fatalf("stat leases: %v", serr)
			}
			if !info.IsDir() {
				t.Errorf("leases is not a directory")
			}
			if perm := info.Mode().Perm(); perm != 0o700 {
				t.Errorf("leases perm = %o, want 0700", perm)
			}
		})
	}
}

// TestLeaseAcquireCorruptEpochFailsClosed proves that a lock file whose persisted
// counter is present but not a valid decimal makes Acquire fail closed with a
// *LeaseCorruptError rather than silently reset the epoch to 0 (which would let a
// prior holder's epoch be reissued, breaking the strictly-increasing guarantee).
func TestLeaseAcquireCorruptEpochFailsClosed(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cases := []struct {
		label   string
		content string
	}{
		{label: "non-numeric", content: "not-a-number\n"},
		{label: "trailing garbage", content: "12x\n"},
		{label: "negative", content: "-1\n"},
	}
	for _, tc := range cases {
		t.Run(tc.label, func(t *testing.T) {
			t.Parallel()
			const name = "sessions/corrupt"
			root := t.TempDir()
			s, err := newLeaserStore(root)
			if err != nil {
				t.Fatalf("newLeaserStore: %v", err)
			}
			path := leaseLockPath(root, name)
			if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
				t.Fatalf("mkdir: %v", err)
			}
			if err := os.WriteFile(path, []byte(tc.content), 0o600); err != nil {
				t.Fatalf("seed corrupt lock file: %v", err)
			}
			_, err = s.Acquire(ctx, name)
			var lce *LeaseCorruptError
			if !errors.As(err, &lce) {
				t.Fatalf("Acquire on corrupt epoch = %v, want *LeaseCorruptError", err)
			}
			if !errors.Is(err, errBadEpoch) {
				t.Errorf("errors.Is(err, errBadEpoch) = false, want true (err = %v)", err)
			}
		})
	}
}

// TestLeaseReleaseFreesLockForReacquire proves Release actually drops the OS
// advisory lock (not just closes Lost), so the same name can be re-acquired by a
// fresh fd afterward — the fs-level counterpart to the conformance grant->release
// ->grant epoch check.
func TestLeaseReleaseFreesLockForReacquire(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	const name = "sessions/reacquire"
	root := t.TempDir()
	s, err := newLeaserStore(root)
	if err != nil {
		t.Fatalf("newLeaserStore: %v", err)
	}
	first, err := s.Acquire(ctx, name)
	if err != nil {
		t.Fatalf("first Acquire: %v", err)
	}
	if err := first.Release(ctx); err != nil {
		t.Fatalf("Release: %v", err)
	}
	second, err := s.Acquire(ctx, name)
	if err != nil {
		t.Fatalf("re-Acquire after Release = %v, want success (lock must be freed)", err)
	}
	t.Cleanup(func() { _ = second.Release(ctx) })
	if second.Epoch() <= first.Epoch() {
		t.Errorf("re-acquired Epoch() = %d, want strictly greater than %d", second.Epoch(), first.Epoch())
	}
}
