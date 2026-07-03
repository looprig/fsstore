package fsstore

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/looprig/storekit"
)

// blobFilePath returns the on-disk path the Blobs backend maps key to, so a test
// can inspect or corrupt the underlying file directly.
func blobFilePath(root, key string) string {
	return filepath.Join(root, "blobs", filepath.FromSlash(key))
}

// TestBlobsIdenticalNoOpVsConflict checks the content-addressed re-Put semantics at
// the filesystem level: a byte-identical re-Put does NOT rewrite the file (its mtime
// is unchanged), and a different-content re-Put is rejected with a conflict leaving
// the original bytes intact.
func TestBlobsIdenticalNoOpVsConflict(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	original := []byte("content-addressed original bytes")

	t.Run("identical re-Put does not rewrite", func(t *testing.T) {
		t.Parallel()
		root := t.TempDir()
		s, err := newBlobStore(root)
		if err != nil {
			t.Fatalf("newBlobStore: %v", err)
		}
		const key = "blobs/identical"
		if err := s.Put(ctx, key, bytes.NewReader(original)); err != nil {
			t.Fatalf("first Put: %v", err)
		}
		// Stamp a distinctive old mtime; a rewrite via temp+rename would replace it.
		path := blobFilePath(root, key)
		old := time.Now().Add(-2 * time.Hour).Truncate(time.Second)
		if err := os.Chtimes(path, old, old); err != nil {
			t.Fatalf("Chtimes: %v", err)
		}
		if err := s.Put(ctx, key, bytes.NewReader(original)); err != nil {
			t.Fatalf("identical re-Put = %v, want nil (no-op)", err)
		}
		info, serr := os.Stat(path)
		if serr != nil {
			t.Fatalf("stat: %v", serr)
		}
		if !info.ModTime().Equal(old) {
			t.Errorf("identical re-Put changed mtime to %v, want unchanged %v (no rewrite)", info.ModTime(), old)
		}
	})

	t.Run("different re-Put conflicts leaving original", func(t *testing.T) {
		t.Parallel()
		root := t.TempDir()
		s, err := newBlobStore(root)
		if err != nil {
			t.Fatalf("newBlobStore: %v", err)
		}
		const key = "blobs/conflict"
		if err := s.Put(ctx, key, bytes.NewReader(original)); err != nil {
			t.Fatalf("first Put: %v", err)
		}
		path := blobFilePath(root, key)
		old := time.Now().Add(-2 * time.Hour).Truncate(time.Second)
		if err := os.Chtimes(path, old, old); err != nil {
			t.Fatalf("Chtimes: %v", err)
		}
		err = s.Put(ctx, key, bytes.NewReader([]byte("totally different bytes")))
		var bc *storekit.BlobConflictError
		if !errors.As(err, &bc) {
			t.Fatalf("different re-Put = %v, want *BlobConflictError", err)
		}
		if bc.Key != key {
			t.Errorf("BlobConflictError.Key = %q, want %q", bc.Key, key)
		}
		info, serr := os.Stat(path)
		if serr != nil {
			t.Fatalf("stat: %v", serr)
		}
		if !info.ModTime().Equal(old) {
			t.Errorf("rejected Put changed mtime, want unchanged (original untouched)")
		}
		on, gerr := os.ReadFile(path) //nolint:gosec // test-controlled path
		if gerr != nil {
			t.Fatalf("read file: %v", gerr)
		}
		if !bytes.Equal(on, original) {
			t.Errorf("on-disk bytes = %q, want original %q", on, original)
		}
	})
}

// TestBlobsTempRenameNoPartial checks that the temp-file + atomic rename write path
// leaves no stray or partial temp file behind after a create followed by a rejected
// conflicting Put.
func TestBlobsTempRenameNoPartial(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	const key = "blobs/atomic"
	root := t.TempDir()
	s, err := newBlobStore(root)
	if err != nil {
		t.Fatalf("newBlobStore: %v", err)
	}
	if err := s.Put(ctx, key, bytes.NewReader([]byte("original"))); err != nil {
		t.Fatalf("Put(create): %v", err)
	}
	if err := s.Put(ctx, key, bytes.NewReader([]byte("different"))); !errors.As(err, new(*storekit.BlobConflictError)) {
		t.Fatalf("Put(conflict) = %v, want *BlobConflictError", err)
	}
	dir := filepath.Dir(blobFilePath(root, key))
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) != 1 || entries[0].Name() != "atomic" {
		var names []string
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Fatalf("dir holds %v, want exactly the one committed file %q", names, "atomic")
	}
}

// TestBlobsGetIndependentHandles verifies each Get returns a fresh, independent
// io.ReadCloser: two concurrent readers of the same key both read the full content,
// and closing one does not disturb the other.
func TestBlobsGetIndependentHandles(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	const key = "blobs/handles"
	root := t.TempDir()
	s, err := newBlobStore(root)
	if err != nil {
		t.Fatalf("newBlobStore: %v", err)
	}
	content := []byte("independent-handle content")
	if err := s.Put(ctx, key, bytes.NewReader(content)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	rc1, err := s.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get 1: %v", err)
	}
	rc2, err := s.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get 2: %v", err)
	}
	got1, err := io.ReadAll(rc1)
	if err != nil {
		t.Fatalf("ReadAll 1: %v", err)
	}
	if cerr := rc1.Close(); cerr != nil {
		t.Errorf("Close 1: %v", cerr)
	}
	// rc2 must still read the full content after rc1 is drained and closed.
	got2, err := io.ReadAll(rc2)
	if err != nil {
		t.Fatalf("ReadAll 2: %v", err)
	}
	if cerr := rc2.Close(); cerr != nil {
		t.Errorf("Close 2: %v", cerr)
	}
	if !bytes.Equal(got1, content) || !bytes.Equal(got2, content) {
		t.Errorf("independent reads = %q / %q, want both %q", got1, got2, content)
	}
}

func TestBlobsNamePathMapping(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cases := []struct {
		label string
		key   string
		rel   []string // path segments under blobs
	}{
		{label: "single segment", key: "solo", rel: []string{"solo"}},
		{label: "two segments", key: "a/b", rel: []string{"a", "b"}},
		{label: "deep nesting", key: "deep/nested/key", rel: []string{"deep", "nested", "key"}},
	}
	for _, tc := range cases {
		t.Run(tc.label, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			s, err := newBlobStore(root)
			if err != nil {
				t.Fatalf("newBlobStore: %v", err)
			}
			if err := s.Put(ctx, tc.key, bytes.NewReader([]byte("p"))); err != nil {
				t.Fatalf("Put: %v", err)
			}
			want := filepath.Join(append([]string{root, "blobs"}, tc.rel...)...)
			if _, err := os.Stat(want); err != nil {
				t.Fatalf("expected blob file at %q: %v", want, err)
			}
			got, perr := s.pathFor(tc.key)
			if perr != nil {
				t.Fatalf("pathFor(%q): %v", tc.key, perr)
			}
			if got != want {
				t.Errorf("pathFor(%q) = %q, want %q", tc.key, got, want)
			}
		})
	}
}

func TestBlobsPathContainment(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	s, err := newBlobStore(root)
	if err != nil {
		t.Fatalf("newBlobStore: %v", err)
	}
	cases := []struct {
		label string
		key   string
	}{
		{label: "empty maps to blobs root", key: ""},
		{label: "parent traversal", key: ".."},
		{label: "escaping traversal", key: "../escape"},
		{label: "deep escape", key: "../../etc/passwd"},
		{label: "interior traversal", key: "a/../../escape"},
	}
	for _, tc := range cases {
		t.Run(tc.label, func(t *testing.T) {
			t.Parallel()
			got, err := s.pathFor(tc.key)
			if !errors.As(err, new(*BlobPathError)) {
				t.Fatalf("pathFor(%q) = (%q, %v), want *BlobPathError", tc.key, got, err)
			}
			if got != "" {
				t.Errorf("pathFor(%q) path = %q, want empty on rejection", tc.key, got)
			}
		})
	}
}

func TestNewBlobStoreValidation(t *testing.T) {
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
			s, err := newBlobStore(tc.root(t))
			if (err != nil) != tc.wantErr {
				t.Fatalf("newBlobStore err = %v, wantErr %v", err, tc.wantErr)
			}
			if tc.wantErr {
				if !errors.As(err, new(*BlobRootError)) {
					t.Errorf("want *BlobRootError, got %T: %v", err, err)
				}
				return
			}
			info, serr := os.Stat(filepath.Join(s.root, "blobs"))
			if serr != nil {
				t.Fatalf("stat blobs: %v", serr)
			}
			if !info.IsDir() {
				t.Errorf("blobs is not a directory")
			}
			if perm := info.Mode().Perm(); perm != 0o700 {
				t.Errorf("blobs perm = %o, want 0700", perm)
			}
		})
	}
}

// TestBlobsLargeRoundTrip exercises the 1 MiB payload floor through the filesystem
// backend and confirms the bytes survive a reopen byte-equal.
func TestBlobsLargeRoundTrip(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	const key = "blobs/large"
	root := t.TempDir()
	s, err := newBlobStore(root)
	if err != nil {
		t.Fatalf("newBlobStore: %v", err)
	}
	content := make([]byte, 1<<20)
	for i := range content {
		content[i] = byte(i * 31)
	}
	if err := s.Put(ctx, key, bytes.NewReader(content)); err != nil {
		t.Fatalf("Put(1 MiB): %v", err)
	}
	s2, err := newBlobStore(root)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	rc, err := s2.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if cerr := rc.Close(); cerr != nil {
		t.Errorf("Close: %v", cerr)
	}
	if !bytes.Equal(got, content) {
		t.Errorf("1 MiB blob did not round-trip byte-equal")
	}
}
