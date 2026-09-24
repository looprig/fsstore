package fsstore

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/looprig/storage"
)

// kvFilePath returns the on-disk path the KV backend maps key to, so a test can
// inspect or corrupt the underlying file directly.
func kvFilePath(root, key string) string {
	return filepath.Join(root, "kv", filepath.FromSlash(key)) + "@kv"
}

// TestKVRevHeaderOnDisk verifies the on-disk file layout: an 8-byte little-endian
// revision header followed by the raw value bytes, and that the revision advances
// by one per successful Put.
func TestKVRevHeaderOnDisk(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	const key = "sessions/hdr"
	root := t.TempDir()
	s, err := newKVStore(root)
	if err != nil {
		t.Fatalf("newKVStore: %v", err)
	}

	cases := []struct {
		label string
		val   []byte
	}{
		{label: "first value", val: []byte("alpha")},
		{label: "second value", val: []byte("beta-longer")},
		{label: "empty value", val: []byte{}},
	}
	var lastRev uint64
	for i, tc := range cases {
		t.Run(tc.label, func(t *testing.T) {
			rev, perr := s.Put(ctx, key, lastRev, tc.val)
			if perr != nil {
				t.Fatalf("Put: %v", perr)
			}
			if rev != uint64(i+1) {
				t.Fatalf("Put rev = %d, want %d", rev, i+1)
			}
			data, rerr := os.ReadFile(kvFilePath(root, key)) //nolint:gosec // test-controlled path
			if rerr != nil {
				t.Fatalf("read file: %v", rerr)
			}
			if len(data) < 8 {
				t.Fatalf("file has %d bytes, want at least the 8-byte rev header", len(data))
			}
			if gotRev := binary.LittleEndian.Uint64(data[:8]); gotRev != rev {
				t.Errorf("on-disk rev header = %d, want %d", gotRev, rev)
			}
			if got := data[8:]; !bytes.Equal(got, tc.val) {
				t.Errorf("on-disk value = %q, want %q", got, tc.val)
			}
			lastRev = rev
		})
	}
}

// TestKVReopenPersistsRev checks that a fresh newKVStore over the same root reads
// the durably-persisted revision and value back, and can continue the CAS chain
// from the persisted revision.
func TestKVReopenPersistsRev(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	const key = "sessions/persist"
	cases := []struct {
		label string
		puts  int
	}{
		{label: "one put", puts: 1},
		{label: "few puts", puts: 3},
		{label: "many puts", puts: 12},
	}
	for _, tc := range cases {
		t.Run(tc.label, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			s, err := newKVStore(root)
			if err != nil {
				t.Fatalf("newKVStore: %v", err)
			}
			var rev uint64
			var lastVal []byte
			for i := 0; i < tc.puts; i++ {
				lastVal = []byte("v" + strings.Repeat("x", i))
				rev, err = s.Put(ctx, key, rev, lastVal)
				if err != nil {
					t.Fatalf("Put %d: %v", i, err)
				}
			}

			// A fresh store on the same root observes the persisted rev + value.
			s2, err := newKVStore(root)
			if err != nil {
				t.Fatalf("reopen newKVStore: %v", err)
			}
			gotVal, gotRev, gerr := s2.Get(ctx, key)
			if gerr != nil {
				t.Fatalf("Get after reopen: %v", gerr)
			}
			if gotRev != uint64(tc.puts) {
				t.Errorf("reopened rev = %d, want %d", gotRev, tc.puts)
			}
			if !bytes.Equal(gotVal, lastVal) {
				t.Errorf("reopened value = %q, want %q", gotVal, lastVal)
			}

			// A stale-rev Put must conflict against the persisted rev.
			if _, err := s2.Put(ctx, key, 0, []byte("stale")); !errors.As(err, new(*storage.ConflictError)) {
				t.Errorf("Put(expectedRev=0) after reopen = %v, want *ConflictError", err)
			}
			// The correct rev continues the chain.
			next, err := s2.Put(ctx, key, uint64(tc.puts), []byte("next"))
			if err != nil {
				t.Fatalf("Put(correct rev) after reopen: %v", err)
			}
			if next != uint64(tc.puts)+1 {
				t.Errorf("continued rev = %d, want %d", next, tc.puts+1)
			}
		})
	}
}

// TestKVTempRenameNoPartial is a best-effort check that the temp-file + atomic
// rename write path never leaves a partial or stray temp file behind: after a
// series of successful Puts and a rejected (conflicting) Put, the key's directory
// holds exactly the one committed file and the original content is intact.
func TestKVTempRenameNoPartial(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	const key = "sessions/atomic"
	root := t.TempDir()
	s, err := newKVStore(root)
	if err != nil {
		t.Fatalf("newKVStore: %v", err)
	}

	rev, err := s.Put(ctx, key, 0, []byte("original"))
	if err != nil {
		t.Fatalf("Put(create): %v", err)
	}
	if _, err := s.Put(ctx, key, rev, []byte("second")); err != nil {
		t.Fatalf("Put(update): %v", err)
	}
	// A conflicting Put must not stage a lingering temp file.
	if _, err := s.Put(ctx, key, 0, []byte("nope")); !errors.As(err, new(*storage.ConflictError)) {
		t.Fatalf("Put(conflict) = %v, want *ConflictError", err)
	}

	dir := filepath.Dir(kvFilePath(root, key))
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) != 1 {
		var names []string
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Fatalf("dir holds %d entries %v, want exactly the one committed file", len(entries), names)
	}
	if got := entries[0].Name(); got != "atomic@kv" {
		t.Errorf("committed file name = %q, want %q", got, "atomic@kv")
	}

	// The committed content is the last successful write, unaffected by the reject.
	gotVal, gotRev, gerr := s.Get(ctx, key)
	if gerr != nil {
		t.Fatalf("Get: %v", gerr)
	}
	if gotRev != 2 || !bytes.Equal(gotVal, []byte("second")) {
		t.Errorf("Get = (%q, %d), want (%q, 2)", gotVal, gotRev, "second")
	}
}

// TestKVCorruptShortHeaderFailsClosed verifies that a file too short to hold the
// rev header fails closed (a *KVCorruptError) on both Get and Put rather than
// silently reporting rev 0.
func TestKVCorruptShortHeaderFailsClosed(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	const key = "sessions/short"
	root := t.TempDir()
	s, err := newKVStore(root)
	if err != nil {
		t.Fatalf("newKVStore: %v", err)
	}
	path := kvFilePath(root, key)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(path, []byte{0x01, 0x02, 0x03}, 0o600); err != nil {
		t.Fatalf("write short file: %v", err)
	}

	if _, _, gerr := s.Get(ctx, key); !errors.As(gerr, new(*KVCorruptError)) {
		t.Errorf("Get on short file = %v, want *KVCorruptError", gerr)
	}
	if _, perr := s.Put(ctx, key, 0, []byte("x")); !errors.As(perr, new(*KVCorruptError)) {
		t.Errorf("Put on short file = %v, want *KVCorruptError", perr)
	}
}

func TestKVNamePathMapping(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cases := []struct {
		label string
		key   string
		rel   []string // path segments under kv; the leaf carries the "@kv" suffix
	}{
		{label: "single segment", key: "solo", rel: []string{"solo@kv"}},
		{label: "two segments", key: "a/b", rel: []string{"a", "b@kv"}},
		{label: "deep nesting", key: "deep/nested/key", rel: []string{"deep", "nested", "key@kv"}},
	}
	for _, tc := range cases {
		t.Run(tc.label, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			s, err := newKVStore(root)
			if err != nil {
				t.Fatalf("newKVStore: %v", err)
			}
			if _, err := s.Put(ctx, tc.key, 0, []byte("p")); err != nil {
				t.Fatalf("Put: %v", err)
			}
			want := filepath.Join(append([]string{root, "kv"}, tc.rel...)...)
			if _, err := os.Stat(want); err != nil {
				t.Fatalf("expected kv file at %q: %v", want, err)
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

func TestKVPathContainment(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	s, err := newKVStore(root)
	if err != nil {
		t.Fatalf("newKVStore: %v", err)
	}
	cases := []struct {
		label string
		key   string
	}{
		{label: "empty maps to kv root", key: ""},
		{label: "parent traversal", key: ".."},
		{label: "escaping traversal", key: "../escape"},
		{label: "deep escape", key: "../../etc/passwd"},
		{label: "interior traversal", key: "a/../../escape"},
	}
	for _, tc := range cases {
		t.Run(tc.label, func(t *testing.T) {
			t.Parallel()
			got, err := s.pathFor(tc.key)
			if !errors.As(err, new(*KVPathError)) {
				t.Fatalf("pathFor(%q) = (%q, %v), want *KVPathError", tc.key, got, err)
			}
			if got != "" {
				t.Errorf("pathFor(%q) path = %q, want empty on rejection", tc.key, got)
			}
		})
	}
}

func TestNewKVStoreValidation(t *testing.T) {
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
			s, err := newKVStore(tc.root(t))
			if (err != nil) != tc.wantErr {
				t.Fatalf("newKVStore err = %v, wantErr %v", err, tc.wantErr)
			}
			if tc.wantErr {
				if !errors.As(err, new(*KVRootError)) {
					t.Errorf("want *KVRootError, got %T: %v", err, err)
				}
				return
			}
			info, serr := os.Stat(filepath.Join(s.root, "kv"))
			if serr != nil {
				t.Fatalf("stat kv: %v", serr)
			}
			if !info.IsDir() {
				t.Errorf("kv is not a directory")
			}
			if perm := info.Mode().Perm(); perm != 0o700 {
				t.Errorf("kv perm = %o, want 0700", perm)
			}
		})
	}
}

// TestKVLargeValueRoundTrip exercises the 1 MiB payload floor directly through the
// filesystem backend (the conformance suite also covers it) and confirms the value
// survives a reopen byte-equal.
func TestKVLargeValueRoundTrip(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	const key = "sessions/large"
	root := t.TempDir()
	s, err := newKVStore(root)
	if err != nil {
		t.Fatalf("newKVStore: %v", err)
	}
	val := make([]byte, 1<<20)
	for i := range val {
		val[i] = byte(i * 31)
	}
	if _, err := s.Put(ctx, key, 0, val); err != nil {
		t.Fatalf("Put(1 MiB): %v", err)
	}
	s2, err := newKVStore(root)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	got, rev, gerr := s2.Get(ctx, key)
	if gerr != nil {
		t.Fatalf("Get: %v", gerr)
	}
	if rev != 1 {
		t.Errorf("rev = %d, want 1", rev)
	}
	if !bytes.Equal(got, val) {
		t.Errorf("1 MiB value did not round-trip byte-equal")
	}
}
