package fsstore

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/looprig/storage"
)

// This file pins fsstore v0.6.0's KV/Blobs leaf layout: every key's leaf file
// carries a suffix whose first byte the storage name grammar forbids, so a leaf
// file can never share a directory entry with the directory that holds a longer
// key's segments. It also pins Open's fail-closed refusal of a pre-v0.6.0 root.

// leafLayoutBackend abstracts KV and Blobs so the injectivity table runs once per
// primitive. put stores a value that encodes the key, get returns it, list is
// Keys/List.
type leafLayoutBackend struct {
	name string
	dir  string // "kv" or "blobs"
	ext  string // the literal on-disk leaf suffix
	open func(t *testing.T, root string) (put func(key string, val []byte) error, get func(key string) ([]byte, error), del func(key string) error, list func(prefix string) ([]string, error))
}

func leafLayoutBackends() []leafLayoutBackend {
	ctx := context.Background()
	return []leafLayoutBackend{
		{
			name: "kv", dir: "kv", ext: "@kv",
			open: func(t *testing.T, root string) (func(string, []byte) error, func(string) ([]byte, error), func(string) error, func(string) ([]string, error)) {
				t.Helper()
				s, err := newKVStore(root)
				if err != nil {
					t.Fatalf("newKVStore: %v", err)
				}
				put := func(key string, val []byte) error {
					_, err := s.Put(ctx, key, 0, val)
					return err
				}
				get := func(key string) ([]byte, error) {
					v, _, err := s.Get(ctx, key)
					return v, err
				}
				del := func(key string) error { return s.Delete(ctx, key) }
				list := func(prefix string) ([]string, error) { return s.Keys(ctx, prefix) }
				return put, get, del, list
			},
		},
		{
			name: "blobs", dir: "blobs", ext: "@blob",
			open: func(t *testing.T, root string) (func(string, []byte) error, func(string) ([]byte, error), func(string) error, func(string) ([]string, error)) {
				t.Helper()
				s, err := newBlobStore(root)
				if err != nil {
					t.Fatalf("newBlobStore: %v", err)
				}
				put := func(key string, val []byte) error { return s.Put(ctx, key, bytes.NewReader(val)) }
				get := func(key string) ([]byte, error) {
					rc, err := s.Get(ctx, key)
					if err != nil {
						return nil, err
					}
					defer func() { _ = rc.Close() }()
					return io.ReadAll(rc)
				}
				del := func(key string) error { return s.Delete(ctx, key) }
				list := func(prefix string) ([]string, error) { return s.List(ctx, prefix) }
				return put, get, del, list
			},
		},
	}
}

// TestLeafSuffixInjective stores, in both creation orders, sets of keys whose
// naive encodings would collide: a key beside its '/' extension, a key beside a
// key spelled like its own suffixed leaf ("a.kv", "a@kv" is not a valid name),
// and a key beside a directory named after another key's plain-dotted suffix. A
// ".kv"-style suffix fails the "dotted suffix directory" row: key "a" would own
// file "a.kv" while key "a.kv/b" needs directory "a.kv".
func TestLeafSuffixInjective(t *testing.T) {
	t.Parallel()
	cases := []struct {
		label string
		keys  []string
	}{
		{label: "key and extension", keys: []string{"sessions/a", "sessions/a/b"}},
		{label: "extension and key", keys: []string{"sessions/a/b", "sessions/a"}},
		{label: "key ending in .kv", keys: []string{"a.kv", "a", "a.kv/b"}},
		{label: "key ending in .blob", keys: []string{"a.blob", "a", "a.blob/b"}},
		{label: "dotted suffix directory kv", keys: []string{"a", "a.kv/b"}},
		{label: "dotted suffix directory blob", keys: []string{"a", "a.blob/b"}},
		{label: "dotted suffix directory reversed", keys: []string{"a.kv/b", "a.blob/b", "a"}},
		{label: "key ending in kv without dot", keys: []string{"akv", "a", "a/kv"}},
		{label: "deep chain", keys: []string{"x/y/z", "x", "x/y", "x/y/z/w"}},
	}
	for _, be := range leafLayoutBackends() {
		for _, tc := range cases {
			t.Run(be.name+"/"+tc.label, func(t *testing.T) {
				t.Parallel()
				root := t.TempDir()
				put, get, del, list := be.open(t, root)
				for _, k := range tc.keys {
					if err := put(k, []byte("v:"+k)); err != nil {
						t.Fatalf("put %q: %v", k, err)
					}
				}
				for _, k := range tc.keys {
					got, err := get(k)
					if err != nil || string(got) != "v:"+k {
						t.Fatalf("get %q = (%q, %v), want %q", k, got, err, "v:"+k)
					}
					// On disk the leaf is the key's path plus the literal suffix.
					leaf := filepath.Join(root, be.dir, filepath.FromSlash(k)) + be.ext
					if info, err := os.Lstat(leaf); err != nil || !info.Mode().IsRegular() {
						t.Fatalf("leaf %q: info=%v err=%v, want regular file", leaf, info, err)
					}
				}
				want := append([]string(nil), tc.keys...)
				sort.Strings(want)
				got, err := list("")
				if err != nil {
					t.Fatalf("list: %v", err)
				}
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("list(\"\") = %q, want %q", got, want)
				}
				// Deleting the first key leaves every other key intact.
				if err := del(tc.keys[0]); err != nil {
					t.Fatalf("delete %q: %v", tc.keys[0], err)
				}
				for _, k := range tc.keys[1:] {
					if got, err := get(k); err != nil || string(got) != "v:"+k {
						t.Fatalf("after delete, get %q = (%q, %v)", k, got, err)
					}
				}
				got, err = list("")
				if err != nil {
					t.Fatalf("list after delete: %v", err)
				}
				if !reflect.DeepEqual(got, without(want, tc.keys[0])) {
					t.Fatalf("list after delete = %q, want %q", got, without(want, tc.keys[0]))
				}
			})
		}
	}
}

// TestLeafListingSkipsNonLeaves plants every kind of non-leaf entry a directory
// under kv/ or blobs/ can hold and proves listing reports exactly the committed
// keys: crash-leftover temp files (both plain and suffix-shaped), unsuffixed
// legacy-shaped files, dotfiles, files whose decoded name is not a valid key,
// and a bare suffix. The planted names bypass the backend and are written raw.
func TestLeafListingSkipsNonLeaves(t *testing.T) {
	t.Parallel()
	for _, be := range leafLayoutBackends() {
		t.Run(be.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			put, get, _, list := be.open(t, root)
			for _, k := range []string{"sessions/a", "sessions/a/b"} {
				if err := put(k, []byte(k)); err != nil {
					t.Fatalf("put %q: %v", k, err)
				}
			}
			base := filepath.Join(root, be.dir)
			planted := []string{
				"sessions/" + fsTmpPrefix + "123456",          // crash-leftover temp
				"sessions/" + fsTmpPrefix + "123456" + be.ext, // temp that happens to end in the suffix
				"sessions/legacy",                             // pre-v0.6.0 leaf, no suffix
				"sessions/.DS_Store",                          // foreign dotfile
				"sessions/Upper" + be.ext,                     // decodes to an invalid key
				"sessions/" + be.ext,                          // a bare suffix decodes to an empty segment
				"sessions/a/b" + be.ext + be.ext,              // decodes to "b@kv", not a valid key
			}
			for _, p := range planted {
				full := filepath.Join(base, filepath.FromSlash(p))
				if err := os.WriteFile(full, []byte("junk"), 0o600); err != nil {
					t.Fatalf("plant %q: %v", p, err)
				}
			}
			for _, prefix := range []string{"", "sessions/", "sessions/a"} {
				got, err := list(prefix)
				if err != nil {
					t.Fatalf("list(%q): %v", prefix, err)
				}
				if want := []string{"sessions/a", "sessions/a/b"}; !reflect.DeepEqual(got, want) {
					t.Fatalf("list(%q) = %q, want %q", prefix, got, want)
				}
			}
			// The prefix filters decoded keys, never encoded file names: a prefix
			// spelled with the suffix matches no key.
			for _, prefix := range []string{"sessions/a" + be.ext, "sessions/a/b@", "@", "sessions/.f"} {
				got, err := list(prefix)
				if err != nil {
					t.Fatalf("list(%q): %v", prefix, err)
				}
				if len(got) != 0 {
					t.Fatalf("list(%q) = %q, want none", prefix, got)
				}
			}
			// The committed keys still read back; a planted temp file does not
			// interfere with a fresh write beside it.
			if err := put("sessions/c", []byte("c")); err != nil {
				t.Fatalf("put beside temp: %v", err)
			}
			if got, err := get("sessions/c"); err != nil || string(got) != "c" {
				t.Fatalf("get beside temp = (%q, %v)", got, err)
			}
		})
	}
}

// TestLeafWriteLeavesNoTempFiles proves a successful write renames its staging
// file away: after several creates and overwrites no fsTmpPrefix entry remains.
func TestLeafWriteLeavesNoTempFiles(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	root := t.TempDir()
	kv, err := newKVStore(root)
	if err != nil {
		t.Fatalf("newKVStore: %v", err)
	}
	blobs, err := newBlobStore(root)
	if err != nil {
		t.Fatalf("newBlobStore: %v", err)
	}
	rev := uint64(0)
	for i := 0; i < 3; i++ {
		if rev, err = kv.Put(ctx, "sessions/a", rev, []byte{byte(i)}); err != nil {
			t.Fatalf("kv put: %v", err)
		}
		if _, err = kv.Put(ctx, "sessions/a/b"+strings.Repeat("x", i), 0, []byte{byte(i)}); err != nil {
			t.Fatalf("kv put nested: %v", err)
		}
		if err = blobs.Put(ctx, "sessions/a"+strings.Repeat("x", i), bytes.NewReader([]byte{byte(i)})); err != nil {
			t.Fatalf("blob put: %v", err)
		}
	}
	for _, dir := range []string{"kv", "blobs"} {
		werr := filepath.WalkDir(filepath.Join(root, dir), func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if strings.HasPrefix(d.Name(), fsTmpPrefix) {
				t.Errorf("stray temp file %q", path)
			}
			return nil
		})
		if werr != nil {
			t.Fatalf("walk %s: %v", dir, werr)
		}
	}
}

// TestOpenLegacyLayout proves Open fails closed, with *LegacyLayoutError
// (errors.Is ErrLegacyLayout), on any root holding a pre-v0.6.0 leaf in any
// primitive's tree — a file whose name is a valid storage segment, e.g. an
// unsuffixed KV/Blob leaf or a "*.log"/"*.lock"/"*.olog" file — at any depth, and
// that roots holding only things v0.6.0 itself writes (or harmless leftovers)
// open and keep their data.
func TestOpenLegacyLayout(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	type plant struct {
		rel  string // path under root, slash-separated
		data string
		dir  bool
	}
	cases := []struct {
		label         string
		seed          func(t *testing.T, root string) // optional: populate through a v0.6.0 Open
		plants        []plant
		wantPrimitive string // "" = Open must succeed
		wantLeaf      string // rel path of the reported legacy leaf
	}{
		{label: "fresh root"},
		{label: "v0.5 kv leaf", plants: []plant{{rel: "kv/sessions/abc", data: "\x01\x00\x00\x00\x00\x00\x00\x00{}"}}, wantPrimitive: "kv", wantLeaf: "kv/sessions/abc"},
		{label: "v0.5 kv top-level leaf", plants: []plant{{rel: "kv/solo", data: "x"}}, wantPrimitive: "kv", wantLeaf: "kv/solo"},
		{label: "first legacy leaf is reported", plants: []plant{{rel: "kv/a/x", data: "x"}, {rel: "kv/b/y", data: "y"}}, wantPrimitive: "kv", wantLeaf: "kv/a/x"},
		{label: "kv reported before blobs", plants: []plant{{rel: "blobs/a", data: "x"}, {rel: "kv/z/z", data: "y"}}, wantPrimitive: "kv", wantLeaf: "kv/z/z"},
		{label: "v0.5 kv deep leaf", plants: []plant{{rel: "kv/a/b/c/d", data: "x"}}, wantPrimitive: "kv", wantLeaf: "kv/a/b/c/d"},
		{label: "v0.5 kv empty leaf", plants: []plant{{rel: "kv/a", data: ""}}, wantPrimitive: "kv", wantLeaf: "kv/a"},
		{label: "v0.5 kv leaf named like the new suffix", plants: []plant{{rel: "kv/a.kv", data: "x"}}, wantPrimitive: "kv", wantLeaf: "kv/a.kv"},
		{label: "v0.5 blob leaf", plants: []plant{{rel: "blobs/sessions/abc/blobs/deadbeef", data: "x"}}, wantPrimitive: "blobs", wantLeaf: "blobs/sessions/abc/blobs/deadbeef"},
		{label: "v0.5 blob leaf named like the new suffix", plants: []plant{{rel: "blobs/a.blob", data: "x"}}, wantPrimitive: "blobs", wantLeaf: "blobs/a.blob"},
		{label: "legacy leaf beside v0.6 data (rollback write)", seed: seedCurrentLayout, plants: []plant{{rel: "kv/sessions/rolled", data: "x"}}, wantPrimitive: "kv", wantLeaf: "kv/sessions/rolled"},
		{label: "legacy blob beside v0.6 data", seed: seedCurrentLayout, plants: []plant{{rel: "blobs/sessions/rolled", data: "x"}}, wantPrimitive: "blobs", wantLeaf: "blobs/sessions/rolled"},
		{label: "only ledger lease ordered data", seed: seedNonLeafPrimitives},
		{label: "v0.6 data reopens", seed: seedCurrentLayout},
		{label: "legacy empty directories", plants: []plant{{rel: "kv/sessions/abc", dir: true}, {rel: "blobs/sessions/abc/blobs", dir: true}}},
		{label: "crash-leftover temp files", seed: seedCurrentLayout, plants: []plant{{rel: "kv/sessions/" + fsTmpPrefix + "4242", data: "partial"}, {rel: "blobs/" + fsTmpPrefix + "77", data: ""}}},
		{label: "foreign dotfiles and invalid names", plants: []plant{{rel: "kv/.DS_Store", data: "x"}, {rel: "blobs/sessions/Thumbs.DB", data: "x"}, {rel: "kv/sessions/UPPER", data: "x"}}},
		{label: "legacy-named file at the root, outside every tree", plants: []plant{{rel: "loose", data: "x"}, {rel: "notes.log", data: "x"}}},
		{label: "v0.5 ledger log", plants: []plant{{rel: "streams/sessions/abc.log", data: "frame"}}, wantPrimitive: "ledger", wantLeaf: "streams/sessions/abc.log"},
		{label: "v0.5 ledger log empty", plants: []plant{{rel: "streams/a.log", data: ""}}, wantPrimitive: "ledger", wantLeaf: "streams/a.log"},
		{label: "v0.5 leftover lock file (epoch counter)", plants: []plant{{rel: "leases/sessions/abc.lock", data: "7\n"}}, wantPrimitive: "leaser", wantLeaf: "leases/sessions/abc.lock"},
		{label: "v0.5 leftover empty lock file", plants: []plant{{rel: "leases/solo.lock", data: ""}}, wantPrimitive: "leaser", wantLeaf: "leases/solo.lock"},
		{label: "v0.5 ordered log", plants: []plant{{rel: "ordered/sessionstore/gates/0.olog", data: "x"}}, wantPrimitive: "ordered", wantLeaf: "ordered/sessionstore/gates/0.olog"},
		{label: "unsuffixed file in streams", plants: []plant{{rel: "streams/loose", data: "x"}}, wantPrimitive: "ledger", wantLeaf: "streams/loose"},
		{label: "legacy ledger beside v0.6 data", seed: seedNonLeafPrimitives, plants: []plant{{rel: "streams/sessions/old.log", data: "x"}}, wantPrimitive: "ledger", wantLeaf: "streams/sessions/old.log"},
		{label: "legacy lock beside v0.6 data", seed: seedNonLeafPrimitives, plants: []plant{{rel: "leases/sessions/a.lock", data: "3\n"}}, wantPrimitive: "leaser", wantLeaf: "leases/sessions/a.lock"},
		{label: "scan order kv blobs ledger leaser ordered", plants: []plant{{rel: "ordered/n.olog", data: "x"}, {rel: "leases/l.lock", data: "x"}, {rel: "streams/s.log", data: "x"}}, wantPrimitive: "ledger", wantLeaf: "streams/s.log"},
		{label: "leaser before ordered", plants: []plant{{rel: "ordered/n.olog", data: "x"}, {rel: "leases/l.lock", data: "x"}}, wantPrimitive: "leaser", wantLeaf: "leases/l.lock"},
		{label: "blobs before ledger", plants: []plant{{rel: "streams/s.log", data: "x"}, {rel: "blobs/b", data: "x"}}, wantPrimitive: "blobs", wantLeaf: "blobs/b"},
		{label: "crash-leftover dotfiles in every tree", plants: []plant{{rel: "streams/." + "x.log", data: "x"}, {rel: "leases/.DS_Store", data: "x"}, {rel: "ordered/.fsstore-tmp-1", data: "x"}}},
	}
	for _, tc := range cases {
		t.Run(tc.label, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			if tc.seed != nil {
				tc.seed(t, root)
			}
			for _, p := range tc.plants {
				full := filepath.Join(root, filepath.FromSlash(p.rel))
				if err := os.MkdirAll(filepath.Dir(full), 0o700); err != nil {
					t.Fatalf("mkdir: %v", err)
				}
				if p.dir {
					if err := os.MkdirAll(full, 0o700); err != nil {
						t.Fatalf("mkdir %q: %v", p.rel, err)
					}
					continue
				}
				if err := os.WriteFile(full, []byte(p.data), 0o600); err != nil {
					t.Fatalf("plant %q: %v", p.rel, err)
				}
			}
			s, err := Open(Options{Root: root})
			if tc.wantPrimitive == "" {
				if err != nil {
					t.Fatalf("Open = %v, want success", err)
				}
				defer func() { _ = s.Close() }()
				if tc.seed != nil {
					assertCurrentLayoutReadable(t, ctx, s, tc.seed)
				}
				return
			}
			if s != nil {
				_ = s.Close()
				t.Fatal("Open returned a Store alongside a legacy-layout root")
			}
			if !errors.Is(err, ErrLegacyLayout) {
				t.Fatalf("Open = %v, want errors.Is ErrLegacyLayout", err)
			}
			var lle *LegacyLayoutError
			if !errors.As(err, &lle) {
				t.Fatalf("Open = %T %v, want *LegacyLayoutError", err, err)
			}
			resolved, rerr := filepath.EvalSymlinks(root)
			if rerr != nil {
				t.Fatalf("EvalSymlinks: %v", rerr)
			}
			if lle.Root != resolved {
				t.Errorf("Root = %q, want %q", lle.Root, resolved)
			}
			if lle.Primitive != tc.wantPrimitive {
				t.Errorf("Primitive = %q, want %q", lle.Primitive, tc.wantPrimitive)
			}
			if want := filepath.Join(resolved, filepath.FromSlash(tc.wantLeaf)); lle.Path != want {
				t.Errorf("Path = %q, want %q", lle.Path, want)
			}
			for _, part := range []string{resolved, "pre-v0.6.0"} {
				if !strings.Contains(lle.Error(), part) {
					t.Errorf("Error() = %q, want it to mention %q", lle.Error(), part)
				}
			}
			// Refusal writes nothing: the legacy leaf is still there, untouched.
			if _, err := os.Stat(lle.Path); err != nil {
				t.Errorf("legacy leaf disturbed: %v", err)
			}
		})
	}
}

// TestOpenLegacyLayoutScanError proves an unreadable subtree of any primitive fails Open
// closed with a typed IO error rather than being treated as clean.
func TestOpenLegacyLayoutScanError(t *testing.T) {
	t.Parallel()
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	for _, dir := range []string{"kv", "blobs", "streams", "leases", "ordered"} {
		t.Run(dir, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			locked := filepath.Join(root, dir, "sessions")
			if err := os.MkdirAll(locked, 0o700); err != nil {
				t.Fatalf("mkdir: %v", err)
			}
			if err := os.Chmod(locked, 0o000); err != nil {
				t.Fatalf("chmod: %v", err)
			}
			t.Cleanup(func() { _ = os.Chmod(locked, 0o700) })
			s, err := Open(Options{Root: root})
			if err == nil {
				_ = s.Close()
				t.Fatal("Open succeeded over an unreadable subtree")
			}
			if errors.Is(err, ErrLegacyLayout) {
				t.Fatalf("Open = %v, want an IO error, not a legacy verdict", err)
			}
			switch dir {
			case "kv":
				var ioe *KVIOError
				if !errors.As(err, &ioe) || ioe.Op != "layout-scan" {
					t.Fatalf("Open = %T %v, want *KVIOError{Op: layout-scan}", err, err)
				}
			case "blobs":
				var ioe *BlobIOError
				if !errors.As(err, &ioe) || ioe.Op != "layout-scan" {
					t.Fatalf("Open = %T %v, want *BlobIOError{Op: layout-scan}", err, err)
				}
			case "streams":
				var ioe *LedgerIOError
				if !errors.As(err, &ioe) || ioe.Op != "layout-scan" {
					t.Fatalf("Open = %T %v, want *LedgerIOError{Op: layout-scan}", err, err)
				}
			case "leases":
				var ioe *LeaseIOError
				if !errors.As(err, &ioe) || ioe.Op != "layout-scan" {
					t.Fatalf("Open = %T %v, want *LeaseIOError{Op: layout-scan}", err, err)
				}
			case "ordered":
				var ioe *OrderedLogIOError
				if !errors.As(err, &ioe) || ioe.Op != "layout-scan" {
					t.Fatalf("Open = %T %v, want *OrderedLogIOError{Op: layout-scan}", err, err)
				}
			}
		})
	}
}

// seedCurrentLayout writes nested KV keys and blobs through a v0.6.0 Store and
// closes it.
func seedCurrentLayout(t *testing.T, root string) {
	t.Helper()
	ctx := context.Background()
	s, err := Open(Options{Root: root})
	if err != nil {
		t.Fatalf("seed Open: %v", err)
	}
	defer func() { _ = s.Close() }()
	for _, k := range []string{"sessions/a", "sessions/a/object-metadata/v1/x", "a.kv"} {
		if _, err := s.KV.Put(ctx, k, 0, []byte(k)); err != nil {
			t.Fatalf("seed kv %q: %v", k, err)
		}
	}
	for _, k := range []string{"sessions/a", "sessions/a/blobs/v1/x", "a.blob"} {
		if err := s.Blobs.Put(ctx, k, bytes.NewReader([]byte(k))); err != nil {
			t.Fatalf("seed blob %q: %v", k, err)
		}
	}
}

// seedNonLeafPrimitives writes ledger, lease and ordered-index data (whose files
// carry their own suffixes under streams/, leases/, ordered/) and nothing in KV
// or Blobs.
func seedNonLeafPrimitives(t *testing.T, root string) {
	t.Helper()
	ctx := context.Background()
	s, err := Open(Options{Root: root})
	if err != nil {
		t.Fatalf("seed Open: %v", err)
	}
	defer func() { _ = s.Close() }()
	if err := s.Ledger.Append(ctx, "sessions/a", 0, []byte("frame")); err != nil {
		t.Fatalf("seed ledger: %v", err)
	}
	lease, err := s.Leaser.Acquire(ctx, "sessions/a")
	if err != nil {
		t.Fatalf("seed lease: %v", err)
	}
	if err := lease.Release(ctx); err != nil {
		t.Fatalf("seed lease release: %v", err)
	}
	id := storage.OrderedID{Namespace: "sessionstore/gates/0", OrderingScope: "gates", StableKey: "g"}
	if _, _, err := s.OrderedIndex.Create(ctx, id, "catalog", []byte("v"), storage.Rank{}, storage.Due{State: storage.NotDue}); err != nil {
		t.Fatalf("seed ordered: %v", err)
	}
}

// assertCurrentLayoutReadable re-reads what seedCurrentLayout wrote, when that is
// the seed in use.
func assertCurrentLayoutReadable(t *testing.T, ctx context.Context, s *Store, seed func(*testing.T, string)) {
	t.Helper()
	if reflect.ValueOf(seed).Pointer() != reflect.ValueOf(seedCurrentLayout).Pointer() {
		return
	}
	for _, k := range []string{"sessions/a", "sessions/a/object-metadata/v1/x", "a.kv"} {
		v, rev, err := s.KV.Get(ctx, k)
		if err != nil || string(v) != k || rev != 1 {
			t.Fatalf("reopen kv %q = (%q, %d, %v)", k, v, rev, err)
		}
	}
	keys, err := s.Blobs.List(ctx, "")
	if err != nil {
		t.Fatalf("reopen blob list: %v", err)
	}
	if want := []string{"a.blob", "sessions/a", "sessions/a/blobs/v1/x"}; !reflect.DeepEqual(keys, want) {
		t.Fatalf("reopen blob list = %q, want %q", keys, want)
	}
}

func without(s []string, drop string) []string {
	out := make([]string, 0, len(s))
	for _, v := range s {
		if v != drop {
			out = append(out, v)
		}
	}
	return out
}

// dottedExtensionNameSets are the name sets every primitive must store at once,
// in the listed order and reversed: a name beside its '/' extension, and a name
// beside a longer name whose first segment is that name plus each extension
// fsstore has ever used as a leaf suffix (v0.5.x ".log"/".lock"/".olog") or
// might (".kv"/".blob"). Under v0.5.x's dotted suffixes "a" owned file "a.log"
// while "a.log/b" needed directory "a.log".
func dottedExtensionNameSets() [][]string {
	sets := [][]string{
		{"a", "a/b"},
		{"sessions/a", "sessions/a/b/c"},
		{"a.log", "a", "a.log/b"},
	}
	for _, ext := range []string{".log", ".lock", ".olog", ".kv", ".blob", ".x"} {
		sets = append(sets, []string{"a", "a" + ext + "/b"}, []string{"sessions/a", "sessions/a" + ext + "/b/c"})
	}
	n := len(sets)
	for i := 0; i < n; i++ {
		rev := make([]string, len(sets[i]))
		for j, v := range sets[i] {
			rev[len(rev)-1-j] = v
		}
		sets = append(sets, rev)
	}
	return sets
}

// TestLedgerNamesWithDottedExtensionsCoexist appends one frame to every name of
// each set and reads each back: no name's log may shadow another's directory.
func TestLedgerNamesWithDottedExtensionsCoexist(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	for _, names := range dottedExtensionNameSets() {
		t.Run(strings.Join(names, "+"), func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			s, err := newLedgerStore(root)
			if err != nil {
				t.Fatalf("newLedgerStore: %v", err)
			}
			t.Cleanup(func() { _ = s.Close() })
			for _, n := range names {
				if err := s.Append(ctx, n, 0, []byte("p:"+n)); err != nil {
					t.Fatalf("Append(%q): %v", n, err)
				}
			}
			assertLeafFiles(t, filepath.Join(root, "streams"), "@log", names)
			for _, n := range names {
				cur, err := s.Read(ctx, n, 1)
				if err != nil {
					t.Fatalf("Read(%q): %v", n, err)
				}
				rec, err := cur.Next(ctx)
				_ = cur.Close()
				if err != nil || rec.Seq != 1 || string(rec.Payload) != "p:"+n {
					t.Fatalf("Read(%q) = (%d %q, %v), want seq 1 %q", n, rec.Seq, rec.Payload, err, "p:"+n)
				}
			}
			// Deleting the first name leaves the rest readable.
			if err := s.Delete(ctx, names[0]); err != nil {
				t.Fatalf("Delete(%q): %v", names[0], err)
			}
			for _, n := range names[1:] {
				if tip, err := s.Tip(ctx, n); err != nil || tip != 1 {
					t.Fatalf("Tip(%q) after delete = (%d, %v), want 1", n, tip, err)
				}
			}
		})
	}
}

// TestLeaserNamesWithDottedExtensionsCoexist holds a grant on every name of each
// set at once; each is a first grant (epoch 1) with its own lock file.
func TestLeaserNamesWithDottedExtensionsCoexist(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	for _, names := range dottedExtensionNameSets() {
		t.Run(strings.Join(names, "+"), func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			s, err := newLeaserStore(root)
			if err != nil {
				t.Fatalf("newLeaserStore: %v", err)
			}
			for _, n := range names {
				l, err := s.Acquire(ctx, n)
				if err != nil {
					t.Fatalf("Acquire(%q): %v", n, err)
				}
				t.Cleanup(func() { _ = l.Release(ctx) })
				if l.Epoch() != 1 {
					t.Fatalf("Acquire(%q) epoch = %d, want 1", n, l.Epoch())
				}
			}
			assertLeafFiles(t, filepath.Join(root, "leases"), "@lock", names)
		})
	}
}

// TestOrderedNamespacesWithDottedExtensionsCoexist creates a record in every
// namespace of each set and reads each back from a fresh index (so the logs,
// not the warm views, are what is read).
func TestOrderedNamespacesWithDottedExtensionsCoexist(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	for _, names := range dottedExtensionNameSets() {
		t.Run(strings.Join(names, "+"), func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			x, err := newOrderedIndex(root)
			if err != nil {
				t.Fatalf("newOrderedIndex: %v", err)
			}
			for _, n := range names {
				id := storage.OrderedID{Namespace: n, OrderingScope: "s", StableKey: "k"}
				if _, _, err := x.Create(ctx, id, "r", []byte("v:"+n), storage.Rank{}, storage.Due{State: storage.NotDue}); err != nil {
					t.Fatalf("Create(%q): %v", n, err)
				}
			}
			assertLeafFiles(t, filepath.Join(root, "ordered"), "@olog", names)
			if err := x.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			y, err := newOrderedIndex(root)
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			t.Cleanup(func() { _ = y.Close() })
			for _, n := range names {
				rec, err := y.Get(ctx, storage.OrderedID{Namespace: n, OrderingScope: "s", StableKey: "k"})
				if err != nil || string(rec.Value) != "v:"+n {
					t.Fatalf("Get(%q) = (%q, %v), want %q", n, rec.Value, err, "v:"+n)
				}
			}
		})
	}
}

// assertLeafFiles checks, after every name's operation succeeded, that each name
// owns a regular file at <dir>/<name><suffix>. It runs last so a layout without
// the '@' suffix fails on the semantic operation (a "not a directory"
// collision), not merely on this literal path.
func assertLeafFiles(t *testing.T, dir, suffix string, names []string) {
	t.Helper()
	for _, n := range names {
		leaf := filepath.Join(dir, filepath.FromSlash(n)) + suffix
		if info, err := os.Lstat(leaf); err != nil || !info.Mode().IsRegular() {
			t.Fatalf("leaf file for %q at %q: info=%v err=%v, want a regular file", n, leaf, info, err)
		}
	}
}
