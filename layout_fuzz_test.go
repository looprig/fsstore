package fsstore

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/looprig/storage"
)

// FuzzLeafEncoding checks the KV/Blobs leaf encoding over arbitrary input:
//
//   - every valid name maps (pathFor) to a path under the primitive's directory
//     whose relative form leafKey decodes back to exactly that name;
//   - no directory component of that path decodes as a leaf, and the leaf's own
//     file name is never a valid segment, so a leaf file can never occupy an
//     entry that a longer key needs as a directory, and Open's legacy scan
//     (findLegacyLeaf's valid-segment test) can never flag a v0.6.0 leaf;
//   - leafKey accepts an arbitrary relative path only when re-encoding its result
//     reproduces that path (decoding is the exact inverse, never a guess).
func FuzzLeafEncoding(f *testing.F) {
	for _, seed := range []string{
		"a", "a/b", "sessions/a/object-metadata/v1/x", "a.kv", "a.blob", "a.kv/b",
		"a@kv", "a@kv@kv", "@kv", "a/@kv", ".fsstore-tmp-1@kv", "Upper@kv", "",
	} {
		f.Add(seed)
	}
	kv := &kvStore{kv: "/root/kv"}
	blobs := &blobStore{blobs: "/root/blobs"}
	f.Fuzz(func(t *testing.T, s string) {
		for _, enc := range []struct {
			dir    string
			suffix string
			path   func(string) (string, error)
		}{
			{dir: kv.kv, suffix: kvLeafSuffix, path: kv.pathFor},
			{dir: blobs.blobs, suffix: blobLeafSuffix, path: blobs.pathFor},
		} {
			if storage.ValidateName(s) == nil {
				p, err := enc.path(s)
				if err != nil {
					t.Fatalf("pathFor(%q): %v", s, err)
				}
				rel := filepath.ToSlash(strings.TrimPrefix(p, enc.dir+"/"))
				got, ok := leafKey(rel, enc.suffix)
				if !ok || got != s {
					t.Fatalf("leafKey(pathFor(%q)) = (%q, %v)", s, got, ok)
				}
				segs := strings.Split(rel, "/")
				leaf := segs[len(segs)-1]
				if storage.ValidateName(leaf) == nil {
					t.Fatalf("leaf file name %q of %q is a valid segment (would read as legacy)", leaf, s)
				}
				for _, d := range segs[:len(segs)-1] {
					if strings.HasSuffix(d, enc.suffix) || storage.ValidateName(d) != nil {
						t.Fatalf("directory %q of %q could collide with a leaf", d, s)
					}
				}
			}
			if key, ok := leafKey(s, enc.suffix); ok {
				if key+enc.suffix != s || storage.ValidateName(key) != nil {
					t.Fatalf("leafKey(%q) = %q is not the exact inverse", s, key)
				}
			}
		}
	})
}
