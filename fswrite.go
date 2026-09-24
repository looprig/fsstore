package fsstore

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/looprig/storage"
)

// This file holds the durable-write and tree-listing mechanics shared by the
// filesystem KV and Blobs backends. Both stage a new file's bytes in a sibling
// temp file, fsync it, and os.Rename it over the target (atomic on POSIX), and
// both list their key space by walking a root directory and mapping each committed
// file's path back to a '/'-joined key. The two primitives differ only in their
// typed IO error, which each injects via an ioErrFunc.

// fsTmpPrefix marks a temp file staged for an atomic rename. It starts with a '.',
// which the storage name grammar forbids as a segment's first byte, so a temp
// file can never collide with a committed key, is never a leaf to walkKeys (its
// stripped name is not a valid key) and is never mistaken for a legacy leaf by
// findLegacyLeaf — a concurrent listing therefore never surfaces a half-written
// value as a key, and a crash-leftover temp file never blocks Open.
const fsTmpPrefix = ".fsstore-tmp-"

// ioErrFunc constructs a backend's typed filesystem IO error from an operation
// label, path, and cause. writeFileAtomic takes one so the shared write path can
// surface KV-typed or Blobs-typed errors without knowing which backend called it.
type ioErrFunc func(op, path string, cause error) error

// writeFileAtomic durably writes content to target via a sibling temp file: it
// creates the temp in dir (so the rename stays within one filesystem), writes and
// fsyncs it, closes it, and atomically renames it over target. On any failure
// before the rename it removes the stray temp file, joining a cleanup failure onto
// the primary error so neither is lost. The parent-directory fsync that makes the
// rename itself durable is the caller's responsibility. The temp file is created
// at 0600 (os.CreateTemp's default).
func writeFileAtomic(dir, target string, content []byte, ioErr ioErrFunc) (err error) {
	tmp, cerr := os.CreateTemp(dir, fsTmpPrefix+"*")
	if cerr != nil {
		return ioErr("create-temp", dir, cerr)
	}
	tmpName := tmp.Name()
	renamed := false
	// Runs last: unless the temp was renamed into place, remove the stray so a
	// failed write never leaves a partial file behind.
	defer func() {
		if !renamed {
			if rmErr := os.Remove(tmpName); rmErr != nil && !errors.Is(rmErr, fs.ErrNotExist) {
				err = errors.Join(err, ioErr("remove-temp", tmpName, rmErr))
			}
		}
	}()

	if _, werr := tmp.Write(content); werr != nil {
		return closeOnError(tmp, tmpName, ioErr("write", tmpName, werr), ioErr)
	}
	if serr := tmp.Sync(); serr != nil {
		return closeOnError(tmp, tmpName, ioErr("fsync", tmpName, serr), ioErr)
	}
	// Close before rename: flush the fd and observe any deferred-write error, and
	// keep the rename valid on platforms that refuse to rename an open file.
	if clerr := tmp.Close(); clerr != nil {
		return ioErr("close", tmpName, clerr)
	}
	if rnerr := os.Rename(tmpName, target); rnerr != nil {
		return ioErr("rename", tmpName, rnerr)
	}
	renamed = true
	return nil
}

// closeOnError closes f on a write/fsync failure path and returns the primary
// error, joining any close failure onto it so neither is masked. It is only used
// after content staging has already failed, so the temp file is discarded by
// writeFileAtomic's deferred cleanup regardless.
func closeOnError(f *os.File, path string, primary error, ioErr ioErrFunc) error {
	if cerr := f.Close(); cerr != nil {
		return errors.Join(primary, ioErr("close", path, cerr))
	}
	return primary
}

// Leaf suffixes. Every fsstore primitive maps a storage name to
// <dir>/<segments...> where every segment but the last is a directory and the
// last is a regular file named "<last segment><suffix>": "@kv" (KV), "@blob"
// (Blobs), "@log" (ledger, ledger.go), "@lock" (leaser, lease.go) and "@olog"
// (OrderedIndex namespace, ordered.go). Every name reaching a path — KV and
// Blobs keys, ledger and lease names, OrderedIndex namespaces — has passed
// storage.ValidateName, whose grammar ([a-z0-9][a-z0-9_.-]* per segment) forbids
// '@', so:
//
//   - no directory entry can be both a leaf file and a directory: a directory's
//     name is a valid segment and never contains '@', a leaf file's always does.
//     A name and any name extending it with "/..." therefore always coexist,
//     whichever is created first: "a" -> "a@kv", "a/b" -> "a/b@kv", and
//     "a.kv/b" -> "a.kv/b@kv";
//   - the mapping is injective and reversible: stripping one trailing suffix from
//     a leaf file recovers exactly the name's last segment, including a name that
//     itself ends in a dotted extension ("a.kv" -> "a.kv@kv").
//
// The dotted suffixes of fsstore <= v0.5.x (".log", ".lock", ".olog", and none at
// all for KV and Blobs) did NOT keep leaves and directories apart: name "a"
// owned file "a.log" while name "a.log/b" needed a directory "a.log" at the same
// entry. An uppercase suffix (".LOG") would fail the same way on a
// case-insensitive filesystem (the macOS default), which folds it onto the valid
// lowercase name, so the suffix uses punctuation the grammar forbids instead.
const (
	kvLeafSuffix   = "@kv"
	blobLeafSuffix = "@blob"
)

// leafKey reports the key a directory entry at rel (a slash-separated path
// relative to a KV or Blobs root) encodes under suffix, and whether it is a
// committed leaf at all. An entry is a leaf only when its name ends in suffix
// and the stripped path is a valid storage name; temp files (fsTmpPrefix, which
// starts with '.'), pre-v0.6.0 unsuffixed files, foreign dotfiles and any other
// name that no valid key produces are rejected, so listing reports exactly the
// keys Get can read.
func leafKey(rel, suffix string) (string, bool) {
	key, ok := strings.CutSuffix(rel, suffix)
	if !ok {
		return "", false
	}
	if storage.ValidateName(key) != nil {
		return "", false
	}
	return key, true
}

// walkKeys lists the committed keys under root whose '/'-joined form has prefix,
// returned lexicographically ascending and duplicate-free. It walks the tree,
// skips directories, and maps each leaf file back to its key with leafKey
// (in-progress temp files, unsuffixed legacy files and foreign entries are not
// leaves and are skipped). prefix is a raw substring filter on the decoded key,
// NOT a validated name (an empty prefix lists all keys). The returned error is
// the raw walk fault; the caller wraps it in its own typed IO error.
func walkKeys(root, prefix, suffix string) ([]string, error) {
	var out []string
	walkErr := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		rel := filepath.ToSlash(strings.TrimPrefix(path, root+string(os.PathSeparator)))
		key, ok := leafKey(rel, suffix)
		if !ok {
			return nil // temp file mid-rename, legacy leaf, or foreign entry
		}
		if strings.HasPrefix(key, prefix) {
			out = append(out, key)
		}
		return nil
	})
	if walkErr != nil {
		return nil, walkErr
	}
	// Keys are unique by path, so no dedup is needed; sort makes the listing
	// canonical (WalkDir's lexical order is over encoded names, where the
	// suffix byte '@' sorts differently from '/', so the sort is load-bearing).
	sort.Strings(out)
	return out, nil
}

// findLegacyLeaf reports the first pre-v0.6.0 leaf file under root (one
// primitive's tree), if any. fsstore <= v0.5.x named a leaf "<last segment>"
// (KV, Blobs) or "<last segment>.log" / ".lock" / ".olog" (ledger, leaser,
// OrderedIndex); every one of those is itself a valid storage segment. v0.6.0
// never creates a non-directory entry with such a name: its leaves end in an '@'
// suffix and its temp files start with '.', both outside the segment grammar.
// So a non-directory entry whose name is a valid segment is exactly a legacy
// leaf, and nothing else is (dotfiles, uppercase names and temp leftovers are
// ignored). "Non-directory" deliberately includes symlinks and other special
// files (WalkDir does not follow a symlink, even to a directory): fsstore never
// creates one, so a valid-named one is refused as legacy rather than trusted. Empty legacy directories carry no data and are reused as-is. The
// walk stops at the first hit; a clean tree costs one directory walk, the same
// as one Keys/List call, and runs only at Open — never on an OrderedIndex page.
func findLegacyLeaf(root string) (string, bool, error) {
	var found string
	walkErr := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if storage.ValidateName(d.Name()) == nil {
			found = path
			return fs.SkipAll
		}
		return nil
	})
	if walkErr != nil {
		return "", false, walkErr
	}
	return found, found != "", nil
}
