package fsstore

import (
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/looprig/storage"
)

// This file implements the durable substrate of fsstore's OrderedIndex: one
// append-only, checksummed mutation log per namespace, laid out at
// <root>/ordered/<namespace>.olog where the namespace's '/'-separated segments
// become directory components. Each record is an ordered mutation frame (see
// ordered_codec.go) and the file is the concatenation of its frames.
//
// # Why a log rather than a file per record
//
// OrderedIndex needs two things a directory of per-record files cannot give.
// First, CreateOrdered must allocate the next immutable order for a scope AND
// publish the new record ATOMICALLY: any design with a separate counter can
// crash between advancing it and writing the record, which either burns an order
// or — far worse — lets the same order be handed out twice. A create frame
// carries the allocated order inside the same durable append as the record, so
// there is no window. Replay recovers each scope's high-water mark as the
// maximum order it observes; nothing else needs to be durable.
//
// Second, invariant #15 of the orchestration plan forbids provider-unbounded
// paging: KV.Keys, filesystem-wide walks, and scans of historical records are
// not acceptable implementations. Reading state must therefore be a bounded
// operation over per-namespace in-memory indexes, and those indexes are built
// once, at open, from a SEQUENTIAL read of one file per namespace — not from a
// directory walk whose cost grows with the record count on every call.
//
// # Locking model
//
// This mirrors the ledger's proven two-layer model, and the append critical
// section holds both layers:
//
//   - In-process: a store mutex guards the namespace->orderedLog registry; each
//     orderedLog carries its own mutex serializing every operation on that one
//     namespace. The registry mutex is only ever held to look a log up, never
//     while a log's mutex is held, so the two cannot deadlock.
//   - Cross-process: an exclusive advisory lock (flock LOCK_EX) is taken on the
//     log file for the duration of each append and each replay, and released
//     before the call returns — a per-operation lock, never held open across
//     calls. A crashed holder's lock is reclaimed by the kernel, so no stale-lock
//     cleanup is ever required.
//
// Both replay and append take the advisory lock because both may TRUNCATE a torn
// tail, which is a write.
//
// # Fault model
//
// A torn tail (an interrupted final append) is recovered by truncating back to
// the last whole frame: the partial mutation was never acknowledged, so dropping
// it loses nothing. Anything else — a checksum mismatch, a frame that decodes to
// an illegal mutation, or a sequence gap — is corruption that truncation cannot
// repair, and every operation fails closed with an *OrderedLogCorruptError rather
// than silently dropping or misreading a committed mutation. In particular an
// append refuses to extend a log it cannot read, which would bury the damage.
//
// # StableKey never reaches a path
//
// Only the NAMESPACE becomes a path, and it passes storage.ValidateName, whose
// grammar cannot express '..', an empty segment, or a leading '/'. The ordering
// scope, ranking scope, and the opaque StableKey live inside frames. A StableKey
// may legally contain '/', '..', uppercase bytes, and arbitrary Unicode; putting
// one in a path would be a directory-traversal vulnerability.

const (
	// orderedDirName is the subdirectory of the store root holding every
	// namespace's mutation log.
	orderedDirName = "ordered"

	// orderedLogExt is the on-disk suffix of a namespace's mutation log. It is
	// deliberately distinct from the ledger's .log so the two file families are
	// never confused by an operator or a recovery tool.
	orderedLogExt = ".olog"
)

// errOrderedNonContiguousSeq is the leaf cause recorded in an
// *OrderedLogCorruptError when a frame's sequence does not follow its
// predecessor. A gap means a committed frame is missing from the middle of the
// log, which truncation cannot repair.
var errOrderedNonContiguousSeq = errors.New("fsstore: non-contiguous ordered log sequence")

// OrderedLogCorruptError reports that a namespace's mutation log could not be
// trusted: the frame at Seq is present-but-invalid (a checksum mismatch or an
// over-ceiling length), decodes to an illegal mutation, or breaks the sequence.
// Unlike a torn tail this is not recoverable by truncation, so every operation
// fails closed. Cause carries the underlying *FrameError, *OrderedCodecError, or
// errOrderedNonContiguousSeq.
type OrderedLogCorruptError struct {
	Path  string
	Seq   uint64
	Cause error
}

func (e *OrderedLogCorruptError) Error() string {
	return "fsstore: ordered log " + strconv.Quote(e.Path) + " corrupt at seq " +
		strconv.FormatUint(e.Seq, 10) + ": " + e.Cause.Error()
}

// Unwrap exposes the underlying frame fault, codec fault, or
// errOrderedNonContiguousSeq.
func (e *OrderedLogCorruptError) Unwrap() error { return e.Cause }

// OrderedLogPathError reports that a namespace mapped to a filesystem location
// outside the ordered log directory. It is defense in depth: ValidateName
// already forbids the '..' and empty segments that could escape, so a valid
// namespace never triggers it — a triggered OrderedLogPathError means an
// unvalidated name reached path mapping.
type OrderedLogPathError struct {
	Namespace string
	Path      string
}

func (e *OrderedLogPathError) Error() string {
	return "fsstore: ordered namespace " + strconv.Quote(e.Namespace) +
		" maps outside the ordered log root: " + strconv.Quote(e.Path)
}

// OrderedLogRootError reports that newOrderedLogStore was given an unusable root
// (for example, an empty or blank path).
type OrderedLogRootError struct {
	Root   string
	Reason string
}

func (e *OrderedLogRootError) Error() string {
	return "fsstore: invalid ordered log root " + strconv.Quote(e.Root) + ": " + e.Reason
}

// OrderedLogIOError wraps an underlying filesystem failure (open, read, write,
// fsync, truncate, mkdir, stat, seek) with the operation and path for diagnosis.
// Cause is the os/syscall error and is exposed via Unwrap.
type OrderedLogIOError struct {
	Op    string
	Path  string
	Cause error
}

func (e *OrderedLogIOError) Error() string {
	return "fsstore: ordered log " + e.Op + " " + strconv.Quote(e.Path) + ": " + e.Cause.Error()
}

// Unwrap exposes the underlying filesystem error.
func (e *OrderedLogIOError) Unwrap() error { return e.Cause }

// orderedIOErr adapts *OrderedLogIOError to the ioErrFunc the shared fsync
// helpers expect, so a directory-fsync failure on an ordered path surfaces as an
// ordered-typed IO error rather than another backend's.
func orderedIOErr(op, path string, cause error) error {
	return &OrderedLogIOError{Op: op, Path: path, Cause: cause}
}

// orderedLogStore owns every namespace mutation log under one store root,
// tracking one orderedLog per namespace it has touched.
type orderedLogStore struct {
	root string // cleaned store root
	dir  string // cleaned <root>/ordered

	mu   sync.Mutex
	logs map[string]*orderedLog
}

// newOrderedLogStore creates <root> and <root>/ordered at 0700 and returns a log
// store rooted there. It rejects a blank root and wraps any mkdir failure in a
// typed error.
func newOrderedLogStore(root string) (*orderedLogStore, error) {
	if strings.TrimSpace(root) == "" {
		return nil, &OrderedLogRootError{Root: root, Reason: "empty root"}
	}
	clean := filepath.Clean(root)
	if err := os.MkdirAll(clean, 0o700); err != nil {
		return nil, &OrderedLogIOError{Op: "mkdir", Path: clean, Cause: err}
	}
	dir := filepath.Join(clean, orderedDirName)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, &OrderedLogIOError{Op: "mkdir", Path: dir, Cause: err}
	}
	return &orderedLogStore{root: clean, dir: dir, logs: make(map[string]*orderedLog)}, nil
}

// Close releases the store's in-process state: it drops the cached
// namespace->orderedLog registry (the sequence/size metadata observed after the
// last operation). The store holds no long-lived file descriptors — every append
// and replay opens and closes its fd within the call — so there are no handles to
// release. Close is idempotent: it resets the registry to an empty map, so a
// second call, or a stray operation afterwards, is safe rather than a nil-map
// panic.
func (s *orderedLogStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.logs = make(map[string]*orderedLog)
	return nil
}

// pathFor maps a namespace to its log file path under <root>/ordered and
// verifies the result stays within that directory. The namespace's
// '/'-separated segments become path components; the file gains the .olog
// suffix. Containment is LEXICAL: it assumes an exclusively-owned root created
// at 0700 and does NOT resolve symlinks, so a symlink planted inside the ordered
// directory by another writer could still redirect a mapped path. Owning the
// store root is the deployment's responsibility — the same model the ledger uses.
func (s *orderedLogStore) pathFor(namespace string) (string, error) {
	full := filepath.Clean(filepath.Join(s.dir, filepath.FromSlash(namespace)))
	// The trailing separator in the prefix rejects both an escaping path and the
	// ordered root itself (an empty namespace), and defends against a sibling
	// directory whose name merely starts with "ordered".
	if !strings.HasPrefix(full, s.dir+string(os.PathSeparator)) {
		return "", &OrderedLogPathError{Namespace: namespace, Path: full}
	}
	return full + orderedLogExt, nil
}

// logFor returns the orderedLog for namespace, creating the registry entry on
// first use. It validates the namespace against the storage name grammar and
// verifies path containment BEFORE touching the registry, so no unvalidated name
// ever reaches a filesystem location. Repeated calls return the same *orderedLog,
// which is what makes its mutex serialize every writer in this process.
func (s *orderedLogStore) logFor(namespace string) (*orderedLog, error) {
	if err := storage.ValidateName(namespace); err != nil {
		return nil, err
	}
	path, err := s.pathFor(namespace)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	log := s.logs[namespace]
	if log == nil {
		log = &orderedLog{namespace: namespace, path: path, stopDir: s.dir}
		s.logs[namespace] = log
	}
	return log, nil
}

// orderedLog is one namespace's append-only mutation log. It holds the
// per-namespace serialization mutex and the tip/size observed after the last
// operation. tip/size are authoritative only while loaded is true AND the
// on-disk size still matches size; a mismatch (a cross-process append or a
// removal) forces a re-scan. By construction, whenever loaded is true, size is
// the byte offset just past the last whole frame — no torn tail is ever cached —
// so an append can extend the file at size directly.
type orderedLog struct {
	namespace string
	path      string
	stopDir   string // <root>/ordered: the top of the dir-fsync chain

	mu     sync.Mutex
	loaded bool
	tip    uint64
	size   int64
}

// replay reads the whole log and returns its mutations in append order — the
// startup path that rebuilds a namespace's in-memory index. An absent log is an
// empty one, not an error. A torn tail is truncated away before the mutations are
// returned, so the log is left in the exact state the next append expects; any
// other fault returns an *OrderedLogCorruptError and no mutations.
//
// It takes the advisory lock because recovery truncates, which is a write.
func (l *orderedLog) replay() (mutations []orderedMutation, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	f, oerr := os.OpenFile(l.path, os.O_RDWR, 0o600) // #nosec G304 -- l.path is validated and contained under <root>/ordered
	if errors.Is(oerr, fs.ErrNotExist) {
		l.loaded, l.tip, l.size = true, 0, 0
		return nil, nil
	}
	if oerr != nil {
		return nil, &OrderedLogIOError{Op: "open", Path: l.path, Cause: oerr}
	}
	// Defers run LIFO: unlock (registered second) releases the advisory lock
	// before close (registered first) releases the fd.
	defer func() {
		if cerr := f.Close(); cerr != nil && err == nil {
			mutations, err = nil, &OrderedLogIOError{Op: "close", Path: l.path, Cause: cerr}
		}
	}()
	if lerr := lockFileEx(f); lerr != nil {
		return nil, lerr
	}
	defer func() {
		if uerr := unlockFile(f); uerr != nil && err == nil {
			mutations, err = nil, uerr
		}
	}()

	decoded, rerr := l.recover(f)
	if rerr != nil {
		return nil, rerr
	}
	return decoded, nil
}

// append durably commits one mutation as the next frame of the log. It
// serializes per-namespace in-process, takes an exclusive cross-process advisory
// lock for the critical section, recovers a torn tail before reading the tip,
// fsyncs the file, and — on first creation — fsyncs the ancestor directory chain
// so the new directory entries are durable too. It returns the assigned frame
// sequence.
//
// The mutation is validated BEFORE any file is opened, so a rejected mutation
// creates no log file and leaves no trace. A corrupt log fails the append closed:
// a writer must never extend a log it cannot read.
func (l *orderedLog) append(m orderedMutation) (seq uint64, err error) {
	if verr := validateOrderedMutation(m); verr != nil {
		return 0, verr
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	dir := filepath.Dir(l.path)
	// Best-effort and idempotent: ensure the directory tree exists before
	// opening. The durability-critical CREATION decision is NOT made here — it is
	// derived below from on-disk state read under the advisory lock, so a
	// concurrent removal racing this MkdirAll cannot skip the directory fsync.
	if merr := os.MkdirAll(dir, 0o700); merr != nil {
		return 0, &OrderedLogIOError{Op: "mkdir", Path: dir, Cause: merr}
	}

	f, oerr := os.OpenFile(l.path, os.O_RDWR|os.O_CREATE, 0o600) // #nosec G304 -- l.path is validated and contained under <root>/ordered
	if oerr != nil {
		return 0, &OrderedLogIOError{Op: "open", Path: l.path, Cause: oerr}
	}
	defer func() {
		if cerr := f.Close(); cerr != nil && err == nil {
			seq, err = 0, &OrderedLogIOError{Op: "close", Path: l.path, Cause: cerr}
		}
	}()
	if lerr := lockFileEx(f); lerr != nil {
		return 0, lerr
	}
	defer func() {
		if uerr := unlockFile(f); uerr != nil && err == nil {
			seq, err = 0, uerr
		}
	}()

	if rerr := l.refresh(f); rerr != nil {
		return 0, rerr
	}
	// Authoritative creation decision from on-disk state observed under the lock:
	// an empty log means its directory entries may be newly created and need an
	// fsync to be crash-durable. A torn-to-zero recovery also lands here, making
	// the directory fsync redundant — a negligible cost.
	created := l.size == 0

	frame, eerr := encodeOrderedFrame(l.tip+1, m)
	if eerr != nil {
		return 0, eerr // an invalid mutation: a definite failure, never ambiguous
	}
	if _, werr := f.WriteAt(frame, l.size); werr != nil {
		return 0, &OrderedLogIOError{Op: "write", Path: l.path, Cause: werr}
	}
	if serr := f.Sync(); serr != nil {
		return 0, &OrderedLogIOError{Op: "fsync", Path: l.path, Cause: serr}
	}
	if created {
		// A nested namespace (factory/hosts) creates directories several levels
		// deep, so fsync the chain from the file's directory up to and including
		// <root>/ordered.
		if derr := fsyncDirChain(dir, l.stopDir, orderedIOErr); derr != nil {
			return 0, derr
		}
	}
	l.tip++
	l.size += int64(len(frame))
	return l.tip, nil
}

// refresh brings the cached tip/size in sync with the on-disk file for the
// append path, which does not need the decoded mutations. The fast path skips
// the scan entirely when the cache is valid and the file size is unchanged.
//
// The size-equality fast path is sound under the same single-writer-per-namespace
// assumption the ledger documents: it detects a cross-process change by size
// alone, so a removal and recreation that happened to land an identical byte
// count would stale the cache. Under single-writer discipline that cannot occur.
func (l *orderedLog) refresh(f *os.File) error {
	st, serr := f.Stat()
	if serr != nil {
		return &OrderedLogIOError{Op: "stat", Path: l.path, Cause: serr}
	}
	if l.loaded && st.Size() == l.size {
		return nil
	}
	_, err := l.recover(f)
	return err
}

// recover scans the whole file, truncates a torn tail back to the last whole
// frame, updates the cached tip/size, and returns the decoded mutations in
// append order. It is called only under both the per-namespace mutex and the
// advisory lock, so the file is quiescent.
//
// On any error the cache is left UNLOADED, so a later operation re-scans rather
// than trusting metadata derived from an unreadable file.
func (l *orderedLog) recover(f *os.File) ([]orderedMutation, error) {
	l.loaded = false
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return nil, &OrderedLogIOError{Op: "seek", Path: l.path, Cause: err}
	}
	data, rerr := io.ReadAll(f)
	if rerr != nil {
		return nil, &OrderedLogIOError{Op: "read", Path: l.path, Cause: rerr}
	}
	mutations, lastSeq, goodEnd, scerr := scanOrderedFrames(data, l.path)
	if scerr != nil {
		return nil, scerr // fail closed: corruption truncation cannot repair
	}
	if goodEnd < int64(len(data)) {
		// A torn tail (a crash mid-append): roll the file back to the last whole
		// frame. The partial mutation was never acknowledged to any caller.
		if terr := f.Truncate(goodEnd); terr != nil {
			return nil, &OrderedLogIOError{Op: "truncate", Path: l.path, Cause: terr}
		}
		if fserr := f.Sync(); fserr != nil {
			return nil, &OrderedLogIOError{Op: "fsync", Path: l.path, Cause: fserr}
		}
	}
	l.tip = lastSeq
	l.size = goodEnd
	l.loaded = true
	return mutations, nil
}

// scanOrderedFrames walks the concatenated frames in data, decoding each
// mutation and verifying that sequences are contiguous from 1. It returns the
// mutations in append order, the last good sequence, and goodEnd — the byte
// offset just past the last whole frame.
//
// A torn tail (an incomplete final frame) stops the walk cleanly at goodEnd and
// is recoverable by truncation. Every other fault — a checksum mismatch, an
// over-ceiling declared length, a frame that decodes to an illegal mutation, or a
// sequence gap — returns an *OrderedLogCorruptError and no mutations.
func scanOrderedFrames(data []byte, path string) (mutations []orderedMutation, lastSeq uint64, goodEnd int64, err error) {
	off := 0
	var expect uint64 = 1
	for off < len(data) {
		seq, m, n, derr := decodeOrderedFrame(data[off:])
		if derr != nil {
			var fe *FrameError
			if errors.As(derr, &fe) && fe.IsTorn() {
				return mutations, lastSeq, int64(off), nil // torn tail: good up to off
			}
			return nil, 0, 0, &OrderedLogCorruptError{Path: path, Seq: expect, Cause: derr}
		}
		if seq != expect {
			return nil, 0, 0, &OrderedLogCorruptError{Path: path, Seq: seq, Cause: errOrderedNonContiguousSeq}
		}
		mutations = append(mutations, m)
		off += n
		lastSeq = seq
		expect++
	}
	return mutations, lastSeq, int64(off), nil
}
