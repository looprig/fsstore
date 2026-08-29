package fsstore

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

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
// there is no window. Reading the log back recovers each scope's high-water mark
// as the maximum order it observes; nothing else needs to be durable.
//
// Second, invariant #15 of the orchestration plan forbids provider-unbounded
// paging: KV.Keys, filesystem-wide walks, and scans of historical records are
// not acceptable implementations. Reading state must therefore be a bounded
// operation over per-namespace in-memory indexes, and those indexes are built
// once, at open, from a SEQUENTIAL read of one file per namespace — not from a
// directory walk whose cost grows with the record count on every call.
//
// # Open item: the log is never compacted
//
// What the previous paragraph buys is a bounded READ path. It does not make the
// COLD path bounded, and this file does not yet solve that. A cold catch-up
// decodes EVERY mutation ever written for a namespace — each carrying up to
// 1 MiB of Value — and reads the whole file into one buffer to do it, so first
// use costs total HISTORY rather than live records: 10k records updated 100
// times each is 1M mutations decoded before the first read is served. There is
// no log-size limit anywhere in this module, and no incremental reader: a
// namespace whose log has outgrown available memory cannot be opened at all.
// That is a real trade of unbounded paging for unbounded startup memory and
// time, not a solved problem.
//
// Closing it needs one of two things, neither built here: a checkpoint/snapshot
// file the cold path starts from and the log tail folds onto, or a
// rewrite-and-rename compaction (fold the log to one frame per live identity
// into a temp file, fsync, rename, fsync the directory) performed under the same
// advisory lock that guards a commit. Until then a namespace's log grows without
// bound and a caller should treat a cold open as O(total mutations).
//
// # Locking model
//
// This mirrors the ledger's proven two-layer model, and ONE critical section —
// withView — holds both layers for every read and every write alike:
//
//   - In-process: a store mutex guards the namespace->orderedLog registry; each
//     orderedLog carries its own mutex serializing every operation on that one
//     namespace. The registry mutex is only ever held to look a log up, never
//     while a log's mutex is held, so the two cannot deadlock.
//   - Cross-process: an exclusive advisory lock (flock LOCK_EX) is taken on the
//     log file for the duration of each operation and released before the call
//     returns — a per-operation lock, never held open across calls. A crashed
//     holder's lock is reclaimed by the kernel, so no stale-lock cleanup is ever
//     required.
//
// A READ takes the exclusive lock too, because a read may discover and truncate
// a torn tail, which is a write.
//
// # There is exactly one path to the file
//
// Every operation — Get, Create, Update, Delete, and all three listings — enters
// through withView, which opens, locks, catches the view up, lets the caller
// decide, and appends at most one frame. There is deliberately no second
// commit path and no second recovery path: two implementations of "open, lock,
// recover, append, fsync" would share every invariant and could drift apart
// silently, and only one of them would be the one production uses.
//
// # Fault model
//
// A torn tail is recovered by truncating back to the last whole frame: the
// partial mutation was never acknowledged, so dropping it loses nothing. THREE
// artifacts count as torn, not just the obvious one — an incomplete final frame,
// a frame stamped with the never-emitted sequence 0, and an all-zero trailing
// region (the common post-crash artifact of a filesystem that journalled the new
// size before the data blocks). scanOrderedLog explains why the last two are not
// self-evident and why misclassifying them would brick a namespace.
//
// "Tail" is load-bearing in all three: a zero region, or a seq-0 frame, with
// anything nonzero behind it is a HOLE where an acknowledged frame used to be,
// and truncating there would discard committed mutations AND free their
// acceptance orders for reuse. Only a zero run that reaches the end of the file
// is recoverable.
//
// Anything else — a checksum mismatch, a frame that decodes to an illegal
// mutation, or a sequence gap — is corruption that truncation cannot repair, and
// every operation fails closed with an *OrderedLogCorruptError rather than
// silently dropping or misreading a committed mutation. Because there is one
// path and it decodes every frame it reads, payload-level damage fails an append
// exactly as framing damage does: a writer never extends a log it cannot read.
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
//
// PRECEDENCE: because a codec fault read off the disk is wrapped in this type,
// errors.As finds BOTH *OrderedLogCorruptError and *OrderedCodecError on one
// disk-corruption error, while a caller's invalid mutation carries only the
// codec type. A caller separating "your input was bad" from "the log is damaged"
// must test *OrderedLogCorruptError FIRST.
type OrderedLogCorruptError struct {
	Path  string
	Seq   uint64
	Cause error
}

func (e *OrderedLogCorruptError) Error() string {
	// causeText guards a nil Cause: a panic while rendering an error message is
	// the worst possible time to fail.
	return "fsstore: ordered log " + strconv.Quote(e.Path) + " corrupt at seq " +
		strconv.FormatUint(e.Seq, 10) + ": " + causeText(e.Cause)
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

// OrderedLogClosedError reports an operation on a closed ordered log store.
// Close drops the namespace->orderedLog registry, so a later logFor would mint a
// FRESH orderedLog with a NEW mutex for a namespace another goroutine may still
// be appending to under the old one — two mutexes guarding one file, surviving
// only because the advisory lock happens to serialize the descriptors. Failing
// closed is the same discipline Store.Close already applies.
type OrderedLogClosedError struct {
	Root      string
	Namespace string
}

func (e *OrderedLogClosedError) Error() string {
	return "fsstore: ordered log store " + strconv.Quote(e.Root) +
		" is closed: cannot open namespace " + strconv.Quote(e.Namespace)
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
	return "fsstore: ordered log " + e.Op + " " + strconv.Quote(e.Path) + ": " + causeText(e.Cause)
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

	mu     sync.Mutex
	closed bool
	logs   map[string]*orderedLog
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
	// fsync the ROOT so the "ordered" dirent itself is durable. append's
	// fsyncDirChain stops AT <root>/ordered and can therefore never make the
	// entry for that directory durable, so without this a crash could take the
	// directory and every log beneath it.
	if err := fsyncDir(clean, orderedIOErr); err != nil {
		return nil, err
	}
	return &orderedLogStore{root: clean, dir: dir, logs: make(map[string]*orderedLog)}, nil
}

// Close releases the store's in-process state: it drops the cached
// namespace->orderedLog registry (the sequence/size metadata observed after the
// last operation). The store holds no long-lived file descriptors — every append
// and replay opens and closes its fd within the call — so there are no handles to
// release. Close is idempotent: a second call is a no-op returning nil.
//
// After Close, logFor fails with an *OrderedLogClosedError rather than minting a
// fresh orderedLog. Handing out a NEW log — and so a new mutex — for a namespace
// an in-flight append still holds the old mutex for would leave two mutexes
// guarding one file, an invariant that survives only by accident of the advisory
// lock. Open a fresh store on the same root to use it again.
func (s *orderedLogStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
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
	if s.closed {
		return nil, &OrderedLogClosedError{Root: s.root, Namespace: namespace}
	}
	log := s.logs[namespace]
	if log == nil {
		log = &orderedLog{namespace: namespace, path: path, stopDir: s.dir, mu: newOrderedContextMutex()}
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

	mu orderedContextMutex

	// The current-view cache, and the ONLY cached position: there is exactly one
	// path that reads or writes this file (withView), so there is exactly one
	// notion of how far it has got. applied is the byte offset just past the last
	// frame folded into view, and appliedSeq is that frame's sequence. Together
	// they are the whole of invariant 15's requirement here: a warm reader
	// re-reads NOTHING before applied, so a page costs the tail written since it
	// last looked, not the history of the namespace.
	//
	// By construction applied is always the offset just past a WHOLE frame — no
	// torn tail is ever cached — so an append extends the file at applied
	// directly.
	view       *orderedView
	viewLoaded bool
	applied    int64
	appliedSeq uint64

	// Cumulative catch-up work, for the bounded-work assertions the conformance
	// lane needs. They only ever grow and are read under mu.
	scannedBytes  int64
	appliedFrames uint64
	catchUps      uint64
}

// orderedContextMutex is a single-permit mutex whose wait path can end with the
// caller's context. Cancellation is honored only while waiting to START file
// work; once withView owns both locks, it completes recovery or mutation rather
// than abandoning a possibly durability-critical operation midway.
type orderedContextMutex struct {
	permit chan struct{}
}

func newOrderedContextMutex() orderedContextMutex {
	permit := make(chan struct{}, 1)
	permit <- struct{}{}
	return orderedContextMutex{permit: permit}
}

func (m *orderedContextMutex) lock(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-m.permit:
	}
	if err := ctx.Err(); err != nil {
		m.unlock()
		return err
	}
	return nil
}

func (m *orderedContextMutex) unlock() { m.permit <- struct{}{} }

// allZero reports whether every byte of b is zero. It is only ever called once
// per scan, on the fault path, so the walk stays linear in the file size.
func allZero(b []byte) bool {
	for _, c := range b {
		if c != 0 {
			return false
		}
	}
	return true
}

// scanOrderedLog walks the concatenated frames in data and verifies that
// sequences are contiguous from expect. It returns the last good sequence and
// goodEnd — the byte offset just past the last whole frame, relative to the
// START of data — plus, when decode is true, every mutation in append order.
//
// expect is the sequence the FIRST frame in data must carry. A cold scan passes
// 1; an incremental catch-up passes the successor of the last sequence it
// already applied and hands in only the unapplied tail, which is what lets a warm
// reader keep its contiguity check without re-reading the prefix. Because the
// prefix is never re-read, the walk is linear in the bytes appended since the
// caller last looked rather than in the history of the namespace — so every
// frame is fully decoded, and a CRC-valid frame carrying an illegal PAYLOAD
// fails closed exactly like a damaged one.
//
// A torn tail stops the walk cleanly at goodEnd and is recoverable by
// truncation. Three artifacts are torn:
//
//   - an incomplete final frame (the frame layer reports it short);
//   - a frame stamped with sequence 0 whose whole remainder is zero. This writer
//     can never emit sequence 0, because a commit always stamps appliedSeq+1 >= 1;
//     a seq-0 frame with anything nonzero behind it is therefore not an unwritten
//     tail but a HOLE, and is corruption (see below);
//   - a trailing region that is entirely zero. This is the most common
//     post-crash artifact of all — a filesystem that journalled the new file
//     size before the data blocks leaves the extended region reading as zeros —
//     and it is NOT self-evidently torn: 16 zero bytes decode as a structurally
//     valid frame (len 0, crc 0, and crc32c of an empty payload is exactly 0)
//     whose empty payload then fails the codec. Classifying that as corruption
//     would brick the namespace: replay fails, append fails closed, and there is
//     no repair path. The seq-0 rule covers an aligned zero region and the
//     explicit all-zero check covers an unaligned one, and BOTH require the zeros
//     to run to the end of the buffer — a zero region with data behind it is
//     damage, not an unwritten tail.
//
// Every other fault — a checksum mismatch, an over-ceiling declared length, a
// frame that decodes to an illegal mutation, or a sequence gap — returns an
// *OrderedLogCorruptError and no mutations.
func scanOrderedLog(data []byte, path string, expect uint64) (mutations []orderedMutation, lastSeq uint64, goodEnd int64, err error) {
	off := 0
	for off < len(data) {
		seq, payload, n, derr := decodeFrame(data[off:])
		if derr != nil {
			var fe *FrameError
			if (errors.As(derr, &fe) && fe.IsTorn()) || allZero(data[off:]) {
				return mutations, lastSeq, int64(off), nil // torn tail: good up to off
			}
			return nil, 0, 0, &OrderedLogCorruptError{Path: path, Seq: expect, Cause: derr}
		}
		if seq == 0 {
			// Never emitted by this writer, so this is not a committed frame —
			// but "not committed" only means "unwritten TAIL" if the zeros run
			// to the end of the file. A zero region with data behind it is a
			// HOLE where an acknowledged frame used to be, and truncating there
			// would discard every frame after it (and free their acceptance
			// orders for reuse) on a path a plain read can take.
			if !allZero(data[off:]) {
				return nil, 0, 0, &OrderedLogCorruptError{Path: path, Seq: expect, Cause: errOrderedNonContiguousSeq}
			}
			return mutations, lastSeq, int64(off), nil
		}
		if seq != expect {
			return nil, 0, 0, &OrderedLogCorruptError{Path: path, Seq: seq, Cause: errOrderedNonContiguousSeq}
		}
		m, cerr := decodeOrderedMutation(payload)
		if cerr != nil {
			return nil, 0, 0, &OrderedLogCorruptError{Path: path, Seq: seq, Cause: cerr}
		}
		mutations = append(mutations, m)
		off += n
		lastSeq = seq
		expect++
	}
	return mutations, lastSeq, int64(off), nil
}

// ---------------------------------------------------------------------------
// Current views
// ---------------------------------------------------------------------------
//
// Everything below turns the append-only log above into the three CURRENT views
// OrderedIndex must serve — acceptance order, rank, and due — and implements the
// public storage.OrderedIndex surface over them.
//
// # Invariant 15: a page is bounded by the page, not by the history
//
// The orchestration plan forbids provider-unbounded paging: no KV.Keys, no
// filesystem walk, no scan of historical records per read. Two things together
// satisfy it here.
//
// First, each view is a MAINTAINED index, not a filter over the record map. The
// acceptance-order slice is append-only and already ascending by order; the
// ranked and due slices are kept sorted by the frozen sort keys as records are
// inserted, updated, and tombstoned. Every listing binary-searches to its start
// position and copies at most limit records, so a page touches O(log n + limit)
// entries regardless of how many records the namespace holds.
//
// Second — and this is the part a naive implementation gets wrong — the view is
// brought up to date INCREMENTALLY. replay above re-decodes a namespace's whole
// log from offset 0, which is the right thing for a cold open and the wrong
// thing for a read: doing it per page would make every page cost the entire
// history, which is precisely the unbounded scan the invariant forbids. catchUp
// therefore reads only [applied, size) and folds those frames in. A warm reader
// with no concurrent writer reads ZERO bytes; a warm reader behind one appended
// frame reads exactly that frame.
//
// # Why the whole mutation is one critical section
//
// Reading the state and appending the mutation derived from it must be atomic
// against the other writers of the same log — including writers in OTHER
// processes, which share nothing but the file. So mutate holds BOTH the
// per-namespace mutex and the exclusive advisory lock across catch-up, decision,
// append, and the fold of the new frame into the view. Catching up before taking
// the lock and appending after would be a textbook check-then-act race: two
// processes would both observe an absent identity and both create it, burning an
// order and putting two create frames for one identity on the disk.
//
// Reads take the same exclusive lock rather than a shared one. It costs read
// concurrency across processes, and it buys the ability to truncate a torn tail
// discovered during a read — a recovery step that is a write and so cannot run
// under LOCK_SH.
//
// # Pagination over a mutable view is weakly consistent, on purpose
//
// The ranked and due cursors name a frozen (rank | due_at, stable_key,
// ordering_scope) position, not a snapshot. A record whose rank or due time
// moves across that position between two pages is therefore SKIPPED if it moved
// behind the cursor and returned TWICE if it moved ahead of it. That is inherent
// to keyset pagination over a live view and the contract permits it explicitly;
// the remedy is on the client, which reconciles by stable identity rather than
// assuming a page sequence is a set partition. A sweep that must observe every
// record exactly once cannot be built from pages alone at any layer.

// errOrderedInconsistentLog is the leaf cause recorded in an
// *OrderedLogCorruptError when a frame contradicts the state it applies to: a
// create for an identity that already exists, an update or delete for one that
// does not, a mutation of a tombstone, an order that fails to increase, or a
// revision that does not advance by exactly one.
//
// None of these can be produced by this package's writers, which decide every
// mutation against the caught-up view inside the same critical section that
// appends it. They are reachable only from a hand-edited or foreign-written log,
// and folding such a frame in would silently corrupt the in-memory indexes
// instead of erroring — so the fold fails closed exactly as the codec does.
var errOrderedInconsistentLog = errors.New("fsstore: ordered log frame contradicts the state it applies to")

// maxOrderedUint64 is the ceiling at which revision and order allocation refuse
// to wrap.
const maxOrderedUint64 = ^uint64(0)

// orderedOrderExhaustedError reports an order scope that has allocated every
// uint64. OrderedIndex has a typed error for revision exhaustion but none for
// acceptance-order exhaustion, so this provider-local type preserves the
// nonzero/no-wrap invariant without inventing a name in the frozen public API.
// It is unreachable short of 2^64 creates in one scope.
type orderedOrderExhaustedError struct {
	ID storage.OrderedID
}

func (e *orderedOrderExhaustedError) Error() string {
	return "fsstore: ordered acceptance order exhausted in scope " +
		strconv.Quote(e.ID.Namespace) + "/" + strconv.Quote(e.ID.OrderingScope)
}

// orderedIdentity is the comparable map key for a record within ONE namespace.
// The namespace is not a field because a view belongs to exactly one namespace —
// the one its log file is named for.
type orderedIdentity struct {
	orderingScope string
	stableKey     storage.StableKey
}

// orderedView is one namespace's current state: the authoritative record map
// plus the three maintained indexes. Entries in the index slices are identities;
// record state lives only in records, so an index can never disagree with a
// record about its own contents.
//
// The slices hold identities rather than records for a specific reason: an
// update rewrites a record in place in the map, and if the indexes held copies
// there would be two truths about one row and no compiler help keeping them
// equal.
type orderedView struct {
	namespace string

	records map[orderedIdentity]storage.OrderedRecord

	// highWater is the largest order ever allocated per ordering scope. It is
	// derived from replay rather than stored, and it is never lowered — a
	// tombstoned identity's order is retained here forever, which is what makes
	// "an order is never reused, including after a delete" true across a reopen.
	highWater map[string]uint64

	// ordered is the acceptance-order index per ordering scope. It is
	// append-only and INCLUDES tombstones, because ListOrdered serves the
	// immutable stream: a consumer resuming from an order cursor has to see that
	// a record it once observed is now deleted.
	ordered map[string][]orderedIdentity

	// ranked is the current descending (rank, stable_key, ordering_scope) index
	// per RANKING scope, and due is the current ascending (due_at, stable_key,
	// ordering_scope) index for the whole namespace. Both hold only live,
	// participating records: a tombstone, an unranked record, and a not-due
	// record are absent rather than filtered at read time.
	ranked map[string][]orderedIdentity
	due    []orderedIdentity
}

func newOrderedView(namespace string) *orderedView {
	return &orderedView{
		namespace: namespace,
		records:   make(map[orderedIdentity]storage.OrderedRecord),
		highWater: make(map[string]uint64),
		ordered:   make(map[string][]orderedIdentity),
		ranked:    make(map[string][]orderedIdentity),
	}
}

// recordFor rebuilds the public record a mutation frame describes. The namespace
// comes from the view, not the frame — it is implied by the log file, which is
// what keeps a StableKey out of every path this package builds.
func (v *orderedView) recordFor(m orderedMutation) storage.OrderedRecord {
	return storage.OrderedRecord{
		ID: storage.OrderedID{
			Namespace:     v.namespace,
			OrderingScope: m.OrderingScope,
			StableKey:     m.StableKey,
		},
		RankingScope: m.RankingScope,
		Revision:     m.Revision,
		Order:        m.Order,
		Due:          m.Due,
		Rank:         m.Rank,
		Value:        m.Value,
		Deleted:      m.deleted(),
	}
}

// apply folds one mutation into the view. It is the ONLY way state enters a
// view, so a replayed frame and a freshly appended one take exactly the same
// path and cannot diverge.
//
// Every consistency rule is checked rather than assumed. This package's writers
// cannot violate them — each mutation is decided against this same view inside
// the critical section that appends it — so a violation means the log was
// written by something else, and folding it in would leave the indexes quietly
// wrong. Failing closed keeps "a page reflects the log" true or reports that it
// cannot be.
func (v *orderedView) apply(m orderedMutation) error {
	key := orderedIdentity{orderingScope: m.OrderingScope, stableKey: m.StableKey}
	record := v.recordFor(m)
	previous, exists := v.records[key]

	if m.Op == orderedOpCreate {
		if exists {
			return errOrderedInconsistentLog
		}
		if record.Revision != 1 {
			return errOrderedInconsistentLog
		}
		// Strictly increasing within the order scope, and never reused — the
		// high-water mark is the whole of that guarantee, and it outlives any
		// tombstone because nothing ever lowers it.
		if record.Order <= v.highWater[m.OrderingScope] {
			return errOrderedInconsistentLog
		}
		v.highWater[m.OrderingScope] = record.Order
		v.records[key] = record
		v.ordered[m.OrderingScope] = append(v.ordered[m.OrderingScope], key)
		v.insertCurrent(key, record)
		return nil
	}

	if !exists {
		return errOrderedInconsistentLog
	}
	if previous.Deleted {
		// A tombstone is terminal: it can be re-read but never mutated again.
		return errOrderedInconsistentLog
	}
	if record.Revision != previous.Revision+1 {
		return errOrderedInconsistentLog
	}
	// Identity, ranking scope, and acceptance order are immutable for the life of
	// a record; a frame that changes one is not an update of it.
	if record.Order != previous.Order || record.RankingScope != previous.RankingScope {
		return errOrderedInconsistentLog
	}
	// Remove under the PREVIOUS record's sort keys before publishing the new one:
	// the index position is derived from the record's current rank and due time,
	// so removing after the rewrite would look for the entry in the wrong place.
	if rerr := v.removeCurrent(key, previous); rerr != nil {
		return rerr
	}
	v.records[key] = record
	v.insertCurrent(key, record)
	return nil
}

// insertCurrent adds key to whichever current indexes record participates in. A
// tombstone participates in neither, and the contract's own rules guarantee that
// without a Deleted test here: a tombstone is unranked and not due.
func (v *orderedView) insertCurrent(key orderedIdentity, record storage.OrderedRecord) {
	if record.Rank.Ranked {
		entries := v.ranked[record.RankingScope]
		position := sort.Search(len(entries), func(i int) bool {
			return !rankedBefore(v.records[entries[i]], record)
		})
		v.ranked[record.RankingScope] = insertOrderedIdentity(entries, position, key)
	}
	if record.Due.State == storage.DueAt {
		position := sort.Search(len(v.due), func(i int) bool {
			return !dueBefore(v.records[v.due[i]], record)
		})
		v.due = insertOrderedIdentity(v.due, position, key)
	}
}

// removeCurrent drops key from the indexes record participated in.
//
// A record that is ranked, or due, MUST be present in the corresponding index —
// insertCurrent put it there under the same two conditions, and the record read
// here is the one the index was built from. A missing entry therefore means the
// view and its indexes disagree, which is the one thing the indexes exist to
// rule out, so it is reported rather than skipped: continuing would publish the
// new record while the stale entry stayed in the sorted slice, and a later page
// would return a record under sort keys it no longer has.
func (v *orderedView) removeCurrent(key orderedIdentity, record storage.OrderedRecord) error {
	if record.Rank.Ranked {
		entries := v.ranked[record.RankingScope]
		position := orderedIdentityPosition(entries, key)
		if position < 0 {
			return errOrderedInconsistentLog
		}
		entries = removeOrderedIdentity(entries, position)
		if len(entries) == 0 {
			delete(v.ranked, record.RankingScope)
		} else {
			v.ranked[record.RankingScope] = entries
		}
	}
	if record.Due.State == storage.DueAt {
		position := orderedIdentityPosition(v.due, key)
		if position < 0 {
			return errOrderedInconsistentLog
		}
		v.due = removeOrderedIdentity(v.due, position)
	}
	return nil
}

func insertOrderedIdentity(entries []orderedIdentity, position int, key orderedIdentity) []orderedIdentity {
	entries = append(entries, orderedIdentity{})
	copy(entries[position+1:], entries[position:])
	entries[position] = key
	return entries
}

func removeOrderedIdentity(entries []orderedIdentity, position int) []orderedIdentity {
	copy(entries[position:], entries[position+1:])
	entries[len(entries)-1] = orderedIdentity{}
	return entries[:len(entries)-1]
}

// orderedIdentityPosition scans LINEARLY, and that is deliberate. A ranked or
// due slice is sorted by keys derived from the CURRENT record, so a binary
// search only finds an entry while the record still carries the rank or due time
// it was inserted under. Removal happens mid-update, in the window between
// reading the old record and publishing the new one, and nothing in the type
// system pins that ordering: reordering two statements in apply would leave a
// binary search silently failing to find the entry — corrupting the index rather
// than erroring. A scan over identity cannot be wrong that way. It makes a
// MUTATION linear in the size of its ranked scope, which invariant 15 does not
// govern; the invariant is about pages, and pages stay logarithmic.
func orderedIdentityPosition(entries []orderedIdentity, want orderedIdentity) int {
	for position, key := range entries {
		if key == want {
			return position
		}
	}
	return -1
}

// rankedBefore is the frozen DESCENDING total order: larger rank first, then
// larger stable key, then larger ordering scope. Namespace and ranking scope are
// fixed by the index the slice belongs to.
//
// The ordering_scope tiebreaker is required, not decorative. Record identity is
// (namespace, ordering_scope, stable_key), so two distinct records may share a
// rank AND a stable key; without the third key the comparison is a preorder, a
// cursor cannot name a unique position among the tied rows, and pagination
// either loops forever or skips them.
func rankedBefore(left storage.OrderedRecord, right storage.OrderedRecord) bool {
	if left.Rank.Value != right.Rank.Value {
		return left.Rank.Value > right.Rank.Value
	}
	if left.ID.StableKey != right.ID.StableKey {
		return left.ID.StableKey > right.ID.StableKey
	}
	return left.ID.OrderingScope > right.ID.OrderingScope
}

// dueBefore is the frozen ASCENDING total order: earlier due time first, then
// smaller stable key, then smaller ordering scope. Only the namespace is fixed
// by the index, so the due view spans every ordering scope in it — which is why
// the ordering_scope tiebreaker matters even more here than for rank.
func dueBefore(left storage.OrderedRecord, right storage.OrderedRecord) bool {
	if left.Due.UnixMillis != right.Due.UnixMillis {
		return left.Due.UnixMillis < right.Due.UnixMillis
	}
	if left.ID.StableKey != right.ID.StableKey {
		return left.ID.StableKey < right.ID.StableKey
	}
	return left.ID.OrderingScope < right.ID.OrderingScope
}

// rankedPositionAfter reports the index of the first entry strictly AFTER the
// position a cursor names, under the descending ranked order.
//
// It searches with rankedBefore itself, against a synthetic record carrying the
// cursor's tuple, rather than restating the comparison. An open-coded copy would
// be a SECOND definition of the sort order: insertion would use one and
// pagination the other, and the two could drift apart silently — a page would
// then resume at a position the index does not actually have, skipping or
// repeating rows for reasons no test of either half could localize.
func (v *orderedView) rankedPositionAfter(entries []orderedIdentity, position rankedCursorPosition) int {
	probe := storage.OrderedRecord{
		ID:   storage.OrderedID{OrderingScope: position.orderingScope, StableKey: position.stableKey},
		Rank: storage.Rank{Ranked: true, Value: position.rank},
	}
	return sort.Search(len(entries), func(i int) bool {
		return rankedBefore(probe, v.records[entries[i]])
	})
}

// duePositionAfter reports the index of the first entry strictly AFTER the
// position a cursor names, under the ascending due order, and searches with
// dueBefore for the same reason rankedPositionAfter searches with rankedBefore.
func (v *orderedView) duePositionAfter(position dueCursorPosition) int {
	probe := storage.OrderedRecord{
		ID:  storage.OrderedID{OrderingScope: position.orderingScope, StableKey: position.stableKey},
		Due: storage.Due{State: storage.DueAt, UnixMillis: position.dueAt},
	}
	return sort.Search(len(v.due), func(i int) bool {
		return dueBefore(probe, v.records[v.due[i]])
	})
}

// dueEndAtOrBefore reports the exclusive end of the due entries eligible for a
// bound. Because the slice is sorted ascending by due time, the bound is a
// binary search rather than a filter — a sweep for "everything due by now" over
// a namespace with a million far-future records inspects none of them.
func (v *orderedView) dueEndAtOrBefore(bound int64) int {
	return sort.Search(len(v.due), func(i int) bool {
		return v.records[v.due[i]].Due.UnixMillis > bound
	})
}

// orderedStartAfter reports the index of the first acceptance-order entry whose
// order exceeds the exclusive numeric cursor.
func (v *orderedView) orderedStartAfter(entries []orderedIdentity, afterOrder uint64) int {
	return sort.Search(len(entries), func(i int) bool {
		return v.records[entries[i]].Order > afterOrder
	})
}

func cloneOrderedRecord(record storage.OrderedRecord) storage.OrderedRecord {
	record.Value = cloneOrderedBytes(record.Value)
	return record
}

// cloneOrderedBytes preserves the nil/empty distinction: a record created with a
// nil Value reads back as nil, not as a zero-length slice.
func cloneOrderedBytes(value []byte) []byte {
	if value == nil {
		return nil
	}
	out := make([]byte, len(value))
	copy(out, value)
	return out
}

// ---------------------------------------------------------------------------
// Incremental catch-up
// ---------------------------------------------------------------------------

// catchUp folds every COMPLETE frame written after the last one already applied
// into the view, and nothing else. It is invariant 15's load-bearing function:
// it never re-reads or re-decodes the applied prefix, so a warm page costs the
// bytes appended since the caller last looked rather than the namespace's
// history. Nothing here walks a directory, lists keys, or opens a second file.
//
// It is called only with both the per-namespace mutex and the exclusive advisory
// lock held, so the file is quiescent for the duration.
//
// The view cache is rebuilt from scratch in exactly two situations: it was never
// loaded, or the file is now SHORTER than the applied offset, which means the log
// was truncated or replaced underneath us and the applied prefix is no longer the
// prefix of this file.
//
// A torn tail found here is truncated away for the same reason replay truncates
// one: the partial frame was never acknowledged to any caller. Any other fault
// leaves the view UNLOADED so the next operation rebuilds rather than serving
// reads from a half-applied index.
func (l *orderedLog) catchUp(f *os.File) error {
	stat, serr := f.Stat()
	if serr != nil {
		return &OrderedLogIOError{Op: "stat", Path: l.path, Cause: serr}
	}
	size := stat.Size()
	if !l.viewLoaded || size < l.applied {
		l.resetView()
	}
	span := size - l.applied
	if span == 0 {
		// Nothing new to fold. This is the warm caught-up reader — not one byte
		// is read — and also the cold reader of an empty or absent-then-created
		// log, which has nothing to rebuild FROM. Neither reads anything, so
		// neither counts as a catch-up.
		l.viewLoaded = true
		return nil
	}
	l.catchUps++

	// One buffer for the whole unapplied span. For a warm reader that span is
	// the bytes appended since it last looked, which is invariant 15's whole
	// point; for a COLD one it is the entire log, and there is no log-size limit
	// anywhere in this module — see the compaction note at the top of this file.
	tail := make([]byte, span)
	if _, rerr := f.ReadAt(tail, l.applied); rerr != nil {
		l.viewLoaded = false
		return &OrderedLogIOError{Op: "read", Path: l.path, Cause: rerr}
	}
	l.scannedBytes += span

	mutations, lastSeq, goodEnd, scerr := scanOrderedLog(tail, l.path, l.appliedSeq+1)
	if scerr != nil {
		l.viewLoaded = false
		return scerr
	}
	if goodEnd < span {
		if terr := f.Truncate(l.applied + goodEnd); terr != nil {
			l.viewLoaded = false
			return &OrderedLogIOError{Op: "truncate", Path: l.path, Cause: terr}
		}
		if fserr := f.Sync(); fserr != nil {
			l.viewLoaded = false
			return &OrderedLogIOError{Op: "fsync", Path: l.path, Cause: fserr}
		}
	}
	for _, m := range mutations {
		if aerr := l.view.apply(m); aerr != nil {
			l.viewLoaded = false
			return &OrderedLogCorruptError{Path: l.path, Seq: l.appliedSeq + 1, Cause: aerr}
		}
		l.appliedSeq++
		l.appliedFrames++
	}
	if lastSeq != 0 && l.appliedSeq != lastSeq {
		// Unreachable: the scan already proved contiguity from appliedSeq+1, so
		// the fold advanced appliedSeq exactly to lastSeq. Asserted anyway,
		// because a silent disagreement here would desynchronize every later
		// incremental scan from the file it is reading.
		l.viewLoaded = false
		return &OrderedLogCorruptError{Path: l.path, Seq: lastSeq, Cause: errOrderedNonContiguousSeq}
	}
	l.applied += goodEnd
	l.viewLoaded = true
	return nil
}

// resetView discards the current view and rewinds the applied position, so the
// next catch-up rebuilds from offset 0.
func (l *orderedLog) resetView() {
	l.view = newOrderedView(l.namespace)
	l.viewLoaded = false
	l.applied = 0
	l.appliedSeq = 0
}

// orderedLogStats reports the cumulative catch-up work one namespace's log has
// performed. It exists so the bounded-work property invariant 15 demands can be
// ASSERTED rather than asserted-about-in-a-comment: a test reads it, performs a
// page, and reads it again.
type orderedLogStats struct {
	// CatchUps counts catch-ups that actually read bytes. A caught-up reader
	// does not increment it.
	CatchUps uint64
	// ScannedBytes is the total number of log bytes read by catch-ups.
	ScannedBytes int64
	// AppliedFrames is the total number of mutation frames folded into the view.
	AppliedFrames uint64
}

// stats reports aggregate catch-up work across every namespace this store has
// touched. The registry lock is held only long enough to take a stable snapshot
// of the log pointers; each log then supplies its own counters under its own
// mutex. This preserves the registry->log lock separation used by logFor while
// giving provider conformance one store-wide measurement seam.
func (s *orderedLogStore) stats() orderedLogStats {
	s.mu.Lock()
	logs := make([]*orderedLog, 0, len(s.logs))
	for _, log := range s.logs {
		logs = append(logs, log)
	}
	s.mu.Unlock()

	var total orderedLogStats
	for _, log := range logs {
		stats := log.stats()
		total.CatchUps += stats.CatchUps
		total.ScannedBytes += stats.ScannedBytes
		total.AppliedFrames += stats.AppliedFrames
	}
	return total
}

// stats returns the log's cumulative catch-up counters.
func (l *orderedLog) stats() orderedLogStats {
	_ = l.mu.lock(context.Background())
	defer l.mu.unlock()
	return orderedLogStats{CatchUps: l.catchUps, ScannedBytes: l.scannedBytes, AppliedFrames: l.appliedFrames}
}

// orderedDecision is what a mutation callback hands back: the frame to append,
// or nil for a read (or for a mutation that turned out to be a no-op, such as a
// duplicate create).
type orderedDecision struct {
	mutation *orderedMutation
}

// withView runs fn against the namespace's caught-up current view, optionally
// appending the one frame fn decides on, under both the per-namespace mutex and
// the exclusive cross-process advisory lock.
//
// This single critical section is what makes a mutation atomic across processes.
// fn observes the state, decides, and its frame is committed and folded in
// before any other writer can look — so two processes cannot both find an
// identity absent and both create it.
//
// mutating selects two behaviors together, because they belong together: the log
// file is created if missing, and fn is allowed to return a frame. A read never
// creates a file, so listing an untouched namespace leaves no trace on disk.
//
// A read still opens O_RDWR and still takes LOCK_EX, though, because catch-up
// may truncate a torn tail and that is a write. Two consequences follow and
// neither is fixed here. Reads do not run against a read-only store or mount:
// the open fails with EACCES/EROFS before anything is read. And reads do not run
// concurrently across processes, because LOCK_EX excludes even other readers.
//
// The alternative is a read that stops at goodEnd, leaves the torn tail in place
// for the next writer to repair — which is what the ledger's Tip and Read
// already do — and so can take O_RDONLY and LOCK_SH. It is a real improvement
// and a real change: it re-walks the damaged tail on every read until a write
// arrives, it needs its own cross-process test for the shared-lock path, and it
// reintroduces a second recovery behaviour to keep straight. It is deliberately
// left for a change that can carry those tests rather than folded in here.
//
// ctx may cancel either lock wait before file work begins. The advisory wait
// polls for the same exclusive LOCK_EX using LOCK_NB, rather than abandoning a
// goroutine blocked in flock that could later acquire a lock nobody releases.
// Once both locks are held, ctx is deliberately ignored: abandoning work midway
// is unsafe for a path that may truncate recovery bytes or durably append.
//
// The seq return is meaningful ON ERROR, and callers must read it: a NONZERO seq
// with a non-nil error means the frame WAS committed, fsynced, and folded into
// the view, and only a later durability step (the ancestor directory fsync, the
// close, or the unlock) failed. Retrying such a mutation would not repeat it —
// the view already holds it, so a Create retry takes the duplicate path — but a
// caller must not report it to ITS caller as "nothing happened".
func (l *orderedLog) withView(ctx context.Context, mutating bool, fn func(v *orderedView) (orderedDecision, error)) (seq uint64, err error) {
	if lerr := l.mu.lock(ctx); lerr != nil {
		return 0, lerr
	}
	defer l.mu.unlock()

	dir := filepath.Dir(l.path)
	flags := os.O_RDWR
	if mutating {
		// Best-effort and idempotent; the durability-critical "was this log just
		// created" decision is made below from on-disk state read under the lock.
		if merr := os.MkdirAll(dir, 0o700); merr != nil {
			return 0, &OrderedLogIOError{Op: "mkdir", Path: dir, Cause: merr}
		}
		flags |= os.O_CREATE
	}

	f, oerr := os.OpenFile(l.path, flags, 0o600) // #nosec G304 -- l.path is validated and contained under <root>/ordered
	if errors.Is(oerr, fs.ErrNotExist) {
		if cerr := ctx.Err(); cerr != nil {
			return 0, cerr
		}
		// A read of a namespace that has never been written. It has no state, and
		// creating a file to say so would litter the store with empty logs.
		l.resetView()
		l.viewLoaded = true
		_, ferr := fn(l.view)
		return 0, ferr
	}
	if oerr != nil {
		return 0, &OrderedLogIOError{Op: "open", Path: l.path, Cause: oerr}
	}
	// Defers run LIFO: unlock (registered second) releases the advisory lock
	// before close (registered first) releases the fd. Neither zeroes seq — they
	// only run once a frame is already durable, and reporting a committed
	// mutation as "nothing happened" is what invites a duplicating retry.
	defer func() {
		if cerr := f.Close(); cerr != nil && err == nil {
			err = &OrderedLogIOError{Op: "close", Path: l.path, Cause: cerr}
		}
	}()
	if lerr := lockFileExContext(ctx, f); lerr != nil {
		return 0, lerr
	}
	defer func() {
		if uerr := unlockFile(f); uerr != nil && err == nil {
			err = uerr
		}
	}()

	if cerr := l.catchUp(f); cerr != nil {
		return 0, cerr
	}
	// Authoritative creation decision from on-disk state observed under the lock.
	created := l.applied == 0

	decision, ferr := fn(l.view)
	if ferr != nil {
		return 0, ferr
	}
	if decision.mutation == nil {
		return 0, nil
	}

	frame, eerr := encodeOrderedFrame(l.appliedSeq+1, *decision.mutation)
	if eerr != nil {
		return 0, eerr // an invalid mutation: a definite failure, never ambiguous
	}
	if _, werr := f.WriteAt(frame, l.applied); werr != nil {
		return 0, &OrderedLogIOError{Op: "write", Path: l.path, Cause: werr}
	}
	if serr := f.Sync(); serr != nil {
		return 0, &OrderedLogIOError{Op: "fsync", Path: l.path, Cause: serr}
	}
	// Durable from here on. Advance the cache and fold the frame in BEFORE the
	// remaining durability step, so everything after this point can fail without
	// the mutation ceasing to exist — and so a retry sees it.
	l.appliedSeq++
	l.applied += int64(len(frame))
	if aerr := l.view.apply(*decision.mutation); aerr != nil {
		// The frame is durable, so the log now holds a mutation the view refuses.
		// fn derives its mutation from this very view, so a caller of this
		// package cannot reach here; a log written by something else can. Drop
		// the view so the next use rebuilds and fails closed on the same frame.
		l.viewLoaded = false
		return l.appliedSeq, &OrderedLogCorruptError{Path: l.path, Seq: l.appliedSeq, Cause: aerr}
	}
	l.appliedFrames++
	if created {
		if derr := fsyncDirChain(dir, l.stopDir, orderedIOErr); derr != nil {
			return l.appliedSeq, derr
		}
	}
	return l.appliedSeq, nil
}

const (
	orderedLockRetryInitial = 100 * time.Microsecond
	orderedLockRetryMax     = 10 * time.Millisecond
)

// lockFileExContext waits for an exclusive advisory lock without entering an
// uncancellable flock syscall. A successful acquisition remains LOCK_EX; only
// the waiting strategy changes. The bounded exponential delay reduces syscall
// pressure under contention while checking cancellation frequently.
func lockFileExContext(ctx context.Context, f *os.File) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	delay := orderedLockRetryInitial
	for {
		acquired, err := tryLockFileEx(f)
		if err != nil {
			return err
		}
		if acquired {
			if err := ctx.Err(); err != nil {
				return errors.Join(err, unlockFile(f))
			}
			return nil
		}

		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return ctx.Err()
		case <-timer.C:
		}
		if delay < orderedLockRetryMax {
			delay *= 2
			if delay > orderedLockRetryMax {
				delay = orderedLockRetryMax
			}
		}
	}
}

// ---------------------------------------------------------------------------
// storage.OrderedIndex
// ---------------------------------------------------------------------------

// orderedIndex is fsstore's OrderedIndex provider. It owns no state of its own:
// every namespace's records live in that namespace's log and the view derived
// from it, so two orderedIndex values over one root are consistent through the
// advisory lock exactly as two processes are.
type orderedIndex struct {
	store *orderedLogStore
}

func (x *orderedIndex) stats() orderedLogStats { return x.store.stats() }

// Compile-time proof that *orderedIndex honors the OrderedIndex contract.
var _ storage.OrderedIndex = (*orderedIndex)(nil)

// newOrderedIndex returns an OrderedIndex over the namespace logs under root.
func newOrderedIndex(root string) (*orderedIndex, error) {
	store, err := newOrderedLogStore(root)
	if err != nil {
		return nil, err
	}
	return &orderedIndex{store: store}, nil
}

// Close releases the provider's in-process state. It is idempotent.
func (x *orderedIndex) Close() error { return x.store.Close() }

// Get returns a caller-owned snapshot of id's current record, tombstones
// included. An identity that has never been created returns
// *OrderedRecordNotFoundError.
//
// The caller's id is validated with storage.ValidateOrderedID FIRST and its
// error is returned UNWRAPPED, so a bad namespace or ordering scope surfaces as
// *storage.InvalidNameError and a bad key as *storage.InvalidStableKeyError —
// the errors the contract names. The codec's own validation stays where it is,
// as the internal backstop that keeps an illegal mutation off the disk; it must
// not be the thing a public caller's classification depends on, because the same
// codec error also reports a DAMAGED LOG when it comes back from replay.
func (x *orderedIndex) Get(ctx context.Context, id storage.OrderedID) (storage.OrderedRecord, error) {
	if err := storage.ValidateOrderedID(id); err != nil {
		return storage.OrderedRecord{}, err
	}
	log, err := x.store.logFor(id.Namespace)
	if err != nil {
		return storage.OrderedRecord{}, err
	}
	var out storage.OrderedRecord
	_, err = log.withView(ctx, false, func(v *orderedView) (orderedDecision, error) {
		record, ok := v.records[identityFor(id)]
		if !ok {
			return orderedDecision{}, &storage.OrderedRecordNotFoundError{ID: id}
		}
		out = cloneOrderedRecord(record)
		return orderedDecision{}, nil
	})
	if err != nil {
		return storage.OrderedRecord{}, err
	}
	return out, nil
}

// Create is idempotent by identity. A duplicate — including a tombstone —
// returns the canonical stored record with created false and WITHOUT validating
// the candidate ranking scope, value, rank, or due state, which is what lets a
// retry of an unknown-outcome create return the original record rather than a
// validation error. Only an absent identity validates the candidate, takes
// revision 1, and allocates the next order in its scope.
//
// The lookup, the allocation, and the append all happen inside one critical
// section that holds the exclusive cross-process lock, so two processes racing
// on one identity cannot both create it and cannot both consume an order.
//
// A non-nil error is NOT automatically "nothing happened". If the returned
// record carries a nonzero Order, the create was committed and fsynced and only
// a later durability step failed; the identity now exists, and a retry returns
// it through the duplicate path rather than allocating a second order.
func (x *orderedIndex) Create(ctx context.Context, id storage.OrderedID, rankingScope string, value []byte, rank storage.Rank, due storage.Due) (storage.OrderedRecord, bool, error) {
	if err := storage.ValidateOrderedID(id); err != nil {
		return storage.OrderedRecord{}, false, err
	}
	log, err := x.store.logFor(id.Namespace)
	if err != nil {
		return storage.OrderedRecord{}, false, err
	}
	var (
		out     storage.OrderedRecord
		created bool
	)
	_, err = log.withView(ctx, true, func(v *orderedView) (orderedDecision, error) {
		key := identityFor(id)
		if record, ok := v.records[key]; ok {
			out, created = cloneOrderedRecord(record), false
			return orderedDecision{}, nil
		}
		// Candidate validation deliberately follows the duplicate lookup: the
		// contract requires a duplicate to be answered without it.
		if verr := validateOrderedCandidate(rankingScope, value, due); verr != nil {
			return orderedDecision{}, verr
		}
		order := v.highWater[id.OrderingScope]
		if order == maxOrderedUint64 {
			return orderedDecision{}, &orderedOrderExhaustedError{ID: id}
		}
		order++
		record := storage.OrderedRecord{
			ID:           id,
			RankingScope: rankingScope,
			Revision:     1,
			Order:        order,
			Due:          due,
			Rank:         rank,
			Value:        cloneOrderedBytes(value),
		}
		// An unranked record's rank value is meaningless and the wire format has
		// exactly one spelling for it, so normalize before the record is either
		// published or encoded — otherwise the record returned now and the record
		// replayed after a reopen would differ.
		record.Rank = normalizedRank(record.Rank)
		if rerr := storage.ValidateOrderedRecord(record); rerr != nil {
			return orderedDecision{}, rerr
		}
		out, created = cloneOrderedRecord(record), true
		return orderedDecision{mutation: mutationFor(orderedOpCreate, record)}, nil
	})
	if err != nil {
		// A nonzero Order means the frame is committed; report it alongside the
		// error rather than erasing it (see the method comment).
		return out, created, err
	}
	return out, created, nil
}

// Update performs one revision CAS over a live record, replacing value, rank,
// and due state together and advancing the revision exactly once. It cannot
// change identity, ranking scope, or acceptance order.
//
// The precedence is the contract's and is not negotiable: an absent record and a
// tombstone are reported regardless of expectedRevision, and only for a live
// record is the candidate validated — before a stale revision is reported.
func (x *orderedIndex) Update(ctx context.Context, id storage.OrderedID, expectedRevision uint64, value []byte, rank storage.Rank, due storage.Due) (storage.OrderedRecord, error) {
	if err := storage.ValidateOrderedID(id); err != nil {
		return storage.OrderedRecord{}, err
	}
	log, err := x.store.logFor(id.Namespace)
	if err != nil {
		return storage.OrderedRecord{}, err
	}
	var out storage.OrderedRecord
	_, err = log.withView(ctx, true, func(v *orderedView) (orderedDecision, error) {
		key := identityFor(id)
		record, ok := v.records[key]
		if !ok {
			return orderedDecision{}, &storage.OrderedRecordNotFoundError{ID: id}
		}
		if record.Deleted {
			return orderedDecision{}, &storage.OrderedDeletedError{ID: id}
		}
		if verr := validateOrderedMutable(value, due); verr != nil {
			return orderedDecision{}, verr
		}
		if expectedRevision != record.Revision {
			return orderedDecision{}, &storage.OrderedRevisionConflictError{
				ID:               id,
				ExpectedRevision: expectedRevision,
				ActualRevision:   record.Revision,
			}
		}
		if record.Revision == maxOrderedUint64 {
			return orderedDecision{}, &storage.OrderedRevisionExhaustedError{ID: id, Revision: record.Revision}
		}
		next := record
		next.Revision++
		next.Value = cloneOrderedBytes(value)
		next.Rank = normalizedRank(rank)
		next.Due = due
		if rerr := storage.ValidateOrderedRecord(next); rerr != nil {
			return orderedDecision{}, rerr
		}
		out = cloneOrderedRecord(next)
		return orderedDecision{mutation: mutationFor(orderedOpUpdate, next)}, nil
	})
	if err != nil {
		return out, err
	}
	return out, nil
}

// Delete turns a live record into its terminal tombstone in one revision CAS:
// the revision advances once, Deleted is set, rank clears to the zero Rank and
// due state to canonical not-due, and value, ranking scope, identity, and
// acceptance order are preserved.
//
// A retry is answered by returning the existing tombstone regardless of
// expectedRevision — including a retry carrying the PRE-delete revision, which
// is exactly what a caller whose first attempt returned an ambiguous error
// holds. The tombstone and its acceptance-order entry are retained forever, so
// the identity and its order can never be reused.
func (x *orderedIndex) Delete(ctx context.Context, id storage.OrderedID, expectedRevision uint64) (storage.OrderedRecord, error) {
	if err := storage.ValidateOrderedID(id); err != nil {
		return storage.OrderedRecord{}, err
	}
	log, err := x.store.logFor(id.Namespace)
	if err != nil {
		return storage.OrderedRecord{}, err
	}
	var out storage.OrderedRecord
	_, err = log.withView(ctx, true, func(v *orderedView) (orderedDecision, error) {
		key := identityFor(id)
		record, ok := v.records[key]
		if !ok {
			return orderedDecision{}, &storage.OrderedRecordNotFoundError{ID: id}
		}
		if record.Deleted {
			out = cloneOrderedRecord(record)
			return orderedDecision{}, nil
		}
		if expectedRevision != record.Revision {
			return orderedDecision{}, &storage.OrderedRevisionConflictError{
				ID:               id,
				ExpectedRevision: expectedRevision,
				ActualRevision:   record.Revision,
			}
		}
		if record.Revision == maxOrderedUint64 {
			return orderedDecision{}, &storage.OrderedRevisionExhaustedError{ID: id, Revision: record.Revision}
		}
		tombstone := record
		tombstone.Revision++
		tombstone.Deleted = true
		tombstone.Rank = storage.Rank{}
		tombstone.Due = storage.Due{State: storage.NotDue}
		if rerr := storage.ValidateOrderedRecord(tombstone); rerr != nil {
			return orderedDecision{}, rerr
		}
		out = cloneOrderedRecord(tombstone)
		return orderedDecision{mutation: mutationFor(orderedOpDelete, tombstone)}, nil
	})
	if err != nil {
		return out, err
	}
	return out, nil
}

// ListOrdered returns the immutable acceptance-order stream for one order scope,
// INCLUDING tombstones, ascending after the exclusive numeric cursor. Tombstones
// belong in it because it is the stream a consumer resumes: a record it once
// observed becoming deleted is an event it has to see.
func (x *orderedIndex) ListOrdered(ctx context.Context, namespace string, orderingScope string, afterOrder uint64, limit int) (storage.OrderedPage, error) {
	if err := storage.ValidateName(namespace); err != nil {
		return storage.OrderedPage{}, err
	}
	if err := storage.ValidateName(orderingScope); err != nil {
		return storage.OrderedPage{}, err
	}
	if err := storage.ValidateOrderedLimit(limit); err != nil {
		return storage.OrderedPage{}, err
	}
	log, err := x.store.logFor(namespace)
	if err != nil {
		return storage.OrderedPage{}, err
	}
	var page storage.OrderedPage
	_, err = log.withView(ctx, false, func(v *orderedView) (orderedDecision, error) {
		entries := v.ordered[orderingScope]
		start := v.orderedStartAfter(entries, afterOrder)
		page.Records = v.collect(entries, start, min(start+limit, len(entries)))
		if len(page.Records) > 0 {
			page.NextAfterOrder = page.Records[len(page.Records)-1].Order
		}
		return orderedDecision{}, nil
	})
	if err != nil {
		return storage.OrderedPage{}, err
	}
	return page, nil
}

// ListRanked returns current, nondeleted, ranked records for one ranking scope
// in descending (rank, stable_key, ordering_scope) order. The cursor is decoded
// and bound-checked BEFORE any lock is taken: an invalid token is a caller
// error, and answering it must not queue behind another process's write.
func (x *orderedIndex) ListRanked(ctx context.Context, namespace string, rankingScope string, after storage.RankedCursor, limit int) (storage.RankedPage, error) {
	if err := storage.ValidateName(namespace); err != nil {
		return storage.RankedPage{}, err
	}
	if err := storage.ValidateName(rankingScope); err != nil {
		return storage.RankedPage{}, err
	}
	if err := storage.ValidateOrderedLimit(limit); err != nil {
		return storage.RankedPage{}, err
	}
	position, hasPosition, err := decodeRankedCursor(after, namespace, rankingScope)
	if err != nil {
		return storage.RankedPage{}, err
	}
	log, err := x.store.logFor(namespace)
	if err != nil {
		return storage.RankedPage{}, err
	}
	var page storage.RankedPage
	_, err = log.withView(ctx, false, func(v *orderedView) (orderedDecision, error) {
		entries := v.ranked[rankingScope]
		start := 0
		if hasPosition {
			start = v.rankedPositionAfter(entries, position)
		}
		end := min(start+limit, len(entries))
		page.Records = v.collect(entries, start, end)
		if end < len(entries) {
			page.NextCursor = encodeRankedCursor(rankedPositionFor(page.Records[len(page.Records)-1]))
		}
		return orderedDecision{}, nil
	})
	if err != nil {
		return storage.RankedPage{}, err
	}
	return page, nil
}

// ListDue returns current, nondeleted records due at or before the bound across
// every ordering scope in the namespace, ascending by (due_at, stable_key,
// ordering_scope). The bound is a binary search over the sorted due index, not a
// filter, so far-future records cost nothing to skip.
func (x *orderedIndex) ListDue(ctx context.Context, namespace string, dueAtOrBefore int64, after storage.DueCursor, limit int) (storage.DuePage, error) {
	if err := storage.ValidateName(namespace); err != nil {
		return storage.DuePage{}, err
	}
	if err := storage.ValidateOrderedLimit(limit); err != nil {
		return storage.DuePage{}, err
	}
	position, hasPosition, err := decodeDueCursor(after, namespace, dueAtOrBefore)
	if err != nil {
		return storage.DuePage{}, err
	}
	log, err := x.store.logFor(namespace)
	if err != nil {
		return storage.DuePage{}, err
	}
	var page storage.DuePage
	_, err = log.withView(ctx, false, func(v *orderedView) (orderedDecision, error) {
		start := 0
		if hasPosition {
			start = v.duePositionAfter(position)
		}
		eligibleEnd := v.dueEndAtOrBefore(dueAtOrBefore)
		start = min(start, eligibleEnd)
		end := min(start+limit, eligibleEnd)
		page.Records = v.collect(v.due, start, end)
		if end < eligibleEnd {
			page.NextCursor = encodeDueCursor(duePositionFor(dueAtOrBefore, page.Records[len(page.Records)-1]))
		}
		return orderedDecision{}, nil
	})
	if err != nil {
		return storage.DuePage{}, err
	}
	return page, nil
}

// collect copies entries[start:end] out as caller-owned record snapshots. Every
// Value is cloned: the view keeps its own copy for the life of the record, and
// handing a caller the backing slice would let it mutate another reader's data.
func (v *orderedView) collect(entries []orderedIdentity, start, end int) []storage.OrderedRecord {
	out := make([]storage.OrderedRecord, 0, end-start)
	for _, key := range entries[start:end] {
		out = append(out, cloneOrderedRecord(v.records[key]))
	}
	return out
}

// identityFor projects a public OrderedID onto the view's namespace-relative map
// key. The namespace is dropped because the view already is one namespace.
func identityFor(id storage.OrderedID) orderedIdentity {
	return orderedIdentity{orderingScope: id.OrderingScope, stableKey: id.StableKey}
}

// mutationFor renders a published record as the frame that records it. The Value
// is the view's own copy rather than the caller's, and the encoder copies it
// again into the payload, so nothing the caller still holds reaches the disk or
// the index.
func mutationFor(op orderedOp, record storage.OrderedRecord) *orderedMutation {
	return &orderedMutation{
		Op:            op,
		OrderingScope: record.ID.OrderingScope,
		StableKey:     record.ID.StableKey,
		RankingScope:  record.RankingScope,
		Revision:      record.Revision,
		Order:         record.Order,
		Due:           record.Due,
		Rank:          record.Rank,
		Value:         record.Value,
	}
}

// normalizedRank gives an unranked rank its single canonical form. storage
// permits Rank{Ranked: false, Value: 7} from a caller and a backend may not be
// stricter than the contract, but the wire format has one spelling for
// "unranked" — so the value published in memory must be the one that survives a
// reopen, or a record would change under a restart.
func normalizedRank(rank storage.Rank) storage.Rank {
	if !rank.Ranked {
		rank.Value = 0
	}
	return rank
}

// validateOrderedCandidate validates the fields only an ABSENT identity's create
// candidate is checked against. Rank has no invalid representation.
func validateOrderedCandidate(rankingScope string, value []byte, due storage.Due) error {
	if err := storage.ValidateName(rankingScope); err != nil {
		return err
	}
	return validateOrderedMutable(value, due)
}

// validateOrderedMutable validates the mutable candidate fields an update checks
// only once it has established that the target is live.
func validateOrderedMutable(value []byte, due storage.Due) error {
	if err := storage.ValidateOrderedValue(value); err != nil {
		return err
	}
	return storage.ValidateDue(due)
}
