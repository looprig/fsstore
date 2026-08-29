package fsstore

import (
	"bytes"
	"errors"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/looprig/storage"
)

// newTestOrderedLogStore returns a mutation-log store rooted in a fresh temp
// directory, plus that root.
func newTestOrderedLogStore(t *testing.T) (*orderedLogStore, string) {
	t.Helper()
	root := t.TempDir()
	store, err := newOrderedLogStore(root)
	if err != nil {
		t.Fatalf("newOrderedLogStore: %v", err)
	}
	t.Cleanup(func() {
		if cerr := store.Close(); cerr != nil {
			t.Errorf("close ordered log store: %v", cerr)
		}
	})
	return store, root
}

// createMutation builds a well-formed create mutation with the given key and
// order. Tests that care about a specific field override it on the result.
func createMutation(scope string, key storage.StableKey, order uint64) orderedMutation {
	return orderedMutation{
		Op:            orderedOpCreate,
		OrderingScope: scope,
		StableKey:     key,
		RankingScope:  "catalog",
		Revision:      1,
		Order:         order,
		Value:         []byte("v" + strconv.FormatUint(order, 10)),
	}
}

// mustAppend appends m and returns the assigned log sequence.
func mustAppend(t *testing.T, log *orderedLog, m orderedMutation) uint64 {
	t.Helper()
	seq, err := log.append(m)
	if err != nil {
		t.Fatalf("append %+v: %v", m, err)
	}
	return seq
}

// mustReplay replays log and returns its mutations.
func mustReplay(t *testing.T, log *orderedLog) []orderedMutation {
	t.Helper()
	got, err := log.replay()
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	return got
}

func TestOrderedLogAppendReplayRoundTrip(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		namespace string
		mutations []orderedMutation
	}{
		{
			name:      "empty log replays as nothing",
			namespace: "sessions",
		},
		{
			name:      "single create",
			namespace: "sessions",
			mutations: []orderedMutation{createMutation("host", "alpha", 1)},
		},
		{
			name:      "create update delete for one identity",
			namespace: "sessions",
			mutations: []orderedMutation{
				createMutation("host", "alpha", 1),
				{
					Op: orderedOpUpdate, OrderingScope: "host", StableKey: "alpha", RankingScope: "catalog",
					Revision: 2, Order: 1,
					Due:   storage.Due{State: storage.DueAt, UnixMillis: 1735689600000},
					Rank:  storage.Rank{Ranked: true, Value: -42},
					Value: []byte("updated"),
				},
				{
					Op: orderedOpDelete, OrderingScope: "host", StableKey: "alpha", RankingScope: "catalog",
					Revision: 3, Order: 1, Value: []byte("updated"),
				},
			},
		},
		{
			name:      "sparse non-contiguous orders across scopes",
			namespace: "factory/hosts",
			mutations: []orderedMutation{
				createMutation("east", "a", 7),
				createMutation("west", "b", 9000),
				createMutation("east", "c", 8),
				createMutation("west", "d", 1<<40),
			},
		},
		{
			name:      "value at the ordered ceiling",
			namespace: "big",
			mutations: []orderedMutation{
				func() orderedMutation {
					m := createMutation("host", "huge", 1)
					m.Value = bytes.Repeat([]byte{0x5A}, storage.MaxOrderedValueBytes)
					return m
				}(),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			store, _ := newTestOrderedLogStore(t)
			log, err := store.logFor(tt.namespace)
			if err != nil {
				t.Fatalf("logFor(%q): %v", tt.namespace, err)
			}
			for i, m := range tt.mutations {
				got := mustAppend(t, log, m)
				if want := uint64(i + 1); got != want {
					t.Fatalf("append %d: seq = %d, want %d", i, got, want)
				}
			}

			// Replay through a SECOND store over the same root, so the result
			// comes off the disk rather than from the appending store's cache.
			cold, cerr := newOrderedLogStore(store.root)
			if cerr != nil {
				t.Fatalf("reopen ordered log store: %v", cerr)
			}
			defer func() {
				if err := cold.Close(); err != nil {
					t.Errorf("close cold store: %v", err)
				}
			}()
			coldLog, lerr := cold.logFor(tt.namespace)
			if lerr != nil {
				t.Fatalf("cold logFor: %v", lerr)
			}
			got := mustReplay(t, coldLog)
			if len(got) != len(tt.mutations) {
				t.Fatalf("replayed %d mutations, want %d", len(got), len(tt.mutations))
			}
			for i := range got {
				if !mutationsEqual(got[i], tt.mutations[i]) {
					t.Errorf("mutation %d:\n got %+v\nwant %+v", i, got[i], tt.mutations[i])
				}
			}
		})
	}
}

func TestOrderedLogRejectsInvalidNamespace(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		namespace string
	}{
		{name: "empty", namespace: ""},
		{name: "uppercase", namespace: "Sessions"},
		{name: "leading slash", namespace: "/sessions"},
		{name: "trailing slash", namespace: "sessions/"},
		{name: "dot dot segment", namespace: "../escape"},
		{name: "embedded dot dot", namespace: "a/../../escape"},
		{name: "doubled slash", namespace: "a//b"},
		{name: "absolute path", namespace: "/etc/passwd"},
		{name: "backslash", namespace: `a\b`},
		{name: "null byte", namespace: "a\x00b"},
		{name: "too long", namespace: strings.Repeat("a", 513)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			store, _ := newTestOrderedLogStore(t)
			log, err := store.logFor(tt.namespace)
			if err == nil {
				t.Fatalf("logFor(%q) succeeded with %+v, want an error", tt.namespace, log)
			}
			if log != nil {
				t.Errorf("log = %+v, want nil on error", log)
			}
			var ine *storage.InvalidNameError
			var pe *OrderedLogPathError
			if !errors.As(err, &ine) && !errors.As(err, &pe) {
				t.Fatalf("want *storage.InvalidNameError or *OrderedLogPathError, got %T: %v", err, err)
			}
		})
	}
}

// TestOrderedLogPathNeverContainsStableKey is a SECURITY test, not a style one.
// A StableKey is opaque user data that may contain '/', '..', uppercase bytes,
// or arbitrary Unicode. It lives inside a frame and must never become a path
// component: were it interpolated into a path, "../../etc/x" would write outside
// the store root. The test appends hostile keys and asserts that the store's
// entire on-disk tree is exactly one file per namespace, that no path contains
// any key's bytes, and that the keys still round-trip intact through replay.
func TestOrderedLogPathNeverContainsStableKey(t *testing.T) {
	t.Parallel()
	hostile := []storage.StableKey{
		"../../../../etc/passwd",
		"..",
		"slash/value",
		"UPPER_CASE",
		"v1:ABC_def-09",
		"Ünïcödé-Ключ-鍵",
		".hidden",
		"space and \ttab",
		storage.StableKey(strings.Repeat("z", storage.MaxStableKeyBytes)),
	}
	store, root := newTestOrderedLogStore(t)
	const namespace = "sessions"
	log, err := store.logFor(namespace)
	if err != nil {
		t.Fatalf("logFor: %v", err)
	}
	for i, key := range hostile {
		mustAppend(t, log, createMutation("host", key, uint64(i+1)))
	}

	wantPath := filepath.Join(root, orderedDirName, namespace+orderedLogExt)
	var files []string
	walkErr := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		files = append(files, path)
		return nil
	})
	if walkErr != nil {
		t.Fatalf("walk store root: %v", walkErr)
	}
	if len(files) != 1 || files[0] != wantPath {
		t.Fatalf("store tree = %v, want exactly [%s]", files, wantPath)
	}
	for _, key := range hostile {
		for _, path := range files {
			if strings.Contains(path, string(key)) {
				t.Errorf("path %q contains stable key %q", path, key)
			}
		}
	}

	got := mustReplay(t, log)
	if len(got) != len(hostile) {
		t.Fatalf("replayed %d mutations, want %d", len(got), len(hostile))
	}
	for i, key := range hostile {
		if got[i].StableKey != key {
			t.Errorf("mutation %d: key = %q, want %q", i, got[i].StableKey, key)
		}
	}
}

// TestOrderedLogRejectsUnwritableStableKey proves the append path is closed to a
// key the codec cannot represent, rather than writing a frame it could not read
// back.
func TestOrderedLogRejectsUnwritableStableKey(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		key  storage.StableKey
	}{
		{name: "empty", key: ""},
		{name: "invalid utf-8", key: storage.StableKey([]byte{0xFF, 0xFE})},
		{name: "one over the byte ceiling", key: storage.StableKey(strings.Repeat("k", storage.MaxStableKeyBytes+1))},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			store, root := newTestOrderedLogStore(t)
			log, err := store.logFor("sessions")
			if err != nil {
				t.Fatalf("logFor: %v", err)
			}
			if seq, aerr := log.append(createMutation("host", tt.key, 1)); aerr == nil {
				t.Fatalf("append of an unrepresentable key succeeded at seq %d", seq)
			} else {
				var ce *OrderedCodecError
				if !errors.As(aerr, &ce) {
					t.Fatalf("want *OrderedCodecError, got %T: %v", aerr, aerr)
				}
			}
			// The rejection must leave nothing durable behind.
			logPath := filepath.Join(root, orderedDirName, "sessions"+orderedLogExt)
			if info, serr := os.Stat(logPath); serr == nil && info.Size() != 0 {
				t.Errorf("log file grew to %d bytes on a rejected append", info.Size())
			}
			if got := mustReplay(t, log); len(got) != 0 {
				t.Errorf("replay = %v, want nothing", got)
			}
		})
	}
}

// cutTail models an interrupted write that stopped mid-frame: the trailing n
// bytes of the file are simply absent. It is the EASY tear — the frame layer
// reports it as short and the recovery path is obvious.
func cutTail(n int) func(t *testing.T, path string, goodEnd, fullEnd int64) {
	return func(t *testing.T, path string, goodEnd, fullEnd int64) {
		t.Helper()
		if int64(n) >= fullEnd-goodEnd {
			t.Fatalf("cut %d would remove a whole frame, not tear one", n)
		}
		if terr := os.Truncate(path, fullEnd-int64(n)); terr != nil {
			t.Fatalf("truncate: %v", terr)
		}
	}
}

// zeroTail models the far more common post-crash artifact: a filesystem that
// journalled the file's new SIZE before its data blocks, so the extended region
// reads back as zeros. Sixteen zero bytes are a STRUCTURALLY VALID frame — len
// 0, crc 0 (crc32c of nothing is exactly 0), seq 0 — so the frame layer accepts
// them and the tail is only recognizable by the sequence this writer can never
// emit. Getting this wrong bricks the namespace permanently: the log is declared
// corrupt, replay and append both fail closed, and the module has no repair path.
func zeroTail(n int) func(t *testing.T, path string, goodEnd, fullEnd int64) {
	return func(t *testing.T, path string, goodEnd, fullEnd int64) {
		t.Helper()
		data := readFile(t, path)
		writeFile(t, path, append(data[:goodEnd:goodEnd], make([]byte, n)...))
	}
}

// TestOrderedTornTailRecovers models a crash mid-append: the log ends with a
// partial or unwritten frame. Replay must roll the file back to the last whole
// frame, keep every complete mutation, physically truncate the tail, and let the
// next append continue the sequence from there.
func TestOrderedTornTailRecovers(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		// damage rewrites the log's tail to model one crash artifact. The file
		// holds two committed frames ending at goodEnd plus a whole third frame
		// ending at fullEnd; damage must leave the first goodEnd bytes intact.
		damage func(t *testing.T, path string, goodEnd, fullEnd int64)
	}{
		{name: "one byte of payload lost", damage: cutTail(1)},
		{name: "most of the payload lost", damage: cutTail(20)},
		{name: "header partially written", damage: cutTail(frameHeaderSize + orderedMutationHeaderSize - 4)},
		{name: "zero-filled tail shorter than a frame header", damage: zeroTail(frameHeaderSize - 1)},
		{name: "zero-filled tail of exactly one frame header", damage: zeroTail(frameHeaderSize)},
		{name: "zero-filled tail one byte past a frame header", damage: zeroTail(frameHeaderSize + 1)},
		{name: "zero-filled tail of 64 bytes", damage: zeroTail(64)},
		{name: "zero-filled tail of a whole 4 KiB block", damage: zeroTail(4096)},
		{
			name: "tail frame stamped with the never-emitted sequence zero",
			damage: func(t *testing.T, path string, goodEnd, fullEnd int64) {
				t.Helper()
				frame, eerr := encodeOrderedFrame(0, createMutation("host", "gamma", 3))
				if eerr != nil {
					t.Fatalf("encode seq-0 frame: %v", eerr)
				}
				data := readFile(t, path)
				writeFile(t, path, append(data[:goodEnd:goodEnd], frame...))
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			store, root := newTestOrderedLogStore(t)
			const namespace = "sessions"
			log, err := store.logFor(namespace)
			if err != nil {
				t.Fatalf("logFor: %v", err)
			}
			kept := []orderedMutation{
				createMutation("host", "alpha", 1),
				createMutation("host", "beta", 2),
			}
			for _, m := range kept {
				mustAppend(t, log, m)
			}
			logPath := filepath.Join(root, orderedDirName, namespace+orderedLogExt)
			goodEnd := fileSize(t, logPath)
			mustAppend(t, log, createMutation("host", "gamma", 3))
			fullEnd := fileSize(t, logPath)
			if fullEnd <= goodEnd {
				t.Fatalf("third append did not grow the log: %d -> %d", goodEnd, fullEnd)
			}
			tt.damage(t, logPath, goodEnd, fullEnd)

			// A cold store must not see the partial third mutation.
			cold, cerr := newOrderedLogStore(root)
			if cerr != nil {
				t.Fatalf("reopen: %v", cerr)
			}
			defer func() {
				if err := cold.Close(); err != nil {
					t.Errorf("close cold store: %v", err)
				}
			}()
			coldLog, lerr := cold.logFor(namespace)
			if lerr != nil {
				t.Fatalf("cold logFor: %v", lerr)
			}
			got := mustReplay(t, coldLog)
			if len(got) != len(kept) {
				t.Fatalf("replayed %d mutations, want %d (the torn frame must be dropped)", len(got), len(kept))
			}
			for i := range got {
				if !mutationsEqual(got[i], kept[i]) {
					t.Errorf("mutation %d:\n got %+v\nwant %+v", i, got[i], kept[i])
				}
			}
			if size := fileSize(t, logPath); size != goodEnd {
				t.Errorf("log size after recovery = %d, want %d (the torn tail must be truncated)", size, goodEnd)
			}

			// Recovery must leave the log writable and its sequence contiguous.
			resumed := createMutation("host", "delta", 4)
			if seq := mustAppend(t, coldLog, resumed); seq != 3 {
				t.Errorf("resumed append seq = %d, want 3", seq)
			}
			after := mustReplay(t, coldLog)
			if len(after) != 3 || !mutationsEqual(after[2], resumed) {
				t.Fatalf("post-recovery replay = %+v, want the two kept mutations plus delta", after)
			}
		})
	}
}

// TestOrderedCorruptionFailsClosed models damage that truncation cannot repair.
// Unlike a torn tail, every declared byte is present, so the log must refuse to
// replay rather than silently drop or misread a committed mutation.
//
// appendFailsClosed records where the append path draws its line. Append
// re-scans at the FRAME layer only (see scanOrderedLog), so it refuses to extend
// a log whose framing it cannot read — but a CRC-valid frame carrying an illegal
// PAYLOAD is invisible to it. That is the deliberate cost of keeping append
// linear rather than re-decoding all history on every cross-process call; the
// damage is not buried either way, because replay still fails closed afterwards.
func TestOrderedCorruptionFailsClosed(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name              string
		appendFailsClosed bool
		corrupt           func(t *testing.T, path string)
	}{
		{
			name:              "checksum mismatch in a committed frame",
			appendFailsClosed: true,
			corrupt: func(t *testing.T, path string) {
				flipByte(t, path, frameHeaderSize+orderedMutationHeaderSize)
			},
		},
		{
			name:              "checksum mismatch in the first frame header",
			appendFailsClosed: true,
			corrupt: func(t *testing.T, path string) {
				flipByte(t, path, 4) // a CRC byte
			},
		},
		{
			// Framing-valid, payload-illegal: replay must fail closed, but the
			// frame-layer append re-scan cannot see it.
			name:              "unknown payload version inside a valid frame",
			appendFailsClosed: false,
			corrupt: func(t *testing.T, path string) {
				// Rebuild frame 1 around a payload whose version byte is bumped,
				// so the CRC agrees and only the codec can reject it.
				data := readFile(t, path)
				_, payload, n, derr := decodeFrame(data)
				if derr != nil {
					t.Fatalf("decode frame 1: %v", derr)
				}
				payload[offOrderedVersion] = orderedMutationVersion + 1
				reframed, eerr := encodeFrame(1, payload)
				if eerr != nil {
					t.Fatalf("re-encode frame 1: %v", eerr)
				}
				if len(reframed) != n {
					t.Fatalf("reframed width %d != original %d", len(reframed), n)
				}
				copy(data, reframed)
				writeFile(t, path, data)
			},
		},
		{
			name:              "non-contiguous frame sequence",
			appendFailsClosed: true,
			corrupt: func(t *testing.T, path string) {
				// Rewrite frame 1's sequence to 5. The frame header's CRC covers
				// only the payload, so the frame stays individually valid and the
				// gap must be caught by the log's own sequence check.
				data := readFile(t, path)
				_, payload, _, derr := decodeFrame(data)
				if derr != nil {
					t.Fatalf("decode frame 1: %v", derr)
				}
				reframed, eerr := encodeFrame(5, payload)
				if eerr != nil {
					t.Fatalf("re-encode frame 1: %v", eerr)
				}
				copy(data, reframed)
				writeFile(t, path, data)
			},
		},
		{
			name:              "garbage appended as a whole oversized frame",
			appendFailsClosed: true,
			corrupt: func(t *testing.T, path string) {
				data := readFile(t, path)
				// A declared length beyond MaxFramePayload is corruption, not a
				// torn tail: it can never become valid by waiting for more bytes.
				tail := []byte{0xFF, 0xFF, 0xFF, 0xFF}
				tail = append(tail, bytes.Repeat([]byte{0xAA}, frameHeaderSize)...)
				writeFile(t, path, append(data, tail...))
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			store, root := newTestOrderedLogStore(t)
			const namespace = "sessions"
			log, err := store.logFor(namespace)
			if err != nil {
				t.Fatalf("logFor: %v", err)
			}
			for i, key := range []storage.StableKey{"alpha", "beta"} {
				mustAppend(t, log, createMutation("host", key, uint64(i+1)))
			}
			logPath := filepath.Join(root, orderedDirName, namespace+orderedLogExt)
			before := readFile(t, logPath)
			tt.corrupt(t, logPath)

			cold, cerr := newOrderedLogStore(root)
			if cerr != nil {
				t.Fatalf("reopen: %v", cerr)
			}
			defer func() {
				if err := cold.Close(); err != nil {
					t.Errorf("close cold store: %v", err)
				}
			}()
			coldLog, lerr := cold.logFor(namespace)
			if lerr != nil {
				t.Fatalf("cold logFor: %v", lerr)
			}

			got, rerr := coldLog.replay()
			if rerr == nil {
				t.Fatalf("replay of a corrupt log returned %d mutations, want an error", len(got))
			}
			if got != nil {
				t.Errorf("replay returned %+v alongside an error, want nil", got)
			}
			var ce *OrderedLogCorruptError
			if !errors.As(rerr, &ce) {
				t.Fatalf("want *OrderedLogCorruptError, got %T: %v", rerr, rerr)
			}
			if ce.Path != logPath {
				t.Errorf("corrupt error path = %q, want %q", ce.Path, logPath)
			}
			if ce.Unwrap() == nil {
				t.Error("corrupt error carried no cause")
			}

			// Appending must fail closed too — a writer must never extend a log
			// whose framing it cannot read, which would bury the damage under
			// new records. Payload-level damage is outside what the frame-layer
			// re-scan can see, and there the requirement is only that the append
			// does not conceal it: replay must still fail closed afterwards.
			seq, aerr := coldLog.append(createMutation("host", "gamma", 3))
			switch {
			case tt.appendFailsClosed:
				if aerr == nil {
					t.Errorf("append to a corrupt log succeeded at seq %d, want an error", seq)
				} else if !errors.As(aerr, &ce) {
					t.Errorf("append error = %T %v, want *OrderedLogCorruptError", aerr, aerr)
				}
			default:
				if aerr != nil {
					t.Errorf("frame-layer-valid log: append = %v, want success", aerr)
				}
				if _, rerr2 := coldLog.replay(); !errors.As(rerr2, &ce) {
					t.Errorf("replay after append = %T %v, want the damage still reported", rerr2, rerr2)
				}
			}
			// Nothing may be silently repaired or discarded: a corrupt log keeps
			// at least the bytes it had, so an operator can still recover them.
			if after := readFile(t, logPath); len(after) < len(before) {
				t.Errorf("corrupt log shrank from %d to %d bytes", len(before), len(after))
			}
		})
	}
}

// TestOrderedLogWarmAppendRescansAfterExternalWrite proves the cached tip/size
// is not trusted blindly: the log is extended behind the warm writer's back, and
// its next append must notice the file grew, re-scan, and continue AFTER the new
// frame rather than overwrite it. A cache that skipped the re-scan would
// silently clobber a committed mutation.
//
// The external frame is written directly, with no lock contention: this test
// covers the CACHE, not the advisory lock. Cross-process locking is pinned by
// TestOrderedAppendBlocksOnAForeignLock and
// TestOrderedLogCrossProcessAppendsSerialize.
func TestOrderedLogWarmAppendRescansAfterExternalWrite(t *testing.T) {
	t.Parallel()
	store, root := newTestOrderedLogStore(t)
	const namespace = "sessions"
	log, err := store.logFor(namespace)
	if err != nil {
		t.Fatalf("logFor: %v", err)
	}
	for i, key := range []storage.StableKey{"alpha", "beta"} {
		mustAppend(t, log, createMutation("host", key, uint64(i+1)))
	}
	logPath := filepath.Join(root, orderedDirName, namespace+orderedLogExt)

	external := createMutation("host", "gamma", 3)
	frame, eerr := encodeOrderedFrame(3, external)
	if eerr != nil {
		t.Fatalf("encode external frame: %v", eerr)
	}
	writeFile(t, logPath, append(readFile(t, logPath), frame...))

	// The SAME warm log must see the growth and continue at 4.
	resumed := createMutation("host", "delta", 4)
	if seq := mustAppend(t, log, resumed); seq != 4 {
		t.Fatalf("warm append seq = %d, want 4 (the external frame must not be overwritten)", seq)
	}
	got := mustReplay(t, log)
	if len(got) != 4 {
		t.Fatalf("replayed %d mutations, want 4", len(got))
	}
	if !mutationsEqual(got[2], external) {
		t.Errorf("mutation 2:\n got %+v\nwant the external frame %+v", got[2], external)
	}
	if !mutationsEqual(got[3], resumed) {
		t.Errorf("mutation 3:\n got %+v\nwant %+v", got[3], resumed)
	}
}

func TestOrderedLogNamespacesAreIndependent(t *testing.T) {
	t.Parallel()
	store, root := newTestOrderedLogStore(t)
	namespaces := []string{"sessions", "factory/hosts", "a.b-c"}
	for i, ns := range namespaces {
		log, err := store.logFor(ns)
		if err != nil {
			t.Fatalf("logFor(%q): %v", ns, err)
		}
		for j := 0; j <= i; j++ {
			mustAppend(t, log, createMutation("host", storage.StableKey(ns+"-"+strconv.Itoa(j)), uint64(j+1)))
		}
	}
	for i, ns := range namespaces {
		log, err := store.logFor(ns)
		if err != nil {
			t.Fatalf("logFor(%q): %v", ns, err)
		}
		got := mustReplay(t, log)
		if len(got) != i+1 {
			t.Errorf("namespace %q replayed %d mutations, want %d", ns, len(got), i+1)
		}
		for _, m := range got {
			if !strings.HasPrefix(string(m.StableKey), ns+"-") {
				t.Errorf("namespace %q leaked mutation for key %q", ns, m.StableKey)
			}
		}
	}
	// logFor must return the SAME log for a repeated namespace, so the
	// per-namespace mutex actually serializes every writer in this process.
	first, err := store.logFor("sessions")
	if err != nil {
		t.Fatalf("logFor: %v", err)
	}
	second, err := store.logFor("sessions")
	if err != nil {
		t.Fatalf("logFor: %v", err)
	}
	if first != second {
		t.Error("logFor returned two distinct logs for one namespace")
	}
	if _, serr := os.Stat(filepath.Join(root, orderedDirName, "factory", "hosts"+orderedLogExt)); serr != nil {
		t.Errorf("nested namespace path: %v", serr)
	}
}

// TestOrderedLogConcurrentAppendsSerialize runs many writers against one log.
// Under -race it proves the per-process mutex covers the whole read-modify-write
// critical section: every append must receive a distinct sequence and every
// mutation must survive, with no interleaved partial frame.
func TestOrderedLogConcurrentAppendsSerialize(t *testing.T) {
	t.Parallel()
	store, _ := newTestOrderedLogStore(t)
	log, err := store.logFor("sessions")
	if err != nil {
		t.Fatalf("logFor: %v", err)
	}
	const writers = 32
	var wg sync.WaitGroup
	var mu sync.Mutex
	seen := make(map[uint64]bool, writers)
	for i := range writers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			seq, aerr := log.append(createMutation("host", storage.StableKey("k"+strconv.Itoa(i)), uint64(i+1)))
			if aerr != nil {
				t.Errorf("concurrent append %d: %v", i, aerr)
				return
			}
			mu.Lock()
			defer mu.Unlock()
			if seen[seq] {
				t.Errorf("sequence %d assigned twice", seq)
			}
			seen[seq] = true
		}(i)
	}
	wg.Wait()
	if len(seen) != writers {
		t.Fatalf("distinct sequences = %d, want %d", len(seen), writers)
	}
	got := mustReplay(t, log)
	if len(got) != writers {
		t.Fatalf("replayed %d mutations, want %d", len(got), writers)
	}
	keys := make(map[storage.StableKey]bool, writers)
	for _, m := range got {
		keys[m.StableKey] = true
	}
	if len(keys) != writers {
		t.Errorf("distinct keys replayed = %d, want %d", len(keys), writers)
	}
}

func TestNewOrderedLogStoreRejectsBadRoot(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		root func(t *testing.T) string
	}{
		{name: "empty", root: func(*testing.T) string { return "" }},
		{name: "blank", root: func(*testing.T) string { return "   " }},
		{
			name: "root is a file",
			root: func(t *testing.T) string {
				path := filepath.Join(t.TempDir(), "not-a-dir")
				if err := os.WriteFile(path, []byte("x"), 0o600); err != nil {
					t.Fatalf("write file: %v", err)
				}
				return path
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			store, err := newOrderedLogStore(tt.root(t))
			if err == nil {
				if cerr := store.Close(); cerr != nil {
					t.Errorf("close: %v", cerr)
				}
				t.Fatal("newOrderedLogStore accepted an unusable root")
			}
			if store != nil {
				t.Errorf("store = %+v, want nil on error", store)
			}
			var re *OrderedLogRootError
			var ioe *OrderedLogIOError
			if !errors.As(err, &re) && !errors.As(err, &ioe) {
				t.Fatalf("want *OrderedLogRootError or *OrderedLogIOError, got %T: %v", err, err)
			}
		})
	}
}

func fileSize(t *testing.T, path string) int64 {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	return info.Size()
}

func readFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path) // #nosec G304 -- test-local path from t.TempDir
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return data
}

func writeFile(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func flipByte(t *testing.T, path string, off int) {
	t.Helper()
	data := readFile(t, path)
	if off >= len(data) {
		t.Fatalf("offset %d beyond file length %d", off, len(data))
	}
	data[off] ^= 0xFF
	writeFile(t, path, data)
}

// --- cross-process advisory locking ---------------------------------------
//
// The flock in append/replay is the only thing standing between two PROCESSES
// and a clobbered log: the in-process mutex is per-*orderedLog, and a second
// process has its own. Both tests below fail if lockFileEx is removed from
// append — the first deterministically (the append proceeds while a foreign
// lock is held), the second by losing committed frames.

// orderedChildRootEnv, when set, makes the test binary run as an APPENDING CHILD
// PROCESS instead of a test suite: TestMain dispatches on it before m.Run.
const (
	orderedChildRootEnv  = "FSSTORE_ORDERED_CHILD_ROOT"
	orderedChildKeyEnv   = "FSSTORE_ORDERED_CHILD_KEY"
	orderedChildCountEnv = "FSSTORE_ORDERED_CHILD_COUNT"
)

// orderedChildNamespace is the namespace both the parent and every child append
// to in TestOrderedLogCrossProcessAppendsSerialize.
const orderedChildNamespace = "sessions"

func TestMain(m *testing.M) {
	if root := os.Getenv(orderedChildRootEnv); root != "" {
		os.Exit(runOrderedAppendChild(root, os.Getenv(orderedChildKeyEnv), os.Getenv(orderedChildCountEnv)))
	}
	os.Exit(m.Run())
}

// runOrderedAppendChild appends count mutations to root's ordered log as a
// SEPARATE PROCESS, so only the advisory lock can serialize it against its
// siblings and the parent. It returns the process exit code.
func runOrderedAppendChild(root, keyPrefix, count string) int {
	n, err := strconv.Atoi(count)
	if err != nil {
		os.Stderr.WriteString("child: bad count: " + err.Error() + "\n")
		return 2
	}
	store, serr := newOrderedLogStore(root)
	if serr != nil {
		os.Stderr.WriteString("child: open store: " + serr.Error() + "\n")
		return 2
	}
	defer func() { _ = store.Close() }()
	log, lerr := store.logFor(orderedChildNamespace)
	if lerr != nil {
		os.Stderr.WriteString("child: logFor: " + lerr.Error() + "\n")
		return 2
	}
	for i := range n {
		key := storage.StableKey(keyPrefix + "-" + strconv.Itoa(i))
		if _, aerr := log.append(createMutation("host", key, uint64(i+1))); aerr != nil {
			os.Stderr.WriteString("child: append: " + aerr.Error() + "\n")
			return 2
		}
	}
	return 0
}

// TestOrderedAppendBlocksOnAForeignLock proves append actually TAKES the
// cross-process lock rather than merely relying on the in-process mutex: while
// another open file description holds LOCK_EX on the log, the append must not
// proceed, and it must complete once that lock is released.
func TestOrderedAppendBlocksOnAForeignLock(t *testing.T) {
	t.Parallel()
	store, root := newTestOrderedLogStore(t)
	log, err := store.logFor("sessions")
	if err != nil {
		t.Fatalf("logFor: %v", err)
	}
	mustAppend(t, log, createMutation("host", "alpha", 1))
	logPath := filepath.Join(root, orderedDirName, "sessions"+orderedLogExt)

	// A distinct open file description over the same file: flock conflicts
	// between OFDs, so this stands in for a second process.
	holder, oerr := os.OpenFile(logPath, os.O_RDWR, 0o600)
	if oerr != nil {
		t.Fatalf("open holder: %v", oerr)
	}
	defer func() {
		if cerr := holder.Close(); cerr != nil {
			t.Errorf("close holder: %v", cerr)
		}
	}()
	if lerr := lockFileEx(holder); lerr != nil {
		t.Fatalf("hold the advisory lock: %v", lerr)
	}

	done := make(chan error, 1)
	go func() { _, aerr := log.append(createMutation("host", "beta", 2)); done <- aerr }()
	select {
	case aerr := <-done:
		t.Fatalf("append completed (%v) while a foreign advisory lock was held", aerr)
	case <-time.After(250 * time.Millisecond):
	}
	if uerr := unlockFile(holder); uerr != nil {
		t.Fatalf("release the advisory lock: %v", uerr)
	}
	select {
	case aerr := <-done:
		if aerr != nil {
			t.Fatalf("append after the lock was released: %v", aerr)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("append did not complete within 10s of the lock being released")
	}
	if got := mustReplay(t, log); len(got) != 2 {
		t.Fatalf("replayed %d mutations, want 2", len(got))
	}
}

// TestOrderedLogCrossProcessAppendsSerialize runs real concurrent PROCESSES
// against one log — the configuration the advisory lock exists for. Every
// mutation must survive with a contiguous sequence; without the lock the
// writers compute their append offsets from stale caches and overwrite each
// other's committed frames.
func TestOrderedLogCrossProcessAppendsSerialize(t *testing.T) {
	t.Parallel()
	const (
		children = 3
		perActor = 12
	)
	store, root := newTestOrderedLogStore(t)
	log, err := store.logFor(orderedChildNamespace)
	if err != nil {
		t.Fatalf("logFor: %v", err)
	}

	var wg sync.WaitGroup
	for i := range children {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			cmd := exec.Command(os.Args[0]) // #nosec G204 -- re-executes this very test binary
			cmd.Env = append(os.Environ(),
				orderedChildRootEnv+"="+root,
				orderedChildKeyEnv+"=child"+strconv.Itoa(i),
				orderedChildCountEnv+"="+strconv.Itoa(perActor),
			)
			if out, cerr := cmd.CombinedOutput(); cerr != nil {
				t.Errorf("child %d: %v\n%s", i, cerr, out)
			}
		}(i)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := range perActor {
			key := storage.StableKey("parent-" + strconv.Itoa(i))
			if _, aerr := log.append(createMutation("host", key, uint64(i+1))); aerr != nil {
				t.Errorf("parent append %d: %v", i, aerr)
				return
			}
		}
	}()
	wg.Wait()

	// replay's own scan enforces a contiguous sequence from 1, so a surviving
	// count of (children+1)*perActor means no frame was clobbered or lost.
	want := (children + 1) * perActor
	got := mustReplay(t, log)
	if len(got) != want {
		t.Fatalf("replayed %d mutations, want %d (a lost frame means the flock did not serialize)", len(got), want)
	}
	keys := make(map[storage.StableKey]bool, want)
	for _, m := range got {
		keys[m.StableKey] = true
	}
	if len(keys) != want {
		t.Errorf("distinct keys replayed = %d, want %d", len(keys), want)
	}
}

// TestOrderedWarmLogDropsItsCacheOnCorruption covers the one corruption path no
// other test reaches: a WARM log (loaded, with a cached tip/size) that hits
// corruption on a re-scan. Every other corruption test uses a cold store, where
// loaded is already false.
//
// The cache MUST be invalidated by the failed scan. If it were not, the log
// would keep a stale tip/size, and a later append whose on-disk size happened to
// match that stale size would take refresh's size-equality fast path and write a
// frame at the cached offset — extending, and eventually overwriting, a log it
// cannot read. The sequence below reproduces exactly that: corrupt a committed
// frame while ALSO changing the length (so the scan runs and fails), then restore
// the original length (so the fast path would fire).
func TestOrderedWarmLogDropsItsCacheOnCorruption(t *testing.T) {
	t.Parallel()
	store, root := newTestOrderedLogStore(t)
	const namespace = "sessions"
	log, err := store.logFor(namespace)
	if err != nil {
		t.Fatalf("logFor: %v", err)
	}
	for i, key := range []storage.StableKey{"alpha", "beta"} {
		mustAppend(t, log, createMutation("host", key, uint64(i+1)))
	}
	logPath := filepath.Join(root, orderedDirName, namespace+orderedLogExt)
	committed := readFile(t, logPath)

	// Damage a committed frame in place AND change the length, so the warm
	// log's size check forces a re-scan that then fails.
	damaged := append([]byte{}, committed...)
	damaged[frameHeaderSize+orderedMutationHeaderSize] ^= 0xFF
	writeFile(t, logPath, append(append([]byte{}, damaged...), make([]byte, 8)...))

	var ce *OrderedLogCorruptError
	if seq, aerr := log.append(createMutation("host", "gamma", 3)); aerr == nil {
		t.Fatalf("warm append over a corrupt log succeeded at seq %d, want an error", seq)
	} else if !errors.As(aerr, &ce) {
		t.Fatalf("warm append error = %T %v, want *OrderedLogCorruptError", aerr, aerr)
	}

	// Restore the ORIGINAL length, leaving the damage in place. A stale cache
	// would now match on size and skip the scan entirely.
	writeFile(t, logPath, damaged)
	before := readFile(t, logPath)
	if seq, aerr := log.append(createMutation("host", "delta", 4)); aerr == nil {
		t.Errorf("append on a stale warm cache succeeded at seq %d, want an error", seq)
	} else if !errors.As(aerr, &ce) {
		t.Errorf("second warm append error = %T %v, want *OrderedLogCorruptError", aerr, aerr)
	}
	if after := readFile(t, logPath); !bytes.Equal(after, before) {
		t.Errorf("a rejected append modified the log: %d bytes -> %d bytes", len(before), len(after))
	}
}
