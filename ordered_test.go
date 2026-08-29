package fsstore

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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

// commitMutation commits m through the log's ONE write path. The log layer has
// no second entry point: withView open-locks-catches up-decides-appends, and
// production and these tests take exactly the same route through it, so no
// durability property proved here is proved about code the index does not run.
//
// It validates m first, mirroring what a public method does before it opens
// anything, so a mutation the codec cannot represent never reaches the file.
func commitMutation(ctx context.Context, log *orderedLog, m orderedMutation) (uint64, error) {
	if verr := validateOrderedMutation(m); verr != nil {
		return 0, verr
	}
	return log.withView(ctx, true, func(*orderedView) (orderedDecision, error) {
		return orderedDecision{mutation: &m}, nil
	})
}

// commitCreate allocates the next acceptance order in scope from the caught-up
// view and commits a create for key in the SAME critical section, exactly as
// CreateOrdered does. Concurrency tests must allocate this way rather than
// pre-assigning an order: an order chosen outside the lock is a check-then-act
// race, and the view refuses a create whose order does not exceed the scope's
// high-water mark.
func commitCreate(ctx context.Context, log *orderedLog, scope string, key storage.StableKey) (seq uint64, order uint64, err error) {
	seq, err = log.withView(ctx, true, func(v *orderedView) (orderedDecision, error) {
		order = v.highWater[scope] + 1
		m := createMutation(scope, key, order)
		return orderedDecision{mutation: &m}, nil
	})
	return seq, order, err
}

// replayMutations brings log's view up to date through the live path — which is
// where a torn tail is recovered — and then reads the recovered file back as the
// mutations it holds. It is an OBSERVATION of the log, not a second way to walk
// it: the recovery it asserts about has already happened, in production code.
func replayMutations(ctx context.Context, t *testing.T, log *orderedLog) ([]orderedMutation, error) {
	t.Helper()
	if _, err := log.withView(ctx, false, func(*orderedView) (orderedDecision, error) {
		return orderedDecision{}, nil
	}); err != nil {
		return nil, err
	}
	data, rerr := os.ReadFile(log.path) // #nosec G304 -- test-local path from t.TempDir
	if errors.Is(rerr, fs.ErrNotExist) {
		return nil, nil
	}
	if rerr != nil {
		t.Fatalf("read %s: %v", log.path, rerr)
	}
	mutations, _, _, serr := scanOrderedLog(data, log.path, 1)
	return mutations, serr
}

// writeRawFrames writes an arbitrary sequence of mutations to a namespace's log
// file as raw frames, bypassing every consistency check the write path makes. It
// is the only way to build a log this package could not have written — which is
// exactly what a fail-closed test needs.
func writeRawFrames(t *testing.T, root, namespace string, mutations []orderedMutation) string {
	t.Helper()
	path := filepath.Join(root, orderedDirName, filepath.FromSlash(namespace)+orderedLogExt)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	var data []byte
	for i, m := range mutations {
		frame, err := encodeOrderedFrame(uint64(i+1), m)
		if err != nil {
			t.Fatalf("encode frame %d: %v", i+1, err)
		}
		data = append(data, frame...)
	}
	writeFile(t, path, data)
	return path
}

// mustAppend appends m and returns the assigned log sequence.
func mustAppend(t *testing.T, log *orderedLog, m orderedMutation) uint64 {
	t.Helper()
	seq, err := commitMutation(context.Background(), log, m)
	if err != nil {
		t.Fatalf("append %+v: %v", m, err)
	}
	return seq
}

// mustReplay reads log back and returns its mutations.
func mustReplay(t *testing.T, log *orderedLog) []orderedMutation {
	t.Helper()
	got, err := replayMutations(context.Background(), t, log)
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
			// Specifically the NAME error: logFor validates before it maps a
			// path, so no namespace in this table may reach the containment
			// guard. Accepting either type would let a validation regression
			// pass silently. (The guard itself is tested directly by
			// TestOrderedPathForRejectsEscape.)
			var ine *storage.InvalidNameError
			if !errors.As(err, &ine) {
				t.Fatalf("want *storage.InvalidNameError, got %T: %v", err, err)
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
			if seq, aerr := commitMutation(context.Background(), log, createMutation("host", tt.key, 1)); aerr == nil {
				t.Fatalf("append of an unrepresentable key succeeded at seq %d", seq)
			} else {
				var ce *OrderedCodecError
				if !errors.As(aerr, &ce) {
					t.Fatalf("want *OrderedCodecError, got %T: %v", aerr, aerr)
				}
			}
			// The rejection must write no FRAME. It may still leave an empty
			// log file: O_CREATE fires before the log reads its own state, so
			// the absence of a file here is a consequence of codec validation
			// running first, not a guarantee the append path makes.
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
// read rather than silently drop or misread a committed mutation — and it must
// refuse to be EXTENDED too, because a new frame written past damage buries it.
//
// There is one path to the file and it decodes every frame it reads, so the line
// is the same for a read and a write: framing damage and payload damage both
// fail closed. (An earlier design re-scanned at the frame layer only on the
// write path, which left a CRC-valid frame carrying an illegal payload invisible
// to an append.)
func TestOrderedCorruptionFailsClosed(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		corrupt func(t *testing.T, path string)
	}{
		{
			name: "checksum mismatch in a committed frame",
			corrupt: func(t *testing.T, path string) {
				flipByte(t, path, frameHeaderSize+orderedMutationHeaderSize)
			},
		},
		{
			name: "checksum mismatch in the first frame header",
			corrupt: func(t *testing.T, path string) {
				flipByte(t, path, 4) // a CRC byte
			},
		},
		{
			// Framing-valid, payload-illegal: replay must fail closed, but the
			// frame-layer append re-scan cannot see it.
			name: "unknown payload version inside a valid frame",
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
			name: "non-contiguous frame sequence",
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
			// A frame stamped with the never-emitted sequence 0 that carries a
			// real payload and a correct CRC is NOT a zero region, so it is not
			// an unwritten tail either: something other than this writer produced
			// it, and truncating there would be guessing. Only a seq-0 frame
			// whose whole remainder is zero is recoverable.
			name: "seq-zero frame carrying a real payload",
			corrupt: func(t *testing.T, path string) {
				frame, eerr := encodeOrderedFrame(0, createMutation("host", "ghost", 3))
				if eerr != nil {
					t.Fatalf("encode seq-0 frame: %v", eerr)
				}
				writeFile(t, path, append(readFile(t, path), frame...))
			},
		},
		{
			name: "garbage appended as a whole oversized frame",
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

			got, rerr := replayMutations(context.Background(), t, coldLog)
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
			// it cannot read, which would bury the damage under new records.
			seq, aerr := commitMutation(context.Background(), coldLog, createMutation("host", "gamma", 3))
			if aerr == nil {
				t.Errorf("append to a corrupt log succeeded at seq %d, want an error", seq)
			} else if !errors.As(aerr, &ce) {
				t.Errorf("append error = %T %v, want *OrderedLogCorruptError", aerr, aerr)
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
	orders := make(map[uint64]bool, writers)
	for i := range writers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			seq, order, aerr := commitCreate(context.Background(), log, "host", storage.StableKey("k"+strconv.Itoa(i)))
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
			if orders[order] {
				t.Errorf("acceptance order %d allocated twice", order)
			}
			orders[order] = true
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
		if _, _, aerr := commitCreate(context.Background(), log, "host", key); aerr != nil {
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
	go func() {
		_, aerr := commitMutation(context.Background(), log, createMutation("host", "beta", 2))
		done <- aerr
	}()
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
			if _, _, aerr := commitCreate(context.Background(), log, "host", key); aerr != nil {
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

// warmView brings log's in-memory view up to date through the live read path and
// leaves it loaded, so a following operation is a WARM one. Every test below
// that models "a warm view meets damage" needs this: the incremental catch-up
// path behaves differently from a cold rebuild, and it is the path production
// takes on every call after the first.
func warmView(t *testing.T, log *orderedLog) {
	t.Helper()
	if _, err := log.withView(context.Background(), false, func(*orderedView) (orderedDecision, error) {
		return orderedDecision{}, nil
	}); err != nil {
		t.Fatalf("warm the view: %v", err)
	}
	if !log.viewLoaded {
		t.Fatal("view is not loaded after a successful read")
	}
}

// appendRaw appends bytes to a log file behind the store's back, modelling
// another writer (or a crash) that changed the file since this handle last
// looked at it.
func appendRaw(t *testing.T, path string, tail []byte) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o600) // #nosec G304 -- test-local path from t.TempDir
	if err != nil {
		t.Fatalf("open for append: %v", err)
	}
	if _, werr := f.Write(tail); werr != nil {
		t.Fatalf("append raw bytes: %v", werr)
	}
	if cerr := f.Close(); cerr != nil {
		t.Fatalf("close: %v", cerr)
	}
}

// TestOrderedWarmViewRecoversATornTail covers the quadrant every other recovery
// test misses: a torn tail in front of a WARM view. A cold rebuild re-reads the
// file from zero, so its truncation is exercised by any torn-tail test at all;
// the incremental catch-up truncates a DIFFERENT offset — l.applied+goodEnd, not
// goodEnd — and advances the applied position by goodEnd rather than to the
// file's size. Neither of those is observable until a warm handle meets damage.
//
// The torn tail here is deliberately LONGER than the frame that replaces it, so
// a catch-up that failed to truncate would leave junk BEHIND the new frame
// instead of overwriting it, and one that advanced applied to the file size
// would write past the junk and leave a hole.
func TestOrderedWarmViewRecoversATornTail(t *testing.T) {
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
	goodEnd := fileSize(t, logPath)
	warmView(t, log)
	if log.applied != goodEnd {
		t.Fatalf("warm applied = %d, want %d", log.applied, goodEnd)
	}

	// A wide third frame, one byte short: a crash mid-append.
	wide := createMutation("host", "gamma", 3)
	wide.Value = bytes.Repeat([]byte{0x7E}, 4096)
	wideFrame, eerr := encodeOrderedFrame(3, wide)
	if eerr != nil {
		t.Fatalf("encode wide frame: %v", eerr)
	}
	appendRaw(t, logPath, wideFrame[:len(wideFrame)-1])

	// The warm handle must truncate that tail and land the next frame exactly at
	// the last whole frame's end.
	narrow := createMutation("host", "gamma", 3)
	seq, aerr := commitMutation(context.Background(), log, narrow)
	if aerr != nil {
		t.Fatalf("warm append over a torn tail: %v", aerr)
	}
	if seq != 3 {
		t.Errorf("warm append seq = %d, want 3", seq)
	}
	narrowFrame, eerr := encodeOrderedFrame(3, narrow)
	if eerr != nil {
		t.Fatalf("encode narrow frame: %v", eerr)
	}
	if len(narrowFrame) >= len(wideFrame)-1 {
		t.Fatalf("the torn tail (%d bytes) must be wider than its replacement (%d)", len(wideFrame)-1, len(narrowFrame))
	}
	wantSize := goodEnd + int64(len(narrowFrame))
	if size := fileSize(t, logPath); size != wantSize {
		t.Fatalf("log size after a warm recovery = %d, want %d: the torn tail was not truncated away", size, wantSize)
	}

	// And the file is genuinely readable from cold: three whole frames, no junk.
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
	if len(got) != 3 || !mutationsEqual(got[2], narrow) {
		t.Fatalf("cold replay after a warm recovery = %+v, want alpha, beta and the replacement gamma", got)
	}
}

// TestOrderedWarmViewFailsClosedOnADamagedTail is the other half of the warm
// quadrant: the frame appended behind the warm handle's back is COMPLETE but
// contradicts the state it applies to, so it is caught by the fold rather than
// by the scan. That is the only way to reach the poison guard inside catch-up's
// fold loop — on a cold catch-up the view is unloaded already, so dropping it
// there changes nothing.
func TestOrderedWarmViewFailsClosedOnADamagedTail(t *testing.T) {
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
	warmView(t, log)

	// A well-framed, CRC-correct frame 3 that re-creates an identity the view
	// already holds: truncation cannot repair it, and folding it in would leave
	// the indexes quietly wrong.
	ghost, eerr := encodeOrderedFrame(3, createMutation("host", "alpha", 1))
	if eerr != nil {
		t.Fatalf("encode contradicting frame: %v", eerr)
	}
	appendRaw(t, logPath, ghost)
	before := readFile(t, logPath)

	var ce *OrderedLogCorruptError
	seq, aerr := commitMutation(context.Background(), log, createMutation("host", "gamma", 3))
	if aerr == nil {
		t.Fatalf("warm append over a contradicting frame succeeded at seq %d, want an error", seq)
	}
	if !errors.As(aerr, &ce) || !errors.Is(aerr, errOrderedInconsistentLog) {
		t.Fatalf("warm append error = %T %v, want *OrderedLogCorruptError wrapping errOrderedInconsistentLog", aerr, aerr)
	}
	if log.viewLoaded {
		t.Error("the view stayed loaded after a half-applied fold; the next read would serve it")
	}
	// The failure is sticky and nothing was written.
	if _, again := commitMutation(context.Background(), log, createMutation("host", "delta", 4)); !errors.As(again, &ce) {
		t.Errorf("second warm append = %v, want the same failure", again)
	}
	if after := readFile(t, logPath); !bytes.Equal(after, before) {
		t.Errorf("a rejected append modified the log: %d bytes -> %d bytes", len(before), len(after))
	}
}

// TestOrderedWarmViewRebuildsWhenTheLogShrinks pins catch-up's shrink guard. A
// warm view holds an applied OFFSET into a file it believes it is a prefix of;
// if the file is now shorter than that offset it is a different file — truncated
// or replaced — and the applied prefix is not its prefix. Without the size check
// the catch-up would size its read buffer to a NEGATIVE span and panic.
//
// The guard's limitation is deliberate and not fixed here: a log REPLACED at the
// same or a greater length is not detected at all, because detection is by size
// alone. That is the same single-writer-per-namespace assumption the ledger
// documents; under it, neither case can occur.
func TestOrderedWarmViewRebuildsWhenTheLogShrinks(t *testing.T) {
	t.Parallel()
	store, root := newTestOrderedLogStore(t)
	const namespace = "sessions"
	log, err := store.logFor(namespace)
	if err != nil {
		t.Fatalf("logFor: %v", err)
	}
	first := createMutation("host", "alpha", 1)
	mustAppend(t, log, first)
	logPath := filepath.Join(root, orderedDirName, namespace+orderedLogExt)
	afterFirst := fileSize(t, logPath)
	mustAppend(t, log, createMutation("host", "beta", 2))
	warmView(t, log)
	if log.applied <= afterFirst {
		t.Fatalf("warm applied = %d, want more than %d", log.applied, afterFirst)
	}

	// Roll the file back to one frame behind the warm view's back.
	if terr := os.Truncate(logPath, afterFirst); terr != nil {
		t.Fatalf("truncate: %v", terr)
	}

	got := mustReplay(t, log)
	if len(got) != 1 || !mutationsEqual(got[0], first) {
		t.Fatalf("warm read of a shrunken log = %+v, want just alpha", got)
	}
	if log.applied != afterFirst {
		t.Errorf("applied after the rebuild = %d, want %d", log.applied, afterFirst)
	}
	// The rebuilt view is usable: the next frame continues from the new tip.
	if seq, aerr := commitMutation(context.Background(), log, createMutation("host", "beta", 2)); aerr != nil || seq != 2 {
		t.Fatalf("append after a rebuild = (%d, %v), want (2, nil)", seq, aerr)
	}
}

// TestOrderedErrorMessages covers the log's five exported error types. Each must
// render a non-empty, fsstore-prefixed message that names its subject, and — the
// reason this is a test rather than a comment — the two that carry a Cause must
// not dereference a nil one. A nil Cause is reachable by any caller that
// constructs the type (a test double, a re-wrap), and a panic inside an error
// message is the worst possible time to fail.
func TestOrderedErrorMessages(t *testing.T) {
	t.Parallel()
	cause := errors.New("underlying fault")
	tests := []struct {
		name     string
		err      error
		contains []string
	}{
		{
			name:     "corrupt with a cause",
			err:      &OrderedLogCorruptError{Path: "/s/ordered/ns.olog", Seq: 7, Cause: cause},
			contains: []string{"ordered log", "ns.olog", "corrupt at seq 7", "underlying fault"},
		},
		{
			name:     "corrupt with no cause",
			err:      &OrderedLogCorruptError{Path: "/s/ordered/ns.olog", Seq: 7},
			contains: []string{"ordered log", "corrupt at seq 7"},
		},
		{
			name:     "io with a cause",
			err:      &OrderedLogIOError{Op: "fsync", Path: "/s/ordered/ns.olog", Cause: cause},
			contains: []string{"ordered log fsync", "ns.olog", "underlying fault"},
		},
		{
			name:     "io with no cause",
			err:      &OrderedLogIOError{Op: "fsync", Path: "/s/ordered/ns.olog"},
			contains: []string{"ordered log fsync", "ns.olog"},
		},
		{
			name:     "path escape",
			err:      &OrderedLogPathError{Namespace: "../escape", Path: "/s/escape.olog"},
			contains: []string{"ordered namespace", "../escape", "maps outside"},
		},
		{
			name:     "closed store",
			err:      &OrderedLogClosedError{Root: "/s", Namespace: "sessions"},
			contains: []string{"ordered log store", "is closed", "sessions"},
		},
		{
			name:     "unusable root",
			err:      &OrderedLogRootError{Root: "  ", Reason: "empty root"},
			contains: []string{"invalid ordered log root", "empty root"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := tt.err.Error()
			if !strings.HasPrefix(got, "fsstore: ") {
				t.Errorf("Error() = %q, want an fsstore-prefixed message", got)
			}
			for _, want := range tt.contains {
				if !strings.Contains(got, want) {
					t.Errorf("Error() = %q, want it to contain %q", got, want)
				}
			}
		})
	}
}

// TestOrderedLogCorruptErrorPrecedence pins the documented classification order.
// replay wraps a codec fault in an *OrderedLogCorruptError, so errors.As finds
// BOTH types on one disk-corruption error while a rejected caller mutation
// carries only the codec type. A caller distinguishing "your input was bad" from
// "the disk is damaged" must therefore test *OrderedLogCorruptError FIRST.
func TestOrderedLogCorruptErrorPrecedence(t *testing.T) {
	t.Parallel()
	store, root := newTestOrderedLogStore(t)
	log, err := store.logFor("sessions")
	if err != nil {
		t.Fatalf("logFor: %v", err)
	}
	var ce *OrderedCodecError
	var lce *OrderedLogCorruptError

	// Caller garbage: the bare codec error, with no corruption wrapper.
	badInput := createMutation("host", "", 1)
	if _, aerr := commitMutation(context.Background(), log, badInput); aerr == nil {
		t.Fatal("append of an invalid mutation succeeded")
	} else if !errors.As(aerr, &ce) {
		t.Fatalf("caller-input error = %T %v, want *OrderedCodecError", aerr, aerr)
	} else if errors.As(aerr, &lce) {
		t.Errorf("caller-input error also matched *OrderedLogCorruptError: %v", aerr)
	}

	// Disk damage: the SAME codec type, wrapped. Both errors.As calls hit.
	mustAppend(t, log, createMutation("host", "alpha", 1))
	logPath := filepath.Join(root, orderedDirName, "sessions"+orderedLogExt)
	data := readFile(t, logPath)
	_, payload, _, derr := decodeFrame(data)
	if derr != nil {
		t.Fatalf("decode frame 1: %v", derr)
	}
	payload[offOrderedVersion] = orderedMutationVersion + 1
	reframed, eerr := encodeFrame(1, payload)
	if eerr != nil {
		t.Fatalf("re-encode frame 1: %v", eerr)
	}
	writeFile(t, logPath, reframed)

	cold, cerr := newOrderedLogStore(root)
	if cerr != nil {
		t.Fatalf("reopen: %v", cerr)
	}
	defer func() {
		if err := cold.Close(); err != nil {
			t.Errorf("close cold store: %v", err)
		}
	}()
	coldLog, lerr := cold.logFor("sessions")
	if lerr != nil {
		t.Fatalf("cold logFor: %v", lerr)
	}
	_, rerr := replayMutations(context.Background(), t, coldLog)
	if !errors.As(rerr, &lce) {
		t.Fatalf("disk-damage error = %T %v, want *OrderedLogCorruptError", rerr, rerr)
	}
	if !errors.As(rerr, &ce) {
		t.Fatalf("disk-damage error = %v, want the codec fault to remain unwrappable", rerr)
	}
}

// TestOrderedPathForRejectsEscape tests the containment guard DIRECTLY. It is
// unreachable through logFor, which runs ValidateName first, so it is defense in
// depth against a future path that forgets to validate — and defense in depth
// that is never exercised is just untested code. Every case here would otherwise
// resolve to the ordered root itself or somewhere above it.
func TestOrderedPathForRejectsEscape(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		namespace string
	}{
		{name: "empty maps to the ordered root itself", namespace: ""},
		{name: "dot maps to the ordered root itself", namespace: "."},
		{name: "parent", namespace: ".."},
		{name: "parent escape", namespace: "../escape"},
		{name: "deep parent escape", namespace: "../../../../etc/passwd"},
		{name: "embedded escape", namespace: "a/../../escape"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			store, _ := newTestOrderedLogStore(t)
			path, err := store.pathFor(tt.namespace)
			if err == nil {
				t.Fatalf("pathFor(%q) = %q, want a containment error", tt.namespace, path)
			}
			if path != "" {
				t.Errorf("pathFor returned %q alongside an error, want an empty path", path)
			}
			var pe *OrderedLogPathError
			if !errors.As(err, &pe) {
				t.Fatalf("want *OrderedLogPathError, got %T: %v", err, err)
			}
			if pe.Namespace != tt.namespace {
				t.Errorf("error namespace = %q, want %q", pe.Namespace, tt.namespace)
			}
		})
	}
}

// TestOrderedPathForAcceptsValidNamespaces is the other half: a namespace the
// grammar accepts must map inside the ordered root, with its segments becoming
// directories and the .olog suffix appended.
func TestOrderedPathForAcceptsValidNamespaces(t *testing.T) {
	t.Parallel()
	store, root := newTestOrderedLogStore(t)
	// A leading slash is NOT an escape here: filepath.Join treats every operand
	// after the first as relative, so "/etc/passwd" maps to <root>/ordered/etc/
	// passwd.olog. It is rejected by ValidateName in logFor, one layer up, which
	// is why that layer is the security boundary and this one is defense in depth.
	for _, ns := range []string{"sessions", "factory/hosts", "a.b-c", "a/b/c/d", "/etc/passwd"} {
		got, err := store.pathFor(ns)
		if err != nil {
			t.Fatalf("pathFor(%q): %v", ns, err)
		}
		want := filepath.Join(root, orderedDirName, filepath.FromSlash(ns)) + orderedLogExt
		if got != want {
			t.Errorf("pathFor(%q) = %q, want %q", ns, got, want)
		}
	}
}

// TestOrderedNameCeilingMatchesStorage guards against silent drift.
// maxOrderedNameBytes duplicates storage's UNEXPORTED name ceiling, and the wire
// format sizes a uint16 field against it. If storage ever moves its limit, this
// test breaks — rather than a write that encodes a name the decoder rejects.
func TestOrderedNameCeilingMatchesStorage(t *testing.T) {
	t.Parallel()
	if err := storage.ValidateName(strings.Repeat("a", maxOrderedNameBytes)); err != nil {
		t.Errorf("storage.ValidateName rejects a name of exactly maxOrderedNameBytes (%d): %v", maxOrderedNameBytes, err)
	}
	if err := storage.ValidateName(strings.Repeat("a", maxOrderedNameBytes+1)); err == nil {
		t.Errorf("storage.ValidateName accepts a name of maxOrderedNameBytes+1 (%d), so the ceiling has drifted", maxOrderedNameBytes+1)
	}
}

// TestOrderedLogStoreFailsClosedAfterClose pins that Close is a one-way door.
// Close drops the namespace->orderedLog registry; if logFor then minted a fresh
// log, a namespace with an append in flight would end up with TWO mutexes
// guarding one file — an invariant surviving only by accident of the advisory
// lock. The store must refuse instead.
func TestOrderedLogStoreFailsClosedAfterClose(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	store, err := newOrderedLogStore(root)
	if err != nil {
		t.Fatalf("newOrderedLogStore: %v", err)
	}
	log, lerr := store.logFor("sessions")
	if lerr != nil {
		t.Fatalf("logFor: %v", lerr)
	}
	mustAppend(t, log, createMutation("host", "alpha", 1))
	if cerr := store.Close(); cerr != nil {
		t.Fatalf("close: %v", cerr)
	}
	if cerr := store.Close(); cerr != nil {
		t.Errorf("second close: %v, want idempotent success", cerr)
	}

	reopened, rerr := store.logFor("sessions")
	if rerr == nil {
		t.Fatalf("logFor after Close returned %+v, want an error", reopened)
	}
	if reopened != nil {
		t.Errorf("log = %+v, want nil on error", reopened)
	}
	var closedErr *OrderedLogClosedError
	if !errors.As(rerr, &closedErr) {
		t.Fatalf("want *OrderedLogClosedError, got %T: %v", rerr, rerr)
	}
	if closedErr.Namespace != "sessions" {
		t.Errorf("error namespace = %q, want %q", closedErr.Namespace, "sessions")
	}
	// A fresh store over the same root is the supported way back in.
	fresh, ferr := newOrderedLogStore(root)
	if ferr != nil {
		t.Fatalf("reopen: %v", ferr)
	}
	defer func() {
		if err := fresh.Close(); err != nil {
			t.Errorf("close fresh store: %v", err)
		}
	}()
	freshLog, flerr := fresh.logFor("sessions")
	if flerr != nil {
		t.Fatalf("fresh logFor: %v", flerr)
	}
	if got := mustReplay(t, freshLog); len(got) != 1 {
		t.Errorf("replayed %d mutations, want 1", len(got))
	}
}

// TestOrderedLogHonorsCancellationBeforeWork pins the documented contract:
// cancellation refuses to START work. A cancelled context must be reported
// before any lock is taken and, for append, before anything is written.
func TestOrderedLogHonorsCancellationBeforeWork(t *testing.T) {
	t.Parallel()
	store, root := newTestOrderedLogStore(t)
	log, err := store.logFor("sessions")
	if err != nil {
		t.Fatalf("logFor: %v", err)
	}
	mustAppend(t, log, createMutation("host", "alpha", 1))
	logPath := filepath.Join(root, orderedDirName, "sessions"+orderedLogExt)
	before := readFile(t, logPath)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if seq, aerr := commitMutation(ctx, log, createMutation("host", "beta", 2)); !errors.Is(aerr, context.Canceled) {
		t.Errorf("append(cancelled) = (%d, %v), want context.Canceled", seq, aerr)
	}
	if _, rerr := replayMutations(ctx, t, log); !errors.Is(rerr, context.Canceled) {
		t.Errorf("replay(cancelled) = %v, want context.Canceled", rerr)
	}
	if after := readFile(t, logPath); !bytes.Equal(after, before) {
		t.Errorf("a cancelled call modified the log: %d bytes -> %d bytes", len(before), len(after))
	}
}

// cancelOnErrObservationContext becomes cancelled on its Nth Err call. Done is
// closed at the same transition, so it remains a valid context while giving a
// test deterministic control over cancellation between internal stages.
type cancelOnErrObservationContext struct {
	context.Context
	cancelAt int32
	calls    atomic.Int32
	done     chan struct{}
	once     sync.Once
}

func (c *cancelOnErrObservationContext) Done() <-chan struct{} { return c.done }

func (c *cancelOnErrObservationContext) Err() error {
	if c.calls.Add(1) < c.cancelAt {
		return nil
	}
	c.once.Do(func() { close(c.done) })
	return context.Canceled
}

// TestOrderedLogCancellationBeforeFreshNamespaceSetup pins the boundary after
// the namespace permit is acquired but before MkdirAll/OpenFile(O_CREATE). A
// cancelled create must return without leaving either the namespace directory
// or an empty log behind.
func TestOrderedLogCancellationBeforeFreshNamespaceSetup(t *testing.T) {
	t.Parallel()
	store, _ := newTestOrderedLogStore(t)
	log, err := store.logFor("fresh/cancelled")
	if err != nil {
		t.Fatalf("logFor: %v", err)
	}
	ctx := &cancelOnErrObservationContext{
		Context:  context.Background(),
		cancelAt: 3, // before-lock, after-permit, then before filesystem setup
		done:     make(chan struct{}),
	}
	seq, err := commitMutation(ctx, log, createMutation("host", "alpha", 1))
	if !errors.Is(err, context.Canceled) || seq != 0 {
		t.Fatalf("fresh create cancelled before setup = (%d, %T %v), want (0, context.Canceled)", seq, err, err)
	}
	for _, path := range []string{log.path, filepath.Dir(log.path)} {
		if _, statErr := os.Stat(path); !errors.Is(statErr, fs.ErrNotExist) {
			t.Errorf("cancelled fresh create left %q on disk: stat error %v", path, statErr)
		}
	}
}

type orderedErrObservedContext struct {
	context.Context
	errObserved chan struct{}
	once        sync.Once
}

func (c *orderedErrObservedContext) Err() error {
	c.once.Do(func() { close(c.errObserved) })
	return c.Context.Err()
}

// TestOrderedLogCancelsWhileWaitingForNamespaceLock proves cancellation can
// stop an operation that has not begun file work while another goroutine owns
// the namespace. The held lock is not released until the waiter returns, so an
// uncancellable mutex deterministically deadlocks this assertion.
func TestOrderedLogCancelsWhileWaitingForNamespaceLock(t *testing.T) {
	t.Parallel()
	store, root := newTestOrderedLogStore(t)
	log, err := store.logFor("sessions")
	if err != nil {
		t.Fatalf("logFor: %v", err)
	}
	mustAppend(t, log, createMutation("host", "alpha", 1))
	before := readFile(t, filepath.Join(root, orderedDirName, "sessions"+orderedLogExt))

	if err := log.mu.lock(context.Background()); err != nil {
		t.Fatalf("hold namespace lock: %v", err)
	}
	deadline, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	ctx := &orderedErrObservedContext{Context: deadline, errObserved: make(chan struct{})}
	result := make(chan error, 1)
	go func() {
		_, aerr := commitMutation(ctx, log, createMutation("host", "beta", 2))
		result <- aerr
	}()
	select {
	case <-ctx.errObserved:
	case <-time.After(time.Second):
		log.mu.unlock()
		t.Fatal("operation did not inspect context before waiting for namespace lock")
	}
	select {
	case aerr := <-result:
		log.mu.unlock()
		if !errors.Is(aerr, context.DeadlineExceeded) {
			t.Fatalf("blocked append error = %T %v, want context.DeadlineExceeded", aerr, aerr)
		}
	case <-time.After(time.Second):
		log.mu.unlock()
		<-result
		t.Fatal("operation remained blocked after its context deadline")
	}
	if after := readFile(t, filepath.Join(root, orderedDirName, "sessions"+orderedLogExt)); !bytes.Equal(after, before) {
		t.Fatalf("cancelled namespace-lock waiter modified log: %d bytes -> %d bytes", len(before), len(after))
	}
	if _, aerr := commitMutation(context.Background(), log, createMutation("host", "beta", 2)); aerr != nil {
		t.Fatalf("append after namespace lock release: %v", aerr)
	}
}

// TestOrderedLogCancelsWhileWaitingForAdvisoryLock holds LOCK_EX through a
// distinct open file description and requires a waiting operation to return on
// context cancellation without modifying the log. The holder remains locked
// until the result arrives, so falling back to blocking flock cannot pass.
func TestOrderedLogCancelsWhileWaitingForAdvisoryLock(t *testing.T) {
	t.Parallel()
	store, root := newTestOrderedLogStore(t)
	log, err := store.logFor("sessions")
	if err != nil {
		t.Fatalf("logFor: %v", err)
	}
	mustAppend(t, log, createMutation("host", "alpha", 1))
	logPath := filepath.Join(root, orderedDirName, "sessions"+orderedLogExt)
	before := readFile(t, logPath)

	holder, err := os.OpenFile(logPath, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatalf("open holder: %v", err)
	}
	defer holder.Close()
	if err := lockFileEx(holder); err != nil {
		t.Fatalf("lock holder: %v", err)
	}
	locked := true
	defer func() {
		if locked {
			_ = unlockFile(holder)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, aerr := commitMutation(ctx, log, createMutation("host", "beta", 2))
		result <- aerr
	}()
	select {
	case aerr := <-result:
		if !errors.Is(aerr, context.DeadlineExceeded) {
			t.Fatalf("blocked append error = %T %v, want context.DeadlineExceeded", aerr, aerr)
		}
	case <-time.After(time.Second):
		if err := unlockFile(holder); err != nil {
			t.Fatalf("unlock holder after timeout: %v", err)
		}
		locked = false
		<-result
		t.Fatal("operation remained blocked on flock after its context deadline")
	}
	if after := readFile(t, logPath); !bytes.Equal(after, before) {
		t.Fatalf("cancelled advisory-lock waiter modified log: %d bytes -> %d bytes", len(before), len(after))
	}
	if err := unlockFile(holder); err != nil {
		t.Fatalf("unlock holder: %v", err)
	}
	locked = false
	if _, aerr := commitMutation(context.Background(), log, createMutation("host", "beta", 2)); aerr != nil {
		t.Fatalf("append after advisory lock release: %v", aerr)
	}
}

// TestOrderedFirstAppendFsyncsTheDirectoryChain asserts that the ancestor
// directory fsync happens on the FIRST append to a namespace and not on later
// ones. It cannot prove durability — that needs crash injection, the same
// limitation the file's f.Sync() carries — but it does prove the chain is
// walked, and when: the observable is that a directory the process cannot OPEN
// fails a creating append (fsyncDir must open it) and does not disturb a
// non-creating one.
//
// Skipped for root, which bypasses the permission bits the test depends on.
func TestOrderedFirstAppendFsyncsTheDirectoryChain(t *testing.T) {
	t.Parallel()
	if os.Geteuid() == 0 {
		t.Skip("running as root: directory permissions are not enforced")
	}
	store, root := newTestOrderedLogStore(t)
	const namespace = "factory/hosts"
	log, err := store.logFor(namespace)
	if err != nil {
		t.Fatalf("logFor: %v", err)
	}
	logPath := filepath.Join(root, orderedDirName, "factory", "hosts"+orderedLogExt)
	leafDir := filepath.Dir(logPath)
	// append's own MkdirAll is idempotent, so pre-creating the directory changes
	// nothing except that the test can adjust its permissions first.
	if merr := os.MkdirAll(leafDir, 0o700); merr != nil {
		t.Fatalf("mkdir %s: %v", leafDir, merr)
	}

	// A write+traverse-only directory: the log file can still be created and
	// written inside it, but it cannot be opened for the fsync.
	unreadable := func() {
		if cerr := os.Chmod(leafDir, 0o300); cerr != nil {
			t.Fatalf("chmod %s: %v", leafDir, cerr)
		}
	}
	readable := func() {
		if cerr := os.Chmod(leafDir, 0o700); cerr != nil {
			t.Fatalf("chmod %s: %v", leafDir, cerr)
		}
	}
	t.Cleanup(readable)

	// First append (the log is absent, so created is true): the chain fsync runs
	// and must surface the failure as a typed IO error rather than being skipped.
	unreadable()
	var ioErr *OrderedLogIOError
	seq, aerr := commitMutation(context.Background(), log, createMutation("host", "alpha", 1))
	if aerr == nil {
		t.Fatalf("creating append succeeded at seq %d despite an unopenable directory", seq)
	}
	if !errors.As(aerr, &ioErr) {
		t.Fatalf("creating append error = %T %v, want *OrderedLogIOError", aerr, aerr)
	}
	if ioErr.Op != "open-dir" {
		t.Fatalf("creating append error op = %q, want %q", ioErr.Op, "open-dir")
	}
	// The frame itself was written and fsynced before the directory chain, so
	// the seq must be reported alongside the error: a caller that retried here
	// would duplicate the mutation, not replace it.
	if seq != 1 {
		t.Errorf("failed creating append returned seq %d, want 1 (the frame is committed)", seq)
	}

	// With the directory readable an append succeeds, continuing the sequence.
	readable()
	if got := mustAppend(t, log, createMutation("host", "beta", 2)); got != 2 {
		t.Errorf("append after the fsync failure = seq %d, want 2", got)
	}

	// A LATER append finds a non-empty log, so created is false and the chain
	// fsync is skipped: the unopenable directory no longer matters.
	unreadable()
	if got, err := commitMutation(context.Background(), log, createMutation("host", "gamma", 3)); err != nil {
		t.Fatalf("non-creating append = (%d, %v), want success (the dir fsync must be skipped)", got, err)
	}
	readable()
	if got := mustReplay(t, log); len(got) != 3 {
		t.Fatalf("replayed %d mutations, want 3", len(got))
	}
}

// ---------------------------------------------------------------------------
// Current views: order, rank, and due
// ---------------------------------------------------------------------------

// newTestOrderedIndex returns an OrderedIndex over a fresh temp root, plus that
// root, and closes it at the end of the test.
func newTestOrderedIndex(t *testing.T) (*orderedIndex, string) {
	t.Helper()
	root := t.TempDir()
	return openTestOrderedIndex(t, root), root
}

// openTestOrderedIndex opens ANOTHER independent handle on root. Two handles
// share no mutex and no view — only the log files and the advisory lock — so a
// test using two of them exercises the same path two processes would.
func openTestOrderedIndex(t *testing.T, root string) *orderedIndex {
	t.Helper()
	index, err := newOrderedIndex(root)
	if err != nil {
		t.Fatalf("newOrderedIndex(%q): %v", root, err)
	}
	t.Cleanup(func() {
		if cerr := index.Close(); cerr != nil {
			t.Errorf("close ordered index: %v", cerr)
		}
	})
	return index
}

// orderedID is shorthand for an identity in the "sessions" namespace.
func orderedID(scope string, key storage.StableKey) storage.OrderedID {
	return storage.OrderedID{Namespace: "sessions", OrderingScope: scope, StableKey: key}
}

func ranked(value int64) storage.Rank { return storage.Rank{Ranked: true, Value: value} }

func dueAt(millis int64) storage.Due {
	return storage.Due{State: storage.DueAt, UnixMillis: millis}
}

// mustCreate creates id and requires that it was newly created.
func mustCreate(t *testing.T, x *orderedIndex, id storage.OrderedID, rank storage.Rank, due storage.Due) storage.OrderedRecord {
	t.Helper()
	record, created, err := x.Create(context.Background(), id, "catalog", []byte("v:"+string(id.StableKey)), rank, due)
	if err != nil {
		t.Fatalf("Create(%+v): %v", id, err)
	}
	if !created {
		t.Fatalf("Create(%+v): created=false, want a fresh record", id)
	}
	return record
}

// keysOf renders a record slice as "orderingScope/stableKey" strings, which is
// what the sort-key assertions compare — the full identity, so a tiebreaker
// failure is visible rather than aliased away.
func keysOf(records []storage.OrderedRecord) []string {
	out := make([]string, 0, len(records))
	for _, record := range records {
		out = append(out, record.ID.OrderingScope+"/"+string(record.ID.StableKey))
	}
	return out
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

// mustListRanked pages ListRanked to exhaustion at the given page size and
// returns every record in order, plus the number of pages it took.
func mustListRanked(t *testing.T, x *orderedIndex, namespace, rankingScope string, limit int) ([]storage.OrderedRecord, int) {
	t.Helper()
	var (
		all    []storage.OrderedRecord
		cursor storage.RankedCursor
		pages  int
	)
	for {
		page, err := x.ListRanked(context.Background(), namespace, rankingScope, cursor, limit)
		if err != nil {
			t.Fatalf("ListRanked(page %d): %v", pages, err)
		}
		pages++
		all = append(all, page.Records...)
		if page.NextCursor == "" {
			return all, pages
		}
		if pages > 100 {
			t.Fatalf("ListRanked did not terminate after %d pages", pages)
		}
		cursor = page.NextCursor
	}
}

// mustListDue pages ListDue to exhaustion at the given bound and page size.
func mustListDue(t *testing.T, x *orderedIndex, namespace string, bound int64, limit int) ([]storage.OrderedRecord, int) {
	t.Helper()
	var (
		all    []storage.OrderedRecord
		cursor storage.DueCursor
		pages  int
	)
	for {
		page, err := x.ListDue(context.Background(), namespace, bound, cursor, limit)
		if err != nil {
			t.Fatalf("ListDue(page %d): %v", pages, err)
		}
		pages++
		all = append(all, page.Records...)
		if page.NextCursor == "" {
			return all, pages
		}
		if pages > 100 {
			t.Fatalf("ListDue did not terminate after %d pages", pages)
		}
		cursor = page.NextCursor
	}
}

// mustListOrdered pages ListOrdered to exhaustion for one order scope.
func mustListOrdered(t *testing.T, x *orderedIndex, namespace, orderingScope string, limit int) []storage.OrderedRecord {
	t.Helper()
	var (
		all   []storage.OrderedRecord
		after uint64
	)
	for i := 0; ; i++ {
		page, err := x.ListOrdered(context.Background(), namespace, orderingScope, after, limit)
		if err != nil {
			t.Fatalf("ListOrdered(after %d): %v", after, err)
		}
		if len(page.Records) == 0 {
			return all
		}
		all = append(all, page.Records...)
		after = page.NextAfterOrder
		if i > 100 {
			t.Fatalf("ListOrdered did not terminate after %d pages", i)
		}
	}
}

// TestOrderedIndexReopenRestoresViews is the durability test for the views
// themselves: everything a reopened index reports about order, rank, due, and
// tombstones must come from replaying the log, because a fresh handle has no
// memory at all. It also pins the two facts a reopen is most likely to lose —
// that a tombstoned identity's order is NOT reused, and that the high-water mark
// survives even though nothing stores it.
func TestOrderedIndexReopenRestoresViews(t *testing.T) {
	t.Parallel()
	first, root := newTestOrderedIndex(t)
	ctx := context.Background()

	alive := mustCreate(t, first, orderedID("host-a", "alpha"), ranked(50), dueAt(1_000))
	moved := mustCreate(t, first, orderedID("host-a", "beta"), ranked(10), dueAt(3_000))
	doomed := mustCreate(t, first, orderedID("host-b", "gamma"), ranked(90), dueAt(2_000))

	// Move beta's rank and due time, then tombstone gamma.
	updated, err := first.Update(ctx, moved.ID, moved.Revision, []byte("moved"), ranked(70), dueAt(500))
	if err != nil {
		t.Fatalf("Update(beta): %v", err)
	}
	tombstone, err := first.Delete(ctx, doomed.ID, doomed.Revision)
	if err != nil {
		t.Fatalf("Delete(gamma): %v", err)
	}

	if cerr := first.Close(); cerr != nil {
		t.Fatalf("close first handle: %v", cerr)
	}

	second := openTestOrderedIndex(t, root)

	for _, want := range []storage.OrderedRecord{alive, updated, tombstone} {
		got, gerr := second.Get(ctx, want.ID)
		if gerr != nil {
			t.Fatalf("reopened Get(%+v): %v", want.ID, gerr)
		}
		if got.Revision != want.Revision || got.Order != want.Order || got.Deleted != want.Deleted ||
			got.Rank != want.Rank || got.Due != want.Due || got.RankingScope != want.RankingScope ||
			!bytes.Equal(got.Value, want.Value) {
			t.Fatalf("reopened Get(%+v) = %+v, want %+v", want.ID, got, want)
		}
	}

	// The ranked view after a reopen must reflect beta's MOVED rank (70), not the
	// rank its create frame carried (10), and must exclude the tombstone.
	rankedRecords, _ := mustListRanked(t, second, "sessions", "catalog", 10)
	if got, want := keysOf(rankedRecords), []string{"host-a/beta", "host-a/alpha"}; !equalStrings(got, want) {
		t.Fatalf("reopened ListRanked = %v, want %v", got, want)
	}
	// Likewise the due view: beta moved to 500, ahead of alpha's 1000, and gamma
	// left the view entirely when it was tombstoned.
	dueRecords, _ := mustListDue(t, second, "sessions", 10_000, 10)
	if got, want := keysOf(dueRecords), []string{"host-a/beta", "host-a/alpha"}; !equalStrings(got, want) {
		t.Fatalf("reopened ListDue = %v, want %v", got, want)
	}

	// The acceptance-order stream INCLUDES the tombstone: it is the immutable
	// stream, and a consumer resuming from an order cursor has to observe the
	// delete.
	streamB := mustListOrdered(t, second, "sessions", "host-b", 10)
	if len(streamB) != 1 || !streamB[0].Deleted {
		t.Fatalf("reopened ListOrdered(host-b) = %+v, want one tombstone", streamB)
	}

	// A tombstoned identity's order is never reused, and the high-water mark that
	// guarantees it was recovered from the log rather than stored.
	fresh := mustCreate(t, second, orderedID("host-b", "delta"), ranked(1), dueAt(9_000))
	if fresh.Order <= tombstone.Order {
		t.Fatalf("post-reopen create took order %d, want strictly greater than the tombstoned %d",
			fresh.Order, tombstone.Order)
	}
	// And the identity itself can never come back.
	if _, _, cerr := second.Create(ctx, doomed.ID, "catalog", []byte("resurrect"), ranked(5), dueAt(1)); cerr != nil {
		t.Fatalf("Create on a tombstone: %v", cerr)
	}
	revived, gerr := second.Get(ctx, doomed.ID)
	if gerr != nil || !revived.Deleted || revived.Revision != tombstone.Revision {
		t.Fatalf("Create on a tombstone changed it: %+v (err %v), want the tombstone %+v", revived, gerr, tombstone)
	}
}

// TestOrderedIndexTwoHandlesCreateOnce drives concurrent creates from two
// INDEPENDENT handles on one root. The handles share no store mutex and no view,
// so within this one process the ONLY thing that can stop both from finding the
// identity absent, both allocating an order, and both writing a frame at the
// offset each believes is the end of the file is the exclusive advisory lock
// held across catch-up, decision, and append. Exactly one create must win, every
// caller must be told the same record, and every distinct identity created
// alongside must get its own order and survive on the disk.
//
// The contention has to be real for any of that to be tested, which takes three
// things a weaker version of this test got wrong: every racer starts from a
// barrier rather than trickling in behind the goroutines before it; each does
// enough work that the window is wide; and NONE of the store calls happens
// inside the test's own bookkeeping mutex, which would serialize the very races
// it is trying to provoke.
func TestOrderedIndexTwoHandlesCreateOnce(t *testing.T) {
	t.Parallel()
	_, root := newTestOrderedIndex(t)
	left := openTestOrderedIndex(t, root)
	right := openTestOrderedIndex(t, root)
	ctx := context.Background()

	const (
		racers   = 16
		perRacer = 4
	)
	contested := orderedID("host", "contested")

	var (
		mu       sync.Mutex
		wins     int
		observed []storage.OrderedRecord
		wg       sync.WaitGroup
	)
	start := make(chan struct{})
	for i := range racers {
		handle := left
		if i%2 == 1 {
			handle = right
		}
		wg.Add(1)
		go func(handle *orderedIndex, i int) {
			defer wg.Done()
			<-start
			// Every racer proposes a DIFFERENT value, so a second winner would be
			// visible as a changed record rather than hidden by identical inputs.
			record, created, err := handle.Create(ctx, contested, "catalog",
				[]byte("racer-"+strconv.Itoa(i)), ranked(int64(i)), dueAt(int64(i)))
			// Distinct identities alongside the contested one, to prove order
			// allocation stays unique and no frame is clobbered under the same
			// contention. These run OUTSIDE the bookkeeping mutex on purpose.
			for j := range perRacer {
				key := storage.StableKey("solo-" + strconv.Itoa(i) + "-" + strconv.Itoa(j))
				if _, _, uerr := handle.Create(ctx, orderedID("host", key),
					"catalog", nil, storage.Rank{}, storage.Due{}); uerr != nil {
					t.Errorf("racer %d solo Create %d: %v", i, j, uerr)
				}
			}
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				t.Errorf("racer %d Create: %v", i, err)
				return
			}
			if created {
				wins++
			}
			observed = append(observed, record)
		}(handle, i)
	}
	close(start)
	wg.Wait()

	if wins != 1 {
		t.Fatalf("concurrent Create on one identity reported created=true %d times, want exactly 1", wins)
	}
	for _, record := range observed {
		if record.Revision != 1 || record.Order != observed[0].Order || !bytes.Equal(record.Value, observed[0].Value) {
			t.Fatalf("racers disagree about the created record: %+v vs %+v", record, observed[0])
		}
	}

	// Every create is on the disk exactly once, with a unique, strictly
	// increasing order — read back through a THIRD cold handle so the assertion
	// is about the log, not about either writer's memory.
	stream := mustListOrdered(t, openTestOrderedIndex(t, root), "sessions", "host", 1000)
	if want := racers*perRacer + 1; len(stream) != want {
		t.Fatalf("ListOrdered returned %d records, want %d (a lost frame means the flock did not serialize)", len(stream), want)
	}
	seen := make(map[uint64]bool, len(stream))
	var previous uint64
	for _, record := range stream {
		if seen[record.Order] {
			t.Fatalf("order %d allocated twice", record.Order)
		}
		if record.Order <= previous {
			t.Fatalf("ListOrdered is not ascending: %d after %d", record.Order, previous)
		}
		seen[record.Order] = true
		previous = record.Order
	}
}

// TestOrderedIndexUpdateCASPrecedence pins the contract's precedence exactly:
// absent and deleted are reported regardless of the expected revision, and only
// for a LIVE record is the candidate validated — before a stale revision is
// reported. Getting that order wrong is invisible in isolation and wrong in
// every caller that distinguishes "retry me" from "your input is bad".
func TestOrderedIndexUpdateCASPrecedence(t *testing.T) {
	t.Parallel()
	x, _ := newTestOrderedIndex(t)
	ctx := context.Background()

	absent := orderedID("host", "absent")
	var notFound *storage.OrderedRecordNotFoundError
	if _, err := x.Update(ctx, absent, 999, nil, storage.Rank{}, storage.Due{}); !errors.As(err, &notFound) {
		t.Fatalf("Update(absent) error = %v, want *OrderedRecordNotFoundError", err)
	}

	live := mustCreate(t, x, orderedID("host", "live"), ranked(5), dueAt(100))

	// A live record with an INVALID candidate and a STALE revision must report
	// the candidate, not the conflict.
	var invalidDue *storage.InvalidDueError
	if _, err := x.Update(ctx, live.ID, live.Revision+7, nil, storage.Rank{},
		storage.Due{State: storage.NotDue, UnixMillis: 5}); !errors.As(err, &invalidDue) {
		t.Fatalf("Update(live, stale, invalid due) error = %v, want *InvalidDueError", err)
	}
	// A valid candidate with a stale revision is the conflict.
	var conflict *storage.OrderedRevisionConflictError
	if _, err := x.Update(ctx, live.ID, live.Revision+7, []byte("x"), ranked(1), dueAt(1)); !errors.As(err, &conflict) {
		t.Fatalf("Update(live, stale, valid) error = %v, want *OrderedRevisionConflictError", err)
	}
	if conflict.ActualRevision != live.Revision || conflict.ExpectedRevision != live.Revision+7 {
		t.Fatalf("conflict = %+v, want expected %d actual %d", conflict, live.Revision+7, live.Revision)
	}
	// The rejected attempts changed nothing.
	if got, gerr := x.Get(ctx, live.ID); gerr != nil || got.Revision != live.Revision || !bytes.Equal(got.Value, live.Value) {
		t.Fatalf("after rejected updates Get = %+v (err %v), want %+v", got, gerr, live)
	}

	// The matching revision replaces value, rank, and due together, advances the
	// revision exactly once, and touches neither order nor ranking scope.
	next, err := x.Update(ctx, live.ID, live.Revision, []byte("after"), ranked(42), dueAt(777))
	if err != nil {
		t.Fatalf("Update(live, matching): %v", err)
	}
	if next.Revision != live.Revision+1 || next.Order != live.Order || next.RankingScope != live.RankingScope {
		t.Fatalf("Update produced %+v, want revision %d order %d scope %q",
			next, live.Revision+1, live.Order, live.RankingScope)
	}
	if next.Rank != ranked(42) || next.Due != dueAt(777) || !bytes.Equal(next.Value, []byte("after")) {
		t.Fatalf("Update did not replace value/rank/due atomically: %+v", next)
	}
	// The old revision is now stale.
	if _, err := x.Update(ctx, live.ID, live.Revision, []byte("again"), ranked(1), dueAt(1)); !errors.As(err, &conflict) {
		t.Fatalf("Update with the consumed revision = %v, want *OrderedRevisionConflictError", err)
	}
}

// TestOrderedIndexTombstoneLeavesTheCurrentViews checks the whole tombstone
// contract in one place: a delete clears rank and due, leaves the ranked and due
// views, STAYS in the acceptance-order stream, keeps its value and order, is
// terminal for Update, and answers a retry with the original tombstone even when
// the retry carries the pre-delete revision — which is exactly what a caller
// whose first attempt failed ambiguously still holds.
func TestOrderedIndexTombstoneLeavesTheCurrentViews(t *testing.T) {
	t.Parallel()
	x, _ := newTestOrderedIndex(t)
	ctx := context.Background()

	keep := mustCreate(t, x, orderedID("host", "keep"), ranked(1), dueAt(10))
	victim := mustCreate(t, x, orderedID("host", "victim"), ranked(2), dueAt(20))

	tombstone, err := x.Delete(ctx, victim.ID, victim.Revision)
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if !tombstone.Deleted || tombstone.Revision != victim.Revision+1 {
		t.Fatalf("Delete produced %+v, want Deleted with revision %d", tombstone, victim.Revision+1)
	}
	if tombstone.Rank != (storage.Rank{}) || tombstone.Due != (storage.Due{State: storage.NotDue}) {
		t.Fatalf("tombstone kept rank/due: %+v", tombstone)
	}
	if tombstone.Order != victim.Order || tombstone.RankingScope != victim.RankingScope ||
		!bytes.Equal(tombstone.Value, victim.Value) {
		t.Fatalf("tombstone lost order/scope/value: %+v, want from %+v", tombstone, victim)
	}

	rankedRecords, _ := mustListRanked(t, x, "sessions", "catalog", 10)
	if got, want := keysOf(rankedRecords), []string{"host/keep"}; !equalStrings(got, want) {
		t.Fatalf("ListRanked after delete = %v, want %v", got, want)
	}
	dueRecords, _ := mustListDue(t, x, "sessions", 1_000, 10)
	if got, want := keysOf(dueRecords), []string{"host/keep"}; !equalStrings(got, want) {
		t.Fatalf("ListDue after delete = %v, want %v", got, want)
	}
	stream := mustListOrdered(t, x, "sessions", "host", 10)
	if got, want := keysOf(stream), []string{"host/keep", "host/victim"}; !equalStrings(got, want) {
		t.Fatalf("ListOrdered after delete = %v, want %v (tombstones stay in the immutable stream)", got, want)
	}
	_ = keep

	// Terminal for Update, regardless of revision.
	var deleted *storage.OrderedDeletedError
	if _, uerr := x.Update(ctx, victim.ID, tombstone.Revision, []byte("no"), ranked(9), dueAt(9)); !errors.As(uerr, &deleted) {
		t.Fatalf("Update on a tombstone = %v, want *OrderedDeletedError", uerr)
	}
	// A retry carrying the PRE-delete revision gets the tombstone, not a conflict.
	retried, rerr := x.Delete(ctx, victim.ID, victim.Revision)
	if rerr != nil {
		t.Fatalf("Delete retry with the pre-delete revision: %v", rerr)
	}
	if retried.Revision != tombstone.Revision || !retried.Deleted {
		t.Fatalf("Delete retry = %+v, want the original tombstone %+v", retried, tombstone)
	}
	// So does a retry carrying a revision that never existed.
	if again, aerr := x.Delete(ctx, victim.ID, 4242); aerr != nil || again.Revision != tombstone.Revision {
		t.Fatalf("Delete retry with a bogus revision = %+v (err %v), want the tombstone", again, aerr)
	}
}

// TestOrderedIndexRankedSortKeyIsTotal pins the frozen descending
// (rank, stable_key, ordering_scope) order INCLUDING the ordering-scope
// tiebreaker the coordinator added on 2026-08-29. Without it the comparison is
// only a preorder: the two records below share a rank AND a stable key across
// two ordering scopes, so a cursor could not name a unique position among them
// and pagination at limit 1 would not terminate — which is why this test pages
// at 1 as well as reading the whole view.
func TestOrderedIndexRankedSortKeyIsTotal(t *testing.T) {
	t.Parallel()
	x, _ := newTestOrderedIndex(t)

	// Two full-tie groups, created in OPPOSITE orders. One group alone is not
	// enough: a comparator that ignores the ordering-scope tiebreaker leaves the
	// most-recently-inserted tied record first, which coincidentally matches the
	// required descending order for one creation order and contradicts it for the
	// other. Only the pair pins the tiebreaker rather than the insertion order.
	mustCreate(t, x, orderedID("host-a", "tie"), ranked(10), storage.Due{})
	mustCreate(t, x, orderedID("host-b", "tie"), ranked(10), storage.Due{})
	mustCreate(t, x, orderedID("host-b", "zit"), ranked(8), storage.Due{})
	mustCreate(t, x, orderedID("host-a", "zit"), ranked(8), storage.Due{})
	mustCreate(t, x, orderedID("host-a", "aaa"), ranked(10), storage.Due{})
	mustCreate(t, x, orderedID("host-a", "top"), ranked(99), storage.Due{})
	mustCreate(t, x, orderedID("host-a", "bottom"), ranked(-5), storage.Due{})
	// Unranked and tombstoned records are absent from the ranked view.
	mustCreate(t, x, orderedID("host-a", "unranked"), storage.Rank{}, storage.Due{})
	doomed := mustCreate(t, x, orderedID("host-a", "doomed"), ranked(1000), storage.Due{})
	if _, err := x.Delete(context.Background(), doomed.ID, doomed.Revision); err != nil {
		t.Fatalf("Delete(doomed): %v", err)
	}

	want := []string{
		"host-a/top",    // rank 99
		"host-b/tie",    // rank 10, key "tie", scope host-b sorts before host-a descending
		"host-a/tie",    // rank 10, key "tie"
		"host-a/aaa",    // rank 10, key "aaa"
		"host-b/zit",    // rank 8, the same tie resolved for the opposite creation order
		"host-a/zit",    // rank 8
		"host-a/bottom", // rank -5
	}
	whole, pages := mustListRanked(t, x, "sessions", "catalog", 1000)
	if got := keysOf(whole); !equalStrings(got, want) {
		t.Fatalf("ListRanked = %v, want %v", got, want)
	}
	if pages != 1 {
		t.Fatalf("ListRanked at limit 1000 took %d pages, want 1", pages)
	}
	// One record per page forces the cursor to resolve every tie.
	paged, pages := mustListRanked(t, x, "sessions", "catalog", 1)
	if got := keysOf(paged); !equalStrings(got, want) {
		t.Fatalf("ListRanked paged at limit 1 = %v, want %v", got, want)
	}
	if pages != len(want) {
		t.Fatalf("ListRanked at limit 1 took %d pages for %d records, want %d", pages, len(want), len(want))
	}
	// A ranking scope with no records is empty and cursorless, not an error.
	page, err := x.ListRanked(context.Background(), "sessions", "empty", "", 10)
	if err != nil || len(page.Records) != 0 || page.NextCursor != "" {
		t.Fatalf("ListRanked(empty scope) = %+v (err %v), want an empty terminal page", page, err)
	}
}

// TestOrderedIndexDueSortKeyIsTotalAndBounded pins the frozen ascending
// (due_at, stable_key, ordering_scope) order, the exclusion of not-due and
// tombstoned records, and the fact that the bound is INCLUSIVE and stops the
// page rather than filtering it. The due view spans every ordering scope in the
// namespace, so the ordering-scope tiebreaker matters here even more than for
// rank.
func TestOrderedIndexDueSortKeyIsTotalAndBounded(t *testing.T) {
	t.Parallel()
	x, _ := newTestOrderedIndex(t)

	// Two full-tie groups created in OPPOSITE orders, for the reason spelled out
	// in TestOrderedIndexRankedSortKeyIsTotal: one group's expected order is
	// reproduced by accident when the tiebreaker is missing.
	mustCreate(t, x, orderedID("host-b", "tie"), storage.Rank{}, dueAt(200))
	mustCreate(t, x, orderedID("host-a", "tie"), storage.Rank{}, dueAt(200))
	mustCreate(t, x, orderedID("host-a", "pair"), storage.Rank{}, dueAt(200))
	mustCreate(t, x, orderedID("host-b", "pair"), storage.Rank{}, dueAt(200))
	mustCreate(t, x, orderedID("host-a", "zzz"), storage.Rank{}, dueAt(200))
	mustCreate(t, x, orderedID("host-a", "early"), storage.Rank{}, dueAt(100))
	mustCreate(t, x, orderedID("host-a", "late"), storage.Rank{}, dueAt(900))
	mustCreate(t, x, orderedID("host-a", "never"), ranked(7), storage.Due{})
	doomed := mustCreate(t, x, orderedID("host-a", "doomed"), storage.Rank{}, dueAt(50))
	if _, err := x.Delete(context.Background(), doomed.ID, doomed.Revision); err != nil {
		t.Fatalf("Delete(doomed): %v", err)
	}

	// Bound 200 is INCLUSIVE and excludes the 900.
	want := []string{
		"host-a/early",
		"host-a/pair", // due 200, key "pair", scope host-a sorts before host-b ascending
		"host-b/pair", // the same tie resolved for the opposite creation order
		"host-a/tie",
		"host-b/tie",
		"host-a/zzz",
	}
	whole, _ := mustListDue(t, x, "sessions", 200, 1000)
	if got := keysOf(whole); !equalStrings(got, want) {
		t.Fatalf("ListDue(bound 200) = %v, want %v", got, want)
	}
	paged, pages := mustListDue(t, x, "sessions", 200, 1)
	if got := keysOf(paged); !equalStrings(got, want) {
		t.Fatalf("ListDue(bound 200) paged at limit 1 = %v, want %v", got, want)
	}
	if pages != len(want) {
		t.Fatalf("ListDue at limit 1 took %d pages for %d records, want %d", pages, len(want), len(want))
	}
	// A bound before everything yields nothing at all.
	if got, _ := mustListDue(t, x, "sessions", 0, 10); len(got) != 0 {
		t.Fatalf("ListDue(bound 0) = %v, want nothing", keysOf(got))
	}
	// A wide bound picks up the late record too, and still never the not-due or
	// tombstoned ones.
	wide, _ := mustListDue(t, x, "sessions", 1_000, 10)
	if got, wantAll := keysOf(wide), append(append([]string{}, want...), "host-a/late"); !equalStrings(got, wantAll) {
		t.Fatalf("ListDue(bound 1000) = %v, want %v", got, wantAll)
	}
}

// TestOrderedIndexCursorsFailClosed exercises all four rules the contract
// requires, for both cursor kinds: a malformed token, an unknown token version,
// a token issued for the OTHER kind, and a token whose bound query does not
// match the live request. It also proves a cursor is position and not authority
// by replaying one issued on a different handle.
func TestOrderedIndexCursorsFailClosed(t *testing.T) {
	t.Parallel()
	x, root := newTestOrderedIndex(t)
	ctx := context.Background()

	mustCreate(t, x, orderedID("host", "one"), ranked(2), dueAt(10))
	mustCreate(t, x, orderedID("host", "two"), ranked(1), dueAt(20))
	// A second ranking scope and namespace, so the mismatch cases have somewhere
	// valid to point.
	if _, _, err := x.Create(ctx, orderedID("host", "other"), "otherscope", nil, ranked(3), dueAt(30)); err != nil {
		t.Fatalf("Create(otherscope): %v", err)
	}

	rankedPage, err := x.ListRanked(ctx, "sessions", "catalog", "", 1)
	if err != nil || rankedPage.NextCursor == "" {
		t.Fatalf("ListRanked page 1 = %+v (err %v), want a continuation cursor", rankedPage, err)
	}
	duePage, err := x.ListDue(ctx, "sessions", 100, "", 1)
	if err != nil || duePage.NextCursor == "" {
		t.Fatalf("ListDue page 1 = %+v (err %v), want a continuation cursor", duePage, err)
	}

	assertCursorRule := func(t *testing.T, err error, kind storage.OrderedCursorKind, rule storage.OrderedCursorRule, what string) {
		t.Helper()
		var invalid *storage.InvalidOrderedCursorError
		if !errors.As(err, &invalid) {
			t.Fatalf("%s: error = %v, want *InvalidOrderedCursorError", what, err)
		}
		if invalid.Kind != kind || invalid.Rule != rule {
			t.Fatalf("%s: got kind %v rule %v, want kind %v rule %v", what, invalid.Kind, invalid.Rule, kind, rule)
		}
	}

	// Rule 1: malformed.
	for _, token := range []string{"not-a-cursor", rankedCursorHeader + "!!!not-base64!!!", rankedCursorHeader} {
		_, rerr := x.ListRanked(ctx, "sessions", "catalog", storage.RankedCursor(token), 1)
		assertCursorRule(t, rerr, storage.RankedCursorKind, storage.OrderedCursorMalformed, "ranked malformed "+strconv.Quote(token))
	}
	for _, token := range []string{"not-a-cursor", dueCursorHeader + "!!!not-base64!!!", dueCursorHeader} {
		_, derr := x.ListDue(ctx, "sessions", 100, storage.DueCursor(token), 1)
		assertCursorRule(t, derr, storage.DueCursorKind, storage.OrderedCursorMalformed, "due malformed "+strconv.Quote(token))
	}

	// Rule 2: unknown version. The scheme is recognized, the version is not.
	unknownRanked := strings.Replace(string(rankedPage.NextCursor), orderedCursorScheme+"1:", orderedCursorScheme+"2:", 1)
	if unknownRanked == string(rankedPage.NextCursor) {
		t.Fatal("failed to build an unknown-version ranked token")
	}
	_, rerr := x.ListRanked(ctx, "sessions", "catalog", storage.RankedCursor(unknownRanked), 1)
	assertCursorRule(t, rerr, storage.RankedCursorKind, storage.OrderedCursorUnknownVersion, "ranked unknown version")
	unknownDue := strings.Replace(string(duePage.NextCursor), orderedCursorScheme+"1:", orderedCursorScheme+"2:", 1)
	_, derr := x.ListDue(ctx, "sessions", 100, storage.DueCursor(unknownDue), 1)
	assertCursorRule(t, derr, storage.DueCursorKind, storage.OrderedCursorUnknownVersion, "due unknown version")

	// Rule 3: wrong kind. A real due cursor handed to ListRanked and vice versa.
	_, rerr = x.ListRanked(ctx, "sessions", "catalog", storage.RankedCursor(duePage.NextCursor), 1)
	assertCursorRule(t, rerr, storage.RankedCursorKind, storage.OrderedCursorWrongKind, "due token into ListRanked")
	_, derr = x.ListDue(ctx, "sessions", 100, storage.DueCursor(rankedPage.NextCursor), 1)
	assertCursorRule(t, derr, storage.DueCursorKind, storage.OrderedCursorWrongKind, "ranked token into ListDue")

	// Rule 4: query mismatch. The bindings are compared against the LIVE request,
	// never adopted from the token.
	_, rerr = x.ListRanked(ctx, "sessions", "otherscope", rankedPage.NextCursor, 1)
	assertCursorRule(t, rerr, storage.RankedCursorKind, storage.OrderedCursorQueryMismatch, "ranked cursor in another ranking scope")
	_, rerr = x.ListRanked(ctx, "elsewhere", "catalog", rankedPage.NextCursor, 1)
	assertCursorRule(t, rerr, storage.RankedCursorKind, storage.OrderedCursorQueryMismatch, "ranked cursor in another namespace")
	_, derr = x.ListDue(ctx, "sessions", 101, duePage.NextCursor, 1)
	assertCursorRule(t, derr, storage.DueCursorKind, storage.OrderedCursorQueryMismatch, "due cursor at another bound")
	_, derr = x.ListDue(ctx, "elsewhere", 100, duePage.NextCursor, 1)
	assertCursorRule(t, derr, storage.DueCursorKind, storage.OrderedCursorQueryMismatch, "due cursor in another namespace")

	// A cursor carries position, not authority: another handle on the same root
	// resumes from it with no shared state.
	other := openTestOrderedIndex(t, root)
	resumed, err := other.ListRanked(ctx, "sessions", "catalog", rankedPage.NextCursor, 10)
	if err != nil {
		t.Fatalf("resume a foreign handle's cursor: %v", err)
	}
	// Page 1 returned "one" (rank 2); resuming from its cursor on a handle that
	// never issued it yields the rest of the descending view.
	if got, want := keysOf(resumed.Records), []string{"host/two"}; !equalStrings(got, want) {
		t.Fatalf("resumed page = %v, want %v", got, want)
	}
}

// TestOrderedIndexStableKeyNeverReachesAPath is the security assertion. A
// StableKey is opaque caller data: it may contain '/', '..', a leading slash, a
// NUL-adjacent oddity, or arbitrary Unicode. Only the NAMESPACE becomes a path
// here, so however hostile the key, the store must contain exactly one file —
// the namespace's log — and the key must still round-trip byte for byte.
func TestOrderedIndexStableKeyNeverReachesAPath(t *testing.T) {
	t.Parallel()
	x, root := newTestOrderedIndex(t)
	ctx := context.Background()

	hostile := []storage.StableKey{
		"../../../etc/passwd",
		"..",
		"/absolute/escape",
		"a/b/c",
		"..\\windows\\escape",
		"key with spaces and ünïcode ☃",
	}
	for i, key := range hostile {
		id := orderedID("host", key)
		record, created, err := x.Create(ctx, id, "catalog", []byte(strconv.Itoa(i)), ranked(int64(i)), dueAt(int64(i)))
		if err != nil {
			t.Fatalf("Create(%q): %v", key, err)
		}
		if !created || record.ID.StableKey != key {
			t.Fatalf("Create(%q) = %+v created=%v, want the key round-tripped", key, record, created)
		}
		got, gerr := x.Get(ctx, id)
		if gerr != nil || got.ID.StableKey != key {
			t.Fatalf("Get(%q) = %+v (err %v), want the key round-tripped", key, got, gerr)
		}
	}

	// The whole store is one directory and one log file. Anything else means a
	// key component escaped into the filesystem.
	var found []string
	if err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		relative, rerr := filepath.Rel(root, path)
		if rerr != nil {
			return rerr
		}
		if relative != "." {
			found = append(found, filepath.ToSlash(relative))
		}
		return nil
	}); err != nil {
		t.Fatalf("walk store root: %v", err)
	}
	want := []string{"ordered", "ordered/sessions.olog"}
	if !equalStrings(found, want) {
		t.Fatalf("store contains %v, want exactly %v", found, want)
	}

	// And a reopen still reads every hostile key back, so nothing was normalized
	// on the way to the disk either.
	reopened := openTestOrderedIndex(t, root)
	for _, key := range hostile {
		if got, gerr := reopened.Get(ctx, orderedID("host", key)); gerr != nil || got.ID.StableKey != key {
			t.Fatalf("reopened Get(%q) = %+v (err %v)", key, got, gerr)
		}
	}
}

// TestOrderedIndexWarmPagesReadNoHistory is invariant 15's assertion, and the
// reason catchUp exists at all. A warm reader must not re-read or re-decode the
// prefix it has already applied: pages over a settled namespace must read ZERO
// bytes, and a page taken after another writer appended one frame must read
// exactly that frame — not the file.
//
// Rebuilding the view with replay per read would still return every correct
// answer, so no other test in this file can fail on it. This one measures the
// bytes.
func TestOrderedIndexWarmPagesReadNoHistory(t *testing.T) {
	t.Parallel()
	x, root := newTestOrderedIndex(t)
	ctx := context.Background()

	const records = 60
	for i := range records {
		mustCreate(t, x, orderedID("host", storage.StableKey("k-"+strconv.Itoa(i))), ranked(int64(i)), dueAt(int64(i)))
	}
	log, err := x.store.logFor("sessions")
	if err != nil {
		t.Fatalf("logFor: %v", err)
	}
	logPath := filepath.Join(root, orderedDirName, "sessions"+orderedLogExt)
	if size := fileSize(t, logPath); size == 0 {
		t.Fatalf("log %q is empty after %d creates", logPath, records)
	}

	// A writer folds its own frames in as it appends them, so a settled writer is
	// already caught up and has never had to read its own history back.
	settled := log.stats()
	if settled.ScannedBytes != 0 {
		t.Fatalf("after %d creates the writer read back %d bytes of its own log, want 0",
			records, settled.ScannedBytes)
	}

	// Twenty pages across all three views read nothing at all.
	for range 5 {
		mustListOrdered(t, x, "sessions", "host", 7)
		mustListRanked(t, x, "sessions", "catalog", 7)
		mustListDue(t, x, "sessions", records, 7)
		if _, gerr := x.Get(ctx, orderedID("host", "k-3")); gerr != nil {
			t.Fatalf("Get: %v", gerr)
		}
	}
	warm := log.stats()
	if warm.ScannedBytes != settled.ScannedBytes || warm.CatchUps != settled.CatchUps {
		t.Fatalf("warm reads scanned %d bytes over %d catch-ups, want no change from %d/%d",
			warm.ScannedBytes, warm.CatchUps, settled.ScannedBytes, settled.CatchUps)
	}
	if warm.AppliedFrames != records {
		t.Fatalf("applied %d frames, want exactly the %d written", warm.AppliedFrames, records)
	}

	// A second handle appends one frame. The first handle's next page must read
	// exactly that frame's bytes — not the whole log.
	sizeBefore := fileSize(t, logPath)
	other := openTestOrderedIndex(t, root)
	mustCreate(t, other, orderedID("host", "appended-elsewhere"), ranked(1_000), dueAt(1))
	frameSize := fileSize(t, logPath) - sizeBefore
	if frameSize <= 0 || frameSize >= sizeBefore {
		t.Fatalf("one appended frame changed the log by %d bytes (log was %d); the test needs a small tail", frameSize, sizeBefore)
	}

	page, err := x.ListRanked(ctx, "sessions", "catalog", "", 1)
	if err != nil {
		t.Fatalf("ListRanked after a foreign append: %v", err)
	}
	if got, want := keysOf(page.Records), []string{"host/appended-elsewhere"}; !equalStrings(got, want) {
		t.Fatalf("ListRanked after a foreign append = %v, want %v", got, want)
	}
	caught := log.stats()
	if delta := caught.ScannedBytes - warm.ScannedBytes; delta != frameSize {
		t.Fatalf("catching up on one appended frame read %d bytes, want exactly the frame's %d "+
			"(the whole log is %d bytes; reading it would be the unbounded scan invariant 15 forbids)",
			delta, frameSize, sizeBefore+frameSize)
	}
	if delta := caught.AppliedFrames - warm.AppliedFrames; delta != 1 {
		t.Fatalf("catching up applied %d frames, want exactly 1", delta)
	}
}

// TestOrderedIndexRejectsInvalidIdentityUnwrapped pins the classification the
// storage contract names, which is NOT the one the codec's shared validation
// gate would produce. The codec returns *OrderedCodecError for caller garbage
// and the same type wrapped in *OrderedLogCorruptError for disk damage, so a
// public method that let the codec do its argument checking would report a bad
// stable key as something a caller cannot distinguish from a damaged log. Every
// public entry point therefore validates with storage.ValidateOrderedID first
// and returns THAT error unwrapped.
func TestOrderedIndexRejectsInvalidIdentityUnwrapped(t *testing.T) {
	t.Parallel()
	x, root := newTestOrderedIndex(t)
	ctx := context.Background()

	longKey := storage.StableKey(strings.Repeat("k", storage.MaxStableKeyBytes+1))
	cases := []struct {
		name string
		id   storage.OrderedID
		want any
	}{
		{"bad namespace", storage.OrderedID{Namespace: "../escape", OrderingScope: "host", StableKey: "k"}, new(*storage.InvalidNameError)},
		{"empty namespace", storage.OrderedID{Namespace: "", OrderingScope: "host", StableKey: "k"}, new(*storage.InvalidNameError)},
		{"bad ordering scope", storage.OrderedID{Namespace: "sessions", OrderingScope: "Host!", StableKey: "k"}, new(*storage.InvalidNameError)},
		{"empty stable key", storage.OrderedID{Namespace: "sessions", OrderingScope: "host", StableKey: ""}, new(*storage.InvalidStableKeyError)},
		{"oversize stable key", storage.OrderedID{Namespace: "sessions", OrderingScope: "host", StableKey: longKey}, new(*storage.InvalidStableKeyError)},
		{"invalid utf-8 stable key", storage.OrderedID{Namespace: "sessions", OrderingScope: "host", StableKey: storage.StableKey([]byte{0xff, 0xfe})}, new(*storage.InvalidStableKeyError)},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			operations := map[string]func() error{
				"Get": func() error {
					_, err := x.Get(ctx, testCase.id)
					return err
				},
				"Create": func() error {
					_, _, err := x.Create(ctx, testCase.id, "catalog", nil, storage.Rank{}, storage.Due{})
					return err
				},
				"Update": func() error {
					_, err := x.Update(ctx, testCase.id, 1, nil, storage.Rank{}, storage.Due{})
					return err
				},
				"Delete": func() error {
					_, err := x.Delete(ctx, testCase.id, 1)
					return err
				},
			}
			for name, operation := range operations {
				err := operation()
				if err == nil {
					t.Fatalf("%s: no error, want a validation failure", name)
				}
				switch want := testCase.want.(type) {
				case **storage.InvalidNameError:
					if !errors.As(err, want) {
						t.Fatalf("%s: error = %#v, want *storage.InvalidNameError", name, err)
					}
				case **storage.InvalidStableKeyError:
					if !errors.As(err, want) {
						t.Fatalf("%s: error = %#v, want *storage.InvalidStableKeyError", name, err)
					}
				}
				// The codec's internal fault types must not surface here: a
				// caller cannot tell those apart from a damaged log.
				var codecErr *OrderedCodecError
				var corruptErr *OrderedLogCorruptError
				if errors.As(err, &codecErr) || errors.As(err, &corruptErr) {
					t.Fatalf("%s: error = %#v leaks an internal log/codec fault type", name, err)
				}
			}
		})
	}

	// The gate is UP FRONT, not merely somewhere along the path: nothing was
	// opened, created, or written for any of these calls. A backstop deeper in
	// (storage.ValidateOrderedRecord, say) would return the same error but only
	// after Create had already laid down a directory and an empty log for an
	// identity that will never exist.
	entries, err := os.ReadDir(filepath.Join(root, orderedDirName))
	if err != nil {
		t.Fatalf("read ordered dir: %v", err)
	}
	if len(entries) != 0 {
		names := make([]string, 0, len(entries))
		for _, entry := range entries {
			names = append(names, entry.Name())
		}
		t.Fatalf("rejected identities left %v on disk, want nothing", names)
	}
}

// TestOrderedIndexRejectsInvalidListArguments covers the listing validators,
// which have no OrderedID to validate but the same obligation to fail before
// touching a log.
func TestOrderedIndexRejectsInvalidListArguments(t *testing.T) {
	t.Parallel()
	x, root := newTestOrderedIndex(t)
	ctx := context.Background()

	var nameErr *storage.InvalidNameError
	if _, err := x.ListOrdered(ctx, "../escape", "host", 0, 10); !errors.As(err, &nameErr) {
		t.Fatalf("ListOrdered(bad namespace) = %v, want *InvalidNameError", err)
	}
	if _, err := x.ListOrdered(ctx, "sessions", "Bad Scope", 0, 10); !errors.As(err, &nameErr) {
		t.Fatalf("ListOrdered(bad scope) = %v, want *InvalidNameError", err)
	}
	if _, err := x.ListRanked(ctx, "../escape", "catalog", "", 10); !errors.As(err, &nameErr) {
		t.Fatalf("ListRanked(bad namespace) = %v, want *InvalidNameError", err)
	}
	if _, err := x.ListRanked(ctx, "sessions", "Bad Scope", "", 10); !errors.As(err, &nameErr) {
		t.Fatalf("ListRanked(bad scope) = %v, want *InvalidNameError", err)
	}
	if _, err := x.ListDue(ctx, "../escape", 0, "", 10); !errors.As(err, &nameErr) {
		t.Fatalf("ListDue(bad namespace) = %v, want *InvalidNameError", err)
	}

	var limitErr *storage.InvalidOrderedLimitError
	for _, limit := range []int{0, -1, storage.MaxOrderedPageLimit + 1} {
		if _, err := x.ListOrdered(ctx, "sessions", "host", 0, limit); !errors.As(err, &limitErr) {
			t.Fatalf("ListOrdered(limit %d) = %v, want *InvalidOrderedLimitError", limit, err)
		}
		if _, err := x.ListRanked(ctx, "sessions", "catalog", "", limit); !errors.As(err, &limitErr) {
			t.Fatalf("ListRanked(limit %d) = %v, want *InvalidOrderedLimitError", limit, err)
		}
		if _, err := x.ListDue(ctx, "sessions", 0, "", limit); !errors.As(err, &limitErr) {
			t.Fatalf("ListDue(limit %d) = %v, want *InvalidOrderedLimitError", limit, err)
		}
	}

	// A rejected read, and a successful read of a namespace that was never
	// written, both leave the store empty: a listing never creates a log file.
	if entries, err := os.ReadDir(filepath.Join(root, orderedDirName)); err != nil || len(entries) != 0 {
		t.Fatalf("ordered dir = %v (err %v), want empty after reads only", entries, err)
	}
	if page, err := x.ListOrdered(ctx, "untouched", "host", 0, 10); err != nil || len(page.Records) != 0 {
		t.Fatalf("ListOrdered(untouched) = %+v (err %v), want an empty page", page, err)
	}
	if entries, err := os.ReadDir(filepath.Join(root, orderedDirName)); err != nil || len(entries) != 0 {
		t.Fatalf("reading an untouched namespace created %v (err %v), want nothing", entries, err)
	}
}

// TestOrderedViewRefusesAnUpdateToAnAbsentIdentity pins the absent-identity
// check specifically, which no LOG can reach: an update frame naming an
// uncreated identity is caught first by the revision-successor check, because a
// missing predecessor reads as revision 0 and any legal frame's revision is at
// least 1 more than that... except for exactly one shape. A frame at revision 1
// with acceptance order 0 and an empty ranking scope matches the zero-valued
// predecessor on every subsequent test, so without this check it would be folded
// in as a silent CREATE performed by an update. The codec rejects that frame, so
// it cannot arrive off the disk — but the check is what makes that a defence
// rather than a coincidence of the codec's ceilings, and it is applied here
// directly for the same reason.
func TestOrderedViewRefusesAnUpdateToAnAbsentIdentity(t *testing.T) {
	t.Parallel()
	for _, op := range []orderedOp{orderedOpUpdate, orderedOpDelete} {
		t.Run(strconv.Itoa(int(op)), func(t *testing.T) {
			t.Parallel()
			view := newOrderedView("sessions")
			ghost := orderedMutation{
				Op:            op,
				OrderingScope: "host",
				StableKey:     "ghost",
				RankingScope:  "",
				Revision:      1,
				Order:         0,
			}
			if err := view.apply(ghost); !errors.Is(err, errOrderedInconsistentLog) {
				t.Fatalf("apply of a %v for an absent identity = %v, want errOrderedInconsistentLog", op, err)
			}
			if len(view.records) != 0 {
				t.Fatalf("the rejected frame still published %+v", view.records)
			}
		})
	}
}

// TestOrderedViewReportsAnIndexThatLostAnEntry covers the one inconsistency no
// log can produce: the view's record map and its sorted indexes disagreeing with
// each other. insertCurrent puts a ranked or due record into the corresponding
// index under exactly the conditions removeCurrent takes it out under, so a
// missing entry means the two halves of the view have drifted — and continuing
// would publish the updated record while the stale entry stayed in the sorted
// slice, so a later page would return a record under sort keys it no longer has.
//
// It is a white-box test because the state it describes is unreachable from
// outside; the alternative to testing it here is not testing it at all.
func TestOrderedViewReportsAnIndexThatLostAnEntry(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		rank    storage.Rank
		due     storage.Due
		corrupt func(v *orderedView)
	}{
		{
			name:    "the ranked index lost the entry",
			rank:    storage.Rank{Ranked: true, Value: 5},
			due:     storage.Due{},
			corrupt: func(v *orderedView) { v.ranked["catalog"] = nil },
		},
		{
			name:    "the due index lost the entry",
			rank:    storage.Rank{},
			due:     storage.Due{State: storage.DueAt, UnixMillis: 100},
			corrupt: func(v *orderedView) { v.due = nil },
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			view := newOrderedView("sessions")
			create := createMutation("host", "victim", 1)
			create.Rank, create.Due = tt.rank, tt.due
			if err := view.apply(create); err != nil {
				t.Fatalf("apply create: %v", err)
			}
			tt.corrupt(view)

			update := updateMutation("host", "victim", 2, 1)
			update.Rank, update.Due = tt.rank, tt.due
			if err := view.apply(update); !errors.Is(err, errOrderedInconsistentLog) {
				t.Fatalf("apply over a lost index entry = %v, want errOrderedInconsistentLog", err)
			}
		})
	}
}

// TestOrderedIndexValidatesArgumentsBeforeTheStore makes the up-front validation
// gates OBSERVABLE, which they otherwise are not.
//
// logFor validates the namespace itself and returns the identical
// *storage.InvalidNameError, so on a healthy store deleting
// storage.ValidateName(namespace) from a listing, or storage.ValidateOrderedID
// from a public method, changes nothing any "bad namespace" case can see: the
// same error still arrives, from one frame deeper. The gates are not redundant
// though, and this test pins the two things that make them load-bearing.
//
// First, ORDER. A caller error is classified by the argument the method
// validates first, not by whichever check happens to be reached. Without the
// namespace gate a listing with a bad namespace AND a bad limit reports the
// limit, and one with a bad namespace and a cursor bound to a different
// namespace reports a query mismatch — telling the caller their cursor is stale
// when in fact their namespace is unusable.
//
// Second, PRECEDENCE OVER STORE STATE. logFor's namespace check runs before its
// closed check, but nothing else does: on a closed store an unvalidated ordering
// scope or stable key would come back as *OrderedLogClosedError, so a caller
// would be told the store is shut when what is actually wrong is their argument.
func TestOrderedIndexValidatesArgumentsBeforeTheStore(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("the namespace is classified before the limit and the cursor", func(t *testing.T) {
		t.Parallel()
		x, _ := newTestOrderedIndex(t)
		mustCreate(t, x, orderedID("host", "one"), ranked(2), dueAt(10))
		mustCreate(t, x, orderedID("host", "two"), ranked(1), dueAt(20))
		rankedPage, err := x.ListRanked(ctx, "sessions", "catalog", "", 1)
		if err != nil || rankedPage.NextCursor == "" {
			t.Fatalf("ListRanked page 1 = %+v (err %v), want a continuation cursor", rankedPage, err)
		}
		duePage, err := x.ListDue(ctx, "sessions", 100, "", 1)
		if err != nil || duePage.NextCursor == "" {
			t.Fatalf("ListDue page 1 = %+v (err %v), want a continuation cursor", duePage, err)
		}

		var nameErr *storage.InvalidNameError
		cases := map[string]func() error{
			"ListOrdered, bad namespace and a bad limit": func() error {
				_, err := x.ListOrdered(ctx, "../escape", "host", 0, 0)
				return err
			},
			"ListRanked, bad namespace and a bad limit": func() error {
				_, err := x.ListRanked(ctx, "../escape", "catalog", "", 0)
				return err
			},
			"ListDue, bad namespace and a bad limit": func() error {
				_, err := x.ListDue(ctx, "../escape", 0, "", 0)
				return err
			},
			"ListRanked, bad namespace and a foreign cursor": func() error {
				_, err := x.ListRanked(ctx, "../escape", "catalog", rankedPage.NextCursor, 10)
				return err
			},
			"ListDue, bad namespace and a foreign cursor": func() error {
				_, err := x.ListDue(ctx, "../escape", 100, duePage.NextCursor, 10)
				return err
			},
		}
		for name, call := range cases {
			if err := call(); !errors.As(err, &nameErr) {
				t.Errorf("%s = %#v, want *storage.InvalidNameError", name, err)
			}
		}
	})

	t.Run("an argument is classified before the store's own state", func(t *testing.T) {
		t.Parallel()
		x, _ := newTestOrderedIndex(t)
		if err := x.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		// The store really is shut, so a well-formed call reports that.
		var closedErr *OrderedLogClosedError
		if _, err := x.Get(ctx, orderedID("host", "fine")); !errors.As(err, &closedErr) {
			t.Fatalf("Get on a closed store with a valid id = %#v, want *OrderedLogClosedError", err)
		}
		if _, err := x.ListOrdered(ctx, "sessions", "host", 0, 10); !errors.As(err, &closedErr) {
			t.Fatalf("ListOrdered on a closed store = %#v, want *OrderedLogClosedError", err)
		}

		badScope := storage.OrderedID{Namespace: "sessions", OrderingScope: "Host!", StableKey: "k"}
		badKey := storage.OrderedID{Namespace: "sessions", OrderingScope: "host", StableKey: ""}
		methods := map[string]func(storage.OrderedID) error{
			"Get": func(id storage.OrderedID) error {
				_, err := x.Get(ctx, id)
				return err
			},
			"Create": func(id storage.OrderedID) error {
				_, _, err := x.Create(ctx, id, "catalog", nil, storage.Rank{}, storage.Due{})
				return err
			},
			"Update": func(id storage.OrderedID) error {
				_, err := x.Update(ctx, id, 1, nil, storage.Rank{}, storage.Due{})
				return err
			},
			"Delete": func(id storage.OrderedID) error {
				_, err := x.Delete(ctx, id, 1)
				return err
			},
		}
		for name, call := range methods {
			var nameErr *storage.InvalidNameError
			if err := call(badScope); !errors.As(err, &nameErr) {
				t.Errorf("%s(bad ordering scope) on a closed store = %#v, want *storage.InvalidNameError", name, err)
			}
			var keyErr *storage.InvalidStableKeyError
			if err := call(badKey); !errors.As(err, &keyErr) {
				t.Errorf("%s(bad stable key) on a closed store = %#v, want *storage.InvalidStableKeyError", name, err)
			}
		}
		// The listings' scope and limit gates answer before the closed store too.
		var nameErr *storage.InvalidNameError
		if _, err := x.ListOrdered(ctx, "sessions", "Bad Scope", 0, 10); !errors.As(err, &nameErr) {
			t.Errorf("ListOrdered(bad scope) on a closed store = %#v, want *storage.InvalidNameError", err)
		}
		if _, err := x.ListRanked(ctx, "sessions", "Bad Scope", "", 10); !errors.As(err, &nameErr) {
			t.Errorf("ListRanked(bad scope) on a closed store = %#v, want *storage.InvalidNameError", err)
		}
		var limitErr *storage.InvalidOrderedLimitError
		if _, err := x.ListDue(ctx, "sessions", 0, "", 0); !errors.As(err, &limitErr) {
			t.Errorf("ListDue(bad limit) on a closed store = %#v, want *storage.InvalidOrderedLimitError", err)
		}
	})
}

// TestOrderedIndexInconsistentLogFailsClosed proves the fold is validated rather
// than trusted. A frame whose framing and payload are both perfectly valid can
// still contradict the state it applies to — an update for an identity that was
// never created, a second create for one that was, a mutation of a tombstone, or
// a revision that skips. None is reachable through this package's writers, so
// only a foreign or hand-edited log produces one; applying it anyway would leave
// the in-memory indexes quietly wrong instead of erroring.
func TestOrderedIndexInconsistentLogFailsClosed(t *testing.T) {
	t.Parallel()

	// Each case writes RAW frames straight to the log file, bypassing every
	// consistency check the write path makes — no caller of this package can
	// produce such a log — and then asks a cold index to read the namespace back.
	cases := []struct {
		name   string
		frames []orderedMutation
	}{
		{
			name:   "update without a create",
			frames: []orderedMutation{updateMutation("host", "ghost", 2, 1)},
		},
		{
			name: "create over an existing identity",
			frames: []orderedMutation{
				createMutation("host", "dup", 1),
				createMutation("host", "dup", 2),
			},
		},
		{
			name: "order that does not increase",
			frames: []orderedMutation{
				createMutation("host", "first", 5),
				createMutation("host", "second", 5),
			},
		},
		{
			name: "revision that skips",
			frames: []orderedMutation{
				createMutation("host", "skipped", 1),
				updateMutation("host", "skipped", 3, 1),
			},
		},
		{
			name: "create stamped with a revision other than 1",
			frames: []orderedMutation{
				func() orderedMutation {
					m := createMutation("host", "reborn", 1)
					m.Revision = 2
					return m
				}(),
			},
		},
		{
			name: "update that changes the ranking scope",
			frames: []orderedMutation{
				createMutation("host", "moved", 1),
				func() orderedMutation {
					m := updateMutation("host", "moved", 2, 1)
					m.RankingScope = "elsewhere"
					return m
				}(),
			},
		},
		{
			name: "update that changes the acceptance order",
			frames: []orderedMutation{
				createMutation("host", "renumbered", 1),
				updateMutation("host", "renumbered", 2, 9),
			},
		},
		{
			name: "mutation after a tombstone",
			frames: []orderedMutation{
				createMutation("host", "gone", 1),
				deleteMutation("host", "gone", 2, 1),
				updateMutation("host", "gone", 3, 1),
			},
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			writeRawFrames(t, root, "sessions", testCase.frames)

			index := openTestOrderedIndex(t, root)
			_, gerr := index.ListOrdered(context.Background(), "sessions", "host", 0, 10)
			var corrupt *OrderedLogCorruptError
			if !errors.As(gerr, &corrupt) {
				t.Fatalf("ListOrdered over a contradictory log = %v, want *OrderedLogCorruptError", gerr)
			}
			if !errors.Is(gerr, errOrderedInconsistentLog) {
				t.Fatalf("corrupt error %v does not unwrap to errOrderedInconsistentLog", gerr)
			}
			// The failure is sticky, not a one-off: a second read must not serve
			// a half-applied view.
			if _, again := index.ListOrdered(context.Background(), "sessions", "host", 0, 10); !errors.As(again, &corrupt) {
				t.Fatalf("second read after corruption = %v, want the same failure", again)
			}
		})
	}
}

// updateMutation builds an update frame for an identity at the given revision.
func updateMutation(scope string, key storage.StableKey, revision, order uint64) orderedMutation {
	m := createMutation(scope, key, order)
	m.Op = orderedOpUpdate
	m.Revision = revision
	return m
}

// deleteMutation builds a tombstone frame, which the codec requires to be
// unranked and not due.
func deleteMutation(scope string, key storage.StableKey, revision, order uint64) orderedMutation {
	m := updateMutation(scope, key, revision, order)
	m.Op = orderedOpDelete
	m.Rank = storage.Rank{}
	m.Due = storage.Due{}
	return m
}

// TestOrderedIndexRecordsAreCallerOwned pins the contract's slice-ownership
// rule in both directions: a caller mutating the slice it passed in must not
// change stored state, and a caller mutating a slice it was handed must not
// change what the next reader sees.
func TestOrderedIndexRecordsAreCallerOwned(t *testing.T) {
	t.Parallel()
	x, _ := newTestOrderedIndex(t)
	ctx := context.Background()
	id := orderedID("host", "owned")

	value := []byte("original")
	created, _, err := x.Create(ctx, id, "catalog", value, ranked(1), dueAt(1))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	copy(value, "clobbered")
	copy(created.Value, "vandalism")

	got, err := x.Get(ctx, id)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got.Value, []byte("original")) {
		t.Fatalf("Get returned %q, want %q: the store retained a caller slice", got.Value, "original")
	}
	copy(got.Value, "again!!!")

	page, err := x.ListRanked(ctx, "sessions", "catalog", "", 10)
	if err != nil {
		t.Fatalf("ListRanked: %v", err)
	}
	if len(page.Records) != 1 || !bytes.Equal(page.Records[0].Value, []byte("original")) {
		t.Fatalf("ListRanked returned %q, want %q: a returned slice aliased the index",
			page.Records[0].Value, "original")
	}
	// Clobber the LISTING's slice, not Get's. Mutating a record Get already
	// cloned proves nothing about collect, which is the function all three
	// listings hand their records out through.
	copy(page.Records[0].Value, "listwreck")
	again, err := x.ListRanked(ctx, "sessions", "catalog", "", 10)
	if err != nil {
		t.Fatalf("ListRanked (second): %v", err)
	}
	if len(again.Records) != 1 || !bytes.Equal(again.Records[0].Value, []byte("original")) {
		t.Fatalf("ListRanked after a caller mutated its page = %q, want %q: collect handed out the index's own slice",
			again.Records[0].Value, "original")
	}
	if record, gerr := x.Get(ctx, id); gerr != nil || !bytes.Equal(record.Value, []byte("original")) {
		t.Fatalf("Get after a caller mutated a listing = %q (err %v), want %q", record.Value, gerr, "original")
	}
	// A nil value stays nil rather than becoming an empty slice.
	nilID := orderedID("host", "nil-value")
	if record, _, cerr := x.Create(ctx, nilID, "catalog", nil, storage.Rank{}, storage.Due{}); cerr != nil || record.Value != nil {
		t.Fatalf("Create(nil value) = %+v (err %v), want a nil Value", record, cerr)
	}
	if record, gerr := x.Get(ctx, nilID); gerr != nil || record.Value != nil {
		t.Fatalf("Get(nil value) = %+v (err %v), want a nil Value", record, gerr)
	}
}

// TestOrderedIndexUnrankedRankIsCanonical pins a subtle reopen hazard. storage
// permits a caller to pass Rank{Ranked: false, Value: 7} and a backend may not
// be stricter than the contract — but the wire format has exactly one spelling
// for "unranked", so the value published in memory has to be the one that
// survives a restart or the record silently changes under a reopen.
func TestOrderedIndexUnrankedRankIsCanonical(t *testing.T) {
	t.Parallel()
	x, root := newTestOrderedIndex(t)
	ctx := context.Background()
	id := orderedID("host", "unranked")

	created, _, err := x.Create(ctx, id, "catalog", nil, storage.Rank{Ranked: false, Value: 7}, storage.Due{})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if created.Rank != (storage.Rank{}) {
		t.Fatalf("Create published rank %+v, want the canonical zero Rank", created.Rank)
	}
	updated, err := x.Update(ctx, id, created.Revision, nil, storage.Rank{Ranked: false, Value: -3}, storage.Due{})
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if updated.Rank != (storage.Rank{}) {
		t.Fatalf("Update published rank %+v, want the canonical zero Rank", updated.Rank)
	}

	reopened := openTestOrderedIndex(t, root)
	got, err := reopened.Get(ctx, id)
	if err != nil {
		t.Fatalf("reopened Get: %v", err)
	}
	if got.Rank != updated.Rank {
		t.Fatalf("reopened rank %+v != in-memory rank %+v: the record changed under a restart", got.Rank, updated.Rank)
	}
}

// TestOrderedIndexPaginationOverAMovedRecord documents, rather than forbids,
// the weak consistency the contract allows: the cursor names a frozen position,
// not a snapshot, so a record whose rank moves across that position between two
// pages is skipped or repeated. This test pins the SKIP, because a provider that
// silently "fixed" it would be holding a snapshot — which is the unbounded
// per-page work invariant 15 forbids. Clients reconcile by stable identity.
func TestOrderedIndexPaginationOverAMovedRecord(t *testing.T) {
	t.Parallel()
	x, _ := newTestOrderedIndex(t)
	ctx := context.Background()

	mustCreate(t, x, orderedID("host", "high"), ranked(30), storage.Due{})
	middle := mustCreate(t, x, orderedID("host", "middle"), ranked(20), storage.Due{})
	mustCreate(t, x, orderedID("host", "low"), ranked(10), storage.Due{})

	first, err := x.ListRanked(ctx, "sessions", "catalog", "", 1)
	if err != nil || len(first.Records) != 1 || first.Records[0].ID.StableKey != "high" {
		t.Fatalf("page 1 = %+v (err %v), want [high]", keysOf(first.Records), err)
	}
	// Move "middle" ABOVE the frozen cursor position while the caller pages.
	if _, uerr := x.Update(ctx, middle.ID, middle.Revision, nil, ranked(99), storage.Due{}); uerr != nil {
		t.Fatalf("Update(middle): %v", uerr)
	}
	second, err := x.ListRanked(ctx, "sessions", "catalog", first.NextCursor, 10)
	if err != nil {
		t.Fatalf("page 2: %v", err)
	}
	if got, want := keysOf(second.Records), []string{"host/low"}; !equalStrings(got, want) {
		t.Fatalf("page 2 = %v, want %v: a record that moved behind the frozen cursor is skipped", got, want)
	}
	// It is still there — the caller just has to reconcile by identity.
	whole, _ := mustListRanked(t, x, "sessions", "catalog", 10)
	if got, want := keysOf(whole), []string{"host/middle", "host/high", "host/low"}; !equalStrings(got, want) {
		t.Fatalf("full listing = %v, want %v", got, want)
	}
}

// TestOrderedIndexNamespacesAreIndependent checks that identity really is
// (namespace, ordering_scope, stable_key): the same scope and key in two
// namespaces are two records, in two logs, with two independent order scopes.
func TestOrderedIndexNamespacesAreIndependent(t *testing.T) {
	t.Parallel()
	x, root := newTestOrderedIndex(t)
	ctx := context.Background()

	left := storage.OrderedID{Namespace: "sessions", OrderingScope: "host", StableKey: "shared"}
	right := storage.OrderedID{Namespace: "factory/hosts", OrderingScope: "host", StableKey: "shared"}

	leftRecord, _, err := x.Create(ctx, left, "catalog", []byte("left"), ranked(1), dueAt(1))
	if err != nil {
		t.Fatalf("Create(left): %v", err)
	}
	rightRecord, created, err := x.Create(ctx, right, "catalog", []byte("right"), ranked(2), dueAt(2))
	if err != nil || !created {
		t.Fatalf("Create(right) = %+v created=%v err=%v, want a fresh record", rightRecord, created, err)
	}
	if !bytes.Equal(rightRecord.Value, []byte("right")) {
		t.Fatalf("the two namespaces collided: %+v", rightRecord)
	}
	// Order scopes are per (namespace, ordering scope), so both take order 1.
	if leftRecord.Order != 1 || rightRecord.Order != 1 {
		t.Fatalf("orders = %d and %d, want each scope to start independently", leftRecord.Order, rightRecord.Order)
	}
	// The due view is per namespace and does not leak across.
	dueLeft, _ := mustListDue(t, x, "sessions", 10, 10)
	if got, want := keysOf(dueLeft), []string{"host/shared"}; !equalStrings(got, want) || len(dueLeft) != 1 {
		t.Fatalf("ListDue(sessions) = %v, want exactly %v", got, want)
	}
	// A nested namespace becomes nested directories, and only the namespace does.
	nested := filepath.Join(root, orderedDirName, "factory", "hosts"+orderedLogExt)
	if _, serr := os.Stat(nested); serr != nil {
		t.Fatalf("stat %q: %v", nested, serr)
	}
}

// TestOrderedIndexCreateReportsACommittedFrameOnAFsyncFailure pins the one
// error case a caller must NOT read as "nothing happened". The frame is written
// and fsynced before the ancestor directory chain is made durable, so a failure
// in that last step leaves the create COMMITTED. Returning a zero record
// alongside the error would invite a retry that burned a second order for one
// identity; Create therefore returns the record it committed, and the retry
// takes the duplicate path.
func TestOrderedIndexCreateReportsACommittedFrameOnAFsyncFailure(t *testing.T) {
	t.Parallel()
	if os.Geteuid() == 0 {
		t.Skip("running as root: directory permissions are not enforced")
	}
	x, root := newTestOrderedIndex(t)
	ctx := context.Background()

	// A nested namespace, so the log lives one directory below <root>/ordered
	// and that directory's permissions can be adjusted independently.
	id := storage.OrderedID{Namespace: "factory/hosts", OrderingScope: "host", StableKey: "alpha"}
	leafDir := filepath.Join(root, orderedDirName, "factory")
	if merr := os.MkdirAll(leafDir, 0o700); merr != nil {
		t.Fatalf("mkdir %s: %v", leafDir, merr)
	}
	t.Cleanup(func() { _ = os.Chmod(leafDir, 0o700) })

	// Write+traverse only: the log can be created and written inside, but the
	// directory itself cannot be opened for the fsync.
	if cerr := os.Chmod(leafDir, 0o300); cerr != nil {
		t.Fatalf("chmod: %v", cerr)
	}
	record, created, err := x.Create(ctx, id, "catalog", []byte("committed"), ranked(5), dueAt(50))
	if err == nil {
		t.Fatalf("Create succeeded despite an unopenable directory: %+v", record)
	}
	var ioErr *OrderedLogIOError
	if !errors.As(err, &ioErr) || ioErr.Op != "open-dir" {
		t.Fatalf("Create error = %T %v, want an *OrderedLogIOError with op open-dir", err, err)
	}
	if !created || record.Order == 0 {
		t.Fatalf("Create returned created=%v order=%d with the error; the frame IS committed, "+
			"so erasing it would invite a retry that burns a second order", created, record.Order)
	}
	if record.Revision != 1 || !bytes.Equal(record.Value, []byte("committed")) {
		t.Fatalf("committed record = %+v, want the create candidate", record)
	}

	if cerr := os.Chmod(leafDir, 0o700); cerr != nil {
		t.Fatalf("chmod back: %v", cerr)
	}
	// The retry is answered by the duplicate path, with the SAME order.
	retried, retriedCreated, rerr := x.Create(ctx, id, "catalog", []byte("second attempt"), ranked(9), dueAt(90))
	if rerr != nil {
		t.Fatalf("Create retry: %v", rerr)
	}
	if retriedCreated {
		t.Fatal("Create retry reported created=true: the first attempt's frame was committed")
	}
	if retried.Order != record.Order || retried.Revision != 1 || !bytes.Equal(retried.Value, []byte("committed")) {
		t.Fatalf("Create retry = %+v, want the originally committed %+v", retried, record)
	}
	// And a cold handle reads the same thing off the disk, so the frame really
	// was durable and not merely remembered.
	if got, gerr := openTestOrderedIndex(t, root).Get(ctx, id); gerr != nil || got.Order != record.Order {
		t.Fatalf("cold Get = %+v (err %v), want the committed record %+v", got, gerr, record)
	}
}

// TestOrderedIndexDuplicateCreateIgnoresTheCandidate pins the ordering that
// makes Create genuinely idempotent: the duplicate lookup happens BEFORE the
// candidate is validated. Validating first would look harmless and quietly break
// the one case idempotency exists for — a retry after an unknown outcome, which
// may well carry a candidate the caller has since changed or a ranking scope a
// newer build no longer considers valid. Such a retry must still be answered
// with the record that exists.
func TestOrderedIndexDuplicateCreateIgnoresTheCandidate(t *testing.T) {
	t.Parallel()
	x, _ := newTestOrderedIndex(t)
	ctx := context.Background()
	id := orderedID("host", "idempotent")

	original := mustCreate(t, x, id, ranked(3), dueAt(30))

	// Every candidate field below is invalid on its own terms: an unusable
	// ranking scope, an oversize value, and a due state whose non-due form
	// carries a nonzero timestamp.
	oversize := make([]byte, storage.MaxOrderedValueBytes+1)
	duplicate, created, err := x.Create(ctx, id, "Not A Valid Scope!", oversize,
		ranked(99), storage.Due{State: storage.NotDue, UnixMillis: 5})
	if err != nil {
		t.Fatalf("duplicate Create with an invalid candidate = %v, want the original record", err)
	}
	if created {
		t.Fatal("duplicate Create reported created=true")
	}
	if duplicate.Order != original.Order || duplicate.Revision != original.Revision ||
		duplicate.Rank != original.Rank || duplicate.Due != original.Due ||
		duplicate.RankingScope != original.RankingScope || !bytes.Equal(duplicate.Value, original.Value) {
		t.Fatalf("duplicate Create returned %+v, want the original %+v", duplicate, original)
	}
	// And the stored record is untouched.
	if got, gerr := x.Get(ctx, id); gerr != nil || got.RankingScope != original.RankingScope ||
		!bytes.Equal(got.Value, original.Value) {
		t.Fatalf("after a duplicate Create, Get = %+v (err %v), want %+v", got, gerr, original)
	}

	// A tombstone is a duplicate too, on the same terms.
	victim := mustCreate(t, x, orderedID("host", "tombstoned"), ranked(1), dueAt(1))
	tombstone, err := x.Delete(ctx, victim.ID, victim.Revision)
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	again, created, err := x.Create(ctx, victim.ID, "Not A Valid Scope!", oversize, ranked(99), storage.Due{})
	if err != nil || created {
		t.Fatalf("Create over a tombstone = created=%v err=%v, want the tombstone", created, err)
	}
	if again.Revision != tombstone.Revision || !again.Deleted {
		t.Fatalf("Create over a tombstone returned %+v, want %+v", again, tombstone)
	}
}

// frameBounds walks data as a concatenation of frames and returns the byte
// offset of the START of each one, plus the offset just past the last. It is a
// test-only reader of the on-disk layout, used to damage a frame in the MIDDLE
// of a log rather than at its tail.
func frameBounds(t *testing.T, data []byte) []int {
	t.Helper()
	bounds := []int{0}
	off := 0
	for off < len(data) {
		_, _, n, err := decodeFrame(data[off:])
		if err != nil {
			t.Fatalf("decode frame at %d: %v", off, err)
		}
		off += n
		bounds = append(bounds, off)
	}
	return bounds
}

// TestOrderedMidFileZeroRegionFailsClosed pins the difference between a zero
// TAIL, which is an unwritten region a crash left behind and which recovery may
// truncate, and a zero region in the MIDDLE of a log, which is damage that
// destroyed an ACKNOWLEDGED mutation.
//
// The two are told apart by one thing only: whether the zero region runs to the
// end of the file. Without that check a hole punched anywhere — by a bad block,
// an overlay/NFS anomaly, or a partially-rewritten file — reads as end-of-stream,
// and because every read path may truncate, a single READ makes the loss
// permanent and frees the acceptance orders of the discarded records for reuse.
func TestOrderedMidFileZeroRegionFailsClosed(t *testing.T) {
	t.Parallel()
	x, root := newTestOrderedIndex(t)
	ctx := context.Background()
	keys := []storage.StableKey{"a", "b", "c", "d"}
	for _, key := range keys {
		if _, _, err := x.Create(ctx, orderedID("host", key), "catalog", []byte(key), storage.Rank{}, storage.Due{}); err != nil {
			t.Fatalf("Create(%q): %v", key, err)
		}
	}
	path := filepath.Join(root, orderedDirName, "sessions"+orderedLogExt)
	data := readFile(t, path)
	bounds := frameBounds(t, data)
	if len(bounds) != len(keys)+1 {
		t.Fatalf("log holds %d frames, want %d", len(bounds)-1, len(keys))
	}
	// Zero the THIRD frame in place: the file keeps its length, so the zero
	// region is provably not a tail.
	for i := bounds[2]; i < bounds[3]; i++ {
		data[i] = 0
	}
	writeFile(t, path, data)
	wantSize := int64(len(data))

	cold := openTestOrderedIndex(t, root)
	page, err := cold.ListOrdered(ctx, "sessions", "host", 0, 10)
	var corrupt *OrderedLogCorruptError
	if !errors.As(err, &corrupt) {
		t.Fatalf("ListOrdered over a mid-file zero region = (%d records, %v), want *OrderedLogCorruptError",
			len(page.Records), err)
	}
	// A READ must not destroy durable data: the file is exactly as it was.
	if size := fileSize(t, path); size != wantSize {
		t.Fatalf("log size after a failed read = %d, want %d: a read truncated the file", size, wantSize)
	}
	// And the failure is sticky rather than a one-off that later reads paper over.
	if _, again := cold.ListOrdered(ctx, "sessions", "host", 0, 10); !errors.As(again, &corrupt) {
		t.Fatalf("second read over a mid-file zero region = %v, want the same failure", again)
	}
	// The acceptance order of a discarded record must never be handed out again.
	if _, created, cerr := cold.Create(ctx, orderedID("host", "c"), "catalog", []byte("c"), storage.Rank{}, storage.Due{}); !errors.As(cerr, &corrupt) {
		t.Fatalf("Create over a mid-file zero region = (created %t, %v), want *OrderedLogCorruptError", created, cerr)
	}
}
