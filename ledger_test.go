package fsstore

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/ciram-co/storekit"
)

// ledgerFilePath returns the on-disk path the ledger backend maps name to, so a
// test can corrupt or truncate the underlying file directly.
func ledgerFilePath(root, name string) string {
	return filepath.Join(root, "streams", filepath.FromSlash(name)+".log")
}

// readAllRecords drains a cursor to exhaustion on the calling goroutine.
func readAllRecords(t *testing.T, l storekit.Ledger, name string, from uint64) []storekit.Record {
	t.Helper()
	ctx := context.Background()
	cur, err := l.Read(ctx, name, from)
	if err != nil {
		t.Fatalf("Read(%q, %d): %v", name, from, err)
	}
	defer func() {
		if cerr := cur.Close(); cerr != nil {
			t.Errorf("cursor Close: %v", cerr)
		}
	}()
	var out []storekit.Record
	for {
		rec, nerr := cur.Next(ctx)
		if errors.Is(nerr, io.EOF) {
			return out
		}
		if nerr != nil {
			t.Fatalf("cursor Next: %v", nerr)
		}
		out = append(out, rec)
	}
}

// frameLen is the on-disk width of a frame carrying a payload of the given length.
func frameLen(payloadLen int) int64 {
	return int64(frameHeaderSize + payloadLen)
}

func TestLedgerTornTailRecovery(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	const name = "sessions/torn"
	// Three records; frame 3 (seq 3) is the one truncated mid-write.
	p0 := []byte("first-rec")            // frame len 16+9 = 25
	p1 := []byte("second-record")        // frame len 16+13 = 29
	p2 := []byte("third-record-payload") // frame len 16+20 = 36
	prefix := frameLen(len(p0)) + frameLen(len(p1))

	cases := []struct {
		label string
		cut   int64 // truncate the file to this many bytes (inside frame 3)
	}{
		{label: "mid header", cut: prefix + 8},
		{label: "header complete, mid payload", cut: prefix + frameHeaderSize + 5},
		{label: "one payload byte short", cut: prefix + frameLen(len(p2)) - 1},
	}
	for _, tc := range cases {
		t.Run(tc.label, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			s, err := newLedgerStore(root)
			if err != nil {
				t.Fatalf("newLedgerStore: %v", err)
			}
			for i, p := range [][]byte{p0, p1, p2} {
				if err := s.Append(ctx, name, uint64(i), p); err != nil {
					t.Fatalf("seed Append %d: %v", i, err)
				}
			}

			// Simulate a crash mid-append to frame 3 by truncating the file.
			path := ledgerFilePath(root, name)
			if err := os.Truncate(path, tc.cut); err != nil {
				t.Fatalf("truncate: %v", err)
			}

			// A fresh store on the same root re-opens and must recover the tip to 2.
			s2, err := newLedgerStore(root)
			if err != nil {
				t.Fatalf("reopen newLedgerStore: %v", err)
			}
			tip, err := s2.Tip(ctx, name)
			if err != nil {
				t.Fatalf("Tip after torn tail: %v", err)
			}
			if tip != 2 {
				t.Fatalf("Tip after torn recovery = %d, want 2", tip)
			}

			// A stale writer at the OLD tip (3) must be rejected: recovery rolled back to 2.
			if err := s2.Append(ctx, name, 3, []byte("stale")); err == nil {
				t.Errorf("Append(expected=3) after recovery = nil, want *ConflictError")
			} else {
				var ce *storekit.ConflictError
				if !errors.As(err, &ce) {
					t.Errorf("Append(expected=3) = %v, want *ConflictError", err)
				}
			}

			// The correct next append (expected=2) lands as seq 3, replacing the torn frame.
			if err := s2.Append(ctx, name, 2, []byte("third-new")); err != nil {
				t.Fatalf("Append(expected=2) after recovery: %v", err)
			}
			recs := readAllRecords(t, s2, name, 1)
			want := [][]byte{p0, p1, []byte("third-new")}
			if len(recs) != len(want) {
				t.Fatalf("read %d records after recovery, want %d", len(recs), len(want))
			}
			for i, rec := range recs {
				if rec.Seq != uint64(i+1) {
					t.Errorf("record[%d].Seq = %d, want %d", i, rec.Seq, i+1)
				}
				if !bytes.Equal(rec.Payload, want[i]) {
					t.Errorf("record[%d].Payload = %q, want %q", i, rec.Payload, want[i])
				}
			}
		})
	}
}

func TestLedgerReopenPreservesTip(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	const name = "sessions/persist"
	cases := []struct {
		label string
		n     int
	}{
		{label: "one record", n: 1},
		{label: "few records", n: 3},
		{label: "many records", n: 25},
	}
	for _, tc := range cases {
		t.Run(tc.label, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			s, err := newLedgerStore(root)
			if err != nil {
				t.Fatalf("newLedgerStore: %v", err)
			}
			payloads := make([][]byte, tc.n)
			for i := 0; i < tc.n; i++ {
				payloads[i] = []byte("record-" + string(rune('a'+i%26)) + "-" + string(rune('0'+i%10)))
				if err := s.Append(ctx, name, uint64(i), payloads[i]); err != nil {
					t.Fatalf("Append %d: %v", i, err)
				}
			}

			// A fresh store on the same root observes the durably-persisted tip and data.
			s2, err := newLedgerStore(root)
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			tip, err := s2.Tip(ctx, name)
			if err != nil {
				t.Fatalf("Tip: %v", err)
			}
			if tip != uint64(tc.n) {
				t.Fatalf("reopened Tip = %d, want %d", tip, tc.n)
			}
			recs := readAllRecords(t, s2, name, 1)
			if len(recs) != tc.n {
				t.Fatalf("reopened read %d records, want %d", len(recs), tc.n)
			}
			for i, rec := range recs {
				if rec.Seq != uint64(i+1) {
					t.Errorf("record[%d].Seq = %d, want %d", i, rec.Seq, i+1)
				}
				if !bytes.Equal(rec.Payload, payloads[i]) {
					t.Errorf("record[%d].Payload = %q, want %q", i, rec.Payload, payloads[i])
				}
			}

			// The reopened store can continue the sequence.
			if err := s2.Append(ctx, name, uint64(tc.n), []byte("next")); err != nil {
				t.Fatalf("Append after reopen: %v", err)
			}
			if tip, err = s2.Tip(ctx, name); err != nil || tip != uint64(tc.n)+1 {
				t.Errorf("Tip after continued append = %d, %v; want %d, nil", tip, err, tc.n+1)
			}
		})
	}
}

func TestLedgerCorruptFailsClosed(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	const name = "sessions/corrupt"

	// crcFlip seeds a two-record ledger, then flips the first payload byte of
	// frame 1: every byte is present but the CRC no longer matches, so the frame
	// is Corrupt (not Torn) at seq 1 and must fail closed.
	crcFlip := func(t *testing.T) string {
		t.Helper()
		root := t.TempDir()
		s, err := newLedgerStore(root)
		if err != nil {
			t.Fatalf("newLedgerStore: %v", err)
		}
		if err := s.Append(ctx, name, 0, []byte("alpha")); err != nil {
			t.Fatalf("Append 0: %v", err)
		}
		if err := s.Append(ctx, name, 1, []byte("beta")); err != nil {
			t.Fatalf("Append 1: %v", err)
		}
		path := ledgerFilePath(root, name)
		data, err := os.ReadFile(path) //nolint:gosec // test-controlled path
		if err != nil {
			t.Fatalf("read file: %v", err)
		}
		data[frameHeaderSize] ^= 0xFF // flip the first payload byte of frame 1
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatalf("write file: %v", err)
		}
		return root
	}

	// seqGap writes two individually well-formed frames whose sequences skip from
	// 1 to 3. Each frame is valid, but the ledger is non-contiguous — a fault
	// truncation cannot repair — so the backend must fail closed at seq 3 rather
	// than silently drop the missing record.
	seqGap := func(t *testing.T) string {
		t.Helper()
		root := t.TempDir()
		if _, err := newLedgerStore(root); err != nil {
			t.Fatalf("newLedgerStore: %v", err)
		}
		f1, err := encodeFrame(1, []byte("one"))
		if err != nil {
			t.Fatalf("encodeFrame 1: %v", err)
		}
		f3, err := encodeFrame(3, []byte("three"))
		if err != nil {
			t.Fatalf("encodeFrame 3: %v", err)
		}
		path := ledgerFilePath(root, name)
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		data := append(append([]byte{}, f1...), f3...)
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatalf("write file: %v", err)
		}
		return root
	}

	corruptions := []struct {
		label         string
		build         func(t *testing.T) string
		wantSeq       uint64
		nonContiguous bool
	}{
		{label: "crc mismatch", build: crcFlip, wantSeq: 1, nonContiguous: false},
		{label: "sequence gap", build: seqGap, wantSeq: 3, nonContiguous: true},
	}
	ops := []struct {
		label string
		call  func(l storekit.Ledger) error
	}{
		{"Tip", func(l storekit.Ledger) error { _, err := l.Tip(ctx, name); return err }},
		{"Read", func(l storekit.Ledger) error { _, err := l.Read(ctx, name, 1); return err }},
		{"Append", func(l storekit.Ledger) error { return l.Append(ctx, name, 2, []byte("x")) }},
	}
	for _, c := range corruptions {
		for _, op := range ops {
			t.Run(c.label+"/"+op.label, func(t *testing.T) {
				t.Parallel()
				root := c.build(t)
				s, err := newLedgerStore(root)
				if err != nil {
					t.Fatalf("reopen: %v", err)
				}
				err = op.call(s)
				var lce *LedgerCorruptError
				if !errors.As(err, &lce) {
					t.Fatalf("%s on %s ledger = %v, want *LedgerCorruptError", op.label, c.label, err)
				}
				if lce.Seq != c.wantSeq {
					t.Errorf("LedgerCorruptError.Seq = %d, want %d", lce.Seq, c.wantSeq)
				}
				if c.nonContiguous && !errors.Is(err, errNonContiguousSeq) {
					t.Errorf("errors.Is(err, errNonContiguousSeq) = false, want true (err = %v)", err)
				}
			})
		}
	}
}

// TestRecordCursorSeqGapFailsClosed exercises the cursor's own seq-contiguity
// guard directly: given a snapshot whose frames skip from seq 1 to seq 3 but a tip
// claiming two records, Next must yield record 1 and then fail closed with a
// *LedgerCorruptError at seq 3 (errNonContiguousSeq) rather than hand back a
// record mislabeled with the cursor's expected sequence.
func TestRecordCursorSeqGapFailsClosed(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	f1, err := encodeFrame(1, []byte("one"))
	if err != nil {
		t.Fatalf("encodeFrame 1: %v", err)
	}
	f3, err := encodeFrame(3, []byte("three"))
	if err != nil {
		t.Fatalf("encodeFrame 3: %v", err)
	}
	data := append(append([]byte{}, f1...), f3...)
	c := &recordCursor{data: data, from: 1, tip: 2, nextSeq: 1, path: "test-ledger"}
	defer func() {
		if cerr := c.Close(); cerr != nil {
			t.Errorf("cursor Close: %v", cerr)
		}
	}()

	rec, err := c.Next(ctx)
	if err != nil {
		t.Fatalf("first Next: %v", err)
	}
	if rec.Seq != 1 {
		t.Errorf("first record Seq = %d, want 1", rec.Seq)
	}
	_, err = c.Next(ctx)
	var lce *LedgerCorruptError
	if !errors.As(err, &lce) {
		t.Fatalf("second Next = %v, want *LedgerCorruptError", err)
	}
	if lce.Seq != 3 {
		t.Errorf("LedgerCorruptError.Seq = %d, want 3", lce.Seq)
	}
	if !errors.Is(err, errNonContiguousSeq) {
		t.Errorf("errors.Is(err, errNonContiguousSeq) = false, want true (err = %v)", err)
	}
}

func TestLedgerNamePathMapping(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cases := []struct {
		label string
		name  string
		rel   []string // path segments under streams, last one gets ".log"
	}{
		{label: "single segment", name: "solo", rel: []string{"solo"}},
		{label: "two segments", name: "a/b", rel: []string{"a", "b"}},
		{label: "deep nesting", name: "deep/nested/name", rel: []string{"deep", "nested", "name"}},
	}
	for _, tc := range cases {
		t.Run(tc.label, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			s, err := newLedgerStore(root)
			if err != nil {
				t.Fatalf("newLedgerStore: %v", err)
			}
			if err := s.Append(ctx, tc.name, 0, []byte("p")); err != nil {
				t.Fatalf("Append: %v", err)
			}
			want := filepath.Join(append([]string{root, "streams"}, tc.rel...)...) + ".log"
			if _, err := os.Stat(want); err != nil {
				t.Fatalf("expected ledger file at %q: %v", want, err)
			}
			// pathFor maps to exactly that location.
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

func TestLedgerPathContainment(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	s, err := newLedgerStore(root)
	if err != nil {
		t.Fatalf("newLedgerStore: %v", err)
	}
	// These names would escape <root>/streams once joined; the containment guard
	// must reject them with a typed error even though ValidateName would also
	// reject most at the public boundary (defense in depth).
	cases := []struct {
		label string
		name  string
	}{
		{label: "empty maps to streams root", name: ""},
		{label: "parent traversal", name: ".."},
		{label: "escaping traversal", name: "../escape"},
		{label: "deep escape", name: "../../etc/passwd"},
		{label: "interior traversal", name: "a/../../escape"},
	}
	for _, tc := range cases {
		t.Run(tc.label, func(t *testing.T) {
			t.Parallel()
			got, err := s.pathFor(tc.name)
			var lpe *LedgerPathError
			if !errors.As(err, &lpe) {
				t.Fatalf("pathFor(%q) = (%q, %v), want *LedgerPathError", tc.name, got, err)
			}
			if got != "" {
				t.Errorf("pathFor(%q) path = %q, want empty on rejection", tc.name, got)
			}
		})
	}
}

func TestNewLedgerStoreValidation(t *testing.T) {
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
			s, err := newLedgerStore(tc.root(t))
			if (err != nil) != tc.wantErr {
				t.Fatalf("newLedgerStore err = %v, wantErr %v", err, tc.wantErr)
			}
			if tc.wantErr {
				var lre *LedgerRootError
				if !errors.As(err, &lre) {
					t.Errorf("want *LedgerRootError, got %T: %v", err, err)
				}
				return
			}
			// The streams directory must exist at 0700.
			info, serr := os.Stat(filepath.Join(s.root, "streams"))
			if serr != nil {
				t.Fatalf("stat streams: %v", serr)
			}
			if !info.IsDir() {
				t.Errorf("streams is not a directory")
			}
			if perm := info.Mode().Perm(); perm != 0o700 {
				t.Errorf("streams perm = %o, want 0700", perm)
			}
		})
	}
}
