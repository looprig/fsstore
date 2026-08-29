package fsstore

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"strings"
	"testing"

	"github.com/looprig/storage"
)

// goldenOrderedMutation is the fixed mutation whose on-disk bytes are pinned by
// goldenOrderedPayloadHex and goldenOrderedFrameHex. It exercises every field
// class the layout carries: a multi-byte opaque StableKey with punctuation and
// uppercase, a nonzero sparse order, a DueAt state, a NEGATIVE rank (so the
// signed encoding is pinned, not just small positives), and a nonempty value.
func goldenOrderedMutation() orderedMutation {
	return orderedMutation{
		Op:            orderedOpCreate,
		OrderingScope: "sessions",
		StableKey:     storage.StableKey("v1:ABC_def-09"),
		RankingScope:  "catalog",
		Revision:      1,
		Order:         12,
		Due:           storage.Due{State: storage.DueAt, UnixMillis: 1735689600000},
		Rank:          storage.Rank{Ranked: true, Value: -5},
		Value:         []byte("hello"),
	}
}

// goldenOrderedPayloadHex is the byte-exact encoding of goldenOrderedMutation.
// It was computed OUTSIDE this package from the documented layout in
// ordered_codec.go, not captured from encodeOrderedMutation's own output, so a
// silent layout change (field order, endianness, width, signedness) fails this
// test instead of moving the goalposts with the implementation.
const goldenOrderedPayloadHex = "0101010101000000000000000c00000000000000007c291f94010000fbffffffffffffff08000d0007000500000073657373696f6e7376313a4142435f6465662d3039636174616c6f6768656c6c6f"

// goldenOrderedFrameHex is goldenOrderedPayloadHex wrapped in the repository's
// checksummed frame at sequence 7: [len u32][crc32c u32][seq u64][payload].
// The CRC was likewise computed independently of the codec's own table.
const goldenOrderedFrameHex = "4f00000055031e2007000000000000000101010101000000000000000c00000000000000007c291f94010000fbffffffffffffff08000d0007000500000073657373696f6e7376313a4142435f6465662d3039636174616c6f6768656c6c6f"

// goldenOrderedFrameSeq is the sequence pinned inside goldenOrderedFrameHex.
const goldenOrderedFrameSeq uint64 = 7

func mustHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatalf("decode golden hex: %v", err)
	}
	return b
}

// mutationsEqual compares two mutations field by field. Value is compared with
// bytes.Equal so a nil and an empty slice are the same wire value (the codec
// stores only a length, so it cannot distinguish them).
func mutationsEqual(a, b orderedMutation) bool {
	return a.Op == b.Op &&
		a.OrderingScope == b.OrderingScope &&
		a.StableKey == b.StableKey &&
		a.RankingScope == b.RankingScope &&
		a.Revision == b.Revision &&
		a.Order == b.Order &&
		a.Due == b.Due &&
		a.Rank == b.Rank &&
		bytes.Equal(a.Value, b.Value)
}

func TestEncodeOrderedMutationGolden(t *testing.T) {
	t.Parallel()
	want := mustHex(t, goldenOrderedPayloadHex)
	got, err := encodeOrderedMutation(goldenOrderedMutation())
	if err != nil {
		t.Fatalf("encodeOrderedMutation: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("golden payload mismatch:\n got %s\nwant %s", hex.EncodeToString(got), goldenOrderedPayloadHex)
	}
}

func TestDecodeOrderedMutationGolden(t *testing.T) {
	t.Parallel()
	want := goldenOrderedMutation()
	got, err := decodeOrderedMutation(mustHex(t, goldenOrderedPayloadHex))
	if err != nil {
		t.Fatalf("decodeOrderedMutation: %v", err)
	}
	if !mutationsEqual(got, want) {
		t.Fatalf("golden decode mismatch:\n got %+v\nwant %+v", got, want)
	}
}

func TestOrderedFrameGolden(t *testing.T) {
	t.Parallel()
	wantFrame := mustHex(t, goldenOrderedFrameHex)
	gotFrame, err := encodeOrderedFrame(goldenOrderedFrameSeq, goldenOrderedMutation())
	if err != nil {
		t.Fatalf("encodeOrderedFrame: %v", err)
	}
	if !bytes.Equal(gotFrame, wantFrame) {
		t.Fatalf("golden frame mismatch:\n got %s\nwant %s", hex.EncodeToString(gotFrame), goldenOrderedFrameHex)
	}
	seq, got, n, derr := decodeOrderedFrame(wantFrame)
	if derr != nil {
		t.Fatalf("decodeOrderedFrame: %v", derr)
	}
	if seq != goldenOrderedFrameSeq {
		t.Errorf("seq = %d, want %d", seq, goldenOrderedFrameSeq)
	}
	if n != len(wantFrame) {
		t.Errorf("bytesConsumed = %d, want %d", n, len(wantFrame))
	}
	if !mutationsEqual(got, goldenOrderedMutation()) {
		t.Errorf("frame decode mismatch:\n got %+v\nwant %+v", got, goldenOrderedMutation())
	}
}

func TestOrderedMutationRoundTrip(t *testing.T) {
	t.Parallel()
	maxName := strings.Repeat("a", 512)
	maxKey := storage.StableKey(strings.Repeat("k", storage.MaxStableKeyBytes))
	tests := []struct {
		name string
		m    orderedMutation
	}{
		{
			name: "create with nil value",
			m: orderedMutation{
				Op: orderedOpCreate, OrderingScope: "s", StableKey: "k", RankingScope: "r",
				Revision: 1, Order: 1,
			},
		},
		{
			name: "update with max value",
			m: orderedMutation{
				Op: orderedOpUpdate, OrderingScope: "a/b/c", StableKey: "slash/value", RankingScope: "cat",
				Revision: 2, Order: 9,
				Due:   storage.Due{State: storage.DueAt, UnixMillis: -1},
				Rank:  storage.Rank{Ranked: true, Value: 1 << 62},
				Value: bytes.Repeat([]byte{0xEE}, storage.MaxOrderedValueBytes),
			},
		},
		{
			name: "delete tombstone preserves value",
			m: orderedMutation{
				Op: orderedOpDelete, OrderingScope: "s", StableKey: "Ünïcödé-Ключ", RankingScope: "r",
				Revision: 3, Order: 1 << 40, Value: []byte("kept"),
			},
		},
		{
			name: "max-length names and key",
			m: orderedMutation{
				Op: orderedOpCreate, OrderingScope: maxName, StableKey: maxKey, RankingScope: maxName,
				Revision: ^uint64(0), Order: ^uint64(0),
				Due:  storage.Due{State: storage.DueAt, UnixMillis: -1 << 62},
				Rank: storage.Rank{Ranked: true, Value: -1 << 62},
			},
		},
		{
			name: "unranked with zero rank value",
			m: orderedMutation{
				Op: orderedOpUpdate, OrderingScope: "s", StableKey: "k", RankingScope: "r",
				Revision: 4, Order: 4, Value: []byte{},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			payload, err := encodeOrderedMutation(tt.m)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			got, derr := decodeOrderedMutation(payload)
			if derr != nil {
				t.Fatalf("decode: %v", derr)
			}
			if !mutationsEqual(got, tt.m) {
				t.Fatalf("round-trip mismatch:\n got %+v\nwant %+v", got, tt.m)
			}
		})
	}
}

// TestDecodeOrderedMutationTruncated feeds every strict prefix of the golden
// payload to the decoder. A frame's CRC has already proved the bytes are all
// present by the time a payload is decoded, so a short payload is corruption,
// never a recoverable torn tail: the codec must fail closed on every prefix.
func TestDecodeOrderedMutationTruncated(t *testing.T) {
	t.Parallel()
	full := mustHex(t, goldenOrderedPayloadHex)
	for cut := 0; cut < len(full); cut++ {
		if _, err := decodeOrderedMutation(full[:cut]); err == nil {
			t.Fatalf("prefix of %d bytes decoded, want error", cut)
		} else {
			var ce *OrderedCodecError
			if !errors.As(err, &ce) {
				t.Fatalf("prefix %d: want *OrderedCodecError, got %T: %v", cut, err, err)
			}
			if cut < orderedMutationHeaderSize && ce.Fault != OrderedFaultShortPayload {
				t.Errorf("prefix %d: fault = %d, want OrderedFaultShortPayload", cut, ce.Fault)
			}
		}
	}
}

// TestDecodeOrderedFrameChecksum corrupts a payload byte inside an otherwise
// well-formed frame. The outer frame checksum must catch it before the mutation
// decoder ever sees the bytes.
func TestDecodeOrderedFrameChecksum(t *testing.T) {
	t.Parallel()
	frame := mustHex(t, goldenOrderedFrameHex)
	for _, off := range []int{frameHeaderSize, frameHeaderSize + 20, len(frame) - 1} {
		bad := append([]byte(nil), frame...)
		bad[off] ^= 0xFF
		_, _, _, err := decodeOrderedFrame(bad)
		var fe *FrameError
		if !errors.As(err, &fe) {
			t.Fatalf("offset %d: want *FrameError, got %T: %v", off, err, err)
		}
		if !fe.IsCorrupt() || fe.Fault != FaultCRCMismatch {
			t.Errorf("offset %d: fault = %d IsCorrupt = %v, want CRC mismatch", off, fe.Fault, fe.IsCorrupt())
		}
	}
	// A truncated frame is a torn tail, which the log recovers by truncation —
	// a materially different outcome from the corruption above.
	_, _, _, err := decodeOrderedFrame(frame[:len(frame)-1])
	var fe *FrameError
	if !errors.As(err, &fe) || !fe.IsTorn() {
		t.Fatalf("truncated frame: want torn *FrameError, got %T: %v", err, err)
	}
}

// mutateGoldenPayload returns the golden payload with a single mutator applied,
// so each malformed-input case differs from a KNOWN-GOOD encoding by exactly the
// field under test.
func mutateGoldenPayload(t *testing.T, mutate func(p []byte) []byte) []byte {
	t.Helper()
	return mutate(mustHex(t, goldenOrderedPayloadHex))
}

func TestDecodeOrderedMutationRejects(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		mutate  func(p []byte) []byte
		want    OrderedCodecFault
		wantErr error // optional inner cause matched with errors.As on a target type
	}{
		{
			name:   "unknown version",
			mutate: func(p []byte) []byte { p[0] = 2; return p },
			want:   OrderedFaultUnknownVersion,
		},
		{
			name:   "version zero",
			mutate: func(p []byte) []byte { p[0] = 0; return p },
			want:   OrderedFaultUnknownVersion,
		},
		{
			name:   "unknown op",
			mutate: func(p []byte) []byte { p[1] = 4; return p },
			want:   OrderedFaultUnknownOp,
		},
		{
			name:   "op zero",
			mutate: func(p []byte) []byte { p[1] = 0; return p },
			want:   OrderedFaultUnknownOp,
		},
		{
			name: "oversized ordering scope length",
			mutate: func(p []byte) []byte {
				binary.LittleEndian.PutUint16(p[36:38], uint16(maxOrderedNameBytes+1))
				return p
			},
			want: OrderedFaultFieldOversize,
		},
		{
			name: "oversized stable key length",
			mutate: func(p []byte) []byte {
				binary.LittleEndian.PutUint16(p[38:40], uint16(storage.MaxStableKeyBytes+1))
				return p
			},
			want: OrderedFaultFieldOversize,
		},
		{
			name: "oversized ranking scope length",
			mutate: func(p []byte) []byte {
				binary.LittleEndian.PutUint16(p[40:42], 0xFFFF)
				return p
			},
			want: OrderedFaultFieldOversize,
		},
		{
			name: "oversized value length",
			mutate: func(p []byte) []byte {
				binary.LittleEndian.PutUint32(p[42:46], storage.MaxOrderedValueBytes+1)
				return p
			},
			want: OrderedFaultFieldOversize,
		},
		{
			name: "declared bodies exceed payload",
			mutate: func(p []byte) []byte {
				binary.LittleEndian.PutUint32(p[42:46], 4096)
				return p
			},
			want: OrderedFaultShortPayload,
		},
		{
			name:   "trailing bytes after declared fields",
			mutate: func(p []byte) []byte { return append(p, 0x00) },
			want:   OrderedFaultTrailingBytes,
		},
		{
			name: "empty stable key",
			mutate: func(p []byte) []byte {
				// Drop the 13 stable-key bytes and declare length 0.
				out := append([]byte(nil), p[:orderedMutationHeaderSize+8]...)
				out = append(out, p[orderedMutationHeaderSize+8+13:]...)
				binary.LittleEndian.PutUint16(out[38:40], 0)
				return out
			},
			want: OrderedFaultInvalidIdentity,
		},
		{
			name: "stable key is invalid UTF-8",
			mutate: func(p []byte) []byte {
				p[orderedMutationHeaderSize+8] = 0xFF
				return p
			},
			want: OrderedFaultInvalidIdentity,
		},
		{
			name: "ordering scope violates the name grammar",
			mutate: func(p []byte) []byte {
				p[orderedMutationHeaderSize] = 'S' // uppercase is not a legal segment byte
				return p
			},
			want: OrderedFaultInvalidIdentity,
		},
		{
			name: "ranking scope violates the name grammar",
			mutate: func(p []byte) []byte {
				p[orderedMutationHeaderSize+8+13] = '/' // leading '/' is illegal
				return p
			},
			want: OrderedFaultInvalidRecord,
		},
		{
			name:   "zero revision",
			mutate: func(p []byte) []byte { binary.LittleEndian.PutUint64(p[4:12], 0); return p },
			want:   OrderedFaultInvalidRecord,
		},
		{
			name:   "zero order",
			mutate: func(p []byte) []byte { binary.LittleEndian.PutUint64(p[12:20], 0); return p },
			want:   OrderedFaultInvalidRecord,
		},
		{
			name:   "unknown due state",
			mutate: func(p []byte) []byte { p[2] = 9; return p },
			want:   OrderedFaultInvalidRecord,
		},
		{
			name: "not-due with nonzero millis",
			mutate: func(p []byte) []byte {
				p[2] = 0 // NotDue, but the golden millis are nonzero
				return p
			},
			want: OrderedFaultInvalidRecord,
		},
		{
			name:   "non-boolean rank flag",
			mutate: func(p []byte) []byte { p[3] = 2; return p },
			want:   OrderedFaultInvalidRecord,
		},
		{
			name: "tombstone carrying a rank",
			mutate: func(p []byte) []byte {
				p[1] = byte(orderedOpDelete)
				p[2] = 0
				binary.LittleEndian.PutUint64(p[20:28], 0) // canonical not-due
				return p                                   // rank flag still set
			},
			want: OrderedFaultInvalidRecord,
		},
		{
			name: "tombstone carrying a due time",
			mutate: func(p []byte) []byte {
				p[1] = byte(orderedOpDelete)
				p[3] = 0
				binary.LittleEndian.PutUint64(p[28:36], 0) // canonical unranked
				return p                                   // due state still DueAt
			},
			want: OrderedFaultInvalidRecord,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			payload := mutateGoldenPayload(t, tt.mutate)
			got, err := decodeOrderedMutation(payload)
			if err == nil {
				t.Fatalf("decoded a malformed payload as %+v, want error", got)
			}
			var ce *OrderedCodecError
			if !errors.As(err, &ce) {
				t.Fatalf("want *OrderedCodecError, got %T: %v", err, err)
			}
			if ce.Fault != tt.want {
				t.Fatalf("fault = %d, want %d (err: %v)", ce.Fault, tt.want, err)
			}
			if got.Op != 0 || got.Value != nil {
				t.Errorf("mutation = %+v, want zero value on error", got)
			}
		})
	}
}

// TestEncodeOrderedMutationRejects proves the encoder is the same gate as the
// decoder: an invalid mutation can never reach the disk in the first place.
func TestEncodeOrderedMutationRejects(t *testing.T) {
	t.Parallel()
	valid := goldenOrderedMutation()
	tests := []struct {
		name string
		m    orderedMutation
		want OrderedCodecFault
	}{
		{
			name: "unknown op",
			m:    func() orderedMutation { m := valid; m.Op = 9; return m }(),
			want: OrderedFaultUnknownOp,
		},
		{
			name: "empty ordering scope",
			m:    func() orderedMutation { m := valid; m.OrderingScope = ""; return m }(),
			want: OrderedFaultInvalidIdentity,
		},
		{
			name: "stable key too long",
			m: func() orderedMutation {
				m := valid
				m.StableKey = storage.StableKey(strings.Repeat("k", storage.MaxStableKeyBytes+1))
				return m
			}(),
			want: OrderedFaultInvalidIdentity,
		},
		{
			name: "stable key with invalid UTF-8",
			m: func() orderedMutation {
				m := valid
				m.StableKey = storage.StableKey([]byte{0xFF, 0xFE})
				return m
			}(),
			want: OrderedFaultInvalidIdentity,
		},
		{
			name: "empty ranking scope",
			m:    func() orderedMutation { m := valid; m.RankingScope = ""; return m }(),
			want: OrderedFaultInvalidRecord,
		},
		{
			name: "zero revision",
			m:    func() orderedMutation { m := valid; m.Revision = 0; return m }(),
			want: OrderedFaultInvalidRecord,
		},
		{
			name: "zero order",
			m:    func() orderedMutation { m := valid; m.Order = 0; return m }(),
			want: OrderedFaultInvalidRecord,
		},
		{
			name: "value over the ordered ceiling",
			m: func() orderedMutation {
				m := valid
				m.Value = make([]byte, storage.MaxOrderedValueBytes+1)
				return m
			}(),
			want: OrderedFaultFieldOversize,
		},
		{
			name: "not-due with nonzero millis",
			m: func() orderedMutation {
				m := valid
				m.Due = storage.Due{State: storage.NotDue, UnixMillis: 5}
				return m
			}(),
			want: OrderedFaultInvalidRecord,
		},
		{
			name: "ranked tombstone",
			m: func() orderedMutation {
				m := valid
				m.Op = orderedOpDelete
				m.Due = storage.Due{}
				return m
			}(),
			want: OrderedFaultInvalidRecord,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			out, err := encodeOrderedMutation(tt.m)
			if err == nil {
				t.Fatalf("encoded an invalid mutation into %d bytes, want error", len(out))
			}
			if out != nil {
				t.Errorf("payload = %v, want nil on error", out)
			}
			var ce *OrderedCodecError
			if !errors.As(err, &ce) {
				t.Fatalf("want *OrderedCodecError, got %T: %v", err, err)
			}
			if ce.Fault != tt.want {
				t.Fatalf("fault = %d, want %d (err: %v)", ce.Fault, tt.want, err)
			}
		})
	}
}

// hostileKeyMarker is embedded in the StableKey the classification test feeds
// the codec on the identity fault — the only fault a hostile key can reach,
// since identity is validated before every other field. No error message may
// echo it: a StableKey is arbitrary user data (credentials, personal data, an
// injection payload) and logging it would leak.
const hostileKeyMarker = "HOSTILE-KEY-\u00dcn\u00efc\u00f6d\u00e9"

// TestOrderedCodecErrorClassification pins the error surface: EVERY fault has a
// distinct, non-empty, fsstore-prefixed message that never embeds payload bytes,
// and Unwrap exposes the underlying storage validation error where one exists.
func TestOrderedCodecErrorClassification(t *testing.T) {
	t.Parallel()

	// A key long enough to be rejected, carrying the hostile marker.
	hostileKey := storage.StableKey(hostileKeyMarker + strings.Repeat("z", storage.MaxStableKeyBytes))
	goldenPayload := mustHex(t, goldenOrderedPayloadHex)
	withByte := func(off int, b byte) []byte {
		out := append([]byte{}, goldenPayload...)
		out[off] = b
		return out
	}

	tests := []struct {
		name  string
		fault OrderedCodecFault
		// err produces the error through the REAL codec, so the table cannot
		// drift from what a caller actually sees.
		err func(t *testing.T) error
		// unwrapsStorage asserts a storage validation error is reachable.
		unwrapsStorage bool
	}{
		{
			name:  "short payload",
			fault: OrderedFaultShortPayload,
			err: func(t *testing.T) error {
				_, err := decodeOrderedMutation(make([]byte, orderedMutationHeaderSize-1))
				return err
			},
		},
		{
			name:  "unknown version",
			fault: OrderedFaultUnknownVersion,
			err: func(t *testing.T) error {
				_, err := decodeOrderedMutation(withByte(offOrderedVersion, orderedMutationVersion+1))
				return err
			},
		},
		{
			name:  "unknown op",
			fault: OrderedFaultUnknownOp,
			err: func(t *testing.T) error {
				_, err := decodeOrderedMutation(withByte(offOrderedOp, 9))
				return err
			},
		},
		{
			name:  "field oversize",
			fault: OrderedFaultFieldOversize,
			err: func(t *testing.T) error {
				// Identity is validated FIRST, so this case keeps a legal key:
				// a hostile one would short-circuit to an identity fault.
				m := goldenOrderedMutation()
				m.Value = make([]byte, storage.MaxOrderedValueBytes+1)
				_, err := encodeOrderedMutation(m)
				return err
			},
			unwrapsStorage: true,
		},
		{
			name:  "trailing bytes",
			fault: OrderedFaultTrailingBytes,
			err: func(t *testing.T) error {
				_, err := decodeOrderedMutation(append(append([]byte{}, goldenPayload...), 0x00))
				return err
			},
		},
		{
			name:  "invalid identity",
			fault: OrderedFaultInvalidIdentity,
			err: func(t *testing.T) error {
				m := goldenOrderedMutation()
				m.StableKey = hostileKey
				_, err := encodeOrderedMutation(m)
				return err
			},
			unwrapsStorage: true,
		},
		{
			name:  "invalid record",
			fault: OrderedFaultInvalidRecord,
			err: func(t *testing.T) error {
				m := goldenOrderedMutation()
				m.Revision = 0
				_, err := encodeOrderedMutation(m)
				return err
			},
		},
	}

	seen := make(map[string]string, len(tests)+1)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.err(t)
			var ce *OrderedCodecError
			if !errors.As(err, &ce) {
				t.Fatalf("want *OrderedCodecError, got %T: %v", err, err)
			}
			if ce.Fault != tt.fault {
				t.Fatalf("fault = %d, want %d", ce.Fault, tt.fault)
			}
			msg := ce.Error()
			if msg == "" {
				t.Fatal("empty message")
			}
			if !strings.HasPrefix(msg, "fsstore: ordered mutation ") {
				t.Errorf("Error() = %q, want an fsstore-prefixed ordered-mutation message", msg)
			}
			if strings.Contains(msg, hostileKeyMarker) {
				t.Errorf("Error() = %q leaks stable key bytes", msg)
			}
			if tt.unwrapsStorage && errors.Unwrap(ce) == nil {
				t.Error("fault carried no unwrappable storage validation error")
			}
			// Distinctness is asserted serially, so no subtest runs in parallel
			// here; the table itself is cheap.
			if prior, dup := seen[msg]; dup {
				t.Errorf("message %q is shared with %q", msg, prior)
			}
			seen[msg] = tt.name
		})
	}

	// The empty StableKey is the one identity fault whose cause is a
	// *storage.InvalidStableKeyError; pin that it stays unwrappable.
	m := goldenOrderedMutation()
	m.StableKey = ""
	_, err := encodeOrderedMutation(m)
	var ike *storage.InvalidStableKeyError
	if !errors.As(err, &ike) {
		t.Fatalf("want an unwrappable *storage.InvalidStableKeyError, got %v", err)
	}

	unknown := &OrderedCodecError{Fault: OrderedCodecFault(200)}
	if unknown.Error() == "" {
		t.Error("unknown fault produced an empty message")
	}
	if prior, dup := seen[unknown.Error()]; dup {
		t.Errorf("unknown-fault message is shared with %q", prior)
	}
}

// TestOrderedCodecSlicesAreCallerOwned pins a FROZEN CONTRACT clause: "returned
// slices are caller-owned". The encoder must not retain the caller's Value, and
// the decoder must not alias the payload buffer it was handed. Aliasing would be
// invisible in normal use and catastrophic in the log's actual use, where one
// mmap-sized read buffer backs every frame in the file: scribbling or reusing
// that buffer would silently rewrite already-decoded mutations.
func TestOrderedCodecSlicesAreCallerOwned(t *testing.T) {
	t.Parallel()

	t.Run("encode does not retain the caller's value", func(t *testing.T) {
		t.Parallel()
		m := goldenOrderedMutation()
		m.Value = []byte("hello")
		payload, err := encodeOrderedMutation(m)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		frozen := append([]byte{}, payload...)
		for i := range m.Value {
			m.Value[i] = 0xFF // the caller reuses its buffer
		}
		if !bytes.Equal(payload, frozen) {
			t.Errorf("mutating the caller's Value changed the encoded payload:\n got %s\nwant %s",
				hex.EncodeToString(payload), hex.EncodeToString(frozen))
		}
	})

	t.Run("decode does not alias the payload buffer", func(t *testing.T) {
		t.Parallel()
		payload := mustHex(t, goldenOrderedPayloadHex)
		got, err := decodeOrderedMutation(payload)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		want := goldenOrderedMutation()
		for i := range payload {
			payload[i] = 0xFF // the caller reuses or truncates its buffer
		}
		if !mutationsEqual(got, want) {
			t.Errorf("scribbling the payload changed the decoded mutation:\n got %+v\nwant %+v", got, want)
		}
	})

	t.Run("frame decode does not alias the frame buffer", func(t *testing.T) {
		t.Parallel()
		frame := mustHex(t, goldenOrderedFrameHex)
		seq, got, n, err := decodeOrderedFrame(frame)
		if err != nil {
			t.Fatalf("decode frame: %v", err)
		}
		if seq != goldenOrderedFrameSeq || n != len(frame) {
			t.Fatalf("seq/bytesConsumed = %d/%d, want %d/%d", seq, n, goldenOrderedFrameSeq, len(frame))
		}
		want := goldenOrderedMutation()
		for i := range frame {
			frame[i] = 0xFF
		}
		if !mutationsEqual(got, want) {
			t.Errorf("scribbling the frame changed the decoded mutation:\n got %+v\nwant %+v", got, want)
		}
	})
}

// TestOrderedRankCanonicality pins the encoding's canonicality claim on the one
// field that could otherwise alias: an UNRANKED record's Value is meaningless
// (storage says a record with Ranked false is absent from ListRanked), so
// Rank{Ranked:false, Value:7} and Rank{} must be the same wire value. Two byte
// sequences meaning "unranked" would break "each field has exactly one
// representation" — and the stray value would surface later in a ranked
// comparator that reads Value without consulting Ranked.
func TestOrderedRankCanonicality(t *testing.T) {
	t.Parallel()

	t.Run("encode normalizes an unranked value to zero", func(t *testing.T) {
		t.Parallel()
		zero := goldenOrderedMutation()
		zero.Rank = storage.Rank{Ranked: false, Value: 0}
		stray := goldenOrderedMutation()
		stray.Rank = storage.Rank{Ranked: false, Value: 7}
		a, err := encodeOrderedMutation(zero)
		if err != nil {
			t.Fatalf("encode canonical: %v", err)
		}
		b, err := encodeOrderedMutation(stray)
		if err != nil {
			t.Fatalf("encode stray: %v", err)
		}
		if !bytes.Equal(a, b) {
			t.Errorf("unranked rank values encode differently:\n zero  %s\n stray %s",
				hex.EncodeToString(a), hex.EncodeToString(b))
		}
		got, derr := decodeOrderedMutation(b)
		if derr != nil {
			t.Fatalf("decode: %v", derr)
		}
		if got.Rank != (storage.Rank{}) {
			t.Errorf("decoded rank = %+v, want the zero Rank", got.Rank)
		}
	})

	t.Run("decode rejects an unranked frame carrying a nonzero value", func(t *testing.T) {
		t.Parallel()
		payload := mustHex(t, goldenOrderedPayloadHex)
		payload[offOrderedRankFlag] = orderedRankFlagUnranked
		binary.LittleEndian.PutUint64(payload[offOrderedRankValue:], 7)
		_, err := decodeOrderedMutation(payload)
		var ce *OrderedCodecError
		if !errors.As(err, &ce) {
			t.Fatalf("want *OrderedCodecError, got %T: %v", err, err)
		}
		if ce.Fault != OrderedFaultInvalidRecord {
			t.Errorf("fault = %d, want OrderedFaultInvalidRecord (%d)", ce.Fault, OrderedFaultInvalidRecord)
		}
	})

	t.Run("an unranked tombstone with a stray value is still a legal tombstone", func(t *testing.T) {
		t.Parallel()
		m := goldenOrderedMutation()
		m.Op = orderedOpDelete
		m.Due = storage.Due{}
		m.Rank = storage.Rank{Ranked: false, Value: 7}
		if _, err := encodeOrderedMutation(m); err != nil {
			t.Fatalf("encode: %v", err)
		}
	})
}
