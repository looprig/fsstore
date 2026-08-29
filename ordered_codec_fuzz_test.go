package fsstore

import (
	"bytes"
	"encoding/hex"
	"errors"
	"testing"

	"github.com/looprig/storage"
)

// orderedFuzzSeeds returns byte seeds that already reach deep into the decoder:
// real encoded payloads (so the fuzzer mutates a structurally valid input rather
// than bouncing off the first length check), header-boundary lengths, and
// garbage. No seed carries a constant prefix that the decoder rejects up front —
// a fuzz target whose corpus cannot get past byte 0 tests nothing.
func orderedFuzzSeeds(t *testing.F) [][]byte {
	t.Helper()
	seeds := [][]byte{
		nil,
		{},
		make([]byte, orderedMutationHeaderSize-1),
		make([]byte, orderedMutationHeaderSize),
		bytes.Repeat([]byte{0xFF}, orderedMutationHeaderSize+8),
	}
	golden, err := hex.DecodeString(goldenOrderedPayloadHex)
	if err != nil {
		t.Fatalf("decode golden payload hex: %v", err)
	}
	seeds = append(seeds, golden)
	for _, m := range []orderedMutation{
		{Op: orderedOpCreate, OrderingScope: "s", StableKey: "k", RankingScope: "r", Revision: 1, Order: 1},
		{
			Op: orderedOpUpdate, OrderingScope: "a/b", StableKey: "slash/value", RankingScope: "r",
			Revision: 2, Order: 300,
			Due:  storage.Due{State: storage.DueAt, UnixMillis: -1},
			Rank: storage.Rank{Ranked: true, Value: -1}, Value: []byte("v"),
		},
		{
			Op: orderedOpDelete, OrderingScope: "s", StableKey: "Ünïcödé", RankingScope: "r",
			Revision: 9, Order: 1 << 33, Value: []byte("kept"),
		},
	} {
		encoded, encErr := encodeOrderedMutation(m)
		if encErr != nil {
			t.Fatalf("seed encode %+v: %v", m, encErr)
		}
		seeds = append(seeds, encoded)
	}
	return seeds
}

// FuzzOrderedMutationDecode drives the mutation decoder with arbitrary bytes. It
// must never panic, every rejection must be a typed *OrderedCodecError, and —
// the load-bearing property — every payload it ACCEPTS must be canonical: it
// re-encodes to exactly the input bytes and passes the storage validators. A
// decoder that admitted an aliased encoding (padding, an out-of-range enum, a
// length that does not account for every byte) would fail here.
func FuzzOrderedMutationDecode(f *testing.F) {
	for _, seed := range orderedFuzzSeeds(f) {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		m, err := decodeOrderedMutation(data)
		if err != nil {
			var ce *OrderedCodecError
			if !errors.As(err, &ce) {
				t.Fatalf("decode error not *OrderedCodecError: %T %v", err, err)
			}
			if ce.Fault == 0 {
				t.Fatalf("decode error carried no fault: %v", err)
			}
			return
		}
		if err := storage.ValidateOrderedID(storage.OrderedID{
			Namespace:     "ns",
			OrderingScope: m.OrderingScope,
			StableKey:     m.StableKey,
		}); err != nil {
			t.Fatalf("accepted a mutation with an invalid identity: %v", err)
		}
		if err := storage.ValidateName(m.RankingScope); err != nil {
			t.Fatalf("accepted a mutation with an invalid ranking scope: %v", err)
		}
		if err := storage.ValidateDue(m.Due); err != nil {
			t.Fatalf("accepted a mutation with an invalid due state: %v", err)
		}
		if m.Revision == 0 || m.Order == 0 {
			t.Fatalf("accepted a mutation with zero revision/order: %+v", m)
		}
		reencoded, encErr := encodeOrderedMutation(m)
		if encErr != nil {
			t.Fatalf("re-encode of a decoded mutation failed: %v", encErr)
		}
		if !bytes.Equal(reencoded, data) {
			t.Fatalf("decoded payload is not canonical:\n got %s\nwant %s",
				hex.EncodeToString(reencoded), hex.EncodeToString(data))
		}
	})
}

// FuzzOrderedFrameDecode drives the framed decoder, which layers the mutation
// codec on the repository's checksummed frame. Every rejection must be either a
// *FrameError (torn or corrupt, exactly one) or an *OrderedCodecError, and an
// accepted frame must report a bytesConsumed within the input.
func FuzzOrderedFrameDecode(f *testing.F) {
	for _, seed := range orderedFuzzSeeds(f) {
		frame, err := encodeFrame(1, seed)
		if err != nil {
			f.Fatalf("seed frame encode: %v", err)
		}
		f.Add(frame)
		f.Add(seed) // raw payload bytes: usually a bad frame header, still parsed
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		seq, m, n, err := decodeOrderedFrame(data)
		if err != nil {
			var fe *FrameError
			var ce *OrderedCodecError
			switch {
			case errors.As(err, &fe):
				if fe.IsTorn() == fe.IsCorrupt() {
					t.Fatalf("fault %d: IsTorn=%v IsCorrupt=%v not mutually exclusive",
						fe.Fault, fe.IsTorn(), fe.IsCorrupt())
				}
			case errors.As(err, &ce):
			default:
				t.Fatalf("frame decode error is neither *FrameError nor *OrderedCodecError: %T %v", err, err)
			}
			if n != 0 {
				t.Fatalf("bytesConsumed = %d on error, want 0", n)
			}
			return
		}
		if n < frameHeaderSize+orderedMutationHeaderSize || n > len(data) {
			t.Fatalf("bytesConsumed %d out of range [%d,%d]", n, frameHeaderSize+orderedMutationHeaderSize, len(data))
		}
		roundTrip, encErr := encodeOrderedFrame(seq, m)
		if encErr != nil {
			t.Fatalf("re-encode of a decoded frame failed: %v", encErr)
		}
		if !bytes.Equal(roundTrip, data[:n]) {
			t.Fatalf("decoded frame is not canonical:\n got %s\nwant %s",
				hex.EncodeToString(roundTrip), hex.EncodeToString(data[:n]))
		}
	})
}

// FuzzOrderedMutationRoundTrip drives encode→decode from arbitrary field values
// rather than arbitrary bytes, so it explores the accept side of the boundary:
// any mutation the encoder accepts must decode back to the identical value.
func FuzzOrderedMutationRoundTrip(f *testing.F) {
	f.Add(uint8(1), "s", "k", "r", uint64(1), uint64(1), uint8(0), int64(0), false, int64(0), []byte(nil))
	f.Add(uint8(2), "a/b", "slash/value", "cat", uint64(2), uint64(300), uint8(1), int64(-1), true, int64(-9), []byte("v"))
	f.Add(uint8(3), "sessions", "v1:ABC_def-09", "catalog", ^uint64(0), ^uint64(0), uint8(0), int64(0), false, int64(0), []byte("kept"))
	f.Add(uint8(9), "", "", "", uint64(0), uint64(0), uint8(7), int64(1), true, int64(1), []byte{0xFF})

	f.Fuzz(func(t *testing.T, op uint8, scope, key, ranking string,
		revision, order uint64, dueState uint8, millis int64, ranked bool, rankValue int64, value []byte) {
		m := orderedMutation{
			Op:            orderedOp(op),
			OrderingScope: scope,
			StableKey:     storage.StableKey(key),
			RankingScope:  ranking,
			Revision:      revision,
			Order:         order,
			Due:           storage.Due{State: storage.DueState(dueState), UnixMillis: millis},
			Rank:          storage.Rank{Ranked: ranked, Value: rankValue},
			Value:         value,
		}
		payload, err := encodeOrderedMutation(m)
		if err != nil {
			var ce *OrderedCodecError
			if !errors.As(err, &ce) {
				t.Fatalf("encode error not *OrderedCodecError: %T %v", err, err)
			}
			if payload != nil {
				t.Fatalf("encode returned %d bytes alongside an error", len(payload))
			}
			return
		}
		got, decErr := decodeOrderedMutation(payload)
		if decErr != nil {
			t.Fatalf("decode of a freshly encoded mutation: %v", decErr)
		}
		if !mutationsEqual(got, m) {
			t.Fatalf("round-trip mismatch:\n got %+v\nwant %+v", got, m)
		}
	})
}
