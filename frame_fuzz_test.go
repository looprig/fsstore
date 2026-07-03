package fsstore

import (
	"bytes"
	"errors"
	"testing"
)

// FuzzFrameDecode drives the decoder with arbitrary bytes. It must never panic
// and never read past the input: every error is a *FrameError with exactly one
// recovery mode, and every successful decode reports bytesConsumed in
// [frameHeaderSize, len(input)] with a payload of the matching width. As a
// stronger invariant, any frame that decodes must re-encode byte-for-byte to
// the consumed prefix.
func FuzzFrameDecode(f *testing.F) {
	seeds := [][]byte{
		{},
		make([]byte, frameHeaderSize-1),
		make([]byte, frameHeaderSize),
	}
	if fr, err := encodeFrame(1, []byte("hello")); err == nil {
		seeds = append(seeds, fr)
	}
	if fr, err := encodeFrame(0, nil); err == nil {
		seeds = append(seeds, fr)
	}
	if a, err := encodeFrame(1, []byte("aa")); err == nil {
		if b, err := encodeFrame(2, []byte("bbbb")); err == nil {
			seeds = append(seeds, append(append([]byte(nil), a...), b...))
		}
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		seq, payload, n, err := decodeFrame(data)
		if err != nil {
			var fe *FrameError
			if !errors.As(err, &fe) {
				t.Fatalf("decode error not *FrameError: %T %v", err, err)
			}
			if fe.IsTorn() == fe.IsCorrupt() {
				t.Fatalf("fault %d: IsTorn=%v IsCorrupt=%v not mutually exclusive",
					fe.Fault, fe.IsTorn(), fe.IsCorrupt())
			}
			return
		}
		if n < frameHeaderSize || n > len(data) {
			t.Fatalf("bytesConsumed %d out of range [%d,%d]", n, frameHeaderSize, len(data))
		}
		if len(payload) != n-frameHeaderSize {
			t.Fatalf("payload len %d != bytesConsumed-header %d", len(payload), n-frameHeaderSize)
		}
		re, err := encodeFrame(seq, payload)
		if err != nil {
			t.Fatalf("re-encode of decoded frame failed: %v", err)
		}
		if !bytes.Equal(re, data[:n]) {
			t.Fatalf("re-encode mismatch: a decoded frame did not round-trip to its bytes")
		}
	})
}

// FuzzFrameRoundTrip drives encode→decode with arbitrary (seq, payload): any
// payload the codec accepts must decode back to the same seq and bytes, and the
// only legitimate encode rejection is an over-ceiling payload.
func FuzzFrameRoundTrip(f *testing.F) {
	f.Add(uint64(0), []byte(nil))
	f.Add(uint64(1), []byte("hello"))
	f.Add(^uint64(0), []byte{0x00, 0xFF})

	f.Fuzz(func(t *testing.T, seq uint64, payload []byte) {
		frame, err := encodeFrame(seq, payload)
		if err != nil {
			var fe *FrameError
			if !errors.As(err, &fe) || !fe.IsCorrupt() {
				t.Fatalf("unexpected encode error: %T %v", err, err)
			}
			if len(payload) <= MaxFramePayload {
				t.Fatalf("encode rejected an in-range payload of %d bytes", len(payload))
			}
			return
		}
		gotSeq, gotPayload, n, err := decodeFrame(frame)
		if err != nil {
			t.Fatalf("decode of freshly encoded frame: %v", err)
		}
		if gotSeq != seq {
			t.Fatalf("seq round-trip: got %d want %d", gotSeq, seq)
		}
		if !bytes.Equal(gotPayload, payload) {
			t.Fatalf("payload round-trip mismatch")
		}
		if n != len(frame) {
			t.Fatalf("bytesConsumed %d != frame len %d", n, len(frame))
		}
	})
}
