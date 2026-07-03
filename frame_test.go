package fsstore

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"testing"
)

// testCastagnoli is an independent CRC-32 Castagnoli table so the tests validate
// the on-disk format against the polynomial directly, not against the codec's
// own table variable.
var testCastagnoli = crc32.MakeTable(crc32.Castagnoli)

func TestEncodeFrame(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		seq     uint64
		payload []byte
		wantErr bool
	}{
		{name: "small payload", seq: 1, payload: []byte("hello")},
		{name: "nil payload", seq: 7, payload: nil},
		{name: "empty non-nil payload", seq: 8, payload: []byte{}},
		{name: "max seq", seq: ^uint64(0), payload: []byte("x")},
		{name: "exactly max payload", seq: 2, payload: bytes.Repeat([]byte{0xAB}, MaxFramePayload)},
		{name: "one over max payload", seq: 3, payload: make([]byte, MaxFramePayload+1), wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			frame, err := encodeFrame(tt.seq, tt.payload)
			if (err != nil) != tt.wantErr {
				t.Fatalf("encodeFrame err = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				var fe *FrameError
				if !errors.As(err, &fe) {
					t.Fatalf("want *FrameError, got %T: %v", err, err)
				}
				if !fe.IsCorrupt() {
					t.Errorf("oversize encode: IsCorrupt() = false, want true")
				}
				if fe.IsTorn() {
					t.Errorf("oversize encode: IsTorn() = true, want false")
				}
				if frame != nil {
					t.Errorf("frame = %v, want nil on error", frame)
				}
				return
			}
			if len(frame) != frameHeaderSize+len(tt.payload) {
				t.Fatalf("frame len = %d, want %d", len(frame), frameHeaderSize+len(tt.payload))
			}
			gotLen := binary.LittleEndian.Uint32(frame[0:4])
			gotCRC := binary.LittleEndian.Uint32(frame[4:8])
			gotSeq := binary.LittleEndian.Uint64(frame[8:16])
			if int(gotLen) != len(tt.payload) {
				t.Errorf("len field = %d, want %d", gotLen, len(tt.payload))
			}
			if wantCRC := crc32.Checksum(tt.payload, testCastagnoli); gotCRC != wantCRC {
				t.Errorf("crc field = %d, want %d", gotCRC, wantCRC)
			}
			if gotSeq != tt.seq {
				t.Errorf("seq field = %d, want %d", gotSeq, tt.seq)
			}
			if !bytes.Equal(frame[frameHeaderSize:], tt.payload) {
				t.Errorf("payload bytes mismatch")
			}
		})
	}
}

func TestDecodeFrameRoundTrip(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		seq     uint64
		payload []byte
	}{
		{name: "small", seq: 1, payload: []byte("hello world")},
		{name: "zero-length nil", seq: 42, payload: nil},
		{name: "zero-length empty", seq: 43, payload: []byte{}},
		{name: "single zero byte", seq: 2, payload: []byte{0x00}},
		{name: "max seq", seq: ^uint64(0), payload: []byte("edge")},
		{name: "1 MiB", seq: 5, payload: bytes.Repeat([]byte{0xCD}, 1<<20)},
		{name: "exactly max payload", seq: 6, payload: bytes.Repeat([]byte{0x5A}, MaxFramePayload)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			frame, err := encodeFrame(tt.seq, tt.payload)
			if err != nil {
				t.Fatalf("encodeFrame: %v", err)
			}
			seq, payload, n, err := decodeFrame(frame)
			if err != nil {
				t.Fatalf("decodeFrame: %v", err)
			}
			if seq != tt.seq {
				t.Errorf("seq = %d, want %d", seq, tt.seq)
			}
			if !bytes.Equal(payload, tt.payload) {
				t.Errorf("payload mismatch: got %d bytes, want %d", len(payload), len(tt.payload))
			}
			if n != len(frame) {
				t.Errorf("bytesConsumed = %d, want %d", n, len(frame))
			}
		})
	}
}

func TestDecodeFrameFaults(t *testing.T) {
	t.Parallel()
	valid, err := encodeFrame(9, []byte("payload-data"))
	if err != nil {
		t.Fatalf("setup encodeFrame: %v", err)
	}

	// A 16-byte header alone that declares a payload larger than MaxFramePayload:
	// must be rejected as Corrupt without sizing any buffer to the bogus length.
	oversize := make([]byte, frameHeaderSize)
	binary.LittleEndian.PutUint32(oversize[0:4], uint32(MaxFramePayload)+1)
	binary.LittleEndian.PutUint64(oversize[8:16], 1)

	// A single-bit flip in the payload with everything present: Corrupt via CRC.
	flipped := append([]byte(nil), valid...)
	flipped[frameHeaderSize] ^= 0x01

	tests := []struct {
		name     string
		buf      []byte
		wantTorn bool
		wantCorr bool
	}{
		{name: "empty buffer", buf: nil, wantTorn: true},
		{name: "partial header 1 byte", buf: valid[:1], wantTorn: true},
		{name: "partial header 15 bytes", buf: valid[:15], wantTorn: true},
		{name: "header only, payload absent", buf: valid[:frameHeaderSize], wantTorn: true},
		{name: "truncated payload by one byte", buf: valid[:len(valid)-1], wantTorn: true},
		{name: "single-bit flip in payload", buf: flipped, wantCorr: true},
		{name: "length field over max", buf: oversize, wantCorr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			seq, payload, n, err := decodeFrame(tt.buf)
			if err == nil {
				t.Fatalf("decodeFrame = nil err, want error")
			}
			var fe *FrameError
			if !errors.As(err, &fe) {
				t.Fatalf("want *FrameError, got %T: %v", err, err)
			}
			if fe.IsTorn() != tt.wantTorn {
				t.Errorf("IsTorn() = %v, want %v (%v)", fe.IsTorn(), tt.wantTorn, err)
			}
			if fe.IsCorrupt() != tt.wantCorr {
				t.Errorf("IsCorrupt() = %v, want %v (%v)", fe.IsCorrupt(), tt.wantCorr, err)
			}
			if fe.IsTorn() == fe.IsCorrupt() {
				t.Errorf("modes not mutually exclusive: torn=%v corrupt=%v", fe.IsTorn(), fe.IsCorrupt())
			}
			if seq != 0 || payload != nil || n != 0 {
				t.Errorf("on error want zero return, got seq=%d payload=%v n=%d", seq, payload, n)
			}
		})
	}
}

func TestDecodeFrameSequence(t *testing.T) {
	t.Parallel()
	type rec struct {
		seq     uint64
		payload []byte
	}
	recs := []rec{
		{1, []byte("first")},
		{2, nil},
		{3, []byte("third-record")},
		{4, []byte{0xFF, 0x00, 0xFF}},
	}
	var stream []byte
	for _, r := range recs {
		f, err := encodeFrame(r.seq, r.payload)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		stream = append(stream, f...)
	}

	off := 0
	for i, want := range recs {
		seq, payload, n, err := decodeFrame(stream[off:])
		if err != nil {
			t.Fatalf("frame %d decode: %v", i, err)
		}
		if seq != want.seq {
			t.Errorf("frame %d seq = %d, want %d", i, seq, want.seq)
		}
		if !bytes.Equal(payload, want.payload) {
			t.Errorf("frame %d payload mismatch", i)
		}
		off += n
	}
	if off != len(stream) {
		t.Errorf("consumed %d bytes, want %d", off, len(stream))
	}

	// A torn tail (two stray bytes after the last good frame): the walk decodes
	// every whole frame, then reports Torn exactly at the tail boundary.
	torn := append(append([]byte(nil), stream...), 0x01, 0x02)
	off = 0
	good := 0
	for {
		_, _, n, err := decodeFrame(torn[off:])
		if err != nil {
			var fe *FrameError
			if !errors.As(err, &fe) || !fe.IsTorn() {
				t.Fatalf("tail: want torn *FrameError, got %v", err)
			}
			break
		}
		good++
		off += n
	}
	if good != len(recs) {
		t.Errorf("decoded %d good frames before torn tail, want %d", good, len(recs))
	}
	if off != len(stream) {
		t.Errorf("torn tail begins at offset %d, want %d", off, len(stream))
	}
}
