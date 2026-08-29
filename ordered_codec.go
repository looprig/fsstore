package fsstore

import (
	"encoding/binary"
	"strconv"

	"github.com/looprig/storage"
)

// This file defines the payload codec for fsstore's ordered-index mutation log.
// The log's OUTER framing is the repository's existing checksummed frame (see
// frame.go): [len uint32][crc32c uint32][seq uint64][payload]. This file defines
// what that payload contains — exactly one mutation of one ordered record.
//
// # Layering
//
// The two layers answer different questions and must not be conflated:
//
//   - frame.go answers "are all these bytes present and unaltered?". Its faults
//     are Torn (a clean truncation, recoverable by rolling the log back) or
//     Corrupt (a checksum mismatch, fail closed).
//   - this file answers "do these bytes describe a legal mutation?". By the time
//     it runs, the CRC has already proved every declared byte is present and
//     unaltered, so a payload that is short, aliased, or semantically invalid is
//     always corruption — there is no torn classification here.
//
// # Why one frame per mutation
//
// A create frame carries BOTH the newly allocated immutable order and the whole
// new record, so allocating an order and making it visible is a single durable
// append. There is no separate counter file that could be advanced without the
// record, or a record that could exist without its order. Replay recovers the
// scope's high-water mark by taking the maximum order it observes; nothing else
// has to be durable for allocation to be safe. An update or a delete is likewise
// one self-contained frame: replay applies frames in order and the final frame
// for an identity is that record's current state, so no frame needs to be read
// against another to be understood.
//
// # StableKey never reaches a path
//
// A StableKey is an opaque 1..256-byte UTF-8 value: it may contain '/', '..',
// uppercase, punctuation, or arbitrary Unicode. It is carried HERE, inside the
// frame, and never becomes a filesystem path component. Only the namespace
// (which the log file is named for) and the scopes are storage names, and those
// pass ValidateName. That is a security boundary, not a stylistic one.
//
// # Wire layout (version 1, little-endian throughout)
//
//	offset  width  field
//	     0      1  version           (orderedMutationVersion)
//	     1      1  op                (orderedOp: create / update / delete)
//	     2      1  due state         (storage.DueState)
//	     3      1  rank ranked flag  (0 or 1)
//	     4      8  revision          uint64
//	    12      8  order             uint64
//	    20      8  due UnixMillis    int64 (two's complement)
//	    28      8  rank Value        int64 (two's complement)
//	    36      2  ordering scope length  uint16
//	    38      2  stable key length      uint16
//	    40      2  ranking scope length   uint16
//	    42      4  value length           uint32
//	    46      -  ordering scope || stable key || ranking scope || value
//
// The encoding is CANONICAL: every byte of a payload is accounted for by the
// header, there is no padding, and each field has exactly one representation.
// decodeOrderedMutation therefore rejects trailing bytes, and any accepted
// payload re-encodes byte-for-byte (a property the fuzz targets assert).

const (
	// orderedMutationVersion is the only payload version this build writes or
	// accepts. A future version bumps this byte; an unknown value fails closed
	// rather than being interpreted under the current layout.
	orderedMutationVersion uint8 = 1

	// orderedMutationHeaderSize is the fixed-width prefix of every payload:
	// 4 single-byte fields + 4 eight-byte fields + 3 uint16 + 1 uint32.
	orderedMutationHeaderSize = 46

	// maxOrderedNameBytes mirrors storage's name ceiling. It bounds the ordering
	// and ranking scope lengths a decoder will size a slice for, so a corrupt
	// header cannot request an implausible field before ValidateName ever runs.
	maxOrderedNameBytes = 512

	// Field offsets within the fixed header. They are named so the layout above
	// is expressed once and every read/write cites it.
	offOrderedVersion       = 0
	offOrderedOp            = 1
	offOrderedDueState      = 2
	offOrderedRankFlag      = 3
	offOrderedRevision      = 4
	offOrderedOrder         = 12
	offOrderedDueMillis     = 20
	offOrderedRankValue     = 28
	offOrderedScopeLen      = 36
	offOrderedKeyLen        = 38
	offOrderedRankScopeLen  = 40
	offOrderedValueLen      = 42
	orderedRankFlagUnranked = 0
	orderedRankFlagRanked   = 1
)

// orderedOp names the three mutations the log records. A create also carries the
// order allocation; an update carries the post-CAS value/rank/due; a delete
// carries the tombstone, which the contract requires to be unranked, not due,
// and to retain its value, ranking scope, identity, and order.
type orderedOp uint8

const (
	orderedOpCreate orderedOp = 1
	orderedOpUpdate orderedOp = 2
	orderedOpDelete orderedOp = 3
)

// valid reports whether op is one of the three defined operations. The zero
// orderedOp is deliberately not an operation, so a zeroed byte range never
// decodes as a meaningful mutation.
func (op orderedOp) valid() bool {
	return op == orderedOpCreate || op == orderedOpUpdate || op == orderedOpDelete
}

func (op orderedOp) String() string {
	switch op {
	case orderedOpCreate:
		return "create"
	case orderedOpUpdate:
		return "update"
	case orderedOpDelete:
		return "delete"
	default:
		return "op(" + strconv.Itoa(int(op)) + ")"
	}
}

// orderedMutation is one durable record state transition. It is a full record
// snapshot minus the namespace, which is implied by the log file the frame lives
// in — so replaying a namespace's log needs no cross-frame reasoning beyond
// "the last frame for an identity wins".
//
// Value is caller-owned on the way in and freshly allocated on the way out:
// encodeOrderedMutation copies it into the payload and decodeOrderedMutation
// returns a copy, so neither side retains or mutates the other's slice.
type orderedMutation struct {
	Op            orderedOp
	OrderingScope string
	StableKey     storage.StableKey
	RankingScope  string
	Revision      uint64
	Order         uint64
	Due           storage.Due
	Rank          storage.Rank
	Value         []byte
}

// deleted reports whether this mutation records a logical tombstone.
func (m orderedMutation) deleted() bool { return m.Op == orderedOpDelete }

// OrderedCodecFault identifies the precise reason an ordered mutation payload
// failed to encode or decode. Every fault is fail-closed: unlike a frame fault,
// none of them is recoverable by truncating the log, because the outer frame
// checksum has already established that the bytes are complete and unaltered.
type OrderedCodecFault uint8

const (
	// OrderedFaultShortPayload: the payload is shorter than the fixed header, or
	// shorter than the field widths its own header declares.
	OrderedFaultShortPayload OrderedCodecFault = iota + 1
	// OrderedFaultUnknownVersion: the version byte is not orderedMutationVersion.
	OrderedFaultUnknownVersion
	// OrderedFaultUnknownOp: the op byte is not one of the defined orderedOp
	// values.
	OrderedFaultUnknownOp
	// OrderedFaultFieldOversize: a declared (or supplied) field length exceeds
	// its contractual ceiling. Checked before any buffer is sized to it.
	OrderedFaultFieldOversize
	// OrderedFaultTrailingBytes: the payload contains bytes beyond the fields its
	// header declares, so the encoding is not canonical.
	OrderedFaultTrailingBytes
	// OrderedFaultInvalidIdentity: the ordering scope or stable key violates the
	// OrderedID rules. Cause carries the storage validation error.
	OrderedFaultInvalidIdentity
	// OrderedFaultInvalidRecord: the mutation's non-identity state is illegal —
	// an invalid ranking scope or due state, a zero revision or order, a
	// non-boolean rank flag, or a tombstone that is still ranked or due.
	OrderedFaultInvalidRecord
)

// OrderedCodecError is the single typed error the ordered mutation codec
// returns. Fault names the exact cause and Cause, when set, carries the
// underlying storage validation error (for example *storage.InvalidNameError or
// *storage.InvalidStableKeyError) so callers can classify with errors.As at
// either level. Field names the offending field where one is implicated.
//
// The Error() text carries only field names and integers, never payload bytes:
// a StableKey may be arbitrary user data and must not be echoed into logs.
//
// # Classification precedence
//
// This one type reports two different things, and the wrapper is what tells them
// apart. An append REJECTING a caller's mutation returns a BARE
// *OrderedCodecError; a replay finding the same fault ON DISK returns it wrapped
// in an *OrderedLogCorruptError. errors.As(&OrderedCodecError) therefore matches
// both, and a caller that must distinguish invalid input from a damaged log has
// to test *OrderedLogCorruptError FIRST.
//
// A public API whose contract names a specific error for bad input — such as
// OrderedIndex.CreateOrdered, which owes *storage.InvalidNameError or
// *storage.InvalidStableKeyError for a malformed OrderedID — must not rely on
// this precedence at all. It should validate the caller's ID up front with
// storage.ValidateOrderedID and return THAT error unwrapped, leaving the codec's
// validation as the internal backstop it is.
type OrderedCodecError struct {
	Fault OrderedCodecFault
	// Field names the implicated field ("stable_key", "value", ...) when the
	// fault is attributable to one.
	Field string
	// Have and Need are byte counts for a short payload.
	Have int
	Need int
	// Length is the declared or supplied field length for an oversize fault.
	Length uint64
	// Max is the ceiling Length exceeded.
	Max uint64
	// Cause is the underlying storage validation error, when there is one.
	Cause error
}

func (e *OrderedCodecError) Error() string {
	const prefix = "fsstore: ordered mutation "
	switch e.Fault {
	case OrderedFaultShortPayload:
		return prefix + "short payload: have " + strconv.Itoa(e.Have) + " of " +
			strconv.Itoa(e.Need) + " bytes"
	case OrderedFaultUnknownVersion:
		return prefix + "unknown payload version " + strconv.FormatUint(e.Length, 10)
	case OrderedFaultUnknownOp:
		return prefix + "unknown op " + strconv.FormatUint(e.Length, 10)
	case OrderedFaultFieldOversize:
		return prefix + "field " + strconv.Quote(e.Field) + " length " +
			strconv.FormatUint(e.Length, 10) + " exceeds max " + strconv.FormatUint(e.Max, 10)
	case OrderedFaultTrailingBytes:
		return prefix + "trailing bytes: payload is " + strconv.Itoa(e.Have) +
			" bytes but declares " + strconv.Itoa(e.Need)
	case OrderedFaultInvalidIdentity:
		return prefix + "invalid identity field " + strconv.Quote(e.Field) + ": " + causeText(e.Cause)
	case OrderedFaultInvalidRecord:
		return prefix + "invalid record field " + strconv.Quote(e.Field) + ": " + causeText(e.Cause)
	default:
		return prefix + "unknown fault " + strconv.Itoa(int(e.Fault))
	}
}

// Unwrap exposes the underlying storage validation error, or nil when the fault
// is purely structural.
func (e *OrderedCodecError) Unwrap() error { return e.Cause }

// causeText renders an optional cause for an error message without an fmt
// dependency, substituting a fixed rule description when there is no cause. It
// is what keeps every Error() in this package nil-safe.
func causeText(cause error) string {
	if cause == nil {
		return "violates the ordered record contract"
	}
	return cause.Error()
}

// validateOrderedMutation is the SINGLE gate both the encoder and the decoder
// pass through, so a payload that can be written is exactly a payload that can
// be read back. It checks, in a fixed precedence: the op, then identity
// (ordering scope and stable key), then the remaining record state.
func validateOrderedMutation(m orderedMutation) error {
	if !m.Op.valid() {
		return &OrderedCodecError{Fault: OrderedFaultUnknownOp, Field: "op", Length: uint64(m.Op)}
	}
	if err := storage.ValidateName(m.OrderingScope); err != nil {
		return &OrderedCodecError{Fault: OrderedFaultInvalidIdentity, Field: "ordering_scope", Cause: err}
	}
	if err := storage.ValidateStableKey(m.StableKey); err != nil {
		return &OrderedCodecError{Fault: OrderedFaultInvalidIdentity, Field: "stable_key", Cause: err}
	}
	if err := storage.ValidateName(m.RankingScope); err != nil {
		return &OrderedCodecError{Fault: OrderedFaultInvalidRecord, Field: "ranking_scope", Cause: err}
	}
	if m.Revision == 0 {
		return &OrderedCodecError{Fault: OrderedFaultInvalidRecord, Field: "revision"}
	}
	if m.Order == 0 {
		return &OrderedCodecError{Fault: OrderedFaultInvalidRecord, Field: "order"}
	}
	if err := storage.ValidateDue(m.Due); err != nil {
		return &OrderedCodecError{Fault: OrderedFaultInvalidRecord, Field: "due", Cause: err}
	}
	if err := storage.ValidateOrderedValue(m.Value); err != nil {
		return &OrderedCodecError{
			Fault:  OrderedFaultFieldOversize,
			Field:  "value",
			Length: uint64(len(m.Value)),
			Max:    storage.MaxOrderedValueBytes,
			Cause:  err,
		}
	}
	// A tombstone is unranked and not due. Enforcing it in the codec means an
	// illegal tombstone cannot reach the disk and cannot be replayed off it.
	if m.deleted() && (m.Rank != (storage.Rank{}) || m.Due != (storage.Due{State: storage.NotDue, UnixMillis: 0})) {
		return &OrderedCodecError{Fault: OrderedFaultInvalidRecord, Field: "tombstone"}
	}
	return nil
}

// encodeOrderedMutation renders one mutation as a canonical version-1 payload.
// It validates first, so an illegal mutation never reaches the disk, and it
// bounds the scope and key lengths against their uint16 fields — a check
// ValidateName and ValidateStableKey already imply, restated here because the
// wire widths, not the validators, are what would silently truncate.
func encodeOrderedMutation(m orderedMutation) ([]byte, error) {
	if err := validateOrderedMutation(m); err != nil {
		return nil, err
	}
	if len(m.OrderingScope) > maxOrderedNameBytes {
		return nil, oversizeField("ordering_scope", uint64(len(m.OrderingScope)), maxOrderedNameBytes)
	}
	if len(m.StableKey) > storage.MaxStableKeyBytes {
		return nil, oversizeField("stable_key", uint64(len(m.StableKey)), storage.MaxStableKeyBytes)
	}
	if len(m.RankingScope) > maxOrderedNameBytes {
		return nil, oversizeField("ranking_scope", uint64(len(m.RankingScope)), maxOrderedNameBytes)
	}

	total := orderedMutationHeaderSize + len(m.OrderingScope) + len(m.StableKey) +
		len(m.RankingScope) + len(m.Value)
	out := make([]byte, orderedMutationHeaderSize, total)
	out[offOrderedVersion] = orderedMutationVersion
	out[offOrderedOp] = byte(m.Op)
	out[offOrderedDueState] = byte(m.Due.State)
	if m.Rank.Ranked {
		out[offOrderedRankFlag] = orderedRankFlagRanked
	} else {
		out[offOrderedRankFlag] = orderedRankFlagUnranked
	}
	binary.LittleEndian.PutUint64(out[offOrderedRevision:], m.Revision)
	binary.LittleEndian.PutUint64(out[offOrderedOrder:], m.Order)
	// #nosec G115 -- a two's-complement reinterpretation, not a narrowing conversion
	binary.LittleEndian.PutUint64(out[offOrderedDueMillis:], uint64(m.Due.UnixMillis))
	// #nosec G115 -- a two's-complement reinterpretation, not a narrowing conversion
	binary.LittleEndian.PutUint64(out[offOrderedRankValue:], uint64(m.Rank.Value))
	// #nosec G115 -- each length is bounded above by the explicit ceiling checks
	binary.LittleEndian.PutUint16(out[offOrderedScopeLen:], uint16(len(m.OrderingScope)))
	// #nosec G115 -- bounded by MaxStableKeyBytes (256) above
	binary.LittleEndian.PutUint16(out[offOrderedKeyLen:], uint16(len(m.StableKey)))
	// #nosec G115 -- bounded by maxOrderedNameBytes (512) above
	binary.LittleEndian.PutUint16(out[offOrderedRankScopeLen:], uint16(len(m.RankingScope)))
	// #nosec G115 -- bounded by MaxOrderedValueBytes (1 MiB) by ValidateOrderedValue
	binary.LittleEndian.PutUint32(out[offOrderedValueLen:], uint32(len(m.Value)))

	out = append(out, m.OrderingScope...)
	out = append(out, m.StableKey...)
	out = append(out, m.RankingScope...)
	out = append(out, m.Value...)
	return out, nil
}

// oversizeField builds the oversize fault for a field whose length exceeds its
// wire ceiling. Both lengths are uint64 so no caller has to convert a signed
// value: an encoder passes a len() and a decoder passes the unsigned width it
// read off the wire, and neither conversion can wrap.
func oversizeField(field string, length, max uint64) *OrderedCodecError {
	return &OrderedCodecError{
		Fault:  OrderedFaultFieldOversize,
		Field:  field,
		Length: length,
		Max:    max,
	}
}

// decodeOrderedMutation parses one canonical version-1 payload. It rejects, in
// this precedence: a payload shorter than the fixed header; an unknown version;
// an unknown op; a declared field length beyond its ceiling (checked BEFORE any
// buffer is sized to it, so a corrupt header cannot force a large allocation); a
// payload shorter than its declared fields; trailing bytes beyond them; and
// finally any mutation that fails validateOrderedMutation.
//
// Every returned byte slice is freshly allocated, so payload may be reused or
// truncated freely by the caller. On any error the returned mutation is the zero
// value.
func decodeOrderedMutation(payload []byte) (orderedMutation, error) {
	if len(payload) < orderedMutationHeaderSize {
		return orderedMutation{}, &OrderedCodecError{
			Fault: OrderedFaultShortPayload, Have: len(payload), Need: orderedMutationHeaderSize,
		}
	}
	if version := payload[offOrderedVersion]; version != orderedMutationVersion {
		return orderedMutation{}, &OrderedCodecError{
			Fault: OrderedFaultUnknownVersion, Field: "version", Length: uint64(version),
		}
	}
	op := orderedOp(payload[offOrderedOp])
	if !op.valid() {
		return orderedMutation{}, &OrderedCodecError{
			Fault: OrderedFaultUnknownOp, Field: "op", Length: uint64(op),
		}
	}

	// Read each declared width as the UNSIGNED type the wire carries and check
	// it against its ceiling BEFORE narrowing to int, so no buffer is ever sized
	// to an unvalidated length and no conversion can wrap.
	wireScopeLen := binary.LittleEndian.Uint16(payload[offOrderedScopeLen:])
	wireKeyLen := binary.LittleEndian.Uint16(payload[offOrderedKeyLen:])
	wireRankScopeLen := binary.LittleEndian.Uint16(payload[offOrderedRankScopeLen:])
	wireValueLen := binary.LittleEndian.Uint32(payload[offOrderedValueLen:])
	if wireScopeLen > maxOrderedNameBytes {
		return orderedMutation{}, oversizeField("ordering_scope", uint64(wireScopeLen), maxOrderedNameBytes)
	}
	if wireKeyLen > storage.MaxStableKeyBytes {
		return orderedMutation{}, oversizeField("stable_key", uint64(wireKeyLen), storage.MaxStableKeyBytes)
	}
	if wireRankScopeLen > maxOrderedNameBytes {
		return orderedMutation{}, oversizeField("ranking_scope", uint64(wireRankScopeLen), maxOrderedNameBytes)
	}
	if wireValueLen > storage.MaxOrderedValueBytes {
		return orderedMutation{}, oversizeField("value", uint64(wireValueLen), storage.MaxOrderedValueBytes)
	}
	scopeLen, keyLen := int(wireScopeLen), int(wireKeyLen)
	rankScopeLen, valueLen := int(wireRankScopeLen), int(wireValueLen)

	// Each length is bounded above, so the sum cannot overflow int on any
	// platform Go supports.
	need := orderedMutationHeaderSize + scopeLen + keyLen + rankScopeLen + valueLen
	if len(payload) < need {
		return orderedMutation{}, &OrderedCodecError{
			Fault: OrderedFaultShortPayload, Have: len(payload), Need: need,
		}
	}
	if len(payload) > need {
		return orderedMutation{}, &OrderedCodecError{
			Fault: OrderedFaultTrailingBytes, Have: len(payload), Need: need,
		}
	}

	rankFlag := payload[offOrderedRankFlag]
	if rankFlag != orderedRankFlagUnranked && rankFlag != orderedRankFlagRanked {
		return orderedMutation{}, &OrderedCodecError{
			Fault: OrderedFaultInvalidRecord, Field: "rank_flag", Length: uint64(rankFlag),
		}
	}

	off := orderedMutationHeaderSize
	scope := string(payload[off : off+scopeLen])
	off += scopeLen
	key := storage.StableKey(payload[off : off+keyLen])
	off += keyLen
	rankScope := string(payload[off : off+rankScopeLen])
	off += rankScopeLen
	var value []byte
	if valueLen > 0 {
		value = make([]byte, valueLen)
		copy(value, payload[off:off+valueLen])
	}

	m := orderedMutation{
		Op:            op,
		OrderingScope: scope,
		StableKey:     key,
		RankingScope:  rankScope,
		Revision:      binary.LittleEndian.Uint64(payload[offOrderedRevision:]),
		Order:         binary.LittleEndian.Uint64(payload[offOrderedOrder:]),
		Due: storage.Due{
			State: storage.DueState(payload[offOrderedDueState]),
			// #nosec G115 -- a two's-complement reinterpretation, not a narrowing conversion
			UnixMillis: int64(binary.LittleEndian.Uint64(payload[offOrderedDueMillis:])),
		},
		Rank: storage.Rank{
			Ranked: rankFlag == orderedRankFlagRanked,
			// #nosec G115 -- a two's-complement reinterpretation, not a narrowing conversion
			Value: int64(binary.LittleEndian.Uint64(payload[offOrderedRankValue:])),
		},
		Value: value,
	}
	if err := validateOrderedMutation(m); err != nil {
		return orderedMutation{}, err
	}
	return m, nil
}

// encodeOrderedFrame renders one mutation as a complete log frame: the payload
// above wrapped in the repository's checksummed frame at sequence seq. It is the
// only function the log's append path calls, so allocation, checksum, and
// sequence stamping stay in one place.
func encodeOrderedFrame(seq uint64, m orderedMutation) ([]byte, error) {
	payload, err := encodeOrderedMutation(m)
	if err != nil {
		return nil, err
	}
	return encodeFrame(seq, payload)
}

// decodeOrderedFrame reads exactly one mutation frame from the front of buf,
// returning its sequence, the mutation, and bytesConsumed so a caller walking a
// log advances by bytesConsumed. The two error layers stay distinguishable: a
// *FrameError (classify with IsTorn/IsCorrupt) means the bytes are incomplete or
// altered, while an *OrderedCodecError means intact bytes do not describe a
// legal mutation. On any error every other return is zero.
func decodeOrderedFrame(buf []byte) (seq uint64, m orderedMutation, bytesConsumed int, err error) {
	frameSeq, payload, n, ferr := decodeFrame(buf)
	if ferr != nil {
		return 0, orderedMutation{}, 0, ferr
	}
	decoded, cerr := decodeOrderedMutation(payload)
	if cerr != nil {
		return 0, orderedMutation{}, 0, cerr
	}
	return frameSeq, decoded, n, nil
}
