package fsstore

import (
	"encoding/base64"
	"strconv"
	"strings"

	"github.com/looprig/storage"
)

// This file defines fsstore's opaque continuation tokens for ListRanked and
// ListDue. storage deliberately leaves the encoding to each provider: a cursor
// is provider-issued, versioned, and query-bound, and the conformance suite
// never writes one as a literal (it asks the provider for malformed and
// unknown-version probes instead). What the contract DOES fix is the set of
// inputs a provider must reject, and it is exactly four: a malformed token, an
// unknown token version, a token issued for the OTHER cursor kind, and a token
// whose bound query does not match the live request.
//
// # A cursor carries position, not authority
//
// These tokens are NOT signed, and that is deliberate rather than an omission —
// storage's reference provider dropped HMAC signing for the same reason. A
// cursor names a position in a public listing the caller is already authorized
// to read; it grants nothing. Forging one can only move a caller to a different
// offset of a result set it may already page through from the beginning, and
// every binding in the token (namespace, ranking scope, due bound) is re-checked
// against the LIVE request rather than trusted from the token — a mismatch is a
// typed failure, not a silent widening. Signing would add key management and a
// rotation story to buy nothing.
//
// # Encoding
//
// A token is a fixed kind/version header followed by base64url of a '.'-joined
// field list, each variable-length field itself base64url-encoded so the
// separator can never occur inside a name or a StableKey:
//
//	fso1:r:<base64url( "1" "." "ranked" "." b64(ns) "." b64(rankScope) "." rank "." b64(key) "." b64(orderScope) )>
//	fso1:d:<base64url( "1" "." "due"    "." b64(ns) "." bound "." dueAt "." b64(key) "." b64(orderScope) )>
//
// The version appears twice on purpose: in the header, so an unsupported version
// is classified before the body is decoded at all, and inside the payload, so a
// decoder never infers a version from a prefix an attacker controls.
//
// Integer fields are parsed CANONICALLY: only the exact spelling
// strconv.FormatInt would emit is accepted, so "+7", "007", and "7" are not
// three tokens for one position. That keeps the encoding injective, which is
// what makes "the same page yields the same cursor" a testable property.

const (
	// orderedCursorVersionField is the payload's version token — the same
	// version the header spells, carried where a prefix rewrite cannot reach it.
	orderedCursorVersionField = "1"

	// The two kind tokens carried inside the payload.
	rankedCursorTokenKind = "ranked"
	dueCursorTokenKind    = "due"

	// orderedCursorScheme prefixes every fsstore cursor header. It is what makes
	// a version recognizable BEFORE the body is decoded, and it is deliberately
	// distinct from another provider's scheme so a token cannot be carried from
	// one backend to another and silently accepted.
	orderedCursorScheme = "fso"

	rankedCursorHeader = orderedCursorScheme + "1:r:"
	dueCursorHeader    = orderedCursorScheme + "1:d:"

	// orderedCursorFieldCount is the exact field count of BOTH payloads: version,
	// kind, and five query/position fields. An exact count (rather than a
	// minimum) is what makes a truncated or padded token malformed.
	orderedCursorFieldCount = 7

	// maxOrderedCursorBytes bounds an untrusted token before any decoding, so a
	// hostile cursor cannot drive a large allocation. It is far above the largest
	// token the contract's bounded names and 256-byte StableKey can produce.
	maxOrderedCursorBytes = 8 << 10
)

// rankedCursorPosition is the frozen descending position a ranked cursor names,
// plus the query it is bound to. The position is the full (rank, stable_key,
// ordering_scope) tuple — the ordering_scope tiebreaker is not optional, because
// record identity is (namespace, ordering_scope, stable_key) and without it two
// records sharing a rank and stable key across ordering scopes are
// indistinguishable to the cursor, so a page could never advance past them.
type rankedCursorPosition struct {
	namespace     string
	rankingScope  string
	rank          int64
	stableKey     storage.StableKey
	orderingScope string
}

// dueCursorPosition is the frozen ascending position a due cursor names. It
// binds the FIXED due bound the page was read with as well as the namespace: a
// token from a sweep at bound T is not valid for a sweep at bound T+1, because
// the two enumerate different result sets.
type dueCursorPosition struct {
	namespace     string
	dueBound      int64
	dueAt         int64
	stableKey     storage.StableKey
	orderingScope string
}

func rankedPositionFor(record storage.OrderedRecord) rankedCursorPosition {
	return rankedCursorPosition{
		namespace:     record.ID.Namespace,
		rankingScope:  record.RankingScope,
		rank:          record.Rank.Value,
		stableKey:     record.ID.StableKey,
		orderingScope: record.ID.OrderingScope,
	}
}

func duePositionFor(dueBound int64, record storage.OrderedRecord) dueCursorPosition {
	return dueCursorPosition{
		namespace:     record.ID.Namespace,
		dueBound:      dueBound,
		dueAt:         record.Due.UnixMillis,
		stableKey:     record.ID.StableKey,
		orderingScope: record.ID.OrderingScope,
	}
}

// encodeRankedCursor renders a ranked position as an opaque token. Encoding is
// TOTAL — every field is a bounded string or an int64 — so issuing a cursor can
// never fail and ListRanked needs no error path for it.
func encodeRankedCursor(position rankedCursorPosition) storage.RankedCursor {
	return storage.RankedCursor(encodeOrderedCursorToken(rankedCursorHeader, []string{
		orderedCursorVersionField,
		rankedCursorTokenKind,
		encodeOrderedCursorField(position.namespace),
		encodeOrderedCursorField(position.rankingScope),
		strconv.FormatInt(position.rank, 10),
		encodeOrderedCursorField(string(position.stableKey)),
		encodeOrderedCursorField(position.orderingScope),
	}))
}

// encodeDueCursor renders a due position, including the fixed bound the page was
// read with, under the same total encoding.
func encodeDueCursor(position dueCursorPosition) storage.DueCursor {
	return storage.DueCursor(encodeOrderedCursorToken(dueCursorHeader, []string{
		orderedCursorVersionField,
		dueCursorTokenKind,
		encodeOrderedCursorField(position.namespace),
		strconv.FormatInt(position.dueBound, 10),
		strconv.FormatInt(position.dueAt, 10),
		encodeOrderedCursorField(string(position.stableKey)),
		encodeOrderedCursorField(position.orderingScope),
	}))
}

func encodeOrderedCursorToken(header string, fields []string) string {
	return header + base64.RawURLEncoding.EncodeToString([]byte(strings.Join(fields, ".")))
}

func encodeOrderedCursorField(value string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(value))
}

func decodeOrderedCursorField(field string) (string, bool) {
	decoded, err := base64.RawURLEncoding.DecodeString(field)
	if err != nil {
		return "", false
	}
	return string(decoded), true
}

// decodeRankedCursor parses a caller-supplied ranked cursor against the LIVE
// query. An empty cursor is the absent cursor, not an error: it starts the
// listing at the beginning.
//
// It is a free function on purpose. Nothing about a token is instance specific —
// it is versioned, kind tagged, and query bound — so a cursor issued by one
// *orderedIndex decodes identically in another process reading the same store.
// Making it a method would imply a per-process authority these tokens do not
// carry.
func decodeRankedCursor(cursor storage.RankedCursor, namespace string, rankingScope string) (rankedCursorPosition, bool, error) {
	if cursor == "" {
		return rankedCursorPosition{}, false, nil
	}
	fields, err := decodeOrderedCursorFields(storage.RankedCursorKind, string(cursor))
	if err != nil {
		return rankedCursorPosition{}, false, err
	}
	position, ok := parseRankedCursorFields(fields)
	if !ok {
		return rankedCursorPosition{}, false, storage.NewInvalidOrderedCursorError(
			storage.RankedCursorKind, string(cursor), storage.OrderedCursorMalformed)
	}
	// The bindings are COMPARED with the live request, never adopted from it.
	if position.namespace != namespace || position.rankingScope != rankingScope {
		return rankedCursorPosition{}, false, storage.NewInvalidOrderedCursorError(
			storage.RankedCursorKind, string(cursor), storage.OrderedCursorQueryMismatch)
	}
	return position, true, nil
}

// decodeDueCursor parses a caller-supplied due cursor against the live namespace
// and due bound, for the same reasons as decodeRankedCursor.
func decodeDueCursor(cursor storage.DueCursor, namespace string, dueBound int64) (dueCursorPosition, bool, error) {
	if cursor == "" {
		return dueCursorPosition{}, false, nil
	}
	fields, err := decodeOrderedCursorFields(storage.DueCursorKind, string(cursor))
	if err != nil {
		return dueCursorPosition{}, false, err
	}
	position, ok := parseDueCursorFields(fields)
	if !ok {
		return dueCursorPosition{}, false, storage.NewInvalidOrderedCursorError(
			storage.DueCursorKind, string(cursor), storage.OrderedCursorMalformed)
	}
	if position.namespace != namespace || position.dueBound != dueBound {
		return dueCursorPosition{}, false, storage.NewInvalidOrderedCursorError(
			storage.DueCursorKind, string(cursor), storage.OrderedCursorQueryMismatch)
	}
	return position, true, nil
}

// decodeOrderedCursorFields parses an untrusted token into its payload fields,
// classifying every rejection under one of the contract's four fail-closed
// rules. It applies the length cap FIRST, before any base64 decoding, so a
// hostile token cannot force an allocation proportional to its size.
//
// Kind detection is deliberately two-layered: a token whose HEADER names the
// other kind is wrong-kind even though its body is never read, and a token whose
// header matched but whose PAYLOAD kind field names the other family is
// wrong-kind too. Neither layer alone is enough — the first would miss a
// hand-assembled body, the second would miss a header-only forgery.
func decodeOrderedCursorFields(kind storage.OrderedCursorKind, cursor string) ([]string, error) {
	if len(cursor) > maxOrderedCursorBytes {
		return nil, storage.NewInvalidOrderedCursorError(kind, cursor, storage.OrderedCursorMalformed)
	}
	if hasUnsupportedCursorVersion(cursor) {
		return nil, storage.NewInvalidOrderedCursorError(kind, cursor, storage.OrderedCursorUnknownVersion)
	}
	expectedHeader := cursorHeader(kind)
	if !strings.HasPrefix(cursor, expectedHeader) {
		if strings.HasPrefix(cursor, rankedCursorHeader) || strings.HasPrefix(cursor, dueCursorHeader) {
			return nil, storage.NewInvalidOrderedCursorError(kind, cursor, storage.OrderedCursorWrongKind)
		}
		return nil, storage.NewInvalidOrderedCursorError(kind, cursor, storage.OrderedCursorMalformed)
	}

	payload, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(cursor, expectedHeader))
	if err != nil {
		return nil, storage.NewInvalidOrderedCursorError(kind, cursor, storage.OrderedCursorMalformed)
	}
	fields := strings.Split(string(payload), ".")
	if len(fields) != orderedCursorFieldCount {
		return nil, storage.NewInvalidOrderedCursorError(kind, cursor, storage.OrderedCursorMalformed)
	}
	if fields[0] != orderedCursorVersionField {
		return nil, storage.NewInvalidOrderedCursorError(kind, cursor, storage.OrderedCursorUnknownVersion)
	}
	if fields[1] != cursorTokenKind(kind) {
		if fields[1] == rankedCursorTokenKind || fields[1] == dueCursorTokenKind {
			return nil, storage.NewInvalidOrderedCursorError(kind, cursor, storage.OrderedCursorWrongKind)
		}
		return nil, storage.NewInvalidOrderedCursorError(kind, cursor, storage.OrderedCursorMalformed)
	}
	return fields, nil
}

// hasUnsupportedCursorVersion recognizes a syntactically versioned token of this
// provider's scheme whose version is not 1, so a future "fso2:" token is
// reported as an unknown VERSION rather than conflated with random garbage. A
// token that does not spell the scheme at all is left to the malformed path.
func hasUnsupportedCursorVersion(cursor string) bool {
	if !strings.HasPrefix(cursor, orderedCursorScheme) {
		return false
	}
	rest := cursor[len(orderedCursorScheme):]
	separator := strings.IndexByte(rest, ':')
	if separator <= 0 {
		return false
	}
	for _, digit := range rest[:separator] {
		if digit < '0' || digit > '9' {
			return false
		}
	}
	return rest[:separator] != orderedCursorVersionField
}

func cursorHeader(kind storage.OrderedCursorKind) string {
	if kind == storage.RankedCursorKind {
		return rankedCursorHeader
	}
	return dueCursorHeader
}

func cursorTokenKind(kind storage.OrderedCursorKind) string {
	if kind == storage.RankedCursorKind {
		return rankedCursorTokenKind
	}
	return dueCursorTokenKind
}

// parseCanonicalInt64 accepts only the exact spelling strconv.FormatInt emits
// for the value it parses. ParseInt alone reads "+10", "010", and "10" as one
// number, which would give a single position three distinct tokens and break the
// injectivity the encoding claims.
func parseCanonicalInt64(field string) (int64, bool) {
	value, err := strconv.ParseInt(field, 10, 64)
	if err != nil {
		return 0, false
	}
	if strconv.FormatInt(value, 10) != field {
		return 0, false
	}
	return value, true
}

func parseRankedCursorFields(fields []string) (rankedCursorPosition, bool) {
	namespace, namespaceOK := decodeOrderedCursorField(fields[2])
	rankingScope, rankingScopeOK := decodeOrderedCursorField(fields[3])
	rank, rankOK := parseCanonicalInt64(fields[4])
	stableKey, stableKeyOK := decodeOrderedCursorField(fields[5])
	orderingScope, orderingScopeOK := decodeOrderedCursorField(fields[6])
	if !namespaceOK || !rankingScopeOK || !rankOK || !stableKeyOK || !orderingScopeOK {
		return rankedCursorPosition{}, false
	}
	return rankedCursorPosition{
		namespace:     namespace,
		rankingScope:  rankingScope,
		rank:          rank,
		stableKey:     storage.StableKey(stableKey),
		orderingScope: orderingScope,
	}, true
}

func parseDueCursorFields(fields []string) (dueCursorPosition, bool) {
	namespace, namespaceOK := decodeOrderedCursorField(fields[2])
	dueBound, dueBoundOK := parseCanonicalInt64(fields[3])
	dueAt, dueAtOK := parseCanonicalInt64(fields[4])
	stableKey, stableKeyOK := decodeOrderedCursorField(fields[5])
	orderingScope, orderingScopeOK := decodeOrderedCursorField(fields[6])
	if !namespaceOK || !dueBoundOK || !dueAtOK || !stableKeyOK || !orderingScopeOK {
		return dueCursorPosition{}, false
	}
	return dueCursorPosition{
		namespace:     namespace,
		dueBound:      dueBound,
		dueAt:         dueAt,
		stableKey:     storage.StableKey(stableKey),
		orderingScope: orderingScope,
	}, true
}
