package fsstore

import (
	"errors"
	"strings"
	"testing"

	"github.com/looprig/storage"
)

// forgeCursor assembles a token from a header and an arbitrary field list,
// bypassing encodeRankedCursor/encodeDueCursor entirely. It is what makes the
// SECOND layer of every check in this codec testable: a token whose header is
// impeccable but whose payload was written by an attacker. Rewriting the header
// of a real token — which is all a black-box test can do — only ever exercises
// the first layer, and the payload's own version and kind fields exist precisely
// because the header is the part a forger controls most easily.
func forgeCursor(header string, fields []string) string {
	return encodeOrderedCursorToken(header, fields)
}

// b64 is the per-field encoding the payload uses for every variable-length
// value, so a forged field is spelled exactly as a genuine one.
func b64(value string) string { return encodeOrderedCursorField(value) }

// rankedFields is a payload that decodes cleanly for namespace "sessions",
// ranking scope "catalog". Each case below changes exactly one field of it.
func rankedFields(version, kind, rank string) []string {
	return []string{version, kind, b64("sessions"), b64("catalog"), rank, b64("key"), b64("host")}
}

// dueFields is the due payload's equivalent, bound to namespace "sessions" and
// due bound 100.
func dueFields(version, kind, bound, dueAt string) []string {
	return []string{version, kind, b64("sessions"), bound, dueAt, b64("key"), b64("host")}
}

// TestOrderedCursorPayloadFailsClosed drives the decoder directly, because the
// checks it covers are unreachable through the public listings with a
// well-formed token: every field below is one a caller can only produce by
// hand-assembling the payload.
func TestOrderedCursorPayloadFailsClosed(t *testing.T) {
	t.Parallel()

	// An otherwise perfectly valid token whose ordering scope pushes it past the
	// pre-decode length cap. Without the cap it decodes SUCCESSFULLY, which is
	// what makes this the only case that can tell the cap apart from its absence.
	oversizeScope := strings.Repeat("z", maxOrderedCursorBytes)

	ranked := []struct {
		name  string
		token string
		rule  storage.OrderedCursorRule
	}{
		{"payload version 2 under a version-1 header", forgeCursor(rankedCursorHeader, rankedFields("2", rankedCursorTokenKind, "1")), storage.OrderedCursorUnknownVersion},
		{"payload version empty", forgeCursor(rankedCursorHeader, rankedFields("", rankedCursorTokenKind, "1")), storage.OrderedCursorUnknownVersion},
		{"payload version not a number", forgeCursor(rankedCursorHeader, rankedFields("v1", rankedCursorTokenKind, "1")), storage.OrderedCursorUnknownVersion},
		{"payload kind names the due family", forgeCursor(rankedCursorHeader, rankedFields(orderedCursorVersionField, dueCursorTokenKind, "1")), storage.OrderedCursorWrongKind},
		{"payload kind names no family", forgeCursor(rankedCursorHeader, rankedFields(orderedCursorVersionField, "ranked!", "1")), storage.OrderedCursorMalformed},
		{"payload kind empty", forgeCursor(rankedCursorHeader, rankedFields(orderedCursorVersionField, "", "1")), storage.OrderedCursorMalformed},
		{"rank with an explicit plus sign", forgeCursor(rankedCursorHeader, rankedFields(orderedCursorVersionField, rankedCursorTokenKind, "+7")), storage.OrderedCursorMalformed},
		{"rank with leading zeros", forgeCursor(rankedCursorHeader, rankedFields(orderedCursorVersionField, rankedCursorTokenKind, "007")), storage.OrderedCursorMalformed},
		{"rank spelled as negative zero", forgeCursor(rankedCursorHeader, rankedFields(orderedCursorVersionField, rankedCursorTokenKind, "-0")), storage.OrderedCursorMalformed},
		{"rank in hexadecimal", forgeCursor(rankedCursorHeader, rankedFields(orderedCursorVersionField, rankedCursorTokenKind, "0x10")), storage.OrderedCursorMalformed},
		{"one field short", forgeCursor(rankedCursorHeader, []string{orderedCursorVersionField, rankedCursorTokenKind, b64("sessions"), b64("catalog"), "1", b64("key")}), storage.OrderedCursorMalformed},
		{"one field long", forgeCursor(rankedCursorHeader, append(rankedFields(orderedCursorVersionField, rankedCursorTokenKind, "1"), b64("extra"))), storage.OrderedCursorMalformed},
		{"stable key not base64", forgeCursor(rankedCursorHeader, []string{orderedCursorVersionField, rankedCursorTokenKind, b64("sessions"), b64("catalog"), "1", "!!!", b64("host")}), storage.OrderedCursorMalformed},
		{
			name: "valid but past the length cap",
			token: forgeCursor(rankedCursorHeader, []string{
				orderedCursorVersionField, rankedCursorTokenKind, b64("sessions"), b64("catalog"), "1", b64("key"), b64(oversizeScope),
			}),
			rule: storage.OrderedCursorMalformed,
		},
	}
	for _, tt := range ranked {
		t.Run("ranked/"+tt.name, func(t *testing.T) {
			t.Parallel()
			position, has, err := decodeRankedCursor(storage.RankedCursor(tt.token), "sessions", "catalog")
			assertCursorFailure(t, err, storage.RankedCursorKind, tt.rule)
			if has || position != (rankedCursorPosition{}) {
				t.Errorf("rejected token still yielded a position %+v (has %t)", position, has)
			}
		})
	}

	due := []struct {
		name  string
		token string
		rule  storage.OrderedCursorRule
	}{
		{"payload version 2 under a version-1 header", forgeCursor(dueCursorHeader, dueFields("2", dueCursorTokenKind, "100", "10")), storage.OrderedCursorUnknownVersion},
		{"payload kind names the ranked family", forgeCursor(dueCursorHeader, dueFields(orderedCursorVersionField, rankedCursorTokenKind, "100", "10")), storage.OrderedCursorWrongKind},
		{"payload kind names no family", forgeCursor(dueCursorHeader, dueFields(orderedCursorVersionField, "due!", "100", "10")), storage.OrderedCursorMalformed},
		{"bound with an explicit plus sign", forgeCursor(dueCursorHeader, dueFields(orderedCursorVersionField, dueCursorTokenKind, "+100", "10")), storage.OrderedCursorMalformed},
		{"due time with leading zeros", forgeCursor(dueCursorHeader, dueFields(orderedCursorVersionField, dueCursorTokenKind, "100", "0010")), storage.OrderedCursorMalformed},
		{"due time out of int64 range", forgeCursor(dueCursorHeader, dueFields(orderedCursorVersionField, dueCursorTokenKind, "100", "9223372036854775808")), storage.OrderedCursorMalformed},
		{
			name: "valid but past the length cap",
			token: forgeCursor(dueCursorHeader, []string{
				orderedCursorVersionField, dueCursorTokenKind, b64("sessions"), "100", "10", b64("key"), b64(oversizeScope),
			}),
			rule: storage.OrderedCursorMalformed,
		},
	}
	for _, tt := range due {
		t.Run("due/"+tt.name, func(t *testing.T) {
			t.Parallel()
			position, has, err := decodeDueCursor(storage.DueCursor(tt.token), "sessions", 100)
			assertCursorFailure(t, err, storage.DueCursorKind, tt.rule)
			if has || position != (dueCursorPosition{}) {
				t.Errorf("rejected token still yielded a position %+v (has %t)", position, has)
			}
		})
	}
}

// TestOrderedCursorAcceptsOnlyItsOwnSpelling is the positive half of the two
// checks above: the forged payloads are rejected for the FIELD that differs, not
// because a hand-assembled token is rejected on principle.
func TestOrderedCursorAcceptsOnlyItsOwnSpelling(t *testing.T) {
	t.Parallel()
	token := forgeCursor(rankedCursorHeader, rankedFields(orderedCursorVersionField, rankedCursorTokenKind, "-7"))
	position, has, err := decodeRankedCursor(storage.RankedCursor(token), "sessions", "catalog")
	if err != nil || !has {
		t.Fatalf("a hand-assembled but canonical token = (%+v, %t, %v), want a position", position, has, err)
	}
	want := rankedCursorPosition{namespace: "sessions", rankingScope: "catalog", rank: -7, stableKey: "key", orderingScope: "host"}
	if position != want {
		t.Fatalf("position = %+v, want %+v", position, want)
	}
	// And the encoder produces exactly that spelling, so the two agree.
	if got := string(encodeRankedCursor(want)); got != token {
		t.Fatalf("encodeRankedCursor = %q, want %q", got, token)
	}
}

func assertCursorFailure(t *testing.T, err error, kind storage.OrderedCursorKind, rule storage.OrderedCursorRule) {
	t.Helper()
	var invalid *storage.InvalidOrderedCursorError
	if !errors.As(err, &invalid) {
		t.Fatalf("error = %v (%T), want *InvalidOrderedCursorError", err, err)
	}
	if invalid.Kind != kind {
		t.Errorf("Kind = %v, want %v", invalid.Kind, kind)
	}
	if invalid.Rule != rule {
		t.Errorf("Rule = %v, want %v", invalid.Rule, rule)
	}
}

// FuzzOrderedCursorDecode fuzzes the only parser in this package that reads
// CALLER-supplied input rather than the store's own bytes. It asserts the two
// properties the decoder owes: it never panics on any string whatsoever, and it
// is the exact inverse of the encoder for every position the encoder can emit.
func FuzzOrderedCursorDecode(f *testing.F) {
	f.Add("", "sessions", "catalog", "key", "host", int64(0), int64(0), int64(0))
	f.Add("fso1:r:", "sessions", "catalog", "key", "host", int64(-1), int64(100), int64(10))
	f.Add(string(encodeRankedCursor(rankedCursorPosition{
		namespace: "sessions", rankingScope: "catalog", rank: 7, stableKey: "k", orderingScope: "host",
	})), "sessions", "catalog", "k", "host", int64(7), int64(1), int64(2))
	f.Add(string(encodeDueCursor(dueCursorPosition{
		namespace: "sessions", dueBound: 100, dueAt: 10, stableKey: "k", orderingScope: "host",
	})), "sessions", "catalog", "k", "host", int64(0), int64(100), int64(10))
	f.Add("fso1:r:"+strings.Repeat("A", 64), "n", "s", "\x00\xff", "..", int64(1<<62), int64(-1<<62), int64(0))
	f.Add("fso9:r:AAAA", "n", "s", "k", "o", int64(0), int64(0), int64(0))

	f.Fuzz(func(t *testing.T, token, namespace, scope, key, orderingScope string, rank, bound, dueAt int64) {
		// 1. No input of any shape may panic, and a rejection is always typed.
		if position, has, err := decodeRankedCursor(storage.RankedCursor(token), namespace, scope); err != nil {
			var invalid *storage.InvalidOrderedCursorError
			if !errors.As(err, &invalid) {
				t.Fatalf("decodeRankedCursor(%q) error = %v (%T), want *InvalidOrderedCursorError", token, err, err)
			}
			if has || position != (rankedCursorPosition{}) {
				t.Fatalf("decodeRankedCursor(%q) returned %+v (has %t) alongside an error", token, position, has)
			}
		}
		if position, has, err := decodeDueCursor(storage.DueCursor(token), namespace, bound); err != nil {
			var invalid *storage.InvalidOrderedCursorError
			if !errors.As(err, &invalid) {
				t.Fatalf("decodeDueCursor(%q) error = %v (%T), want *InvalidOrderedCursorError", token, err, err)
			}
			if has || position != (dueCursorPosition{}) {
				t.Fatalf("decodeDueCursor(%q) returned %+v (has %t) alongside an error", token, position, has)
			}
		}

		// 2. decode(encode(p)) == p, for every position the encoder can emit that
		//    fits under the pre-decode length cap.
		rankedPosition := rankedCursorPosition{
			namespace: namespace, rankingScope: scope, rank: rank,
			stableKey: storage.StableKey(key), orderingScope: orderingScope,
		}
		if rankedToken := encodeRankedCursor(rankedPosition); len(rankedToken) <= maxOrderedCursorBytes {
			got, has, err := decodeRankedCursor(rankedToken, namespace, scope)
			if err != nil || !has {
				t.Fatalf("round trip %+v: decode(%q) = (%+v, %t, %v), want the position back", rankedPosition, rankedToken, got, has, err)
			}
			if got != rankedPosition {
				t.Fatalf("round trip: got %+v, want %+v", got, rankedPosition)
			}
		}
		duePosition := dueCursorPosition{
			namespace: namespace, dueBound: bound, dueAt: dueAt,
			stableKey: storage.StableKey(key), orderingScope: orderingScope,
		}
		if dueToken := encodeDueCursor(duePosition); len(dueToken) <= maxOrderedCursorBytes {
			got, has, err := decodeDueCursor(dueToken, namespace, bound)
			if err != nil || !has {
				t.Fatalf("round trip %+v: decode(%q) = (%+v, %t, %v), want the position back", duePosition, dueToken, got, has, err)
			}
			if got != duePosition {
				t.Fatalf("round trip: got %+v, want %+v", got, duePosition)
			}
		}
	})
}
