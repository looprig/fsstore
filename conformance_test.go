package fsstore

import (
	"testing"

	"github.com/ciram-co/storekit"
	"github.com/ciram-co/storekit/storetest"
)

// TestLedgerConformance runs the shared storekit ledger conformance suite against
// the filesystem ledger backend. A fresh newLedgerStore over a per-call t.TempDir
// yields a fresh, empty ledger, so every suite subtest is fully independent.
func TestLedgerConformance(t *testing.T) {
	t.Parallel()
	storetest.TestLedger(t, func(t *testing.T) storekit.Ledger {
		s, err := newLedgerStore(t.TempDir())
		if err != nil {
			t.Fatalf("newLedgerStore: %v", err)
		}
		return s
	})
}
