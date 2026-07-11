package fsstore

import (
	"testing"

	"github.com/looprig/storage"
	"github.com/looprig/storage/storetest"
)

// TestLedgerConformance runs the shared storage ledger conformance suite against
// the filesystem ledger backend. A fresh newLedgerStore over a per-call t.TempDir
// yields a fresh, empty ledger, so every suite subtest is fully independent.
func TestLedgerConformance(t *testing.T) {
	t.Parallel()
	storetest.TestLedger(t, func(t *testing.T) storage.Ledger {
		s, err := newLedgerStore(t.TempDir())
		if err != nil {
			t.Fatalf("newLedgerStore: %v", err)
		}
		return s
	})
}

// TestKVConformance runs the shared storage KV conformance suite against the
// filesystem KV backend. A fresh newKVStore over a per-call t.TempDir yields a
// fresh, empty key space, so every suite subtest is fully independent.
func TestKVConformance(t *testing.T) {
	t.Parallel()
	storetest.TestKV(t, func(t *testing.T) storage.KV {
		s, err := newKVStore(t.TempDir())
		if err != nil {
			t.Fatalf("newKVStore: %v", err)
		}
		return s
	})
}

// TestBlobsConformance runs the shared storage Blobs conformance suite against
// the filesystem Blobs backend. A fresh newBlobStore over a per-call t.TempDir
// yields a fresh, empty key space, so every suite subtest is fully independent.
func TestBlobsConformance(t *testing.T) {
	t.Parallel()
	storetest.TestBlobs(t, func(t *testing.T) storage.Blobs {
		s, err := newBlobStore(t.TempDir())
		if err != nil {
			t.Fatalf("newBlobStore: %v", err)
		}
		return s
	})
}

// TestLeaserConformance runs the shared storage leaser conformance suite against
// the filesystem leaser backend. A fresh newLeaserStore over a per-call t.TempDir
// yields a fresh, empty lease slot per name, so every suite subtest is independent.
// The held->LeaseHeldError case works in-process because flock binds to the open
// file description: a second Acquire opens a FRESH fd, whose non-blocking LOCK_EX
// conflicts with the first even within this one process.
func TestLeaserConformance(t *testing.T) {
	t.Parallel()
	storetest.TestLeaser(t, func(t *testing.T) storage.Leaser {
		s, err := newLeaserStore(t.TempDir())
		if err != nil {
			t.Fatalf("newLeaserStore: %v", err)
		}
		return s
	})
}
