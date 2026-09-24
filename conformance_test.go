package fsstore

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/looprig/storage"
	"github.com/looprig/storage/storetest"
)

type fsOrderedCursorProbe struct{}

var _ storetest.OrderedCursorProbe = fsOrderedCursorProbe{}

func (fsOrderedCursorProbe) MalformedCursor(t *testing.T, kind storage.OrderedCursorKind) string {
	t.Helper()
	return "fsstore-malformed-" + kind.String()
}

func (fsOrderedCursorProbe) UnknownVersionCursor(t *testing.T, kind storage.OrderedCursorKind) string {
	t.Helper()
	var token string
	switch kind {
	case storage.RankedCursorKind:
		token = string(encodeRankedCursor(rankedCursorPosition{
			namespace: "sessions", rankingScope: "catalog", rank: 1,
			stableKey: "cursor", orderingScope: "commands",
		}))
	case storage.DueCursorKind:
		token = string(encodeDueCursor(dueCursorPosition{
			namespace: "sessions", dueBound: 10, dueAt: 1,
			stableKey: "cursor", orderingScope: "commands",
		}))
	default:
		t.Fatalf("unsupported cursor kind %v", kind)
	}
	return strings.Replace(token, "fso1:", "fso2:", 1)
}

func TestOrderedIndexConformance(t *testing.T) {
	t.Parallel()
	storetest.TestOrderedIndex(t, func(t *testing.T) storage.OrderedIndex {
		index, err := newOrderedIndex(t.TempDir())
		if err != nil {
			t.Fatalf("newOrderedIndex: %v", err)
		}
		t.Cleanup(func() {
			if cerr := index.Close(); cerr != nil {
				t.Errorf("close ordered index: %v", cerr)
			}
		})
		return index
	}, fsOrderedCursorProbe{}, storetest.OrderedIndexCounterFunc(assertFSOrderedIndexCounters))
}

func assertFSOrderedIndexCounters(t *testing.T, ctx context.Context, index storage.OrderedIndex) {
	t.Helper()
	x, ok := index.(*orderedIndex)
	if !ok {
		t.Fatalf("ordered index is %T, want *orderedIndex", index)
	}
	const (
		namespace = "counter/pages"
		records   = 60
	)
	for i := range records {
		id := storage.OrderedID{Namespace: namespace, OrderingScope: "commands", StableKey: storage.StableKey("key-" + strconv.Itoa(i))}
		if _, _, err := x.Create(ctx, id, "catalog", []byte("value"), storage.Rank{Ranked: true, Value: int64(i)}, storage.Due{State: storage.DueAt, UnixMillis: int64(i)}); err != nil {
			t.Fatalf("Create(%d): %v", i, err)
		}
	}
	otherNamespaceID := storage.OrderedID{Namespace: "counter/other", OrderingScope: "commands", StableKey: "other"}
	if _, _, err := x.Create(ctx, otherNamespaceID, "catalog", []byte("other"), storage.Rank{}, storage.Due{State: storage.NotDue}); err != nil {
		t.Fatalf("Create(other namespace): %v", err)
	}
	settled := x.stats()
	if settled.AppliedFrames != records+1 {
		t.Fatalf("aggregate applied frames = %d, want %d across both namespaces", settled.AppliedFrames, records+1)
	}
	if settled.ScannedBytes != 0 {
		t.Fatalf("writer scanned %d historical bytes after its own creates, want 0", settled.ScannedBytes)
	}
	for range 5 {
		if _, err := x.ListOrdered(ctx, namespace, "commands", 0, 7); err != nil {
			t.Fatalf("ListOrdered: %v", err)
		}
		if _, err := x.ListRanked(ctx, namespace, "catalog", "", 7); err != nil {
			t.Fatalf("ListRanked: %v", err)
		}
		if _, err := x.ListDue(ctx, namespace, records, "", 7); err != nil {
			t.Fatalf("ListDue: %v", err)
		}
	}
	warm := x.stats()
	if warm != settled {
		t.Fatalf("warm pages changed aggregate query work from %+v to %+v", settled, warm)
	}

	path := filepath.Join(x.store.dir, filepath.FromSlash(namespace)+orderedLogExt)
	beforeInfo, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat ordered log: %v", err)
	}
	other, err := newOrderedIndex(x.store.root)
	if err != nil {
		t.Fatalf("new second ordered index: %v", err)
	}
	t.Cleanup(func() {
		if cerr := other.Close(); cerr != nil {
			t.Errorf("close second ordered index: %v", cerr)
		}
	})
	id := storage.OrderedID{Namespace: namespace, OrderingScope: "commands", StableKey: "foreign-tail"}
	if _, _, err := other.Create(ctx, id, "catalog", []byte("tail"), storage.Rank{Ranked: true, Value: 1000}, storage.Due{State: storage.DueAt, UnixMillis: 1}); err != nil {
		t.Fatalf("second handle Create: %v", err)
	}
	afterInfo, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat ordered log after foreign append: %v", err)
	}
	tailBytes := afterInfo.Size() - beforeInfo.Size()
	if tailBytes <= 0 {
		t.Fatalf("foreign append grew log by %d bytes, want positive tail", tailBytes)
	}
	if _, err := x.ListRanked(ctx, namespace, "catalog", "", 1); err != nil {
		t.Fatalf("ListRanked after foreign append: %v", err)
	}
	caught := x.stats()
	if got := caught.ScannedBytes - warm.ScannedBytes; got != tailBytes {
		t.Fatalf("warm page after foreign append scanned %d bytes, want only %d new tail bytes", got, tailBytes)
	}
	if got := caught.AppliedFrames - warm.AppliedFrames; got != 1 {
		t.Fatalf("warm page after foreign append applied %d frames, want 1", got)
	}
}

// TestOrderedIndexProductionHasNoUnapprovedDirectoryEnumeration makes the
// filesystem-walk half of the bounded-page contract structural. Byte/frame
// counters detect prefix replay but cannot observe directory-entry reads, so
// package-qualified standard enumeration calls require an explicit allowlist.
func TestOrderedIndexProductionHasNoUnapprovedDirectoryEnumeration(t *testing.T) {
	t.Parallel()
	paths, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("glob ordered production files: %v", err)
	}
	parsed := 0
	allowedWalkImplementations := 0
	allowedWalkCalls := 0
	allowedLegacyScanImplementations := 0
	allowedLegacyScanCalls := 0
	for _, path := range paths {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		fileSet := token.NewFileSet()
		file, err := parser.ParseFile(fileSet, path, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		parsed++
		imports := make(map[string]string)
		for _, spec := range file.Imports {
			importPath, err := strconv.Unquote(spec.Path.Value)
			if err != nil {
				t.Fatalf("unquote import in %s: %v", path, err)
			}
			name := filepath.Base(importPath)
			if spec.Name != nil {
				name = spec.Name.Name
			}
			if name == "." && (importPath == "path/filepath" || importPath == "io/fs" || importPath == "os") {
				t.Errorf("%s dot-imports directory API package %q; enumeration calls would be unqualified", path, importPath)
			}
			imports[name] = importPath
		}
		for _, declaration := range file.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok || function.Body == nil {
				continue
			}
			ast.Inspect(function.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				if ident, ok := call.Fun.(*ast.Ident); ok && ident.Name == "findLegacyLeaf" {
					if path == "fsstore.go" && function.Name.Name == "refuseLegacyLayout" {
						allowedLegacyScanCalls++
						return true
					}
					position := fileSet.Position(ident.Pos())
					t.Errorf("unapproved findLegacyLeaf call: %s:%d %s", path, position.Line, function.Name.Name)
					return true
				}
				if ident, ok := call.Fun.(*ast.Ident); ok && ident.Name == "walkKeys" {
					if (path == "kv.go" && function.Name.Name == "Keys") ||
						(path == "blobs.go" && function.Name.Name == "List") {
						allowedWalkCalls++
						return true
					}
					position := fileSet.Position(ident.Pos())
					t.Errorf("unapproved walkKeys call: %s:%d %s", path, position.Line, function.Name.Name)
					return true
				}
				selector, ok := call.Fun.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				ident, ok := selector.X.(*ast.Ident)
				if !ok {
					return true // a local method call, not a package-qualified API
				}
				qualifier := imports[ident.Name]
				forbidden := (qualifier == "path/filepath" && (selector.Sel.Name == "Walk" || selector.Sel.Name == "WalkDir" || selector.Sel.Name == "Glob")) ||
					(qualifier == "io/fs" && (selector.Sel.Name == "WalkDir" || selector.Sel.Name == "ReadDir" || selector.Sel.Name == "Glob")) ||
					(qualifier == "os" && selector.Sel.Name == "ReadDir")
				if !forbidden {
					return true
				}
				// walkKeys is the one intentional full-tree implementation, shared
				// only by KV.Keys and Blobs.List. OrderedIndex's Get/List methods
				// and withView path do not call it.
				if path == "fswrite.go" && function.Name.Name == "walkKeys" && qualifier == "path/filepath" && selector.Sel.Name == "WalkDir" {
					allowedWalkImplementations++
					return true
				}
				// findLegacyLeaf is Open's one-shot pre-v0.6.0 layout scan of
				// kv/ and blobs/; it never runs on an OrderedIndex path.
				if path == "fswrite.go" && function.Name.Name == "findLegacyLeaf" && qualifier == "path/filepath" && selector.Sel.Name == "WalkDir" {
					allowedLegacyScanImplementations++
					return true
				}
				position := fileSet.Position(selector.Pos())
				t.Errorf("unapproved directory enumeration: %s:%d %s calls %s.%s", path, position.Line, function.Name.Name, ident.Name, selector.Sel.Name)
				return true
			})
		}
	}
	if parsed == 0 {
		t.Fatal("no production files parsed")
	}
	if allowedWalkImplementations != 1 {
		t.Fatalf("approved fswrite.go/walkKeys filepath.WalkDir calls = %d, want 1", allowedWalkImplementations)
	}
	if allowedWalkCalls != 2 {
		t.Fatalf("approved KV.Keys/Blobs.List walkKeys calls = %d, want 2", allowedWalkCalls)
	}
	if allowedLegacyScanImplementations != 1 {
		t.Fatalf("approved fswrite.go/findLegacyLeaf filepath.WalkDir calls = %d, want 1", allowedLegacyScanImplementations)
	}
	if allowedLegacyScanCalls != 2 {
		t.Fatalf("approved refuseLegacyLayout findLegacyLeaf calls = %d, want 2", allowedLegacyScanCalls)
	}
}

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

// TestLeaserLifecycleConformance exercises the released storage lifecycle suite
// with genuinely separate leaserStore values over one filesystem root. A local
// flock grant has no TTL to refresh, so Renew is the successful no-op that keeps
// its fd-held epoch live; Expire deterministically releases that fd for the
// suite's expiry/reclaim scenarios.
func TestLeaserLifecycleConformance(t *testing.T) {
	t.Parallel()
	storetest.TestLeaserLifecycle(t, func(t *testing.T) storetest.LeaserLifecycleHarness {
		root := t.TempDir()
		primary, err := newLeaserStore(root)
		if err != nil {
			t.Fatalf("new primary leaser store: %v", err)
		}
		var nextViewID uint64 = 1
		return storetest.LeaserLifecycleHarness{
			Primary:       primary,
			PrimaryViewID: nextViewID,
			OpenIndependent: func(t *testing.T) storetest.LeaserLifecycleClient {
				t.Helper()
				independent, err := newLeaserStore(root)
				if err != nil {
					t.Fatalf("new independent leaser store: %v", err)
				}
				nextViewID++
				return storetest.LeaserLifecycleClient{Leaser: independent, ViewID: nextViewID}
			},
			Renew: func(t *testing.T, lease storage.Lease) {
				t.Helper()
				if _, ok := lease.(*fsLease); !ok {
					t.Fatalf("lease is %T, want *fsLease", lease)
				}
			},
			Expire: func(t *testing.T, lease storage.Lease) {
				t.Helper()
				if err := lease.Release(context.Background()); err != nil {
					t.Fatalf("expire filesystem lease: %v", err)
				}
			},
		}
	})
}
