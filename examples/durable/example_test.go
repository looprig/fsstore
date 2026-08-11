package durable_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/looprig/fsstore"
	"github.com/looprig/storage"
)

func Example_durableCompositeStore() {
	ctx := context.Background()

	var optionsErr *fsstore.OptionsError
	_, err := fsstore.Open(fsstore.Options{})
	fmt.Println("validated", errors.As(err, &optionsErr), optionsErr.Field)

	temporaryParent, err := os.MkdirTemp("", "fsstore-example-")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(temporaryParent)
	root := filepath.Join(temporaryParent, "store")

	store, err := fsstore.Open(fsstore.Options{Root: root})
	if err != nil {
		panic(err)
	}
	backend := store.Backend()

	const ledgerName = "sessions/example"
	if err := backend.Ledger.Append(ctx, ledgerName, 0, []byte("created")); err != nil {
		panic(err)
	}
	if err := backend.Ledger.Append(ctx, ledgerName, 1, []byte("completed")); err != nil {
		panic(err)
	}
	tip, err := backend.Ledger.Tip(ctx, ledgerName)
	if err != nil {
		panic(err)
	}
	cursor, err := backend.Ledger.Read(ctx, ledgerName, 1)
	if err != nil {
		panic(err)
	}
	record, err := cursor.Next(ctx)
	if err != nil {
		panic(err)
	}
	if err := cursor.Close(); err != nil {
		panic(err)
	}
	fmt.Println("ledger", tip, record.Seq, string(record.Payload))

	const metadataKey = "catalog/example"
	revision, err := backend.KV.Put(ctx, metadataKey, 0, []byte("ready"))
	if err != nil {
		panic(err)
	}
	metadata, persistedRevision, err := backend.KV.Get(ctx, metadataKey)
	if err != nil {
		panic(err)
	}
	fmt.Println("kv", revision, persistedRevision, string(metadata))

	const blobKey = "snapshots/example"
	if err := backend.Blobs.Put(ctx, blobKey, bytes.NewBufferString("snapshot")); err != nil {
		panic(err)
	}
	err = backend.Blobs.Put(ctx, blobKey, bytes.NewBufferString("different"))
	var blobConflict *storage.BlobConflictError
	fmt.Println("blob conflict", errors.As(err, &blobConflict))

	if err := store.Close(); err != nil {
		panic(err)
	}

	reopened, err := fsstore.Open(fsstore.Options{Root: root})
	if err != nil {
		panic(err)
	}
	reopenedTip, err := reopened.Ledger.Tip(ctx, ledgerName)
	if err != nil {
		panic(err)
	}
	reopenedMetadata, reopenedRevision, err := reopened.KV.Get(ctx, metadataKey)
	if err != nil {
		panic(err)
	}
	reader, err := reopened.Blobs.Get(ctx, blobKey)
	if err != nil {
		panic(err)
	}
	reopenedBlob, err := io.ReadAll(reader)
	if err != nil {
		panic(err)
	}
	if err := reader.Close(); err != nil {
		panic(err)
	}
	fmt.Println("reopened", reopenedTip, reopenedRevision, string(reopenedMetadata), string(reopenedBlob))
	if err := reopened.Close(); err != nil {
		panic(err)
	}

	// Output:
	// validated true Root
	// ledger 2 1 created
	// kv 1 1 ready
	// blob conflict true
	// reopened 2 1 ready snapshot
}
