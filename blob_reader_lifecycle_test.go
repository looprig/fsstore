package fsstore

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"testing"
	"time"
)

func TestBlobReaderLifecycleBoundAndConcreteReader(t *testing.T) {
	t.Parallel()
	store, err := newBlobStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if bound := store.BlobReaderCloseBound(); bound <= 0 || bound > time.Second {
		t.Fatalf("BlobReaderCloseBound = %v, want positive and at most 1s", bound)
	}
	const key = "blobs/lifecycle-concrete"
	if err := store.Put(context.Background(), key, bytes.NewReader([]byte("body"))); err != nil {
		t.Fatal(err)
	}
	reader, err := store.Get(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	if reader == nil {
		t.Fatal("Get returned nil reader with nil error")
	}
	if _, ok := reader.(*blobFileReader); !ok {
		t.Fatalf("Get reader = %T, want *blobFileReader", reader)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestBlobReaderImmediateCloseIsStableAndTerminal(t *testing.T) {
	t.Parallel()
	store, err := newBlobStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	const key = "blobs/lifecycle-immediate"
	if err := store.Put(context.Background(), key, bytes.NewReader([]byte("unread"))); err != nil {
		t.Fatal(err)
	}
	reader, err := store.Get(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	first := reader.Close()
	second := reader.Close()
	if first != second {
		t.Fatalf("Close results differ: first=%v second=%v", first, second)
	}
	for _, p := range [][]byte{make([]byte, 1), nil} {
		if n, err := reader.Read(p); n != 0 || !errors.Is(err, fs.ErrClosed) || err == io.EOF {
			t.Fatalf("Read after Close = %d, %v; want 0, non-EOF fs.ErrClosed", n, err)
		}
	}
}

func TestBlobReaderCloseErrorIsStableAndStillPublishesClosed(t *testing.T) {
	t.Parallel()
	path := t.TempDir() + "/reader"
	if err := os.WriteFile(path, []byte("body"), 0o600); err != nil {
		t.Fatal(err)
	}
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	reader := &blobFileReader{file: file}
	first := reader.Close()
	second := reader.Close()
	if first == nil || first != second {
		t.Fatalf("Close results = %v / %v, want same non-nil error", first, second)
	}
	if n, err := reader.Read(make([]byte, 1)); n != 0 || !errors.Is(err, fs.ErrClosed) || err == io.EOF {
		t.Fatalf("Read after failed underlying Close = %d, %v", n, err)
	}
}

func TestBlobReaderReadAndCloseAreConcurrentSafe(t *testing.T) {
	t.Parallel()
	store, err := newBlobStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	const key = "blobs/lifecycle-concurrent"
	content := bytes.Repeat([]byte("x"), 8<<20)
	if err := store.Put(context.Background(), key, bytes.NewReader(content)); err != nil {
		t.Fatal(err)
	}
	reader, err := store.Get(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		var one [1]byte
		close(started)
		for {
			_, err := reader.Read(one[:])
			if err != nil {
				result <- err
				return
			}
		}
	}()
	<-started
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if !errors.Is(err, fs.ErrClosed) || err == io.EOF {
			t.Fatalf("concurrent Read ended with %v, want non-EOF fs.ErrClosed", err)
		}
	case <-time.After(store.BlobReaderCloseBound()):
		t.Fatal("Read remained active after Close bound")
	}
	if n, err := reader.Read(make([]byte, 1)); n != 0 || !errors.Is(err, fs.ErrClosed) || err == io.EOF {
		t.Fatalf("post-Close Read = %d, %v", n, err)
	}
}
