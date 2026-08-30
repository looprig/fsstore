package fsstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
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
	content := bytes.Repeat([]byte("x"), 1<<20)
	if err := store.Put(context.Background(), key, bytes.NewReader(content)); err != nil {
		t.Fatal(err)
	}
	for attempt := range 20 {
		reader, err := store.Get(context.Background(), key)
		if err != nil {
			t.Fatal(err)
		}
		started := make(chan struct{})
		resume := make(chan struct{})
		readDone := make(chan error, 1)
		go func() {
			var one [1]byte
			n, err := reader.Read(one[:])
			if n != 1 || err != nil {
				readDone <- fmt.Errorf("initial Read = %d, %v", n, err)
				close(started)
				return
			}
			close(started)
			<-resume
			for {
				_, err := reader.Read(one[:])
				if err != nil {
					readDone <- err
					return
				}
			}
		}()
		<-started

		closeDone := make(chan error, 1)
		deadline := time.Now().Add(store.BlobReaderCloseBound())
		go func() { closeDone <- reader.Close() }()
		close(resume)

		if err := receiveBlobLifecycleBefore(t, closeDone, deadline, "Close"); err != nil {
			t.Fatalf("attempt %d Close = %v", attempt, err)
		}
		readErr := receiveBlobLifecycleBefore(t, readDone, deadline, "active Read")
		if !errors.Is(readErr, fs.ErrClosed) || readErr == io.EOF {
			t.Fatalf("attempt %d concurrent Read ended with %v, want non-EOF fs.ErrClosed", attempt, readErr)
		}
		if n, err := reader.Read(make([]byte, 1)); n != 0 || !errors.Is(err, fs.ErrClosed) || err == io.EOF {
			t.Fatalf("attempt %d post-Close Read = %d, %v", attempt, n, err)
		}
	}
}

func receiveBlobLifecycleBefore(t *testing.T, result <-chan error, deadline time.Time, operation string) error {
	t.Helper()
	remaining := time.Until(deadline)
	if remaining <= 0 {
		t.Fatalf("%s did not finish within BlobReaderCloseBound", operation)
	}
	timer := time.NewTimer(remaining)
	defer timer.Stop()
	select {
	case err := <-result:
		return err
	case <-timer.C:
		t.Fatalf("%s did not finish within BlobReaderCloseBound", operation)
		return nil
	}
}
