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

func TestBlobReaderGetReturnsConcreteReaderWithoutBoundClaim(t *testing.T) {
	t.Parallel()
	store, err := newBlobStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := any(store).(interface{ BlobReaderCloseBound() time.Duration }); ok {
		t.Fatal("fsstore must not claim a finite Blob reader close bound")
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

func TestBlobReaderReadAndCloseAreRaceSafe(t *testing.T) {
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
		go func() { closeDone <- reader.Close() }()
		close(resume)

		firstClose := receiveBlobReaderTestResult(t, closeDone, "first Close")
		if firstClose != nil {
			t.Fatalf("attempt %d first Close = %v", attempt, firstClose)
		}
		readErr := receiveBlobReaderTestResult(t, readDone, "active Read")
		if !errors.Is(readErr, fs.ErrClosed) || readErr == io.EOF {
			t.Fatalf("attempt %d concurrent Read ended with %v, want non-EOF fs.ErrClosed", attempt, readErr)
		}
		secondCloseDone := make(chan error, 1)
		go func() { secondCloseDone <- reader.Close() }()
		secondClose := receiveBlobReaderTestResult(t, secondCloseDone, "second Close")
		if (firstClose == nil) != (secondClose == nil) || (firstClose != nil && !errors.Is(firstClose, secondClose) && !errors.Is(secondClose, firstClose)) {
			t.Fatalf("attempt %d Close results are not stable: first=%v second=%v", attempt, firstClose, secondClose)
		}
		if n, err := reader.Read(make([]byte, 1)); n != 0 || !errors.Is(err, fs.ErrClosed) || err == io.EOF {
			t.Fatalf("attempt %d post-Close Read = %d, %v", attempt, n, err)
		}
	}
}

// receiveBlobReaderTestResult is only a test-suite stall guard; it is not a
// provider lifecycle promise and is deliberately unrelated to production API.
func receiveBlobReaderTestResult(t *testing.T, result <-chan error, operation string) error {
	t.Helper()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	select {
	case err := <-result:
		return err
	case <-timer.C:
		t.Fatalf("%s did not finish within the test stall guard", operation)
		return nil
	}
}
