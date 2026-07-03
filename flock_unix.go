//go:build unix

package fsstore

import (
	"os"
	"syscall"
)

// lockFileEx takes an exclusive (LOCK_EX) advisory lock on f, blocking until it
// is granted. The lock is associated with the open file description, so it is
// released by unlockFile or automatically when the process exits or the fd is
// closed — a crashed holder never leaves a stale lock behind.
func lockFileEx(f *os.File) error {
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		return &FlockError{Op: "lock", Path: f.Name(), Cause: err}
	}
	return nil
}

// unlockFile releases the advisory lock held on f (LOCK_UN).
func unlockFile(f *os.File) error {
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_UN); err != nil {
		return &FlockError{Op: "unlock", Path: f.Name(), Cause: err}
	}
	return nil
}
