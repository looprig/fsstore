//go:build !unix

package fsstore

import (
	"errors"
	"os"
)

// errFlockUnsupported is the leaf cause reported on a platform that provides no
// advisory file locking. It is a sentinel with no context fields.
//
// It lives beside the only code that returns it. Declared in the portable
// flock.go it was invisible to every unix build -- staticcheck reported it as
// unused (U1000) there, and deleting it on that evidence would have broken the
// !unix build outright.
var errFlockUnsupported = errors.New("fsstore: advisory file locking is not supported on this platform")

// lockFileEx fails closed on a platform without advisory file locking: fsstore
// cannot guarantee two processes will not corrupt a shared file, so it refuses
// to proceed rather than degrade to a silent no-op that would permit corruption.
func lockFileEx(f *os.File) error {
	return &FlockError{Op: "lock", Path: f.Name(), Cause: errFlockUnsupported}
}

// tryLockFileEx fails closed on a platform without advisory file locking, mirroring
// lockFileEx: it cannot prove exclusivity, so it reports (false, *FlockError)
// rather than pretend the lock was granted (which would let two holders believe
// they each own the same lease). The Leaser surfaces this as a hard error, never
// as a *LeaseHeldError.
func tryLockFileEx(f *os.File) (acquired bool, err error) {
	return false, &FlockError{Op: "trylock", Path: f.Name(), Cause: errFlockUnsupported}
}

// unlockFile mirrors lockFileEx: with no advisory locking to release, it reports
// the same fail-closed error so no caller mistakes a no-op for success.
func unlockFile(f *os.File) error {
	return &FlockError{Op: "unlock", Path: f.Name(), Cause: errFlockUnsupported}
}
