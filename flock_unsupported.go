//go:build !unix

package fsstore

import "os"

// lockFileEx fails closed on a platform without advisory file locking: fsstore
// cannot guarantee two processes will not corrupt a shared file, so it refuses
// to proceed rather than degrade to a silent no-op that would permit corruption.
func lockFileEx(f *os.File) error {
	return &FlockError{Op: "lock", Path: f.Name(), Cause: errFlockUnsupported}
}

// unlockFile mirrors lockFileEx: with no advisory locking to release, it reports
// the same fail-closed error so no caller mistakes a no-op for success.
func unlockFile(f *os.File) error {
	return &FlockError{Op: "unlock", Path: f.Name(), Cause: errFlockUnsupported}
}
