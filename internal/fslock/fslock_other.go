//go:build !unix

package fslock

import "os"

// flock is a no-op on platforms without flock(2). The LOCK file is still
// created, but mutual exclusion is not enforced - single-writer discipline
// is the operator's responsibility there.
func flock(_ *os.File) error { return nil }
