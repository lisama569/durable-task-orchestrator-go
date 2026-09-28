//go:build !darwin && !dragonfly && !freebsd && !linux && !netbsd && !openbsd

package store

import "os"

// flock is a no-op on platforms without advisory POSIX locks.
func flock(f *os.File) error   { return nil }
func funlock(f *os.File) error { return nil }
