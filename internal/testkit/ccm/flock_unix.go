// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

//go:build unix

package ccm

import (
	"errors"
	"os"
	"syscall"
)

func tryLockFile(f *os.File) error {
	err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if errors.Is(err, syscall.EWOULDBLOCK) {
		return errLocked
	}
	return err
}

func unlockFile(f *os.File) { _ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN) }

// processAlive reports whether pid still runs.
func processAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	err := syscall.Kill(pid, 0)
	return err == nil || errors.Is(err, syscall.EPERM)
}
