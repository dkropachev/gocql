// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

//go:build !unix

package ccm

import (
	"errors"
	"os"
)

func tryLockFile(*os.File) error { return errors.New("ccm: address locks need a unix system") }

func unlockFile(*os.File) {}

func processAlive(int) bool { return false }
