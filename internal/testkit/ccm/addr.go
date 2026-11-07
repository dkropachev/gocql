// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package ccm

import (
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Address prefixes 127.0.N. for N in [firstSlot, lastSlot] are handed to
// created clusters. 127.0.0. to 127.0.9. stay free for the clusters make
// scylla-start and cassandra-start create (127.0.1., 127.0.2.) and anything
// else a developer runs.
const (
	firstSlot = 10
	lastSlot  = 250
)

// prefixLease is a claimed address prefix.
type prefixLease struct {
	file   *os.File
	Prefix string
	Slot   int
}

// Release frees the prefix for other clusters.
func (l *prefixLease) Release() {
	if l == nil || l.file == nil {
		return
	}
	unlockFile(l.file)
	l.file.Close()
	l.file = nil
	inProcess.Lock()
	delete(inProcess.slots, l.Slot)
	inProcess.Unlock()
}

// inProcess guards slots claimed by this process: flock is per open file,
// so a second open of the same lock file in one process would succeed.
var inProcess = struct {
	slots map[int]bool
	sync.Mutex
}{slots: map[int]bool{}}

// DefaultLockDir is $GOCQL_CCM_LOCKDIR or <tmp>/gocql-ccm-locks.
func DefaultLockDir() string {
	if d := os.Getenv("GOCQL_CCM_LOCKDIR"); d != "" {
		return d
	}
	return filepath.Join(os.TempDir(), "gocql-ccm-locks")
}

// claimPrefix claims the first free address prefix: one whose lock file no
// other process holds and on which nothing listens.
func claimPrefix(lockDir string) (*prefixLease, error) {
	if err := os.MkdirAll(lockDir, 0o777); err != nil {
		return nil, fmt.Errorf("ccm: address lock dir: %w", err)
	}
	for slot := firstSlot; slot <= lastSlot; slot++ {
		l, err := lockSlot(lockDir, slot)
		if errors.Is(err, errLocked) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if addressInUse(l.Prefix + "1") {
			// A cluster nobody locked (left over from a crashed run, or
			// created by hand) still runs there.
			l.Release()
			continue
		}
		return l, nil
	}
	return nil, fmt.Errorf("ccm: no free address prefix in 127.0.%d. to 127.0.%d.", firstSlot, lastSlot)
}

// lockSlot takes the lock of one address slot; errLocked when another
// process or another claim in this one holds it.
func lockSlot(lockDir string, slot int) (*prefixLease, error) {
	inProcess.Lock()
	if inProcess.slots[slot] {
		inProcess.Unlock()
		return nil, errLocked
	}
	inProcess.slots[slot] = true
	inProcess.Unlock()
	release := func() {
		inProcess.Lock()
		delete(inProcess.slots, slot)
		inProcess.Unlock()
	}
	f, err := os.OpenFile(filepath.Join(lockDir, fmt.Sprintf("127.0.%d.lock", slot)), os.O_CREATE|os.O_RDWR, 0o666)
	if err != nil {
		release()
		return nil, fmt.Errorf("ccm: address lock: %w", err)
	}
	if err := tryLockFile(f); err != nil {
		f.Close()
		release()
		if errors.Is(err, errLocked) {
			return nil, errLocked
		}
		return nil, fmt.Errorf("ccm: address lock: %w", err)
	}
	return &prefixLease{Slot: slot, Prefix: fmt.Sprintf("127.0.%d.", slot), file: f}, nil
}

var errLocked = errors.New("locked")

func addressInUse(ip string) bool {
	for _, port := range []string{"9042", "7000", "10000"} {
		c, err := net.DialTimeout("tcp", net.JoinHostPort(ip, port), 200*time.Millisecond)
		if err == nil {
			c.Close()
			return true
		}
	}
	return false
}
