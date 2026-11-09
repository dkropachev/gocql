// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package ccm

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
)

// Runner runs one ccm command against the clusters in configDir. args are
// the ccm arguments without --config-dir, for example {"node1", "stop"}.
type Runner interface {
	Run(ctx context.Context, configDir string, args ...string) ([]byte, error)
}

// CommandError is a ccm command that failed.
type CommandError struct {
	Err      error
	Stdout   string
	Stderr   string
	Args     []string
	ExitCode int
}

func (e *CommandError) Error() string {
	msg := strings.TrimSpace(e.Stderr)
	if msg == "" {
		msg = strings.TrimSpace(e.Stdout)
	}
	if len(msg) > 2000 {
		msg = "..." + msg[len(msg)-2000:]
	}
	return fmt.Sprintf("ccm %s: exit %d: %v: %s", strings.Join(e.Args, " "), e.ExitCode, e.Err, msg)
}

func (e *CommandError) Unwrap() error { return e.Err }

// ExecRunner runs the ccm binary.
type ExecRunner struct {
	// Binary is the ccm executable; empty means DefaultBinary().
	Binary string
}

// DefaultBinary is $GOCQL_CCM_BIN, or "ccm" from PATH.
func DefaultBinary() string {
	if b := os.Getenv("GOCQL_CCM_BIN"); b != "" {
		return b
	}
	return "ccm"
}

var lookPath = struct {
	found map[string]error
	sync.Mutex
}{found: map[string]error{}}

// Available reports whether the ccm binary can be found.
func (r ExecRunner) Available() error {
	bin := r.binary()
	lookPath.Lock()
	defer lookPath.Unlock()
	if err, ok := lookPath.found[bin]; ok {
		return err
	}
	_, err := exec.LookPath(bin)
	lookPath.found[bin] = err
	return err
}

func (r ExecRunner) binary() string {
	if r.Binary != "" {
		return r.Binary
	}
	return DefaultBinary()
}

func (r ExecRunner) Run(ctx context.Context, configDir string, args ...string) ([]byte, error) {
	full := withConfigDir(args, configDir)
	cmd := exec.CommandContext(ctx, r.binary(), full...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	// ccm resolves its download cache from CCM_CONFIG_DIR, falling back to
	// ~/.ccm; --config-dir only moves the cluster files. Keep the caller's
	// value so every cluster shares one download cache.
	cmd.Env = os.Environ()
	err := cmd.Run()
	if err != nil {
		code := -1
		if exitErr, ok := errors.AsType[*exec.ExitError](err); ok {
			code = exitErr.ExitCode()
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			err = fmt.Errorf("%w (%w)", ctxErr, err)
		}
		return stdout.Bytes(), &CommandError{Args: full, ExitCode: code, Stdout: stdout.String(), Stderr: stderr.String(), Err: err}
	}
	return stdout.Bytes(), nil
}

// withConfigDir inserts --config-dir right after the command words, before
// any argument ccm passes through to another tool (nodetool, cqlsh).
func withConfigDir(args []string, configDir string) []string {
	if configDir == "" || len(args) == 0 {
		return args
	}
	at := 1
	if isNodeName(args[0]) && len(args) > 1 {
		at = 2
	}
	out := make([]string, 0, len(args)+1)
	out = append(out, args[:at]...)
	out = append(out, "--config-dir="+configDir)
	return append(out, args[at:]...)
}

func isNodeName(s string) bool {
	rest, ok := strings.CutPrefix(s, "node")
	if !ok || rest == "" {
		return false
	}
	for _, r := range rest {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}
