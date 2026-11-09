#!/bin/sh
# usage: three/loader.sh BINARY ARGS...
#
# Runs one benchmark client in its own transient systemd scope, in the
# benchmark slice: pinned to GOCQL_LOADER_CPU (default CPU 6, core 6 of the
# CCD that holds the nodes; its SMT sibling stays idle) and limited to half a
# CPU with CFS bandwidth control (cpu.max 5000/10000: 5 ms per 10 ms period, a
# short period so that throttling does not add whole 50 ms stalls). Every
# driver runs through this wrapper, so they get the same budget. The client
# verifies its own thread affinity against GOCQL_BENCH_CPUS.
set -eu
CPU=${GOCQL_LOADER_CPU:-6}
QUOTA=${GOCQL_LOADER_QUOTA:-50%}
SLICE=${GOCQL_THREE_SLICE:-gocqlbench.slice}
exec sudo -n systemd-run --quiet --scope --collect --slice="$SLICE" \
	-p CPUQuota="$QUOTA" -p CPUQuotaPeriodSec=10ms -p AllowedCPUs="$CPU" \
	--uid="$(id -u)" --gid="$(id -g)" -- \
	env GOMAXPROCS=1 GOCQL_BENCH_CPUS="$CPU" GOCQL_BENCH_GOMAXPROCS=1 HOME="$HOME" PATH="$PATH" \
	taskset -c "$CPU" "$@"
