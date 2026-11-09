#!/bin/sh
# usage: three/loader.sh BINARY ARGS...
#
# Runs one benchmark client in its own transient systemd scope, in the
# benchmark slice: pinned to GOCQL_LOADER_CPU (default CPU 6, core 6 of the
# CCD that holds the nodes; its SMT sibling stays idle) and limited to half a
# CPU with CFS bandwidth control (cpu.max 5000/10000: 5 ms per 10 ms period, a
# short period so that throttling does not add whole 50 ms stalls). Every
# driver runs through this wrapper, so they get the same budget. The client
# verifies its own thread affinity against GOCQL_BENCH_CPUS. From inside the
# scope the wrapper writes to stderr the scope's enforced settings
# (LOADER-CGROUP: cpu.max, cpuset.cpus.effective) and, after the client exits,
# its CPU accounting over the client's lifetime (LOADER-CPU: usage_usec,
# nr_periods, nr_throttled, throttled_usec, elapsed_usec); three/bench.py
# checks both for every measured run.
set -eu
CPU=${GOCQL_LOADER_CPU:-6}
QUOTA=${GOCQL_LOADER_QUOTA:-50%}
SLICE=${GOCQL_THREE_SLICE:-gocqlbench.slice}
exec sudo -n systemd-run --quiet --scope --collect --expand-environment=no --slice="$SLICE" \
	-p CPUQuota="$QUOTA" -p CPUQuotaPeriodSec=10ms -p AllowedCPUs="$CPU" \
	--uid="$(id -u)" --gid="$(id -g)" -- \
	env GOMAXPROCS=1 GOCQL_BENCH_CPUS="$CPU" GOCQL_BENCH_GOMAXPROCS=1 HOME="$HOME" PATH="$PATH" \
	sh -c '
	cg=/sys/fs/cgroup$(sed -n "s/^0:://p" /proc/self/cgroup)
	stat() { awk -v k="$1" "\$1 == k {print \$2}" "$cg/cpu.stat"; }
	echo "LOADER-CGROUP path=${cg#/sys/fs/cgroup} cpu.max=$(tr " " / <"$cg/cpu.max") cpuset=$(cat "$cg/cpuset.cpus.effective")" >&2
	u0=$(stat usage_usec) p0=$(stat nr_periods) n0=$(stat nr_throttled) t0=$(stat throttled_usec) e0=$(date +%s%6N)
	rc=0
	taskset -c "$0" "$@" || rc=$?
	e1=$(date +%s%6N)
	echo "LOADER-CPU usage_usec=$(($(stat usage_usec) - u0)) nr_periods=$(($(stat nr_periods) - p0)) nr_throttled=$(($(stat nr_throttled) - n0)) throttled_usec=$(($(stat throttled_usec) - t0)) elapsed_usec=$((e1 - e0))" >&2
	exit $rc
	' "$CPU" "$@"
