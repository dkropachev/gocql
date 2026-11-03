#!/bin/sh
# usage: HP_DIR=... profile.sh LABEL CASE   (CASE e.g. pread/w64)
# Clean CPU profile and a separate alloc profile; never combined, since a low
# memprofilerate distorts the CPU profile.
set -eu
. "$(dirname "$0")/env.sh"
bin=$HP_GATES/$1.root.test; p=$HP_DIR/prof/$1.$(echo "$2" | tr / -)
[ -x "$bin" ] || hp_build "$1"
taskset -c 4 "$bin" -test.run '^$' -test.bench "BenchmarkReusableQueryLoopback/$2\$" -test.benchmem -test.cpu 1 -test.benchtime 5s -test.cpuprofile "$p.cpu" | grep Benchmark
taskset -c 4 "$bin" -test.run '^$' -test.bench "BenchmarkReusableQueryLoopback/$2\$" -test.benchmem -test.cpu 1 -test.benchtime 3s -test.memprofile "$p.mem" -test.memprofilerate 512 | grep Benchmark
echo "$p.cpu $p.mem"
