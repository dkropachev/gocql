#!/bin/sh
# usage: HP_DIR=... profile.sh LABEL CASE   (CASE e.g. pread/w64)
# Clean CPU profile and a separate alloc profile; never combined, since a low
# memprofilerate distorts the CPU profile.
set -eu
. "$(dirname "$0")/env.sh"
p=$HP_DIR/prof/$1.$(echo "$2" | tr / -)
[ -x "$HP_GATES/$1.exec.test" ] || hp_build "$1"
bin=$(hp_bin "$1")
sel='^BenchmarkReusableQuery$/^Loopback$'
"$bin" -test.list '^BenchmarkReusableQuery$' | grep -q . || sel='^BenchmarkReusableQueryLoopback$'
taskset -c 4 "$bin" -test.run '^$' -test.bench "$sel/$2\$" -test.benchmem -test.cpu 1 -test.benchtime 5s -test.cpuprofile "$p.cpu" | grep Benchmark
taskset -c 4 "$bin" -test.run '^$' -test.bench "$sel/$2\$" -test.benchmem -test.cpu 1 -test.benchtime 3s -test.memprofile "$p.mem" -test.memprofilerate 512 | grep Benchmark
echo "$p.cpu $p.mem"
