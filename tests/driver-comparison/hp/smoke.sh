#!/bin/sh
# usage: HP_DIR=... smoke.sh  -- emulator correctness smoke for every scenario.
# Requires the emulator (tests/driver-comparison/emulator) on 127.0.0.2-4 with
# stats on 127.0.0.1:19080. Its failure counter must not change.
set -eu
. "$(dirname "$0")/env.sh"
cd "$HP_REPO"
GOWORK=off go -C tests/driver-comparison build -o "$HP_DIR/cmp-v2" .
stat() { curl -s 127.0.0.1:19080/stats | sed 's/.*"failures":\([0-9]*\).*/\1/'; }
before=$(stat)
for sc in read write paged; do for st in false true; do
	timeout 120 taskset -c 8 env GOMAXPROCS=1 GOCQL_BENCH_CPUS=8 GOCQL_BENCH_GOMAXPROCS=1 "$HP_DIR/cmp-v2" -hosts 127.0.0.2,127.0.0.3,127.0.0.4 -mode check -scenario $sc -stream=$st >/dev/null 2>"$HP_DIR/smoke-$sc-$st.log" || { echo "SMOKE FAIL $sc stream=$st"; tail -5 "$HP_DIR/smoke-$sc-$st.log"; exit 1; }
done; done
after=$(stat)
[ "$before" = "$after" ] || { echo "SMOKE FAIL emulator failures $before -> $after"; exit 1; }
echo SMOKE-OK
