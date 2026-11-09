#!/bin/sh
# usage: three/host.sh setup|restore|snapshot
#
# Prepares the host for the three-driver comparison and restores it after.
#
#   setup    saves the current settings to $STATE, then
#            - sets every CPU to the performance governor and energy policy and
#              turns boost off, so that every core runs at its fixed base clock
#              and no run is slowed by thermal or power limits mid-way;
#            - confines user.slice, system.slice and init.scope (every other
#              process on the host) to OTHER_CPUS, and gives the benchmark
#              slice, which holds the Scylla containers and the loader, the
#              whole benchmark CCD (BENCH_CPUS). Runtime properties only.
#   restore  puts back what setup saved.
#   snapshot prints the clocks of the benchmark CPUs, the CPU temperature and
#            the busiest processes on the benchmark CPUs outside the slice.
set -eu
BENCH_CPUS=${GOCQL_THREE_BENCH_CPUS:-0-7,16-23}
OTHER_CPUS=${GOCQL_THREE_OTHER_CPUS:-8-15,24-31}
SLICE=${GOCQL_THREE_SLICE:-gocqlbench.slice}
STATE=${GOCQL_THREE_HOST_STATE:-/var/tmp/gocql-three-host.state}
UNITS="user.slice system.slice init.scope"
CPUFREQ=/sys/devices/system/cpu

setup() {
	if [ ! -e "$STATE" ]; then
		{
			echo "boost=$(cat $CPUFREQ/cpufreq/boost 2>/dev/null || echo)"
			for d in $CPUFREQ/cpu[0-9]*/cpufreq; do
				echo "governor $d $(cat $d/scaling_governor)"
				[ -e $d/energy_performance_preference ] && echo "epp $d $(cat $d/energy_performance_preference)"
			done
			for u in $UNITS $SLICE; do
				echo "unit $u $(systemctl show -p AllowedCPUs --value $u)"
			done
		} | sudo tee "$STATE" >/dev/null
	fi
	for d in $CPUFREQ/cpu[0-9]*/cpufreq; do
		echo performance | sudo tee $d/scaling_governor >/dev/null
		[ -e $d/energy_performance_preference ] && { echo performance | sudo tee $d/energy_performance_preference >/dev/null || true; }
	done
	[ -e $CPUFREQ/cpufreq/boost ] && echo 0 | sudo tee $CPUFREQ/cpufreq/boost >/dev/null
	sudo systemctl set-property --runtime $SLICE AllowedCPUs="$BENCH_CPUS"
	for u in $UNITS; do sudo systemctl set-property --runtime $u AllowedCPUs="$OTHER_CPUS"; done
	snapshot
}

restore() {
	[ -e "$STATE" ] || { echo "no saved state in $STATE" >&2; exit 1; }
	while read -r kind a b; do
		case $kind in
		boost=*) v=${kind#boost=}; [ -n "$v" ] && echo "$v" | sudo tee $CPUFREQ/cpufreq/boost >/dev/null ;;
		governor) echo "$b" | sudo tee "$a/scaling_governor" >/dev/null ;;
		epp) echo "$b" | sudo tee "$a/energy_performance_preference" >/dev/null || true ;;
		unit) sudo systemctl set-property --runtime "$a" AllowedCPUs="${b:-}" ;;
		esac
	done <"$STATE"
	sudo rm -f "$STATE"
	echo restored
}

snapshot() {
	echo "boost=$(cat $CPUFREQ/cpufreq/boost 2>/dev/null) governor=$(cat $CPUFREQ/cpu0/cpufreq/scaling_governor)"
	printf 'MHz:'
	for c in $(echo "$BENCH_CPUS" | tr ',' ' '); do
		case $c in *-*) seq_list=$(seq ${c%-*} ${c#*-}) ;; *) seq_list=$c ;; esac
		for i in $seq_list; do printf ' %s=%s' $i $(($(cat $CPUFREQ/cpu$i/cpufreq/scaling_cur_freq) / 1000)); done
	done
	echo
	sensors 2>/dev/null | grep -E '^(Tctl|Tccd[0-9]+):' | tr -s ' ' | tr '\n' ' '
	echo
	for u in $UNITS $SLICE; do printf '%s=%s ' $u "$(systemctl show -p AllowedCPUs --value $u)"; done
	echo
}

case "${1:-}" in
setup) setup ;;
restore) restore ;;
snapshot) snapshot ;;
*) echo "usage: $0 setup|restore|snapshot" >&2; exit 2 ;;
esac
