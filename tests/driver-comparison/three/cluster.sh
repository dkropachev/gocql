#!/bin/sh
# usage: three/cluster.sh start|stop|restart|status|flush|clearsnapshot|settle
#
# Three Scylla nodes for the three-driver comparison, four shards each, every
# node pinned to two whole physical cores (both SMT threads). The CPU sets
# come from GOCQL_THREE_NODE_CPUS, one per node; the defaults fit an AMD
# Ryzen 9 9950X3D, whose first CCD holds cores 0-7 as CPUs 0-7 and 16-23:
# node1 cores 0-1, node2 cores 2-3, node3 cores 4-5, and three/loader.sh puts
# the client on core 6, so every vCPU in the test is the same core type on
# the same L3. GOCQL_CONTAINER picks the runtime (docker, or "sudo podman").
# Developer mode skips I/O calibration; every driver sees the same cluster.
set -eu
IMAGE=${GOCQL_CLUSTER_IMAGE:-docker.io/scylladb/scylla:2026.3.3}
CTR=${GOCQL_CONTAINER:-docker}
NET=gocql-bench3
SUBNET=172.30.8.0/24
CPUS=${GOCQL_THREE_NODE_CPUS:-0,1,16,17 2,3,18,19 4,5,20,21}
SEED=172.30.8.2
NODES=$(i=1; for c in $CPUS; do printf '%s:%s:172.30.8.%s ' $i "$c" $((i + 1)); i=$((i + 1)); done)
# Containers go to a slice of their own, so that host-wide CPU confinement of
# other work (three/host.sh) does not apply to them.
SLICE=${GOCQL_THREE_SLICE:-gocqlbench.slice}
# 1 GiB per shard; the dataset (100 MiB point, 512 MiB paged) stays in cache.
SCYLLA_MEMORY=${GOCQL_THREE_SCYLLA_MEMORY:-4G}
CONTAINER_MEMORY=${GOCQL_THREE_CONTAINER_MEMORY:-6g}

start() {
	$CTR network inspect $NET >/dev/null 2>&1 || $CTR network create --subnet $SUBNET $NET >/dev/null
	for spec in $NODES; do
		n=${spec%%:*}; rest=${spec#*:}; cpus=${rest%%:*}; ip=${rest#*:}
		$CTR rm -f gocql-bench3-$n >/dev/null 2>&1 || true
		$CTR run -d --name gocql-bench3-$n --network $NET --ip $ip --cgroup-parent $SLICE \
			--cpuset-cpus $cpus --memory $CONTAINER_MEMORY --memory-swap $CONTAINER_MEMORY \
			$IMAGE --smp 4 --memory $SCYLLA_MEMORY --overprovisioned 0 --developer-mode 1 \
			--seeds $SEED --listen-address $ip --rpc-address $ip --broadcast-rpc-address $ip \
			--api-address 0.0.0.0 >/dev/null
		wait_up $n "$ip" "$n"
	done
	wait_all
}

# restart stops every node and starts them again, seed first, so that each
# measurement group begins with empty memtables and caches.
restart() {
	# drain flushes memtables and stops accepting writes, so that the nodes
	# shut down cleanly instead of being killed with hints and commitlog
	# segments pending (podman's SIGTERM alone left two of three nodes running
	# past a 60 s stop timeout).
	for spec in $NODES; do $CTR exec gocql-bench3-${spec%%:*} nodetool drain >/dev/null; done
	for spec in $NODES; do $CTR stop -t 120 gocql-bench3-${spec%%:*} >/dev/null; done
	for spec in $NODES; do
		n=${spec%%:*}; rest=${spec#*:}; ip=${rest#*:}
		$CTR start gocql-bench3-$n >/dev/null
		wait_up $n "$ip" "$n"
	done
	wait_all
}

wait_all() {
	for spec in $NODES; do
		n=${spec%%:*}; rest=${spec#*:}; ip=${rest#*:}
		wait_up $n "$ip" 3
	done
	$CTR exec gocql-bench3-1 nodetool status | grep -E '^(UN|DN|UJ)'
}

wait_up() { # node ip expected-UN-count
	i=0
	while [ $i -lt 300 ]; do
		count=$($CTR exec gocql-bench3-$1 nodetool status 2>/dev/null | grep -c '^UN' || true)
		if [ "${count:-0}" -ge "$3" ] && $CTR exec gocql-bench3-$1 cqlsh "$2" -e 'SELECT now() FROM system.local' >/dev/null 2>&1; then
			return 0
		fi
		i=$((i + 1)); sleep 2
	done
	echo "node $1 did not come up" >&2
	$CTR logs --tail 30 gocql-bench3-$1 >&2
	exit 1
}

# clearsnapshot removes the benchmark keyspace's snapshots, which every
# TRUNCATE takes (auto_snapshot), so that resets do not accumulate them.
clearsnapshot() {
	for spec in $NODES; do $CTR exec gocql-bench3-${spec%%:*} nodetool clearsnapshot -- bench_compare >/dev/null; done
}

# flush writes memtables to disk on every node.
flush() {
	for spec in $NODES; do $CTR exec gocql-bench3-${spec%%:*} nodetool flush >/dev/null; done
}

# settle waits until no node has pending or running compactions.
settle() {
	i=0
	while [ $i -lt 600 ]; do
		busy=0
		for spec in $NODES; do
			p=$($CTR exec gocql-bench3-${spec%%:*} nodetool compactionstats 2>/dev/null | awk '/pending tasks/ {print $3}')
			[ "${p:-0}" = 0 ] || busy=1
		done
		[ $busy = 0 ] && return 0
		i=$((i + 1)); sleep 2
	done
	echo "compactions did not settle" >&2
}

stop() {
	for spec in $NODES; do $CTR rm -f gocql-bench3-${spec%%:*} >/dev/null 2>&1 || true; done
	$CTR network rm $NET >/dev/null 2>&1 || true
}

case "${1:-}" in
start) start ;;
stop) stop ;;
restart) restart ;;
status) $CTR exec gocql-bench3-1 nodetool status ;;
flush) flush ;;
clearsnapshot) clearsnapshot ;;
settle) settle ;;
*) echo "usage: $0 start|stop|restart|status|flush|clearsnapshot|settle" >&2; exit 2 ;;
esac
