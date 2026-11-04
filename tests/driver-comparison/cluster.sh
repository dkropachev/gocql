#!/bin/sh
# usage: cluster.sh start|stop|status
#
# A three-node Scylla cluster in Docker with each node pinned to its own
# physical core pair, as in the parked comparison (provision.py, whose
# Podman/Alternator provisioning dependency is no longer available):
# node1 CPUs 0-1, node2 4-5, node3 6-7, two shards each, 3 GiB Scylla
# memory in a 5 GiB container without swap. Clients run on CPU 8 with
# GOMAXPROCS=1 (see cluster_bench.sh). Developer mode skips I/O calibration.
set -eu
IMAGE=${GOCQL_CLUSTER_IMAGE:-scylladb/scylla:2026.3.3}
NET=gocql-bench
SUBNET=172.30.7.0/24
NODES="1:0-1:172.30.7.2 2:4-5:172.30.7.3 3:6-7:172.30.7.4"
SEED=172.30.7.2

start() {
	docker network inspect $NET >/dev/null 2>&1 || docker network create --subnet $SUBNET $NET >/dev/null
	for spec in $NODES; do
		n=${spec%%:*}; rest=${spec#*:}; cpus=${rest%%:*}; ip=${rest#*:}
		docker rm -f gocql-bench-$n >/dev/null 2>&1 || true
		docker run -d --name gocql-bench-$n --network $NET --ip $ip \
			--cpuset-cpus $cpus --memory 5g --memory-swap 5g \
			$IMAGE --smp 2 --memory 3G --overprovisioned 0 --developer-mode 1 \
			--seeds $SEED --listen-address $ip --rpc-address $ip --broadcast-rpc-address $ip \
			--api-address 0.0.0.0 >/dev/null
		# Join one node at a time.
		wait_up $n "$ip" 1
	done
	for spec in $NODES; do
		n=${spec%%:*}
		wait_up $n "" 3
	done
	docker exec gocql-bench-1 nodetool status
}

wait_up() { # node ip expected-UN-count
	i=0
	while [ $i -lt 180 ]; do
		count=$(docker exec gocql-bench-$1 nodetool status 2>/dev/null | grep -c '^UN' || true)
		if [ "${count:-0}" -ge "$3" ]; then
			if [ -z "$2" ] || docker exec gocql-bench-$1 cqlsh "$2" -e 'SELECT now() FROM system.local' >/dev/null 2>&1; then
				return 0
			fi
		fi
		i=$((i + 1)); sleep 2
	done
	echo "node $1 did not come up" >&2
	docker logs --tail 30 gocql-bench-$1 >&2
	exit 1
}

stop() {
	for spec in $NODES; do docker rm -f gocql-bench-${spec%%:*} >/dev/null 2>&1 || true; done
	docker network rm $NET >/dev/null 2>&1 || true
}

case "${1:-}" in
start) start ;;
stop) stop ;;
status) docker exec gocql-bench-1 nodetool status ;;
*) echo "usage: $0 start|stop|status" >&2; exit 2 ;;
esac
