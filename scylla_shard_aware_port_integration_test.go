//go:build integration
// +build integration

package gocql

import "testing"

func TestShardAwarePortIntegrationNoReconnections(t *testing.T) {
	testShardAwarePortNoReconnections(t, func() *ClusterConfig {
		c := createCluster()
		c.Port = 9042
		return c
	})
}

func TestShardAwarePortIntegrationMaliciousNAT(t *testing.T) {
	testShardAwarePortMaliciousNAT(t, func() *ClusterConfig {
		c := createCluster()
		c.Port = 9042
		return c
	})
}

func TestShardAwarePortIntegrationUnreachable(t *testing.T) {
	testShardAwarePortUnreachable(t, func() *ClusterConfig {
		c := createCluster()
		c.Port = 9042
		return c
	})
}

func TestShardAwarePortIntegrationUnusedIfNotEnabled(t *testing.T) {
	testShardAwarePortUnusedIfNotEnabled(t, func() *ClusterConfig {
		c := createCluster()
		c.Port = 9042
		return c
	})
}
