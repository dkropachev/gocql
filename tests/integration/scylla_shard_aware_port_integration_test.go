//go:build integration

package integration

import (
	"context"
	"net"

	gocql "github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/endpoint"
)

func makeIntegrationShardAwareCluster() shardAwareTestCluster {
	config := createV2Cluster()
	return shardAwareTestCluster{
		useTLS: config.TLS != nil || config.TLSForHost != nil,
		port:   shardTestContactPort(config.ContactPoints, config.Port),
		connect: func(ctx context.Context, dialer legacyDialer, disableShardAwarePort bool) (*gocql.Session, error) {
			config.Dialer = endpoint.DialerFunc(func(ctx context.Context, target endpoint.DialTarget) (net.Conn, error) {
				if target.LocalAddress.IsValid() {
					ctx = context.WithValue(ctx, scyllaSourcePortCtx{}, target.LocalAddress.Port())
				}
				return dialer.DialContext(ctx, target.Network, target.Address)
			})
			config.Pool.ShardAwarePort = !disableShardAwarePort
			return gocql.Connect(ctx, config)
		},
	}
}
