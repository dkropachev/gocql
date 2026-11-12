//go:build integration

package integration

import (
	"context"
	"net"

	gocql "github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/endpoint"
)

func makeIntegrationShardAwareCluster() shardAwareTestCluster {
	return shardAwareTestCluster{
		useTLS: *flagRunSslTest,
		port:   shardTestContactPort(getClusterHosts(), fixturePort()),
		connect: func(ctx context.Context, dialer legacyDialer, noAdvancedShardAwareness bool) (*gocql.Session[testDialect], error) {
			config := createV2Cluster().WithDialer(endpoint.DialerFunc(func(ctx context.Context, target endpoint.DialTarget) (net.Conn, error) {
				if target.LocalAddress.IsValid() {
					ctx = context.WithValue(ctx, scyllaSourcePortCtx{}, target.LocalAddress.Port())
				}
				return dialer.DialContext(ctx, target.Network, target.Address)
			}))
			if noAdvancedShardAwareness {
				config.NoAdvancedShardAwareness()
			}
			built, err := config.Build()
			if err != nil {
				return nil, err
			}
			return gocql.Connect[testDialect](ctx, built)
		},
	}
}
