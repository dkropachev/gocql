//go:build integration

package integration

import (
	"sort"
)

func makeClientRoutePairs(connectionIDs, hostIDs []string) []pair {
	pairs := make([]pair, 0, len(connectionIDs)*len(hostIDs))
	for _, connectionID := range connectionIDs {
		for _, hostID := range hostIDs {
			pairs = append(pairs, pair{InternalConnectionID: connectionID, InternalHostID: hostID})
		}
	}
	return pairs
}

func filterClientRoutesByHostID(routes []clientRoute, hostID string) []clientRoute {
	var filtered []clientRoute
	for _, route := range routes {
		if route.InternalHostID == hostID {
			filtered = append(filtered, route)
		}
	}
	return filtered
}

func sortClientRoutes(xs []clientRoute) {
	sort.Slice(xs, func(i, j int) bool {
		a, b := xs[i], xs[j]

		if a.InternalConnectionID != b.InternalConnectionID {
			return a.InternalConnectionID < b.InternalConnectionID // or bytes.Compare if raw [16]byte
		}
		return a.InternalHostID < b.InternalHostID
	})
}
