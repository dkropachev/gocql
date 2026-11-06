//go:build integration

package integration

import (
	"flag"
	"os"
	"strings"
)

func v2SmokeOption(name, fallback string) string {
	if value := os.Getenv("GOCQL_V2_SMOKE_" + strings.ToUpper(name)); value != "" {
		return value
	}
	if f := flag.Lookup(name); f != nil {
		return f.Value.String()
	}
	return fallback
}
