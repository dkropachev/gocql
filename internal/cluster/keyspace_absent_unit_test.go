package cluster

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/scylladb/gocql/v2/cqlerr"
	implMetadata "github.com/scylladb/gocql/v2/internal/metadata"
)

// A keyspace that system_schema no longer lists is schema-refresh control
// flow, not an error a caller sees, which is why the driver has no public
// sentinel for it. The refreshes a public call runs turn it into a dropped
// keyspace. Only RefreshV2Keyspace returns it, to the post-DDL refresh in
// internal/session and to the schema event handler, which both drop it.
func TestKeyspaceAbsenceStaysInternal(t *testing.T) {
	t.Parallel()

	noSentinel := func(t *testing.T, s *Core, err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("refresh of a dropped keyspace failed: %v", err)
		}
		if _, ok := s.Metadata().Keyspace("ks2"); ok {
			t.Fatal("dropped keyspace still published")
		}
		for _, diagnostic := range s.Metadata().Diagnostics() {
			if errors.Is(diagnostic, implMetadata.InternalErrKeyspaceDoesNotExist) {
				t.Fatalf("diagnostic reports the internal sentinel: %v", diagnostic)
			}
		}
	}

	// Session.RefreshMetadata runs this refresh after the topology one.
	t.Run("FullRefresh", func(t *testing.T) {
		t.Parallel()
		s, mock := refreshAllSchemaSession(t)
		mock.mu.Lock()
		delete(mock.knownKeyspaces, "ks2")
		mock.mu.Unlock()
		noSentinel(t, s, s.RefreshV2AllSchema(context.Background()))
	})

	// The keyspace is listed, then dropped while its tables are read: the
	// re-read of system_schema.keyspaces finds no row.
	t.Run("DroppedDuringFullRefresh", func(t *testing.T) {
		t.Parallel()
		s, mock := refreshAllSchemaSession(t)
		var once sync.Once
		mock.iterFor = func(statement string) *nativeResultFixture {
			if strings.HasPrefix(statement, "SELECT * FROM system_schema.tables WHERE keyspace_name = ?") {
				once.Do(func() {
					mock.mu.Lock()
					delete(mock.knownKeyspaces, "ks2")
					mock.mu.Unlock()
				})
			}
			return nil
		}
		noSentinel(t, s, s.RefreshV2AllSchema(context.Background()))
	})

	// The single-keyspace refresh reports the drop to its internal callers.
	// The sentinel wraps cqlerr.ErrNoKeyspace, so it classifies as that
	// public sentinel should it ever escape.
	t.Run("SingleKeyspace", func(t *testing.T) {
		t.Parallel()
		s, mock := refreshAllSchemaSession(t)
		mock.mu.Lock()
		delete(mock.knownKeyspaces, "ks2")
		mock.mu.Unlock()
		err := s.RefreshV2Keyspace(context.Background(), "ks2")
		if !errors.Is(err, implMetadata.InternalErrKeyspaceDoesNotExist) {
			t.Fatalf("RefreshV2Keyspace = %v, want the internal sentinel", err)
		}
		if !errors.Is(err, cqlerr.ErrNoKeyspace) || cqlerr.KindOf(err) != cqlerr.KindInvalid {
			t.Fatalf("sentinel does not classify as cqlerr.ErrNoKeyspace: %v", err)
		}
		if _, ok := s.Metadata().Keyspace("ks2"); ok {
			t.Fatal("dropped keyspace still published")
		}
	})
}
