package events

import (
	"strings"
	"testing"
)

// Every cluster event type is a cluster event and has a CLUSTER<...> name; the
// session event and values outside the enum are neither.
func TestEventTypeClassification(t *testing.T) {
	t.Parallel()

	cluster := []EventType{
		ClusterEventTypeTopologyChange,
		ClusterEventTypeStatusChange,
		ClusterEventTypeSchemaChangeKeyspace,
		ClusterEventTypeSchemaChangeTable,
		ClusterEventTypeSchemaChangeType,
		ClusterEventTypeSchemaChangeFunction,
		ClusterEventTypeSchemaChangeAggregate,
		ClusterEventTypeClientRoutesChanged,
	}
	seen := map[string]bool{}
	for _, et := range cluster {
		name := et.String()
		if !et.IsClusterEvent() || !strings.HasPrefix(name, "CLUSTER<") {
			t.Fatalf("%d: IsClusterEvent=%v String=%q, want a cluster event", et, et.IsClusterEvent(), name)
		}
		if seen[name] {
			t.Fatalf("%d: name %q shared with another event type", et, name)
		}
		seen[name] = true
	}

	session := SessionEventTypeControlConnectionRecreated
	if session.IsClusterEvent() || !strings.HasPrefix(session.String(), "SESSION<") {
		t.Fatalf("session event: IsClusterEvent=%v String=%q", session.IsClusterEvent(), session.String())
	}

	unknown := EventType(1000)
	if unknown.IsClusterEvent() || unknown.String() != "UNKNOWN(1000)" {
		t.Fatalf("unknown event: IsClusterEvent=%v String=%q", unknown.IsClusterEvent(), unknown.String())
	}
}
