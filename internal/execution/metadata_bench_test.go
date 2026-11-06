package execution

import (
	"fmt"

	"github.com/scylladb/gocql/v2/internal/cluster"

	"testing"

	implCluster "github.com/scylladb/gocql/v2/internal/cluster"

	implMetadata "github.com/scylladb/gocql/v2/internal/metadata"

	routing "github.com/scylladb/gocql/v2/internal/routing"
)

func BenchmarkMetadataRead(b *testing.B) {
	s := func() *cluster.Session {
		opaqueStorageValuexxxx := new(cluster.Session)
		return opaqueStorageValuexxxx
	}()

	s.Metadata()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s.Metadata()
	}
}

func BenchmarkEventPublication(b *testing.B) {
	s := func() *cluster.Session {
		opaqueStorageValuexxxxx := new(cluster.Session)
		return opaqueStorageValuexxxxx
	}()

	sub, _ := s.Subscribe(cluster.EventFilter{Capacity: 1})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		implCluster.InternalSubscriptionPublish(sub, cluster.Event{Kind: cluster.StatusEvent})
		<-sub.Events()
	}
}

func BenchmarkSchemaSnapshotPublication(b *testing.B) {
	for _, count := range []int{1, 1000} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			ks := &implMetadata.KeyspaceMetadata{Name: "ks", Tables: make(map[string]*implMetadata.TableMetadata, count)}
			for i := 0; i < count; i++ {
				name := fmt.Sprint(i)
				ks.Tables[name] = &implMetadata.TableMetadata{Name: name, Columns: map[string]*implMetadata.ColumnMetadata{"id": {Name: "id", Type: "int"}}}
			}
			s := func() *cluster.Session {
				opaqueStorageValuexxxxxxxxxx := new(cluster.Session)
				implCluster.SessionData(opaqueStorageValuexxxxxxxxxx).InternalMetadataDescriber = &cluster.InternalMetadataDescriber{InternalMetadata: func() *cluster.Metadata {
					opaqueStorageValuexxxxxxxxx := new(cluster.Metadata)
					return opaqueStorageValuexxxxxxxxx
				}(),
				}
				return opaqueStorageValuexxxxxxxxxx
			}()
			implCluster.MetadataData(implCluster.SessionData(s).InternalMetadataDescriber.InternalMetadata).InternalKeyspaceMetadata.InternalSet("ks", ks)
			implCluster.InternalSessionPublishV2Metadata(s)
			prior := routing.SnapshotData(s.Metadata()).InternalKeyspaces["ks"]
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				implCluster.InternalSessionPublishV2Metadata(s)
			}
			b.StopTimer()
			if routing.SnapshotData(s.Metadata()).InternalKeyspaces["ks"] != prior {
				b.Fatal("unchanged schema was cloned")
			}
		})
	}
}
