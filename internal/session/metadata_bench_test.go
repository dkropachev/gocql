package session

import (
	"fmt"
	"testing"

	"github.com/scylladb/gocql/v2/event"
	"github.com/scylladb/gocql/v2/internal/cluster"
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	"github.com/scylladb/gocql/v2/internal/routing"
	"github.com/scylladb/gocql/v2/metadata"
)

func BenchmarkMetadata(b *testing.B) {
	b.Run("EventPublication", func(b *testing.B) {
		s := func() *cluster.Core {
			opaqueStorageValuexxxxx := new(cluster.Core)
			return opaqueStorageValuexxxxx
		}()

		sub, _ := s.Subscribe(event.Filter{Capacity: 1})
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			implCluster.InternalSubscriptionPublish(sub, event.Event{Kind: event.KindStatus})
			<-sub.Events()
		}
	})

	b.Run("Read", func(b *testing.B) {
		s := func() *cluster.Core {
			opaqueStorageValuexxxx := new(cluster.Core)
			return opaqueStorageValuexxxx
		}()

		s.Metadata()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = s.Metadata()
		}
	})

	b.Run("SchemaSnapshotPublication", func(b *testing.B) {
		for _, count := range []int{1, 1000} {
			b.Run(fmt.Sprint(count), func(b *testing.B) {
				ks := &metadata.Keyspace{Name: "ks", Tables: make(map[string]*metadata.Table, count)}
				for i := 0; i < count; i++ {
					name := fmt.Sprint(i)
					ks.Tables[name] = &metadata.Table{Name: name, Columns: map[string]*metadata.Column{"id": {Name: "id", Type: "int"}}}
				}
				s := func() *cluster.Core {
					opaqueStorageValuexxxxxxxxxx := new(cluster.Core)
					implCluster.SessionData(opaqueStorageValuexxxxxxxxxx).InternalMetadataDescriber = &cluster.InternalMetadataDescriber{InternalMetadata: func() *cluster.Metadata {
						opaqueStorageValuexxxxxxxxx := new(cluster.Metadata)
						return opaqueStorageValuexxxxxxxxx
					}(),
					}
					return opaqueStorageValuexxxxxxxxxx
				}()
				implCluster.MetadataData(implCluster.SessionData(s).InternalMetadataDescriber.InternalMetadata).InternalKeyspaceMetadata.InternalSet("ks", ks)
				s.PublishV2Metadata()
				prior := routing.SnapshotData(s.Metadata()).InternalKeyspaces["ks"]
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					s.PublishV2Metadata()
				}
				b.StopTimer()
				if routing.SnapshotData(s.Metadata()).InternalKeyspaces["ks"] != prior {
					b.Fatal("unchanged schema was cloned")
				}
			})
		}
	})
}
