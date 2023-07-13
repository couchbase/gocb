package gocb

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

func BenchmarkBulkUpsert(b *testing.B) {
	b.ReportAllocs()
	// Generate 256 bytes of random data for the document
	randomBytes := make([]byte, 256)
	for i := 0; i < len(randomBytes); i++ {
		randomBytes[i] = byte(i)
	}
	doc := benchDoc{Data: randomBytes}

	// We need to ensure that connections are up and ready before starting the benchmark
	err := globalBucket.WaitUntilReady(5*time.Second, &WaitUntilReadyOptions{
		ServiceTypes: []ServiceType{ServiceTypeKeyValue},
	})
	if err != nil {
		b.Fatalf("Wait until ready failed: %v", err)
	}

	bulkSize := 200

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			var ops []BulkOp
			for i := 0; i < bulkSize; i++ {
				ops = append(ops, &UpsertOp{
					ID:    uuid.NewString()[:6],
					Value: doc,
				})
			}

			err := globalCollection.Do(ops, nil)
			if err != nil {
				b.Fatalf("failed to bulk upsert %v", err)
				return
			}

			for _, op := range ops {
				item := op.(*UpsertOp)
				if item.Err != nil {
					b.Fatalf("bulk upsert op failed %s: %v", item.ID, err)
				}
			}
		}
	})
}
