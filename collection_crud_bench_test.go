package gocb

import (
	"fmt"
	"sync/atomic"
	"testing"
)

func BenchmarkUpsert(b *testing.B) {
	b.ReportAllocs()
	// Generate 256 bytes of random data for the document
	randomBytes := make([]byte, 256)
	for i := 0; i < len(randomBytes); i++ {
		randomBytes[i] = byte(i)
	}

	b.RunParallel(func(pb *testing.PB) {
		var i uint32
		for pb.Next() {
			keyNum := atomic.AddUint32(&i, 1)
			_, err := globalCollection.Upsert(fmt.Sprintf("upsert-%d", keyNum), randomBytes, nil)
			if err != nil {
				b.Fatalf("failed to upsert %d: %v", keyNum, err)
			}
			atomic.AddUint32(&i, 1)
		}
	})
}

func BenchmarkReplace(b *testing.B) {
	b.ReportAllocs()
	// Generate 256 bytes of random data for the document
	randomBytes := make([]byte, 256)
	for i := 0; i < len(randomBytes); i++ {
		randomBytes[i] = byte(i)
	}

	_, err := globalCollection.Upsert("upsert-replace-1", randomBytes, nil)
	if err != nil {
		b.Fatalf("failed to upsert %v", err)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := globalCollection.Replace("upsert-replace-1", randomBytes, nil)
			if err != nil {
				b.Fatalf("failed to replace %v", err)
			}
		}
	})
}

func BenchmarkGet(b *testing.B) {
	b.ReportAllocs()
	// Generate 256 bytes of random data for the document
	randomBytes := make([]byte, 256)
	for i := 0; i < len(randomBytes); i++ {
		randomBytes[i] = byte(i)
	}

	_, err := globalCollection.Upsert("upsert-get-1", randomBytes, nil)
	if err != nil {
		b.Fatalf("failed to upsert %v", err)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err = globalCollection.Get("upsert-get-1", nil)
			if err != nil {
				b.Fatalf("failed to get %v", err)
			}
		}
	})
}

func BenchmarkExists(b *testing.B) {
	b.ReportAllocs()
	// Generate 256 bytes of random data for the document
	randomBytes := make([]byte, 256)
	for i := 0; i < len(randomBytes); i++ {
		randomBytes[i] = byte(i)
	}

	_, err := globalCollection.Upsert("upsert-exists-1", randomBytes, nil)
	if err != nil {
		b.Fatalf("failed to upsert %v", err)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err = globalCollection.Exists("upsert-exists-1", nil)
			if err != nil {
				b.Fatalf("failed to exists %v", err)
			}
		}
	})
}

func BenchmarkGetFromReplica(b *testing.B) {
	b.ReportAllocs()
	// Generate 256 bytes of random data for the document
	randomBytes := make([]byte, 256)
	for i := 0; i < len(randomBytes); i++ {
		randomBytes[i] = byte(i)
	}

	_, err := globalCollection.Upsert("upsert-replica-1", randomBytes, nil)
	if err != nil {
		b.Fatalf("failed to upsert %v", err)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err = globalCollection.GetFromReplica("upsert-replica-1", 1, nil)
			if err != nil {
				b.Fatalf("failed to get replica %v", err)
			}
		}
	})
}

func BenchmarkGetAndTouch(b *testing.B) {
	b.ReportAllocs()
	// Generate 256 bytes of random data for the document
	randomBytes := make([]byte, 256)
	for i := 0; i < len(randomBytes); i++ {
		randomBytes[i] = byte(i)
	}

	_, err := globalCollection.Upsert("upsert-get-and-touch-1", randomBytes, nil)
	if err != nil {
		b.Fatalf("failed to upsert %v", err)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err = globalCollection.GetAndTouch("upsert-get-and-touch-1", 10, nil)
			if err != nil {
				b.Fatalf("failed to get and touch %v", err)
			}
		}
	})
}

func BenchmarkTouch(b *testing.B) {
	b.ReportAllocs()
	// Generate 256 bytes of random data for the document
	randomBytes := make([]byte, 256)
	for i := 0; i < len(randomBytes); i++ {
		randomBytes[i] = byte(i)
	}

	_, err := globalCollection.Upsert("upsert-touch-1", randomBytes, nil)
	if err != nil {
		b.Fatalf("failed to upsert %v", err)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err = globalCollection.Touch("upsert-touch-1", 10, nil)
			if err != nil {
				b.Fatalf("failed to touch %v", err)
			}
		}
	})
}
