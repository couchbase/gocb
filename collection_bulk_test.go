package gocb

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestUpsertGetBulk(t *testing.T) {
	var ops []BulkOp
	for i := 0; i < 20; i++ {
		ops = append(ops, &UpsertOp{
			Key:    fmt.Sprintf("%d", i),
			Value:  "test",
			Expiry: 20,
		})
	}

	err := globalCollection.Do(ops, nil)
	if err != nil {
		t.Fatalf("Expected Do to not error for upserts %v", err)
	}

	for _, op := range ops {
		upsertOp, ok := op.(*UpsertOp)
		if !ok {
			t.Fatalf("Could not type assert BulkOp into UpsertOp")
		}

		if upsertOp.Err != nil {
			t.Fatalf("Expected UpsertOp Err to be nil but was %v", upsertOp.Err)
		}

		if upsertOp.Result.Cas() == 0 {
			t.Fatalf("Expected UpsertOp Cas to be non zero")
		}
	}

	var getOps []BulkOp
	for i := 0; i < 20; i++ {
		getOps = append(getOps, &GetOp{
			Key: fmt.Sprintf("%d", i),
		})
	}

	err = globalCollection.Do(getOps, nil)
	if err != nil {
		t.Fatalf("Expected Do to not error for gets %v", err)
	}

	for _, op := range getOps {
		getOp, ok := op.(*GetOp)
		if !ok {
			t.Fatalf("Could not type assert BulkOp into GetOp")
		}

		if getOp.Err != nil {
			t.Fatalf("Expected GetOp Err to be nil but was %v", getOp.Err)
		}

		if getOp.Result.Cas() == 0 {
			t.Fatalf("Expected GetOp Cas to be non zero")
		}

		var val string
		err = getOp.Result.Content(&val)
		if err != nil {
			t.Fatalf("Failed to get content from GetOp %v", err)
		}

		if val != "test" {
			t.Fatalf("Expected GetOp value to be test but was %s", val)
		}
	}
}

func TestBulkUpsertTimeout(t *testing.T) {
	var ops []BulkOp
	for i := 0; i < 20; i++ {
		ops = append(ops, &UpsertOp{
			Key:    fmt.Sprintf("%d", i),
			Value:  "test",
			Expiry: 20,
		})
	}

	err := globalCollection.Do(ops, &BulkOpOptions{
		Timeout: 50 * time.Microsecond,
	})
	if err == nil {
		t.Fatalf("Expected Do to error for upserts")
	}

	if !IsTimeoutError(err) {
		t.Fatalf("Expected Do error to be timeout but was %v", err)
	}

	for _, op := range ops {
		upsertOp, ok := op.(*UpsertOp)
		if !ok {
			t.Fatalf("Could not type assert BulkOp into UpsertOp")
		}

		if upsertOp.Err == nil {
			t.Fatalf("Expected UpsertOp Err to be not nil")
		}

		if !IsTimeoutError(upsertOp.Err) {
			t.Fatalf("Expected UpsertOp error to be timeout but was %v", upsertOp.Err)
		}

		if upsertOp.Result != nil {
			t.Fatalf("Expected UpsertOp Result to be nil")
		}
	}
}

func TestBulkUpsertContextTimeout(t *testing.T) {
	var ops []BulkOp
	for i := 0; i < 20; i++ {
		ops = append(ops, &UpsertOp{
			Key:    fmt.Sprintf("%d", i),
			Value:  "test",
			Expiry: 20,
		})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Microsecond)
	defer cancel()

	err := globalCollection.Do(ops, &BulkOpOptions{
		Context: ctx,
	})
	if err == nil {
		t.Fatalf("Expected Do to error for upserts")
	}

	if !IsTimeoutError(err) {
		t.Fatalf("Expected Do error to be timeout but was %v", err)
	}

	for _, op := range ops {
		upsertOp, ok := op.(*UpsertOp)
		if !ok {
			t.Fatalf("Could not type assert BulkOp into UpsertOp")
		}

		if upsertOp.Err == nil {
			t.Fatalf("Expected UpsertOp Err to be not nil")
		}

		if !IsTimeoutError(upsertOp.Err) {
			t.Fatalf("Expected UpsertOp error to be timeout but was %v", upsertOp.Err)
		}

		if upsertOp.Result != nil {
			t.Fatalf("Expected UpsertOp Result to be nil")
		}
	}
}
