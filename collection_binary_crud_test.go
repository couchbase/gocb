package gocb

import "testing"

func TestBinaryAppend(t *testing.T) {
	if !globalCluster.SupportsFeature(AdjoinFeature) {
		t.Skip("Skipping due to serverside bug")
	}
	colBinary := globalCollection.Binary()

	res, err := globalCollection.Upsert("binaryAppend", "foo", nil)
	if err != nil {
		t.Fatalf("Failed to Upsert, err: %v", err)
	}

	if res.Cas() == 0 {
		t.Fatalf("Expected Cas to be non-zero")
	}

	appendRes, err := colBinary.Append("binaryAppend", []byte("bar"), nil)
	if err != nil {
		t.Fatalf("Failed to Append, err: %v", err)
	}

	if appendRes.Cas() == 0 {
		t.Fatalf("Expected Cas to be non-zero")
	}

	appendDoc, err := globalCollection.Get("binaryAppend", nil)
	if err != nil {
		t.Fatalf("Get failed, error was %v", err)
	}

	var appendContent string
	err = appendDoc.Content(&appendContent)
	if err != nil {
		t.Fatalf("Content failed, error was %v", err)
	}

	if appendContent != "foobar" {
		t.Fatalf("Expected append result to be foobar but was %s", appendContent)
	}
}

func TestBinaryPrepend(t *testing.T) {
	if !globalCluster.SupportsFeature(AdjoinFeature) {
		t.Skip("Skipping due to serverside bug")
	}
	colBinary := globalCollection.Binary()

	res, err := globalCollection.Upsert("binaryPrepend", "foo", nil)
	if err != nil {
		t.Fatalf("Failed to Upsert, err: %v", err)
	}

	if res.Cas() == 0 {
		t.Fatalf("Expected Cas to be non-zero")
	}

	appendRes, err := colBinary.Prepend("binaryPrepend", []byte("bar"), nil)
	if err != nil {
		t.Fatalf("Failed to Append, err: %v", err)
	}

	if appendRes.Cas() == 0 {
		t.Fatalf("Expected Cas to be non-zero")
	}

	appendDoc, err := globalCollection.Get("binaryPrepend", nil)
	if err != nil {
		t.Fatalf("Get failed, error was %v", err)
	}

	var appendContent string
	err = appendDoc.Content(&appendContent)
	if err != nil {
		t.Fatalf("Content failed, error was %v", err)
	}

	if appendContent != "barfoo" {
		t.Fatalf("Expected prepend result to be boofar but was %s", appendContent)
	}
}

func TestBinaryIncrement(t *testing.T) {
	colBinary := globalCollection.Binary()

	res, err := colBinary.Increment("binaryIncrement", &CounterOptions{
		Delta: 10,
	})
	if err != nil {
		t.Fatalf("Failed to Increment, err: %v", err)
	}

	if res.Cas() == 0 {
		t.Fatalf("Expected Cas to be non-zero")
	}

	if res.Content() != 0 {
		t.Fatalf("Expected counter value to be 0 but was %d", res.Content())
	}

	res, err = colBinary.Increment("binaryIncrement", &CounterOptions{
		Delta: 10,
	})
	if err != nil {
		t.Fatalf("Failed to Increment, err: %v", err)
	}

	if res.Cas() == 0 {
		t.Fatalf("Expected Cas to be non-zero")
	}

	if res.Content() != 10 {
		t.Fatalf("Expected counter value to be 10 but was %d", res.Content())
	}

	res, err = colBinary.Increment("binaryIncrement", &CounterOptions{
		Delta: 10,
	})
	if err != nil {
		t.Fatalf("Failed to Increment, err: %v", err)
	}

	if res.Cas() == 0 {
		t.Fatalf("Expected Cas to be non-zero")
	}

	if res.Content() != 20 {
		t.Fatalf("Expected counter value to be 20 but was %d", res.Content())
	}

	incrementDoc, err := globalCollection.Get("binaryIncrement", nil)
	if err != nil {
		t.Fatalf("Get failed, error was %v", err)
	}

	var incrementContent int
	err = incrementDoc.Content(&incrementContent)
	if err != nil {
		t.Fatalf("Content failed, error was %v", err)
	}

	if incrementContent != 20 {
		t.Fatalf("Expected counter value to be 20 but was %d", res.Content())
	}
}

func TestBinaryDecrement(t *testing.T) {
	colBinary := globalCollection.Binary()

	res, err := colBinary.Decrement("binaryDecrement", &CounterOptions{
		Delta:   10,
		Initial: 100,
	})
	if err != nil {
		t.Fatalf("Failed to Decrement, err: %v", err)
	}

	if res.Cas() == 0 {
		t.Fatalf("Expected Cas to be non-zero")
	}

	if res.Content() != 100 {
		t.Fatalf("Expected counter value to be 100 but was %d", res.Content())
	}

	res, err = colBinary.Decrement("binaryDecrement", &CounterOptions{
		Delta: 10,
	})
	if err != nil {
		t.Fatalf("Failed to Decrement, err: %v", err)
	}

	if res.Cas() == 0 {
		t.Fatalf("Expected Cas to be non-zero")
	}

	if res.Content() != 90 {
		t.Fatalf("Expected counter value to be 90 but was %d", res.Content())
	}

	res, err = colBinary.Decrement("binaryDecrement", &CounterOptions{
		Delta: 10,
	})
	if err != nil {
		t.Fatalf("Failed to Decrement, err: %v", err)
	}

	if res.Cas() == 0 {
		t.Fatalf("Expected Cas to be non-zero")
	}

	if res.Content() != 80 {
		t.Fatalf("Expected counter value to be 80 but was %d", res.Content())
	}

	incrementDoc, err := globalCollection.Get("binaryDecrement", nil)
	if err != nil {
		t.Fatalf("Get failed, error was %v", err)
	}

	var incrementContent int
	err = incrementDoc.Content(&incrementContent)
	if err != nil {
		t.Fatalf("Content failed, error was %v", err)
	}

	if incrementContent != 80 {
		t.Fatalf("Expected counter value to be 80 but was %d", res.Content())
	}
}
