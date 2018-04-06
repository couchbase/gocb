package gocb

import "testing"

func TestReplaceCas(t *testing.T) {
	origCas, err := globalBucket.Upsert("replaceCas", "hello", 0)
	if err != nil {
		t.Fatalf("Failed to setup main document %v", err)
	}

	INVALID_CAS := origCas + 0xFECA
	_, err = globalBucket.Replace("replaceCas", "lol", INVALID_CAS, 0)
	if err == nil {
		t.Fatalf("Replace expected to have failed %v", err)
	}

	_, err = globalBucket.Replace("replaceCas", "world", origCas, 0)
	if err != nil {
		t.Fatalf("Replace expected to succeed %v", err)
	}
}
