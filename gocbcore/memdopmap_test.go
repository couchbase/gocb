package gocbcore

import "testing"

func TestOpMap(t *testing.T) {
	var rd memdOpMap

	testOp1 := &memdQRequest{
		memdRequest: memdRequest{},
	}
	testOp2 := &memdQRequest{
		memdRequest: memdRequest{},
	}

	// Single Remove
	rd.Add(testOp1)
	if rd.Remove(testOp1) != true {
		t.Fatalf("The op should be there")
	}
	if rd.Remove(testOp1) != false {
		t.Fatalf("There should be nothing to remove")
	}

	// Single opaque remove
	rd.Add(testOp1)
	if rd.FindAndMaybeRemove(testOp1.Opaque, false) != testOp1 {
		t.Fatalf("The op should have been found")
	}
	if rd.FindAndMaybeRemove(testOp1.Opaque, false) != nil {
		t.Fatalf("The op should not have been there")
	}

	// In order remove
	rd.Add(testOp1)
	rd.Add(testOp2)
	if rd.Remove(testOp1) != true {
		t.Fatalf("The op should be there")
	}
	if rd.Remove(testOp2) != true {
		t.Fatalf("The op should be there")
	}
	if rd.Remove(testOp1) != false {
		t.Fatalf("There should be nothing to remove")
	}
	if rd.Remove(testOp2) != false {
		t.Fatalf("There should be nothing to remove")
	}

	// Out of order remove
	rd.Add(testOp1)
	rd.Add(testOp2)
	if rd.Remove(testOp2) != true {
		t.Fatalf("The op should be there")
	}
	if rd.Remove(testOp1) != true {
		t.Fatalf("The op should be there")
	}
	if rd.Remove(testOp2) != false {
		t.Fatalf("There should be nothing to remove")
	}
	if rd.Remove(testOp1) != false {
		t.Fatalf("There should be nothing to remove")
	}

	// In order opaque remove
	rd.Add(testOp1)
	rd.Add(testOp2)
	if rd.FindAndMaybeRemove(testOp1.Opaque, false) != testOp1 {
		t.Fatalf("The op should have been found")
	}
	if rd.FindAndMaybeRemove(testOp2.Opaque, false) != testOp2 {
		t.Fatalf("The op should have been found")
	}
	if rd.FindAndMaybeRemove(testOp1.Opaque, false) != nil {
		t.Fatalf("The op should not have been there")
	}
	if rd.FindAndMaybeRemove(testOp2.Opaque, false) != nil {
		t.Fatalf("The op should not have been there")
	}

	// Out of order opaque remove
	rd.Add(testOp1)
	rd.Add(testOp2)
	if rd.FindAndMaybeRemove(testOp2.Opaque, false) != testOp2 {
		t.Fatalf("The op should have been found")
	}
	if rd.FindAndMaybeRemove(testOp1.Opaque, false) != testOp1 {
		t.Fatalf("The op should have been found")
	}
	if rd.FindAndMaybeRemove(testOp2.Opaque, false) != nil {
		t.Fatalf("The op should not have been there")
	}
	if rd.FindAndMaybeRemove(testOp1.Opaque, false) != nil {
		t.Fatalf("The op should not have been there")
	}

	// Drain
	rd.Add(testOp2)
	rd.Add(testOp1)
	found1 := 0
	found2 := 0
	rd.Drain(func(op *memdQRequest) {
		if op == testOp1 {
			found1++
		}
		if op == testOp2 {
			found2++
		}
	})
	if found1 != 1 || found2 != 1 {
		t.Fatalf("Drain behaved incorrected")
	}
}
