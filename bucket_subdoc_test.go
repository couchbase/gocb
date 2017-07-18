package gocb

import "testing"

func TestSubDocXattrs(t *testing.T) {
	itemMap := make(map[string]string)
	itemMap["x"] = "x value 1"

	_, err := globalBucket.Upsert("subDocXattrs", itemMap, 0)
	if err != nil {
		t.Fatalf("Failed to setup main document %v", err)
	}

	_, err = globalBucket.MutateIn("subDocXattrs", 0, 0).
		UpsertEx("xatest.test", "test value", SubdocFlagXattr|SubdocFlagCreatePath).
		UpsertEx("x", "x value 2", SubdocFlagNone).
		Execute()
	if err != nil {
		t.Fatalf("Failed to sub-doc mutate (%s)", err)
	}

	_, err = globalBucket.MutateInEx("subDocMkDoc", SubdocDocFlagMkDoc, 0, 0).
		UpsertEx("x", "x value 4", SubdocFlagNone).
		Execute()
	if err != nil {
		t.Fatalf("Failed to sub-doc mkdoc mutate (%s)", err)
	}

	res, err := globalBucket.LookupIn("subDocXattrs").
		GetEx("xatest", SubdocFlagXattr).
		GetEx("x", SubdocFlagNone).
		Execute()
	if err != nil {
		t.Fatalf("Failed to sub-doc lookup (%s)", err)
	}

	var xatest map[string]string
	err = res.Content("xatest", &xatest)
	if err != nil {
		t.Fatalf("Failed to get xatest xattr contents (%s)", err)
	}
	if len(xatest) != 1 {
		t.Fatalf("xatest xattr had wrong len")
	}
	if xatest["test"] != "test value" {
		t.Fatalf("xatest test attribute had wrong value")
	}

	var x string
	err = res.Content("x", &x)
	if err != nil {
		t.Fatalf("Failed to get x contents (%s)", err)
	}
	if x != "x value 2" {
		t.Fatalf("document x attribute had wrong value")
	}
}
