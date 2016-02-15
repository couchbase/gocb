package gocbcore

import (
	"encoding/json"
	"io/ioutil"
	"testing"
)

func getConfig(t *testing.T, filename string) (cfg *routeConfig) {
	s, err := ioutil.ReadFile(filename)
	if err != nil {
		t.Fatal(err.Error())
	}
	rawCfg, err := parseConfig(s, "localhost")
	if err != nil {
		t.Fatal(err.Error())
	}

	cfg = buildRouteConfig(rawCfg, false)
	return
}

func TestGoodConfig(t *testing.T) {
	// Parsing an old 2.x config
	cfg := getConfig(t, "testdata/full_25.json")
	if cfg.bktType != BktTypeCouchbase {
		t.Fatal("Wrong bucket type!")
	}
	if cfg.numReplicas != 1 {
		t.Fatal("Expected 2 replicas!")
	}
}

func TestBadConfig(t *testing.T) {
	// Has an empty vBucket map
	cfg := getConfig(t, "testdata/bad.json")
	if cfg.IsValid() {
		t.Fatal("Config without vbuckets should be invalid!")
	}
}

func TestKetama(t *testing.T) {
	cfg := getConfig(t, "testdata/memd_4node.config.json")
	if cfg.bktType != BktTypeMemcached {
		t.Fatal("Wrong bucket type!")
	}
	if len(cfg.kvServerList) != 4 {
		t.Fatal("Expected 4 nodes!")
	}
	if len(cfg.ketamaMap) != 160*4 {
		t.Fatal("Bad length for ketama map")
	}

	type routeEnt struct {
		Hash  uint32 `json:"hash"`
		Index uint32 `json:"index"`
	}
	exp := make(map[string]routeEnt)
	rawExp, err := ioutil.ReadFile("testdata/memd_4node.exp.json")
	if err != nil {
		t.Fatalf("Couldn't open expected results! %v", err)
	}

	if err = json.Unmarshal(rawExp, &exp); err != nil {
		t.Fatalf("Bad JSON in expected output")
	}

	for k, exp_cur := range exp {
		hash := cfg.KetamaHash([]byte(k))
		if hash != exp_cur.Hash {
			t.Fatalf("Bad hash for %s. Expected %d but got %d", k, exp_cur.Hash, hash)
		}

		index := cfg.KetamaNode(hash)
		if index != exp_cur.Index {
			t.Fatalf("Bad index for %s (hash=%d). Expected %v but got %d", k, exp_cur.Hash, exp_cur.Index, index)
		}
	}
}
