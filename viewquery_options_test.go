package gocb

import (
	"fmt"
	"math/rand"
	"net/url"
	"testing"
)

func TestViewQueryOptionsToURLValues(t *testing.T) {
	for i := 0; i < 50; i++ {
		opts := testCreateViewQueryOptions(int64(i))

		optValues, err := opts.toURLValues()

		if opts.ScanConsistency > ViewScanConsistencyUpdateAfter || opts.ScanConsistency < 0 {
			if err == nil {
				t.Fatalf("Expected an error for invalid stale value")
			} else {
				continue
			}
		}

		if opts.Order > ViewOrderingDescending || opts.Order < 0 {
			if err == nil {
				t.Fatalf("Expected an error for invalid order value")
			} else {
				continue
			}
		}

		if err != nil {
			t.Fatalf("Expected no error but was %v", err)
		}

		if opts.ScanConsistency == 0 {
			testAssertViewOption(t, "", "stale", optValues)
		} else if opts.ScanConsistency == ViewScanConsistencyRequestPlus {
			testAssertViewOption(t, "false", "stale", optValues)
		} else if opts.ScanConsistency == ViewScanConsistencyNotBounded {
			testAssertViewOption(t, "ok", "stale", optValues)
		} else if opts.ScanConsistency == ViewScanConsistencyUpdateAfter {
			testAssertViewOption(t, "update_after", "stale", optValues)
		}

		if opts.Skip == 0 {
			testAssertViewOption(t, "", "skip", optValues)
		} else {
			testAssertViewOption(t, fmt.Sprintf("%d", opts.Skip), "skip", optValues)
		}

		if opts.Limit == 0 {
			testAssertViewOption(t, "", "limit", optValues)
		} else {
			testAssertViewOption(t, fmt.Sprintf("%d", opts.Limit), "limit", optValues)
		}

		if opts.Order == 0 {
			testAssertViewOption(t, "", "descending", optValues)
		} else if opts.Order == ViewOrderingAscending {
			testAssertViewOption(t, "false", "descending", optValues)
		} else if opts.Order == ViewOrderingDescending {
			testAssertViewOption(t, "true", "descending", optValues)
		}

		if opts.Reduce {
			testAssertViewOption(t, "true", "reduce", optValues)

			if opts.Group {
				testAssertViewOption(t, "true", "group", optValues)
			} else {
				testAssertViewOption(t, "false", "group", optValues)
			}

			if opts.GroupLevel == 0 {
				testAssertViewOption(t, "", "group_level", optValues)
			} else {
				testAssertViewOption(t, fmt.Sprintf("%d", opts.GroupLevel), "group_level", optValues)
			}
		} else {
			testAssertViewOption(t, "false", "reduce", optValues)
			testAssertViewOption(t, "", "group", optValues)
			testAssertViewOption(t, "", "group_level", optValues)
		}

		if opts.Key == nil {
			testAssertViewOption(t, "", "key", optValues)
		} else {
			testAssertViewOption(t, "[\"key1\"]\n", "key", optValues)
		}

		if len(opts.Keys) == 0 {
			testAssertViewOption(t, "", "keys", optValues)
		} else {
			testAssertViewOption(t, "[\"key2\",\"key3\",{\"key\":\"key4\"}]\n", "keys", optValues)
		}
	}
}

func testAssertViewOption(t *testing.T, expected string, key string, optValues *url.Values) {
	val := optValues.Get(key)
	if val != expected {
		t.Fatalf("Values had incorrect %s, expected %s but was %s", key, expected, val)
	}
}

func testCreateViewQueryOptions(seed int64) *ViewOptions {
	opts := &ViewOptions{}
	rand.Seed(seed)

	randVal := rand.Intn(6)
	if randVal == 1 {
		opts.ScanConsistency = ViewScanConsistencyRequestPlus
	} else if randVal == 2 {
		opts.ScanConsistency = ViewScanConsistencyNotBounded
	} else if randVal == 3 {
		opts.ScanConsistency = ViewScanConsistencyUpdateAfter
	} else if randVal == 4 {
		opts.ScanConsistency = 5
	}

	randVal = rand.Intn(2)
	if randVal == 1 {
		opts.Skip = uint32(rand.Intn(10))
	}

	randVal = rand.Intn(2)
	if randVal == 1 {
		opts.Limit = uint32(rand.Intn(10))
	}

	randVal = rand.Intn(4)
	if randVal == 1 {
		opts.Order = ViewOrderingAscending
	} else if randVal == 2 {
		opts.Order = ViewOrderingDescending
	} else if randVal == 3 {
		opts.Order = 3
	}

	randVal = rand.Intn(2)
	if randVal == 1 {
		opts.Reduce = true
	}

	randVal = rand.Intn(2)
	if randVal == 1 {
		opts.Group = true
	}

	randVal = rand.Intn(2)
	if randVal == 1 {
		opts.GroupLevel = uint32(rand.Intn(5))
	}

	randVal = rand.Intn(2)
	if randVal == 1 {
		opts.Key = []string{"key1"}
	}

	randVal = rand.Intn(2)
	if randVal == 1 {
		opts.Keys = []interface{}{"key2", "key3", struct {
			Key string `json:"key"`
		}{"key4"}}
	}

	randVal = rand.Intn(2)
	if randVal == 1 {
		randVal = rand.Intn(2)
		if randVal == 1 {
			opts.StartKey = "keystart"
		}

		randVal = rand.Intn(2)
		if randVal == 1 {
			opts.EndKey = "keyend"
		}

		randVal = rand.Intn(3)
		if randVal == 1 {
			opts.InclusiveEnd = true
		} else if randVal == 2 {
			opts.InclusiveEnd = false
		}
	}

	randVal = rand.Intn(2)
	if randVal == 1 {
		opts.StartKeyDocID = "rangeStart"
	}

	randVal = rand.Intn(2)
	if randVal == 1 {
		opts.EndKeyDocID = "rangeEnd"
	}

	randVal = rand.Intn(2)
	if randVal == 1 {
		opts.Raw = map[string]string{"key1": "param1", "$key2": "param2"}
	}

	return opts
}
