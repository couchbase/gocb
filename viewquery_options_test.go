package gocb

import (
	"fmt"
	"math/rand"
	"net/url"
)

func (suite *UnitTestSuite) TestViewQueryOptionsToURLValues() {
	for i := 0; i < 50; i++ {
		opts := suite.testCreateViewQueryOptions(int64(i))

		optValues, err := opts.toURLValues()

		if opts.ScanConsistency > ViewScanConsistencyUpdateAfter || opts.ScanConsistency < 0 {
			if err == nil {
				suite.T().Fatalf("Expected an error for invalid stale value")
			} else {
				continue
			}
		}

		if opts.Order > ViewOrderingDescending || opts.Order < 0 {
			if err == nil {
				suite.T().Fatalf("Expected an error for invalid order value")
			} else {
				continue
			}
		}

		if err != nil {
			suite.T().Fatalf("Expected no error but was %v", err)
		}

		if opts.ScanConsistency == 0 {
			suite.testAssertViewOption("", "stale", optValues)
		} else if opts.ScanConsistency == ViewScanConsistencyRequestPlus {
			suite.testAssertViewOption("false", "stale", optValues)
		} else if opts.ScanConsistency == ViewScanConsistencyNotBounded {
			suite.testAssertViewOption("ok", "stale", optValues)
		} else if opts.ScanConsistency == ViewScanConsistencyUpdateAfter {
			suite.testAssertViewOption("update_after", "stale", optValues)
		}

		if opts.Skip == 0 {
			suite.testAssertViewOption("", "skip", optValues)
		} else {
			suite.testAssertViewOption(fmt.Sprintf("%d", opts.Skip), "skip", optValues)
		}

		if opts.Limit == 0 {
			suite.testAssertViewOption("", "limit", optValues)
		} else {
			suite.testAssertViewOption(fmt.Sprintf("%d", opts.Limit), "limit", optValues)
		}

		if opts.Order == 0 {
			suite.testAssertViewOption("", "descending", optValues)
		} else if opts.Order == ViewOrderingAscending {
			suite.testAssertViewOption("false", "descending", optValues)
		} else if opts.Order == ViewOrderingDescending {
			suite.testAssertViewOption("true", "descending", optValues)
		}

		if opts.Reduce {
			suite.testAssertViewOption("true", "reduce", optValues)

			if opts.Group {
				suite.testAssertViewOption("true", "group", optValues)
			} else {
				suite.testAssertViewOption("", "group", optValues)
			}

			if opts.GroupLevel == 0 {
				suite.testAssertViewOption("", "group_level", optValues)
			} else {
				suite.testAssertViewOption(fmt.Sprintf("%d", opts.GroupLevel), "group_level", optValues)
			}
		} else {
			suite.testAssertViewOption("false", "reduce", optValues)
			suite.testAssertViewOption("", "group", optValues)
			suite.testAssertViewOption("", "group_level", optValues)
		}

		if opts.Key == nil {
			suite.testAssertViewOption("", "key", optValues)
		} else {
			suite.testAssertViewOption("[\"key1\"]\n", "key", optValues)
		}

		if len(opts.Keys) == 0 {
			suite.testAssertViewOption("", "keys", optValues)
		} else {
			suite.testAssertViewOption("[\"key2\",\"key3\",{\"key\":\"key4\"}]\n", "keys", optValues)
		}
	}
}

func (suite *UnitTestSuite) testAssertViewOption(expected string, key string, optValues *url.Values) {
	val := optValues.Get(key)
	if val != expected {
		suite.T().Fatalf("Values had incorrect %s, expected %s but was %s", key, expected, val)
	}
}

func (suite *UnitTestSuite) testCreateViewQueryOptions(seed int64) *ViewOptions {
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
