package gocb

import (
	"context"
	"fmt"
	"math/rand"
	"net/url"
	"testing"
)

func TestViewQueryOptionsToURLValues(t *testing.T) {
	for i := 0; i < 50; i++ {
		opts := testCreateViewQueryOptions(int64(i))

		optValues, err := opts.toURLValues()

		if opts.Stale > After || opts.Stale < 0 {
			if err == nil {
				t.Fatalf("Expected an error for invalid stale value")
			} else {
				continue
			}
		}

		if opts.Order > Descending || opts.Order < 0 {
			if err == nil {
				t.Fatalf("Expected an error for invalid order value")
			} else {
				continue
			}
		}

		if err != nil {
			t.Fatalf("Expected no error but was %v", err)
		}

		if opts.Stale == 0 {
			testAssertViewOption(t, "", "stale", optValues)
		} else if opts.Stale == Before {
			testAssertViewOption(t, "false", "stale", optValues)
		} else if opts.Stale == None {
			testAssertViewOption(t, "ok", "stale", optValues)
		} else if opts.Stale == After {
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
		} else if opts.Order == Ascending {
			testAssertViewOption(t, "false", "descending", optValues)
		} else if opts.Order == Descending {
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
		opts.Stale = Before
	} else if randVal == 2 {
		opts.Stale = None
	} else if randVal == 3 {
		opts.Stale = After
	} else if randVal == 4 {
		opts.Stale = 5
	}

	randVal = rand.Intn(2)
	if randVal == 1 {
		opts.Skip = uint(rand.Intn(10))
	}

	randVal = rand.Intn(2)
	if randVal == 1 {
		opts.Limit = uint(rand.Intn(10))
	}

	randVal = rand.Intn(4)
	if randVal == 1 {
		opts.Order = Ascending
	} else if randVal == 2 {
		opts.Order = Descending
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
		opts.GroupLevel = uint(rand.Intn(5))
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
		r := &Range{}

		randVal = rand.Intn(2)
		if randVal == 1 {
			r.Start = "keystart"
		}

		randVal = rand.Intn(2)
		if randVal == 1 {
			r.End = "keyend"
		}

		randVal = rand.Intn(3)
		if randVal == 1 {
			r.InclusiveEnd = true
		} else if randVal == 2 {
			r.InclusiveEnd = false
		}
	}

	randVal = rand.Intn(2)
	if randVal == 1 {
		opts.IDRangeStart = "rangeStart"
	}

	randVal = rand.Intn(2)
	if randVal == 1 {
		opts.IDRangeEnd = "rangeEnd"
	}

	randVal = rand.Intn(2)
	if randVal == 1 {
		opts.Custom = map[string]string{"key1": "param1", "$key2": "param2"}
	}

	randVal = rand.Intn(2)
	if randVal == 1 {
		opts.Context = context.Background()
	}

	return opts
}
