package gocb

import (
	"context"
	"math/rand"
	"strings"
	"testing"
	"time"
)

func TestAnalyticsQueryOptionsToMap(t *testing.T) {
	for i := 0; i < 50; i++ {
		opts := testCreateAnalyticsQueryOptions(int64(i))

		statement := "select * from default"
		optMap, err := opts.toMap(statement)

		if len(opts.NamedParameters) > 0 && len(opts.PositionalParameters) > 0 {
			if err == nil {
				t.Fatalf("Expected error when NamedParameters and PositionalParameters are both set")
			} else {
				continue
			}
		}

		if err != nil {
			t.Fatalf("Expected no error but was %v", err)
		}

		testAssertOption(t, statement, "statement", optMap)

		if opts.Deferred {
			testAssertOption(t, "async", "mode", optMap)
		} else {
			testAssertOption(t, nil, "mode", optMap)
		}

		if opts.Priority {
			testAssertOption(t, -1, "priority", optMap)
		} else {
			testAssertOption(t, nil, "priority", optMap)
		}

		if opts.Pretty {
			testAssertOption(t, true, "pretty", optMap)
		} else {
			testAssertOption(t, nil, "pretty", optMap)
		}

		if opts.ServerSideTimeout == 0 {
			testAssertOption(t, nil, "timeout", optMap)
		} else {
			testAssertOption(t, opts.ServerSideTimeout.String(), "timeout", optMap)
		}

		if len(opts.RawParam) > 0 {
			for k, v := range opts.RawParam {
				testAssertOption(t, v, k, optMap)
			}
		}

		if len(opts.NamedParameters) == 0 && len(opts.PositionalParameters) == 0 {
			testAssertOption(t, nil, "args", optMap)
		}

		if len(opts.NamedParameters) > 0 {
			for k, v := range opts.NamedParameters {
				if !strings.HasPrefix(k, "$") {
					k = "$" + k
				}
				testAssertOption(t, v, k, optMap)
			}
		}

		if len(opts.PositionalParameters) > 0 {
			args, ok := optMap["args"]
			if !ok {
				t.Fatalf("Options had no args")
			}
			castArgs, ok := args.([]interface{})
			if !ok {
				t.Fatalf("Options was not []interface{}")
			}
			for i, v := range opts.PositionalParameters {
				if castArgs[i] != v {
					t.Fatalf("Positional parameters index %d should have been %v, was %v", i, v, castArgs[i])
				}
			}
		}
	}
}

func testCreateAnalyticsQueryOptions(seed int64) *AnalyticsQueryOptions {
	opts := &AnalyticsQueryOptions{}
	rand.Seed(seed)

	randVal := rand.Intn(2)
	if randVal == 1 {
		opts.ContextID = "62d29101-0c9f-400d-af2b-9bd44a557a7c"
	}

	randVal = rand.Intn(2)
	if randVal == 1 {
		opts.Deferred = true
	}

	randVal = rand.Intn(2)
	if randVal == 1 {
		opts.Priority = true
	}

	randVal = rand.Intn(2)
	if randVal == 1 {
		opts.Pretty = true
	}

	randVal = rand.Intn(2)
	if randVal == 1 {
		opts.ServerSideTimeout = 60 * time.Second
	}

	randVal = rand.Intn(2)
	if randVal == 1 {
		opts.PositionalParameters = []interface{}{"param"}
	}

	randVal = rand.Intn(2)
	if randVal == 1 {
		opts.NamedParameters = map[string]interface{}{"key": "param"}
	}

	randVal = rand.Intn(2)
	if randVal == 1 {
		opts.RawParam = map[string]interface{}{"key1": "param1", "$key2": "param2"}
	}

	randVal = rand.Intn(2)
	if randVal == 1 {
		opts.Context = context.Background()
	}

	return opts
}
