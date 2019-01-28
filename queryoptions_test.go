package gocb

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"
)

func TestQueryOptionsToMap(t *testing.T) {
	for i := 0; i < 50; i++ {
		opts := testCreateQueryOptions(int64(i))

		statement := "select * from default"
		optMap, err := opts.toMap(statement)

		if opts.Consistency != 0 && opts.ConsistentWith != nil {
			if err == nil {
				t.Fatalf("Expected error when ConsistentWith and Conistency are both set")
			} else {
				continue
			}
		}

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

		if opts.Consistency == 0 && opts.ConsistentWith == nil {
			testAssertOption(t, nil, "scan_consistency", optMap)
		}

		if opts.Consistency == 1 {
			testAssertOption(t, "not_bounded", "scan_consistency", optMap)
		} else if opts.Consistency == 2 {
			testAssertOption(t, "request_plus", "scan_consistency", optMap)
		} else if opts.Consistency == 3 {
			testAssertOption(t, "statement_plus", "scan_consistency", optMap)
		}

		if opts.ConsistentWith == nil {
			testAssertOption(t, nil, "scan_vectors", optMap)
		} else {
			testAssertOption(t, "at_plus", "scan_consistency", optMap)
			testAssertOption(t, opts.ConsistentWith, "scan_vectors", optMap)
		}

		if opts.Profile == "" {
			testAssertOption(t, nil, "profile", optMap)
		} else if opts.Profile == QueryProfileNone {
			testAssertOption(t, QueryProfileNone, "profile", optMap)
		} else if opts.Profile == QueryProfilePhases {
			testAssertOption(t, QueryProfilePhases, "profile", optMap)
		} else if opts.Profile == QueryProfileTimings {
			testAssertOption(t, QueryProfileTimings, "profile", optMap)
		}

		if opts.ScanCap == 0 {
			testAssertOption(t, nil, "scan_cap", optMap)
		} else {
			testAssertOption(t, fmt.Sprintf("%d", opts.ScanCap), "scan_cap", optMap)
		}

		if opts.PipelineBatch == 0 {
			testAssertOption(t, nil, "pipeline_batch", optMap)
		} else {
			testAssertOption(t, fmt.Sprintf("%d", opts.PipelineBatch), "pipeline_batch", optMap)
		}

		if opts.PipelineCap == 0 {
			testAssertOption(t, nil, "pipeline_cap", optMap)
		} else {
			testAssertOption(t, fmt.Sprintf("%d", opts.PipelineCap), "pipeline_cap", optMap)
		}

		if opts.ReadOnly {
			testAssertOption(t, true, "readonly", optMap)
		} else {
			testAssertOption(t, nil, "readonly", optMap)
		}

		if opts.Timeout == 0 {
			testAssertOption(t, nil, "timeout", optMap)
		} else {
			testAssertOption(t, opts.Timeout.String(), "timeout", optMap)
		}

		if len(opts.Custom) > 0 {
			for k, v := range opts.Custom {
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

func testAssertOption(t *testing.T, expected interface{}, key string, optMap map[string]interface{}) {
	if expected == nil {
		if val, ok := optMap[key]; ok {
			t.Fatalf("Expected %s to be missing from options but was %v", key, val)
		}
	} else {
		if val, ok := optMap[key]; ok {
			if val != expected {
				t.Fatalf("Options had incorrect %s, expected %v but was %v", key, expected, val)
			}
		} else {
			t.Fatalf("Options had no %s", key)
		}
	}
}

func testCreateQueryOptions(seed int64) *QueryOptions {
	opts := &QueryOptions{}
	rand.Seed(seed)

	randVal := rand.Intn(4)
	if randVal == 1 {
		opts.Consistency = NotBounded
	} else if randVal == 2 {
		opts.Consistency = RequestPlus
	} else if randVal == 3 {
		opts.Consistency = StatementPlus
	}

	randVal = rand.Intn(2)
	if randVal == 1 {
		opts.ConsistentWith = NewMutationState(MutationToken{bucketName: "default"})
	}

	randVal = rand.Intn(2)
	if randVal == 1 {
		opts.Prepared = true
	}

	randVal = rand.Intn(4)
	if randVal == 1 {
		opts.Profile = QueryProfileNone
	} else if randVal == 2 {
		opts.Profile = QueryProfilePhases
	} else if randVal == 3 {
		opts.Profile = QueryProfileTimings
	}

	randVal = rand.Intn(2)
	if randVal == 1 {
		opts.ScanCap = 1
	}

	randVal = rand.Intn(2)
	if randVal == 1 {
		opts.PipelineBatch = 1
	}

	randVal = rand.Intn(2)
	if randVal == 1 {
		opts.PipelineCap = 1
	}

	randVal = rand.Intn(2)
	if randVal == 1 {
		opts.ReadOnly = true
	}

	randVal = rand.Intn(2)
	if randVal == 1 {
		opts.Timeout = 60 * time.Second
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
		opts.Custom = map[string]interface{}{"key1": "param1", "$key2": "param2"}
	}

	randVal = rand.Intn(2)
	if randVal == 1 {
		opts.Context = context.Background()
	}

	return opts
}
