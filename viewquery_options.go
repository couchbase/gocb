package gocb

import (
	"bytes"
	"context"
	"encoding/json"
	"net/url"
	"strconv"

	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
)

// StaleMode specifies the consistency required for a view query.
type StaleMode int

const (
	// Before indicates to update the index before querying it.
	Before = StaleMode(1)
	// None indicates that no special behaviour should be used.
	None = StaleMode(2)
	// After indicates to update the index asynchronously after querying.
	After = StaleMode(3)
)

// SortOrder specifies the ordering for the view queries results.
type SortOrder int

const (
	// Ascending indicates the query results should be sorted from lowest to highest.
	Ascending = SortOrder(1)
	// Descending indicates the query results should be sorted from highest to lowest.
	Descending = SortOrder(2)
)

// Range specifies a value range to get results between.
type Range struct {
	Start        interface{}
	End          interface{}
	InclusiveEnd bool
}

// ViewOptions represents the options available when executing view query.
type ViewOptions struct {
	Stale             StaleMode
	Skip              uint
	Limit             uint
	Order             SortOrder
	Reduce            bool
	Group             bool
	GroupLevel        uint
	Key               interface{}
	Keys              []interface{}
	Range             *Range
	IDRangeStart      string
	IDRangeEnd        string
	Development       bool
	Custom            map[string]string
	Context           context.Context
	ParentSpanContext opentracing.SpanContext
}

func (opts *ViewOptions) toURLValues() (*url.Values, error) {
	options := &url.Values{}

	if opts.Stale != 0 {
		if opts.Stale == Before {
			options.Set("stale", "false")
		} else if opts.Stale == None {
			options.Set("stale", "ok")
		} else if opts.Stale == After {
			options.Set("stale", "update_after")
		} else {
			return nil, errors.New("Unexpected stale option")
		}
	}

	if opts.Skip != 0 {
		options.Set("skip", strconv.FormatUint(uint64(opts.Skip), 10))
	}

	if opts.Limit != 0 {
		options.Set("limit", strconv.FormatUint(uint64(opts.Limit), 10))
	}

	if opts.Order != 0 {
		if opts.Order == Ascending {
			options.Set("descending", "false")
		} else if opts.Order == Descending {
			options.Set("descending", "true")
		} else {
			return nil, errors.New("Unexpected order option")
		}
	}

	options.Set("reduce", "false") // is this line necessary?
	if opts.Reduce {
		options.Set("reduce", "true")

		// Only set group if a reduce view
		options.Set("group", "false") // is this line necessary?
		if opts.Group {
			options.Set("group", "true")
		}

		if opts.GroupLevel != 0 {
			options.Set("group_level", strconv.FormatUint(uint64(opts.GroupLevel), 10))
		}
	}

	if opts.Key != nil {
		jsonKey, err := opts.marshalJson(opts.Key)
		if err != nil {
			return nil, err
		}
		options.Set("key", string(jsonKey))
	}

	if len(opts.Keys) > 0 {
		jsonKeys, err := opts.marshalJson(opts.Keys)
		if err != nil {
			return nil, err
		}
		options.Set("keys", string(jsonKeys))
	}

	if opts.Range != nil {
		if opts.Range.Start != nil {
			jsonStartKey, err := opts.marshalJson(opts.Range.Start)
			if err != nil {
				return nil, err
			}
			options.Set("startkey", string(jsonStartKey))
		} else {
			options.Del("startkey")
		}

		if opts.Range.End != nil {
			jsonEndKey, err := opts.marshalJson(opts.Range.End)
			if err != nil {
				return nil, err
			}
			options.Set("endkey", string(jsonEndKey))
		} else {
			options.Del("endkey")
		}

		if opts.Range.Start != nil || opts.Range.End != nil {
			if opts.Range.InclusiveEnd {
				options.Set("inclusive_end", "true")
			} else {
				options.Set("inclusive_end", "false")
			}
		} else {
			options.Del("inclusive_end")
		}
	}

	if opts.IDRangeStart == "" {
		options.Del("startkey_docid")
	} else {
		options.Set("startkey_docid", opts.IDRangeStart)
	}

	if opts.IDRangeEnd == "" {
		options.Del("endkey_docid")
	} else {
		options.Set("endkey_docid", opts.IDRangeEnd)
	}

	if opts.Custom != nil {
		for k, v := range opts.Custom {
			options.Set(k, v)
		}
	}

	return options, nil
}

func (opts *ViewOptions) marshalJson(value interface{}) ([]byte, error) {
	buf := new(bytes.Buffer)
	enc := json.NewEncoder(buf)
	enc.SetEscapeHTML(false)
	err := enc.Encode(value)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
