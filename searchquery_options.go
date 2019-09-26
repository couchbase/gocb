package gocb

import (
	"context"
	"time"
)

// SearchHighlightStyle indicates the type of highlighting to use for a search query.
type SearchHighlightStyle string

const (
	// DefaultHighlightStyle specifies to use the default to highlight search result hits.
	DefaultHighlightStyle = SearchHighlightStyle("")

	// HtmlHighlightStyle specifies to use HTML tags to highlight search result hits.
	HtmlHighlightStyle = SearchHighlightStyle("html")

	// AnsiHightlightStyle specifies to use ANSI tags to highlight search result hits.
	AnsiHightlightStyle = SearchHighlightStyle("ansi")
)

// SearchScanConsistency indicates the level of data consistency desired for a search query.
type SearchScanConsistency int

const (
	// SearchScanConsistencyNotBounded indicates no data consistency is required.
	SearchScanConsistencyNotBounded = SearchScanConsistency(1)
)

type searchQueryHighlightData struct {
	Style  string   `json:"style,omitempty"`
	Fields []string `json:"fields,omitempty"`
}
type searchQueryConsistencyData struct {
	Level   string         `json:"level,omitempty"`
	Vectors *MutationState `json:"vectors,omitempty"`
}
type searchQueryCtlData struct {
	Timeout     uint                        `json:"timeout,omitempty"`
	Consistency *searchQueryConsistencyData `json:"consistency,omitempty"`
}
type searchQueryOptionsData struct {
	Size      int                       `json:"size,omitempty"`
	From      int                       `json:"from,omitempty"`
	Explain   bool                      `json:"explain,omitempty"`
	Highlight *searchQueryHighlightData `json:"highlight,omitempty"`
	Fields    []string                  `json:"fields,omitempty"`
	Sort      []interface{}             `json:"sort,omitempty"`
	Facets    map[string]interface{}    `json:"facets,omitempty"`
	Ctl       *searchQueryCtlData       `json:"ctl,omitempty"`
}

// SearchHighlightOptions are the options available for search highlighting.
type SearchHighlightOptions struct {
	Style  SearchHighlightStyle
	Fields []string
}

// SearchOptions represents a pending search query.
type SearchOptions struct {
	Limit     int
	Skip      int
	Explain   bool
	Highlight *SearchHighlightOptions
	Fields    []string
	Sort      []interface{}
	Facets    map[string]interface{}
	// Timeout and context are used to control cancellation of the data stream. Any timeout or deadline will also be
	// propagated to the server.
	Timeout         time.Duration
	Context         context.Context
	ScanConsistency SearchScanConsistency
	ConsistentWith  *MutationState

	// JSONSerializer is used to deserialize each row in the result. This should be a JSON deserializer as results are JSON.
	// NOTE: if not set then query will always default to DefaultJSONSerializer.
	Serializer JSONSerializer
}

func (opts *SearchOptions) toOptionsData() (*searchQueryOptionsData, error) {
	data := &searchQueryOptionsData{}

	data.Size = opts.Limit
	data.From = opts.Skip
	data.Explain = opts.Explain
	data.Fields = opts.Fields
	data.Sort = opts.Sort

	if opts.Highlight != nil {
		data.Highlight = &searchQueryHighlightData{}
		data.Highlight.Style = string(opts.Highlight.Style)
		data.Highlight.Fields = opts.Highlight.Fields
	}

	if opts.Facets != nil {
		data.Facets = make(map[string]interface{})
		for k, v := range opts.Facets {
			data.Facets[k] = v
		}
	}

	if opts.Timeout != 0 {
		if data.Ctl == nil {
			data.Ctl = &searchQueryCtlData{}
		}
		data.Ctl.Timeout = uint(opts.Timeout / time.Millisecond)
	}

	if opts.ScanConsistency != 0 && opts.ConsistentWith != nil {
		return nil, invalidArgumentsError{message: "ScanConsistency and ConsistentWith must be used exclusively"}
	}

	if opts.ScanConsistency != 0 {
		if data.Ctl == nil {
			data.Ctl = &searchQueryCtlData{}
		}

		data.Ctl.Consistency = &searchQueryConsistencyData{}
		if opts.ScanConsistency == SearchScanConsistencyNotBounded {
			data.Ctl.Consistency.Level = "not_bounded"
		} else {
			return nil, invalidArgumentsError{message: "unexpected consistency option"}
		}
	}

	if opts.ConsistentWith != nil {
		if data.Ctl == nil {
			data.Ctl = &searchQueryCtlData{}
		}

		data.Ctl.Consistency = &searchQueryConsistencyData{}
		data.Ctl.Consistency.Level = "at_plus"
		data.Ctl.Consistency.Vectors = opts.ConsistentWith
	}

	return data, nil
}
