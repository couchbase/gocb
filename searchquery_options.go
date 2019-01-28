package gocb

import (
	"context"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
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

// SearchQueryOptions represents a pending search query.
type SearchQueryOptions struct {
	Limit             int
	Skip              int
	Explain           bool
	Highlight         *SearchHighlightOptions
	Fields            []string
	Sort              []interface{}
	Facets            map[string]interface{}
	Timeout           time.Duration
	Consistency       ConsistencyMode
	ConsistentWith    *MutationState
	Context           context.Context
	ParentSpanContext opentracing.SpanContext
}

func (opts *SearchQueryOptions) toOptionsData() (*searchQueryOptionsData, error) {
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

	if opts.Consistency != 0 && opts.ConsistentWith != nil {
		return nil, errors.New("Unexpected consistency option")
	}

	if opts.Consistency != 0 {
		if data.Ctl == nil {
			data.Ctl = &searchQueryCtlData{}
		}

		data.Ctl.Consistency = &searchQueryConsistencyData{}
		if opts.Consistency == NotBounded {
			data.Ctl.Consistency.Level = "not_bounded"
		} else {
			return nil, errors.New("Unexpected consistency option")
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
