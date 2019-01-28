package gocb

import (
	"context"
	"fmt"
	"net/url"
	"strconv"

	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
)

// SpatialViewOptions represents the options available when executing a spatial query.
type SpatialViewOptions struct {
	Stale StaleMode
	Skip  uint
	Limit uint
	// Bbox specifies the bounding region to use for the spatial query.
	Bbox              []float64
	Development       bool
	Custom            map[string]string
	Context           context.Context
	ParentSpanContext opentracing.SpanContext
}

func (opts *SpatialViewOptions) toURLValues() (*url.Values, error) {
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
		options.Set("limit", strconv.FormatUint(uint64(opts.Skip), 10))
	}

	if len(opts.Bbox) == 4 {
		options.Set("bbox", fmt.Sprintf("%f,%f,%f,%f", opts.Bbox[0], opts.Bbox[1], opts.Bbox[2], opts.Bbox[3]))
	} else {
		options.Del("bbox")
	}

	if opts.Custom != nil {
		for k, v := range opts.Custom {
			options.Set(k, v)
		}
	}

	return options, nil
}
