package search

import (
	"encoding/json"

	"github.com/couchbase/gocb/v2"
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/helpers"

	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/sdk/search"
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/shared"
)

func ParseSearchRows(fieldContentAs *shared.ContentAs, res *gocb.SearchResult) ([]*search.SearchRow, error) {
	var rows []*search.SearchRow
	for res.Next() {
		cbRow := res.Row()
		row, err := ParseSearchRow(cbRow, fieldContentAs)
		if err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}

	if err := res.Err(); err != nil {
		return nil, err
	}

	return rows, nil
}

func ParseSearchRow(row gocb.SearchRow, fieldContentAs *shared.ContentAs) (*search.SearchRow, error) {
	protoRow := &search.SearchRow{
		Index:     row.Index,
		Id:        row.ID,
		Score:     row.Score,
		Fragments: nil,
		Fields:    nil,
	}
	if row.Explanation != nil {
		b, err := json.Marshal(row.Explanation)
		if err != nil {
			return nil, err
		}
		protoRow.Explanation = b
	}
	if len(row.Locations) > 0 {
		for field, terms := range row.Locations {
			for term, locations := range terms {
				for _, location := range locations {
					protoRow.Locations = append(protoRow.Locations, &search.SearchRowLocation{
						Field:          field,
						Term:           term,
						Position:       location.Position,
						Start:          location.Start,
						End:            location.End,
						ArrayPositions: location.ArrayPositions,
					})
				}
			}
		}
	}
	if len(row.Fragments) > 0 {
		protoRow.Fragments = make(map[string]*search.SearchFragments)
		for k, fragments := range row.Fragments {
			protoRow.Fragments[k] = &search.SearchFragments{
				Fragments: fragments,
			}
		}
	}

	if fieldContentAs != nil {
		fields, err := helpers.ParseContentAs(fieldContentAs, func(content interface{}) error {
			return row.Fields(content)
		})
		if err != nil {
			return nil, err
		}
		protoRow.Fields = fields
	}

	return protoRow, nil
}

func ParseSearchResultMeta(metadata *gocb.SearchMetaData) (*search.SearchMetaData, error) {
	metrics := metadata.Metrics
	protoMetrics := &search.SearchMetrics{
		TookMsec:              metrics.Took.Milliseconds(),
		TotalRows:             int64(metrics.TotalRows),
		MaxScore:              metrics.MaxScore,
		TotalPartitionCount:   int64(metrics.TotalPartitionCount),
		SuccessPartitionCount: int64(metrics.SuccessPartitionCount),
		ErrorPartitionCount:   int64(metrics.ErrorPartitionCount),
	}

	protoMeta := &search.SearchMetaData{
		Metrics: protoMetrics,
		Errors:  metadata.Errors,
	}

	return protoMeta, nil
}

func ParseSearchResultFacets(facets map[string]gocb.SearchFacetResult) (*search.SearchFacets, error) {
	protoFacets := make(map[string]*search.SearchFacetResult, len(facets))
	for k, facetRes := range facets {
		protoFacets[k] = &search.SearchFacetResult{
			Name:    facetRes.Name,
			Field:   facetRes.Field,
			Total:   facetRes.Total,
			Missing: facetRes.Missing,
			Other:   facetRes.Other,
		}
	}

	return &search.SearchFacets{
		Facets: protoFacets,
	}, nil
}
