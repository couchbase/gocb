package gocb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"google.golang.org/grpc/status"

	cbsearch "github.com/couchbase/gocb/v2/search"
	"github.com/couchbase/goprotostellar/genproto/search_v1"
)

type searchProviderPs struct {
	provider search_v1.SearchServiceClient

	timeouts TimeoutsConfig
	tracer   RequestTracer
	meter    *meterWrapper
}

var _ searchProvider = &searchProviderPs{}

// executes a search query against PS, taking care of the translation.
func (search *searchProviderPs) SearchQuery(indexName string, query cbsearch.Query, opts *SearchOptions) (*SearchResult, error) {
	start := time.Now()
	defer search.meter.ValueRecord(meterValueServiceSearch, "search", start)

	span := createSpan(search.tracer, opts.ParentSpan, "search", "search")
	span.SetAttribute("db.operation", indexName)
	defer span.End()

	timeout := opts.Timeout
	if timeout == 0 {
		timeout = search.timeouts.SearchTimeout
	}
	ctx := opts.Context
	if ctx == nil {
		ctx = context.Background()
	}

	ctx, cancel := context.WithTimeout(ctx, timeout)
	var err error

	defer func() {
		if err != nil {
			cancel()
		}
	}()

	psQuery, err := cbsearch.Internal{}.MapQueryToPs(query)
	if err != nil {
		return nil, err
	}

	psSort, err := cbsearch.Internal{}.MapSortToPs(opts.Sort)
	if err != nil {
		return nil, err
	}

	facets, err := cbsearch.Internal{}.MapFacetsToPs(opts.Facets)
	if err != nil {
		return nil, err
	}

	request := search_v1.SearchQueryRequest{
		IndexName: indexName,
		Query:     psQuery,

		Sort:   psSort,
		Facets: facets,
	}

	if opts != nil {
		request.DisableScoring = opts.DisableScoring
		request.Collections = opts.Collections
		request.IncludeLocations = opts.IncludeLocations
		request.Limit = opts.Limit
		request.Skip = opts.Skip
		request.IncludeExplanation = opts.Explain
		request.Fields = opts.Fields
	}

	if opts.ScanConsistency > 0 {
		switch opts.ScanConsistency { // only supports not bounded, not unset
		case SearchScanConsistencyNotBounded:
			request.ScanConsistency = search_v1.SearchQueryRequest_SCAN_CONSISTENCY_NOT_BOUNDED
		default:
			err = makeInvalidArgumentsError("invalid scan consistency option specified")
			return nil, err
		}
	}

	if opts.Highlight != nil {
		request.HighlightFields = opts.Highlight.Fields
		switch opts.Highlight.Style {
		case DefaultHighlightStyle:
			request.HighlightStyle = search_v1.SearchQueryRequest_HIGHLIGHT_STYLE_DEFAULT
		case AnsiHightlightStyle:
			request.HighlightStyle = search_v1.SearchQueryRequest_HIGHLIGHT_STYLE_ANSI
		case HTMLHighlightStyle:
			request.HighlightStyle = search_v1.SearchQueryRequest_HIGHLIGHT_STYLE_HTML
		default:
			err = makeInvalidArgumentsError("invalid highlight option specified")
			return nil, err
		}

	}

	client, err := search.provider.SearchQuery(ctx, &request)
	if err != nil {
		st, ok := status.FromError(err)
		if !ok {
			return nil, &SearchError{
				InnerError: err,
				Query:      query,
			}
		}

		gocbErr := tryMapPsErrorStatusToGocbError(st, true)
		if gocbErr == nil {
			gocbErr = err
		}

		if errors.Is(gocbErr, ErrTimeout) {
			return nil, &TimeoutError{
				InnerError:    gocbErr,
				TimeObserved:  time.Since(start),
				RetryReasons:  nil,
				RetryAttempts: 0,
			}
		}

		return nil, &SearchError{
			InnerError: gocbErr,
			Query:      query,
			ErrorText:  st.Message(),
		}
	}

	return newSearchResult(&psSearchRowReader{
		client:     client,
		cancelFunc: cancel,
		query:      query,
	}), nil
}

// wrapper around the PS result to make it compatible with
// the searchRowReader interface.
type psSearchRowReader struct {
	client        search_v1.SearchService_SearchQueryClient
	nextRowsIndex int
	nextRows      []*search_v1.SearchQueryResponse_SearchQueryRow
	err           error
	meta          *search_v1.SearchQueryResponse_MetaData
	cancelFunc    context.CancelFunc
	facets        map[string]*search_v1.SearchQueryResponse_FacetResult
	query         cbsearch.Query
}

// returns the next search row, either from local or fetches it from the client.
func (reader *psSearchRowReader) NextRow() []byte {
	// we have results so lets use them.
	if reader.nextRowsIndex < len(reader.nextRows) {
		row := reader.nextRows[reader.nextRowsIndex]
		reader.nextRowsIndex++

		convertedRow, err := psSearchRowToJSONBytes(row)
		if err != nil {
			reader.finishWithError(err)
			return nil
		}

		return convertedRow
	}

	// check if there are anymore available results.
	res, err := reader.client.Recv()
	if err != nil {
		if errors.Is(err, io.EOF) {
			reader.finishWithoutError()
			return nil
		}
		reader.finishWithError(err)
		return nil
	}

	reader.nextRows = res.GetHits()
	reader.nextRowsIndex = 1
	reader.meta = res.MetaData
	reader.facets = res.Facets
	if len(res.Hits) > 0 {
		convertedRow, err := psSearchRowToJSONBytes(res.Hits[0])
		if errors.Is(err, io.EOF) {
			reader.finishWithoutError()
			return nil
		}
		return convertedRow
	}

	return nil
}

func (reader *psSearchRowReader) Close() error {
	if reader.err != nil {
		return reader.err
	}
	// if the client is nil then we must be closed already.
	if reader.client == nil {
		return nil
	}
	err := reader.client.CloseSend()
	reader.client = nil
	return err
}

func (reader *psSearchRowReader) MetaData() ([]byte, error) {
	if reader.err != nil {
		return nil, reader.Err()
	}
	if reader.client != nil {
		return nil, errors.New("the result must be fully read before accessing the meta-data")
	}
	if reader.meta == nil {
		return nil, errors.New("an error occurred during querying which has made the meta-data unavailable")
	}
	facets, err := psSearchFacetToJSONSearchFacet(reader.facets)
	if err != nil {
		return nil, err
	}

	meta := jsonSearchResponse{
		TotalHits: reader.meta.Metrics.TotalRows,
		MaxScore:  reader.meta.Metrics.MaxScore,
		Took:      uint64(reader.meta.Metrics.ExecutionTime.GetNanos()), // this is in nano seconds
		Status: jsonSearchResponseStatus{
			Errors: reader.meta.Errors,
		},
		Facets: facets,
	}

	return json.Marshal(meta)
}

func (reader *psSearchRowReader) Err() error {
	err := reader.err
	if err == nil {
		return nil
	}
	st, ok := status.FromError(err)
	if !ok {
		return &SearchError{
			InnerError: err,
			Query:      reader.query,
		}
	}

	gocbErr := tryMapPsErrorStatusToGocbError(st, true)
	if gocbErr == nil {
		gocbErr = err
	}

	return &SearchError{
		InnerError: gocbErr,
		Query:      reader.query,
		ErrorText:  st.Message(),
	}
}

func (reader *psSearchRowReader) finishWithoutError() {
	reader.cancelFunc()
	// Close the stream now that we are done with it
	err := reader.client.CloseSend()
	if err != nil {
		logWarnf("query stream close failed after meta-data: %s", err)
	}

	reader.client = nil
}

func (reader *psSearchRowReader) finishWithError(err error) {
	reader.cancelFunc()
	// Lets record the error that happened
	reader.err = err

	// Lets close the underlying stream
	closeErr := reader.client.CloseSend()
	if closeErr != nil {
		// We log this at debug level, but its almost always going to be an
		// error since thats the most likely reason we are in finishWithError
		logDebugf("query stream close failed after error: %s", closeErr)
	}

	// Our client is invalidated as soon as an error occurs
	reader.client = nil
}

// Helper functions to convert from PS world into something gocb can process
func psSearchRowLocationToJSONSearchRowLocations(locations []*search_v1.SearchQueryResponse_Location) jsonSearchRowLocations {
	jsonForm := make(jsonSearchRowLocations)

	for _, location := range locations {
		field := location.GetField()
		term := location.GetTerm()

		if _, ok := jsonForm[field]; !ok {
			jsonForm[field] = make(map[string][]jsonRowLocation)
		}

		jsonForm[field][term] = append(jsonForm[field][term], jsonRowLocation{
			Field:          field,
			Term:           term,
			Position:       location.GetPosition(),
			Start:          location.GetStart(),
			End:            location.GetEnd(),
			ArrayPositions: location.GetArrayPositions(),
		})
	}

	return jsonForm
}

func psSearchRowFragmentToMap(fragmentMap map[string]*search_v1.SearchQueryResponse_Fragment) map[string][]string {
	var result = make(map[string][]string)
	for key, fragment := range fragmentMap {
		result[key] = fragment.GetContent()
	}

	return result
}

// helper util to convert PS's SearchQueryRow to jsonSearchRow.
func psSearchRowToJSONSearchRow(row *search_v1.SearchQueryResponse_SearchQueryRow) (jsonSearchRow, error) {
	fieldRaw, err := json.Marshal(row.Fields)
	if err != nil {
		return jsonSearchRow{}, err
	}

	return jsonSearchRow{
		ID:          row.Id,
		Index:       row.Index,
		Score:       row.Score,
		Explanation: row.Explanation,
		Locations:   psSearchRowLocationToJSONSearchRowLocations(row.Locations),
		Fragments:   psSearchRowFragmentToMap(row.Fragments),
		Fields:      fieldRaw,
	}, nil

}

// converts from ps search results to jsonRowMessage as bytes for compatibility with existing gocb code.
func psSearchRowToJSONBytes(row *search_v1.SearchQueryResponse_SearchQueryRow) ([]byte, error) {
	convertedRow, err := psSearchRowToJSONSearchRow(row)
	if err != nil {
		return nil, err
	}

	rowBytes, err := json.Marshal(convertedRow)
	if err != nil {
		return nil, err
	}
	return rowBytes, nil

}

func psSearchFacetToJSONSearchFacet(facets map[string]*search_v1.SearchQueryResponse_FacetResult) (map[string]jsonSearchFacet, error) {
	out := make(map[string]jsonSearchFacet)

	for key, facet := range facets {
		switch f := facet.SearchFacet.(type) {
		case *search_v1.SearchQueryResponse_FacetResult_TermFacet:
			terms := make([]jsonSearchTermFacet, len(f.TermFacet.Terms))
			for index, psTerm := range f.TermFacet.Terms {
				terms[index] = jsonSearchTermFacet{
					Term:  psTerm.Name,
					Count: int(psTerm.Size), // TODO: safely convert this.
				}

			}
			out[key] = jsonSearchFacet{
				Name:    f.TermFacet.Name,
				Field:   f.TermFacet.Field,
				Total:   uint64(f.TermFacet.Total), // we can't have negative results and we're casting into a larger space
				Missing: uint64(f.TermFacet.Missing),
				Other:   uint64(f.TermFacet.Other),
				Terms:   terms,
			}
		case *search_v1.SearchQueryResponse_FacetResult_DateRangeFacet:
			ranges := make([]jsonSearchDateFacet, len(f.DateRangeFacet.DateRanges))
			for index, psRange := range f.DateRangeFacet.DateRanges {
				ranges[index] = jsonSearchDateFacet{
					Name:  psRange.Name,
					Start: psRange.Start.AsTime().Format(time.RFC3339),
					End:   psRange.End.AsTime().Format(time.RFC3339),
					Count: int(psRange.Size),
				}

			}
			out[key] = jsonSearchFacet{
				Name:       f.DateRangeFacet.Name,
				Field:      f.DateRangeFacet.Field,
				Total:      uint64(f.DateRangeFacet.Total), // we can't have negative results and we're casting into a larger space
				Missing:    uint64(f.DateRangeFacet.Missing),
				Other:      uint64(f.DateRangeFacet.Other),
				DateRanges: ranges,
			}

		case *search_v1.SearchQueryResponse_FacetResult_NumericRangeFacet:
			ranges := make([]jsonSearchNumericFacet, len(f.NumericRangeFacet.NumericRanges))
			for index, psRange := range f.NumericRangeFacet.NumericRanges {
				ranges[index] = jsonSearchNumericFacet{
					Name:  psRange.Name,
					Min:   float64(psRange.Min),
					Max:   float64(psRange.Max),
					Count: int(psRange.Size),
				}

			}
			out[key] = jsonSearchFacet{
				Name:          f.NumericRangeFacet.Name,
				Field:         f.NumericRangeFacet.Field,
				Total:         uint64(f.NumericRangeFacet.Total), // we can't have negative results and we're casting into a larger space
				Missing:       uint64(f.NumericRangeFacet.Missing),
				Other:         uint64(f.NumericRangeFacet.Other),
				NumericRanges: ranges,
			}

		default:
			return nil, makeInvalidArgumentsError(fmt.Sprintf("invalid search facet return: %s", key))
		}
	}

	return out, nil
}
