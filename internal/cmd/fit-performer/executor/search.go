package executor

import (
	"errors"
	"fmt"
	"time"

	"github.com/couchbase/gocb/v2"
	cbsearch "github.com/couchbase/gocb/v2/search"

	cbvector "github.com/couchbase/gocb/v2/vector"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/helpers"
	fitSearch "github.com/couchbase/gocb/v2/internal/cmd/fit-performer/search"

	searchpb "github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/sdk/search"
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/shared"
)

func (e *Executor) parseSearchQuery(searchQuery *searchpb.SearchQuery) (cbsearch.Query, error) {
	var gocbQuery cbsearch.Query

	switch sharedOp := searchQuery.Query.(type) {
	case *searchpb.SearchQuery_Match:
		q := cbsearch.NewMatchQuery(sharedOp.Match.Match)

		if sharedOp.Match.Operator != nil {
			if sharedOp.Match.GetOperator() == searchpb.MatchOperator_SEARCH_MATCH_OPERATOR_AND {
				q = q.Operator(cbsearch.MatchOperatorAnd)
			} else if sharedOp.Match.GetOperator() == searchpb.MatchOperator_SEARCH_MATCH_OPERATOR_OR {
				q = q.Operator(cbsearch.MatchOperatorOr)
			} else {
				return nil, status.Error(codes.Unimplemented, "unknown operator type")
			}
		}

		if sharedOp.Match.Boost != nil {
			q = q.Boost(sharedOp.Match.GetBoost())
		}
		if sharedOp.Match.Field != nil {
			q = q.Field(sharedOp.Match.GetField())
		}
		if sharedOp.Match.Analyzer != nil {
			q = q.Analyzer(sharedOp.Match.GetAnalyzer())
		}
		if sharedOp.Match.Fuzziness != nil {
			q = q.Fuzziness(uint64(sharedOp.Match.GetFuzziness()))
		}
		if sharedOp.Match.PrefixLength != nil {
			q = q.PrefixLength(uint64(sharedOp.Match.GetPrefixLength()))
		}
		gocbQuery = q
	case *searchpb.SearchQuery_MatchPhrase:
		q := cbsearch.NewMatchPhraseQuery(sharedOp.MatchPhrase.MatchPhrase)
		if sharedOp.MatchPhrase.Boost != nil {
			q = q.Boost(sharedOp.MatchPhrase.GetBoost())
		}
		if sharedOp.MatchPhrase.Field != nil {
			q = q.Field(sharedOp.MatchPhrase.GetField())
		}
		if sharedOp.MatchPhrase.Analyzer != nil {
			q = q.Analyzer(sharedOp.MatchPhrase.GetAnalyzer())
		}
		gocbQuery = q
	case *searchpb.SearchQuery_Regexp:
		q := cbsearch.NewRegexpQuery(sharedOp.Regexp.Regexp)
		if sharedOp.Regexp.Boost != nil {
			q = q.Boost(sharedOp.Regexp.GetBoost())
		}
		if sharedOp.Regexp.Field != nil {
			q = q.Field(sharedOp.Regexp.GetField())
		}
		gocbQuery = q
	case *searchpb.SearchQuery_QueryString:
		q := cbsearch.NewQueryStringQuery(sharedOp.QueryString.Query)
		if sharedOp.QueryString.Boost != nil {
			q = q.Boost(sharedOp.QueryString.GetBoost())
		}
		gocbQuery = q
	case *searchpb.SearchQuery_Wildcard:
		q := cbsearch.NewWildcardQuery(sharedOp.Wildcard.Wildcard)
		if sharedOp.Wildcard.Boost != nil {
			q = q.Boost(sharedOp.Wildcard.GetBoost())
		}
		if sharedOp.Wildcard.Field != nil {
			q = q.Field(sharedOp.Wildcard.GetField())
		}
		gocbQuery = q
	case *searchpb.SearchQuery_DocId:
		q := cbsearch.NewDocIDQuery(sharedOp.DocId.Ids...)
		if sharedOp.DocId.Boost != nil {
			q = q.Boost(sharedOp.DocId.GetBoost())
		}
		gocbQuery = q
	case *searchpb.SearchQuery_SearchBooleanField:
		q := cbsearch.NewBooleanFieldQuery(sharedOp.SearchBooleanField.Bool)
		if sharedOp.SearchBooleanField.Boost != nil {
			q = q.Boost(sharedOp.SearchBooleanField.GetBoost())
		}
		if sharedOp.SearchBooleanField.Field != nil {
			q = q.Field(sharedOp.SearchBooleanField.GetField())
		}
		gocbQuery = q
	case *searchpb.SearchQuery_DateRange:
		q := cbsearch.NewDateRangeQuery()
		if sharedOp.DateRange.Start != nil {
			q.Start(sharedOp.DateRange.GetStart(), sharedOp.DateRange.GetInclusiveStart())
		}
		if sharedOp.DateRange.End != nil {
			q.End(sharedOp.DateRange.GetEnd(), sharedOp.DateRange.GetInclusiveEnd())
		}
		if sharedOp.DateRange.Boost != nil {
			q = q.Boost(sharedOp.DateRange.GetBoost())
		}
		if sharedOp.DateRange.Field != nil {
			q = q.Field(sharedOp.DateRange.GetField())
		}
		if sharedOp.DateRange.DatetimeParser != nil {
			q = q.DateTimeParser(sharedOp.DateRange.GetDatetimeParser())
		}
		gocbQuery = q
	case *searchpb.SearchQuery_NumericRange:
		q := cbsearch.NewNumericRangeQuery()
		if sharedOp.NumericRange.Max != nil {
			q.Max(sharedOp.NumericRange.GetMax(), sharedOp.NumericRange.GetInclusiveMax())
		}
		if sharedOp.NumericRange.Min != nil {
			q.Min(sharedOp.NumericRange.GetMin(), sharedOp.NumericRange.GetInclusiveMin())
		}
		if sharedOp.NumericRange.Boost != nil {
			q = q.Boost(sharedOp.NumericRange.GetBoost())
		}
		if sharedOp.NumericRange.Field != nil {
			q = q.Field(sharedOp.NumericRange.GetField())
		}
		gocbQuery = q
	case *searchpb.SearchQuery_TermRange:

		q := cbsearch.NewTermRangeQuery()
		// [else]

		if sharedOp.TermRange.Max != nil {
			q.Max(sharedOp.TermRange.GetMax(), sharedOp.TermRange.GetInclusiveMax())
		}
		if sharedOp.TermRange.Min != nil {
			q.Min(sharedOp.TermRange.GetMin(), sharedOp.TermRange.GetInclusiveMin())
		}
		if sharedOp.TermRange.Boost != nil {
			q = q.Boost(sharedOp.TermRange.GetBoost())
		}
		if sharedOp.TermRange.Field != nil {
			q = q.Field(sharedOp.TermRange.GetField())
		}
		gocbQuery = q
	case *searchpb.SearchQuery_GeoDistance:
		q := cbsearch.NewGeoDistanceQuery(
			float64(sharedOp.GeoDistance.Location.GetLon()),
			float64(sharedOp.GeoDistance.Location.GetLat()),
			sharedOp.GeoDistance.Distance)
		if sharedOp.GeoDistance.Boost != nil {
			q = q.Boost(sharedOp.GeoDistance.GetBoost())
		}
		if sharedOp.GeoDistance.Field != nil {
			q = q.Field(sharedOp.GeoDistance.GetField())
		}
		gocbQuery = q
	case *searchpb.SearchQuery_GeoBoundingBox:
		q := cbsearch.NewGeoBoundingBoxQuery(
			float64(sharedOp.GeoBoundingBox.TopLeft.GetLon()),
			float64(sharedOp.GeoBoundingBox.TopLeft.GetLat()),
			float64(sharedOp.GeoBoundingBox.BottomRight.GetLon()),
			float64(sharedOp.GeoBoundingBox.BottomRight.GetLat()))
		if sharedOp.GeoBoundingBox.Boost != nil {
			q = q.Boost(sharedOp.GeoBoundingBox.GetBoost())
		}
		if sharedOp.GeoBoundingBox.Field != nil {
			q = q.Field(sharedOp.GeoBoundingBox.GetField())
		}
		gocbQuery = q
	case *searchpb.SearchQuery_Conjunction:
		q := cbsearch.NewConjunctionQuery()
		for _, cQuery := range sharedOp.Conjunction.Conjuncts {
			parsed, err := e.parseSearchQuery(cQuery)
			if err != nil {
				return nil, err
			}
			q = q.And(parsed)
		}
		if sharedOp.Conjunction.Boost != nil {
			q = q.Boost(sharedOp.Conjunction.GetBoost())
		}
		gocbQuery = q
	case *searchpb.SearchQuery_Disjunction:
		q := cbsearch.NewDisjunctionQuery()
		for _, cQuery := range sharedOp.Disjunction.Disjuncts {
			parsed, err := e.parseSearchQuery(cQuery)
			if err != nil {
				return nil, err
			}
			q = q.Or(parsed)
		}
		if sharedOp.Disjunction.Boost != nil {
			q = q.Boost(sharedOp.Disjunction.GetBoost())
		}

		if sharedOp.Disjunction.Min != nil {
			q = q.Min(*sharedOp.Disjunction.Min)
		}

		gocbQuery = q
	case *searchpb.SearchQuery_Boolean:
		q := cbsearch.NewBooleanQuery()

		mustQueries, err := e.parseSearchQueries(sharedOp.Boolean.Must)
		if err != nil {
			return nil, err
		}
		mustNotQueries, err := e.parseSearchQueries(sharedOp.Boolean.MustNot)
		if err != nil {
			return nil, err
		}
		shouldQueries, err := e.parseSearchQueries(sharedOp.Boolean.Should)
		if err != nil {
			return nil, err
		}

		q.Must(mustQueries...).MustNot(mustNotQueries...).Should(shouldQueries...)

		// [else]

		if sharedOp.Boolean.Boost != nil {
			q = q.Boost(sharedOp.Boolean.GetBoost())
		}
		if sharedOp.Boolean.ShouldMin != nil {
			q = q.ShouldMin(*sharedOp.Boolean.ShouldMin)
		}
		gocbQuery = q
	case *searchpb.SearchQuery_Term:
		q := cbsearch.NewTermQuery(sharedOp.Term.Term)
		if sharedOp.Term.Boost != nil {
			q = q.Boost(sharedOp.Term.GetBoost())
		}
		if sharedOp.Term.Field != nil {
			q = q.Field(sharedOp.Term.GetField())
		}
		if sharedOp.Term.Fuzziness != nil {
			q = q.Fuzziness(uint64(sharedOp.Term.GetFuzziness()))
		}
		if sharedOp.Term.PrefixLength != nil {
			q = q.PrefixLength(uint64(sharedOp.Term.GetPrefixLength()))
		}
		gocbQuery = q
	case *searchpb.SearchQuery_Prefix:
		q := cbsearch.NewPrefixQuery(sharedOp.Prefix.Prefix)
		if sharedOp.Prefix.Boost != nil {
			q = q.Boost(sharedOp.Prefix.GetBoost())
		}
		if sharedOp.Prefix.Field != nil {
			q = q.Field(sharedOp.Prefix.GetField())
		}
		gocbQuery = q
	case *searchpb.SearchQuery_Phrase:
		q := cbsearch.NewPhraseQuery(sharedOp.Phrase.Terms...)
		if sharedOp.Phrase.Boost != nil {
			q = q.Boost(sharedOp.Phrase.GetBoost())
		}
		if sharedOp.Phrase.Field != nil {
			q = q.Field(sharedOp.Phrase.GetField())
		}
		gocbQuery = q
	case *searchpb.SearchQuery_MatchAll:
		q := cbsearch.NewMatchAllQuery()
		gocbQuery = q
	case *searchpb.SearchQuery_MatchNone:
		q := cbsearch.NewMatchNoneQuery()
		gocbQuery = q
	default:
		return nil, status.Error(codes.Unimplemented, "unknown search query type")
	}

	return gocbQuery, nil
}

func (e *Executor) parseSearchQueries(searchQueries []*searchpb.SearchQuery) ([]cbsearch.Query, error) {
	if len(searchQueries) == 0 {
		return nil, nil
	}

	gocbQueries := make([]cbsearch.Query, len(searchQueries))
	for i, searchQuery := range searchQueries {
		query, err := e.parseSearchQuery(searchQuery)
		if err != nil {
			return nil, err
		}
		gocbQueries[i] = query
	}

	return gocbQueries, nil
}

func (e *Executor) parseSearchFacetsOption(facets map[string]*searchpb.SearchFacet) (map[string]cbsearch.Facet, error) {
	if len(facets) == 0 {
		return nil, nil
	}

	gocbFacets := make(map[string]cbsearch.Facet, len(facets))
	for k, facet := range facets {
		switch f := facet.Facet.(type) {
		case *searchpb.SearchFacet_Term:
			gocbFacet := cbsearch.NewTermFacet(f.Term.Field, uint64(f.Term.GetSize()))
			gocbFacets[k] = gocbFacet
		case *searchpb.SearchFacet_DateRange:
			gocbFacet := cbsearch.NewDateFacet(f.DateRange.Field, uint64(f.DateRange.GetSize()))
			for _, dateRange := range f.DateRange.DateRanges {
				gocbFacet.AddRange(dateRange.Name, dateRange.Start.AsTime().Format("2006-01-02 15:04:05"), dateRange.End.AsTime().Format("2006-01-02 15:04:05"))
			}

			gocbFacets[k] = gocbFacet
		case *searchpb.SearchFacet_NumericRange:
			gocbFacet := cbsearch.NewNumericFacet(f.NumericRange.Field, uint64(f.NumericRange.GetSize()))
			for _, numRange := range f.NumericRange.NumericRanges {
				gocbFacet.AddRange(numRange.Name, float64(numRange.GetMin()), float64(numRange.GetMax()))
			}
			gocbFacets[k] = gocbFacet
		default:
			return nil, errors.New("unknown facet type")
		}
	}

	return gocbFacets, nil
}

func (e *Executor) parseSearchSortOption(sorts []*searchpb.SearchSort) ([]cbsearch.Sort, error) {
	if len(sorts) == 0 {
		return nil, nil
	}

	gocbSorts := make([]cbsearch.Sort, len(sorts))
	for i, sort := range sorts {
		switch s := sort.Sort.(type) {
		case *searchpb.SearchSort_Id:
			gocbSort := cbsearch.NewSearchSortID()
			if s.Id.Desc != nil {
				gocbSort.Descending(s.Id.GetDesc())
			}
			gocbSorts[i] = gocbSort
		case *searchpb.SearchSort_Field:
			gocbSort := cbsearch.NewSearchSortField(s.Field.Field)
			if s.Field.Desc != nil {
				gocbSort.Descending(s.Field.GetDesc())
			}
			if s.Field.Mode != nil {
				gocbSort.Mode(s.Field.GetMode())
			}
			if s.Field.Missing != nil {
				gocbSort.Missing(s.Field.GetMissing())
			}
			if s.Field.Type != nil {
				gocbSort.Type(s.Field.GetType())
			}
			gocbSorts[i] = gocbSort
		case *searchpb.SearchSort_GeoDistance:
			gocbSort := cbsearch.NewSearchSortGeoDistance(
				s.GeoDistance.Field,
				float64(s.GeoDistance.Location.Lon),
				float64(s.GeoDistance.Location.Lat),
			)
			if s.GeoDistance.Desc != nil {
				gocbSort.Descending(s.GeoDistance.GetDesc())
			}
			if s.GeoDistance.Unit != nil {
				unit, err := convertGeoDistanceUnit(s.GeoDistance.GetUnit())
				if err != nil {
					return nil, err
				}
				gocbSort.Unit(unit)
			}
			gocbSorts[i] = gocbSort
		case *searchpb.SearchSort_Score:
			gocbSort := cbsearch.NewSearchSortScore()
			if s.Score.Desc != nil {
				gocbSort.Descending(s.Score.GetDesc())
			}
			gocbSorts[i] = gocbSort
		default:
			return nil, errors.New("unknown sort type")
		}
	}

	return gocbSorts, nil
}

func (e *Executor) parseSearchResult(fieldContentAs *shared.ContentAs, res *gocb.SearchResult) (*searchpb.BlockingSearchResult, error) {
	rows, err := fitSearch.ParseSearchRows(fieldContentAs, res)
	if err != nil {
		return nil, err
	}
	gocbFacets, err := res.Facets()
	if err != nil {
		return nil, err
	}
	facets, err := fitSearch.ParseSearchResultFacets(gocbFacets)
	if err != nil {
		return nil, err
	}
	gocbMeta, err := res.MetaData()
	if err != nil {
		return nil, err
	}
	meta, err := fitSearch.ParseSearchResultMeta(gocbMeta)
	if err != nil {
		return nil, err
	}

	return &searchpb.BlockingSearchResult{
		Rows:     rows,
		Facets:   facets,
		MetaData: meta,
	}, nil
}

func (e *Executor) parseSearchOptions(protoOpts *searchpb.SearchOptions) (*gocb.SearchOptions, error) {
	if protoOpts == nil {
		return nil, nil
	}

	facets, err := e.parseSearchFacetsOption(protoOpts.Facets)
	if err != nil {
		return nil, status.Error(codes.Unimplemented, "unknown command type")
	}

	sorts, err := e.parseSearchSortOption(protoOpts.Sort)
	if err != nil {
		return nil, status.Error(codes.Unimplemented, "unknown command type")
	}

	opts := &gocb.SearchOptions{
		Fields: protoOpts.Fields,
		Sort:   sorts,
		Facets: facets,
	}
	if protoOpts.TimeoutMillis != nil {
		opts.Timeout = time.Duration(protoOpts.GetTimeoutMillis()) * time.Millisecond
	}
	if protoOpts.ScanConsistency != nil {
		switch *protoOpts.ScanConsistency {
		case searchpb.SearchScanConsistency_SEARCH_SCAN_CONSISTENCY_NOT_BOUNDED:
			opts.ScanConsistency = gocb.SearchScanConsistencyNotBounded
		default:
			return nil, status.Error(codes.Unimplemented, "unknown scan consistency type")
		}
	}
	if protoOpts.Limit != nil {
		opts.Limit = protoOpts.GetLimit()
	}
	if protoOpts.Skip != nil {
		opts.Skip = protoOpts.GetSkip()
	}
	if protoOpts.Explain != nil {
		opts.Explain = protoOpts.GetExplain()
	}
	if protoOpts.Highlight != nil {
		opts.Highlight = &gocb.SearchHighlightOptions{
			Fields: protoOpts.Fields,
		}
		if protoOpts.Highlight.Style != nil {
			switch *protoOpts.Highlight.Style {
			case searchpb.HighlightStyle_HIGHLIGHT_STYLE_HTML:
				opts.Highlight.Style = gocb.HTMLHighlightStyle
			case searchpb.HighlightStyle_HIGHLIGHT_STYLE_ANSI:
				opts.Highlight.Style = gocb.AnsiHightlightStyle
			default:
				return nil, status.Error(codes.Unimplemented, "unknown highlight style type")
			}
		}
	}
	if protoOpts.ConsistentWith != nil {
		consistentWith, err := helpers.ProtoMutationStateToGocb(protoOpts.ConsistentWith)
		if err != nil {
			return nil, err
		}
		opts.ConsistentWith = consistentWith
	}
	if protoOpts.Raw != nil {
		opts.Raw = make(map[string]interface{}, len(protoOpts.Raw))
		for k, v := range protoOpts.Raw {
			opts.Raw[k] = v
		}
	}

	if protoOpts.IncludeLocations != nil {
		opts.IncludeLocations = protoOpts.GetIncludeLocations()
	}

	if protoOpts.ParentSpanId != nil {
		parent, ok := e.spanOwner.GetSpan(*protoOpts.ParentSpanId)
		if !ok {
			return nil, fmt.Errorf("unknown parent span id: %s", *protoOpts.ParentSpanId)
		}
		opts.ParentSpan = parent
	}

	return opts, nil
}

func (e *Executor) parseSearchRequest(request *searchpb.SearchRequest) (*gocb.SearchRequest, error) {
	var query cbsearch.Query
	if request.SearchQuery != nil {
		q, err := e.parseSearchQuery(request.SearchQuery)
		if err != nil {
			return nil, err
		}
		query = q
	}
	var search *cbvector.Search
	if request.VectorSearch != nil {
		s, err := e.parseVectorSearch(request.VectorSearch)
		if err != nil {
			return nil, err
		}
		search = s
	}
	return &gocb.SearchRequest{
		VectorSearch: search,
		SearchQuery:  query,
	}, nil
}

func (e *Executor) parseVectorSearch(vectorSearch *searchpb.VectorSearch) (*cbvector.Search, error) {
	queries := make([]*cbvector.Query, len(vectorSearch.VectorQuery))
	for i, query := range vectorSearch.VectorQuery {
		if len(query.VectorQuery) > 0 && query.Base64VectorQuery != nil {
			return nil, errors.New("only one of vector query and base64 vector query can be set")
		}

		var q *cbvector.Query
		if query.Base64VectorQuery != nil {
			var err error
			q, err = makeBase64VectorQuery(query)
			if err != nil {
				return nil, err
			}
		} else {
			q = cbvector.NewQuery(query.VectorFieldName, query.VectorQuery)
		}
		if query.Options != nil {
			if query.Options.NumCandidates != nil {
				q = q.NumCandidates(uint32(query.Options.GetNumCandidates()))
			}
			if query.Options.Boost != nil {
				q = q.Boost(query.Options.GetBoost())
			}

			if query.Options.Prefilter != nil {
				prefilter, err := e.parseSearchQuery(query.Options.Prefilter)
				if err != nil {
					return nil, err
				}
				q = q.Prefilter(prefilter)
			}

		}

		queries[i] = q
	}

	opts := &cbvector.SearchOptions{}
	if vectorSearch.Options != nil {
		if vectorSearch.Options.VectorQueryCombination != nil {
			switch *vectorSearch.Options.VectorQueryCombination {
			case searchpb.VectorQueryCombination_AND:
				opts.VectorQueryCombination = cbvector.VectorQueryCombinationAnd
			case searchpb.VectorQueryCombination_OR:
				opts.VectorQueryCombination = cbvector.VectorQueryCombinationOr
			default:
				return nil, errors.New("unknown vector query combination")
			}
		}
	}

	return cbvector.NewSearch(queries, opts), nil
}

// This is lifted out of the above so that we can wrap it in a start/end block.

func makeBase64VectorQuery(query *searchpb.VectorQuery) (*cbvector.Query, error) {
	return cbvector.NewBase64Query(query.VectorFieldName, *query.Base64VectorQuery), nil
}

func convertGeoDistanceUnit(units searchpb.SearchGeoDistanceUnits) (string, error) {
	// Temporarily returns string until GOCBC-1580.
	switch units {
	case searchpb.SearchGeoDistanceUnits_SEARCH_GEO_DISTANCE_UNITS_METERS:
		return "meters", nil
	case searchpb.SearchGeoDistanceUnits_SEARCH_GEO_DISTANCE_UNITS_CENTIMETERS:
		return "centimeters", nil
	case searchpb.SearchGeoDistanceUnits_SEARCH_GEO_DISTANCE_UNITS_FEET:
		return "feet", nil
	case searchpb.SearchGeoDistanceUnits_SEARCH_GEO_DISTANCE_UNITS_INCHES:
		return "inches", nil
	case searchpb.SearchGeoDistanceUnits_SEARCH_GEO_DISTANCE_UNITS_KILOMETERS:
		return "kilometers", nil
	case searchpb.SearchGeoDistanceUnits_SEARCH_GEO_DISTANCE_UNITS_MILES:
		return "miles", nil
	case searchpb.SearchGeoDistanceUnits_SEARCH_GEO_DISTANCE_UNITS_MILLIMETERS:
		return "millimeters", nil
	case searchpb.SearchGeoDistanceUnits_SEARCH_GEO_DISTANCE_UNITS_NAUTICAL_MILES:
		return "nauticalmiles", nil
	case searchpb.SearchGeoDistanceUnits_SEARCH_GEO_DISTANCE_UNITS_YARDS:
		return "yards", nil
	default:
		return "", errors.New("unknown geo distance units")
	}
}
