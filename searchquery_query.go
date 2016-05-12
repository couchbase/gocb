package gocb

import (
	"encoding/json"
)

type FtsQuery interface {
	json.Marshaler
}

type ftsQueryBase struct {
	options map[string]interface{}
}

func newFtsQueryBase() ftsQueryBase {
	return ftsQueryBase{
		options: make(map[string]interface{}),
	}
}

func (q ftsQueryBase) MarshalJSON() ([]byte, error) {
	return json.Marshal(q.options)
}

type MatchQuery struct {
	ftsQueryBase
}

func NewMatchQuery(match string) *MatchQuery {
	mq := &MatchQuery{newFtsQueryBase()}
	mq.options["match"] = match
	return mq
}

func (mq *MatchQuery) Field(field string) *MatchQuery {
	mq.options["field"] = field
	return mq
}

func (mq *MatchQuery) Analyzer(analyzer string) *MatchQuery {
	mq.options["analyzer"] = analyzer
	return mq
}

func (mq *MatchQuery) PrefixLength(length int) *MatchQuery {
	mq.options["prefix_length"] = length
	return mq
}

func (mq *MatchQuery) Fuzziness(fuzziness int) *MatchQuery {
	mq.options["fuzziness"] = fuzziness
	return mq
}

func (mq *MatchQuery) Boost(boost float32) *MatchQuery {
	mq.options["boost"] = boost
	return mq
}

type MatchPhraseQuery struct {
	ftsQueryBase
}

func NewMatchPhraseQuery(phrase string) *MatchPhraseQuery {
	mq := &MatchPhraseQuery{newFtsQueryBase()}
	mq.options["match_phrase"] = phrase
	return mq
}

func (mq *MatchPhraseQuery) Field(field string) *MatchPhraseQuery {
	mq.options["field"] = field
	return mq
}

func (mq *MatchPhraseQuery) Analyzer(analyzer string) *MatchPhraseQuery {
	mq.options["analyzer"] = analyzer
	return mq
}

func (mq *MatchPhraseQuery) Boost(boost float32) *MatchPhraseQuery {
	mq.options["boost"] = boost
	return mq
}

type RegexpQuery struct {
	ftsQueryBase
}

func NewRegexpQuery(regexp string) *RegexpQuery {
	mq := &RegexpQuery{newFtsQueryBase()}
	mq.options["regexp"] = regexp
	return mq
}

func (mq *RegexpQuery) Field(field string) *RegexpQuery {
	mq.options["field"] = field
	return mq
}

func (mq *RegexpQuery) Boost(boost float32) *RegexpQuery {
	mq.options["boost"] = boost
	return mq
}

type StringQuery struct {
	ftsQueryBase
}

func NewStringQuery(query string) *StringQuery {
	mq := &StringQuery{newFtsQueryBase()}
	mq.options["query"] = query
	return mq
}

func (mq *StringQuery) Boost(boost float32) *StringQuery {
	mq.options["boost"] = boost
	return mq
}

type NumericRangeQuery struct {
	ftsQueryBase
}

func NewNumericRangeQuery() *NumericRangeQuery {
	mq := &NumericRangeQuery{newFtsQueryBase()}
	return mq
}

func (mq *NumericRangeQuery) Min(min float32, inclusive bool) *NumericRangeQuery {
	mq.options["min"] = min
	mq.options["inclusive_min"] = inclusive
	return mq
}

func (mq *NumericRangeQuery) Max(max float32, inclusive bool) *NumericRangeQuery {
	mq.options["max"] = max
	mq.options["inclusive_max"] = inclusive
	return mq
}

func (mq *NumericRangeQuery) Field(field string) *NumericRangeQuery {
	mq.options["field"] = field
	return mq
}

func (mq *NumericRangeQuery) Boost(boost float32) *NumericRangeQuery {
	mq.options["boost"] = boost
	return mq
}

type DateRangeQuery struct {
	ftsQueryBase
}

func NewDateRangeQuery() *DateRangeQuery {
	mq := &DateRangeQuery{newFtsQueryBase()}
	return mq
}

func (mq *DateRangeQuery) Start(start string, inclusive bool) *DateRangeQuery {
	mq.options["start"] = start
	mq.options["inclusive_start"] = inclusive
	return mq
}

func (mq *DateRangeQuery) End(end string, inclusive bool) *DateRangeQuery {
	mq.options["end"] = end
	mq.options["inclusive_end"] = inclusive
	return mq
}

func (mq *DateRangeQuery) DateTimeParser(parser string) *DateRangeQuery {
	mq.options["datetime_parser"] = parser
	return mq
}

func (mq *DateRangeQuery) Field(field string) *DateRangeQuery {
	mq.options["field"] = field
	return mq
}

func (mq *DateRangeQuery) Boost(boost float32) *DateRangeQuery {
	mq.options["boost"] = boost
	return mq
}

type ConjunctionQuery struct {
	ftsQueryBase
}

func NewConjunctionQuery(queries ...FtsQuery) *ConjunctionQuery {
	mq := &ConjunctionQuery{newFtsQueryBase()}
	mq.options["conjuncts"] = []FtsQuery{}
	return mq.And(queries...)
}

func (mq *ConjunctionQuery) And(queries ...FtsQuery) *ConjunctionQuery {
	mq.options["conjuncts"] = append(mq.options["conjuncts"].([]FtsQuery), queries...)
	return mq
}

func (mq *ConjunctionQuery) Boost(boost float32) *ConjunctionQuery {
	mq.options["boost"] = boost
	return mq
}

type DisjunctionQuery struct {
	ftsQueryBase
}

func NewDisjunctionQuery(queries ...FtsQuery) *DisjunctionQuery {
	mq := &DisjunctionQuery{newFtsQueryBase()}
	mq.options["disjuncts"] = []FtsQuery{}
	return mq.Or(queries...)
}

func (mq *DisjunctionQuery) Or(queries ...FtsQuery) *DisjunctionQuery {
	mq.options["disjuncts"] = append(mq.options["disjuncts"].([]FtsQuery), queries...)
	return mq
}

func (mq *DisjunctionQuery) Boost(boost float32) *DisjunctionQuery {
	mq.options["boost"] = boost
	return mq
}

type booleanQueryData struct {
	Must    *ConjunctionQuery `json:"must,omitempty"`
	Should  *DisjunctionQuery `json:"should,omitempty"`
	MustNot *DisjunctionQuery `json:"must_not,omitempty"`
	Boost   float32           `json:"boost,omitempty"`
}

type BooleanQuery struct {
	data      booleanQueryData
	shouldMin int
}

func NewBooleanQuery() *BooleanQuery {
	mq := &BooleanQuery{}
	return mq
}

func (mq *BooleanQuery) Must(query FtsQuery) *BooleanQuery {
	switch val := query.(type) {
	case ConjunctionQuery:
		query = &val
	case *ConjunctionQuery:
		// Do nothing
	default:
		query = NewConjunctionQuery(val)
	}
	mq.data.Must = query.(*ConjunctionQuery)
	return mq
}

func (mq *BooleanQuery) Should(query FtsQuery) *BooleanQuery {
	switch val := query.(type) {
	case DisjunctionQuery:
		query = &val
	case *DisjunctionQuery:
	// Do nothing
	default:
		query = NewDisjunctionQuery(val)
	}
	mq.data.Should = query.(*DisjunctionQuery)
	return mq
}

func (mq *BooleanQuery) MustNot(query FtsQuery) *BooleanQuery {
	switch val := query.(type) {
	case DisjunctionQuery:
		query = &val
	case *DisjunctionQuery:
	// Do nothing
	default:
		query = NewDisjunctionQuery(val)
	}
	mq.data.MustNot = query.(*DisjunctionQuery)
	return mq
}

func (mq *BooleanQuery) ShouldMin(min int) *BooleanQuery {
	mq.shouldMin = min
	return mq
}

func (mq *BooleanQuery) Boost(boost float32) *BooleanQuery {
	mq.data.Boost = boost
	return mq
}

func (q *BooleanQuery) MarshalJSON() ([]byte, error) {
	if q.data.Should != nil {
		q.data.Should.options["min"] = q.shouldMin
	}
	bytes, err := json.Marshal(q.data)
	if q.data.Should != nil {
		delete(q.data.Should.options, "min")
	}
	return bytes, err
}

type WildcardQuery struct {
	ftsQueryBase
}

func NewWildcardQuery(wildcard string) *WildcardQuery {
	mq := &WildcardQuery{newFtsQueryBase()}
	mq.options["wildcard"] = wildcard
	return mq
}

func (mq *WildcardQuery) Field(field string) *WildcardQuery {
	mq.options["field"] = field
	return mq
}

func (mq *WildcardQuery) Boost(boost float32) *WildcardQuery {
	mq.options["boost"] = boost
	return mq
}

type DocIdQuery struct {
	ftsQueryBase
}

func NewDocIdQuery(ids ...string) *DocIdQuery {
	mq := &DocIdQuery{newFtsQueryBase()}
	mq.options["ids"] = []string{}
	return mq.AddDocIds(ids...)
}

func (mq *DocIdQuery) AddDocIds(ids ...string) *DocIdQuery {
	mq.options["ids"] = append(mq.options["ids"].([]string), ids...)
	return mq
}

func (mq *DocIdQuery) Field(field string) *DocIdQuery {
	mq.options["field"] = field
	return mq
}

func (mq *DocIdQuery) Boost(boost float32) *DocIdQuery {
	mq.options["boost"] = boost
	return mq
}

type BooleanFieldQuery struct {
	ftsQueryBase
}

func NewBooleanFieldQuery(val bool) *BooleanFieldQuery {
	mq := &BooleanFieldQuery{newFtsQueryBase()}
	mq.options["bool"] = val
	return mq
}

func (mq *BooleanFieldQuery) Field(field string) *BooleanFieldQuery {
	mq.options["field"] = field
	return mq
}

func (mq *BooleanFieldQuery) Boost(boost float32) *BooleanFieldQuery {
	mq.options["boost"] = boost
	return mq
}

type TermQuery struct {
	ftsQueryBase
}

func NewTermQuery(term string) *TermQuery {
	mq := &TermQuery{newFtsQueryBase()}
	mq.options["term"] = term
	return mq
}

func (mq *TermQuery) Field(field string) *TermQuery {
	mq.options["field"] = field
	return mq
}

func (mq *TermQuery) PrefixLength(length int) *TermQuery {
	mq.options["prefix_length"] = length
	return mq
}

func (mq *TermQuery) Fuzziness(fuzziness int) *TermQuery {
	mq.options["fuzziness"] = fuzziness
	return mq
}

func (mq *TermQuery) Boost(boost float32) *TermQuery {
	mq.options["boost"] = boost
	return mq
}

type PhraseQuery struct {
	ftsQueryBase
}

func NewPhraseQuery(terms ...string) *PhraseQuery {
	mq := &PhraseQuery{newFtsQueryBase()}
	mq.options["terms"] = terms
	return mq
}

func (mq *PhraseQuery) Field(field string) *PhraseQuery {
	mq.options["field"] = field
	return mq
}

func (mq *PhraseQuery) Boost(boost float32) *PhraseQuery {
	mq.options["boost"] = boost
	return mq
}

type PrefixQuery struct {
	ftsQueryBase
}

func NewPrefixQuery(prefix string) *PrefixQuery {
	mq := &PrefixQuery{newFtsQueryBase()}
	mq.options["prefix"] = prefix
	return mq
}

func (mq *PrefixQuery) Field(field string) *PrefixQuery {
	mq.options["field"] = field
	return mq
}

func (mq *PrefixQuery) Boost(boost float32) *PrefixQuery {
	mq.options["boost"] = boost
	return mq
}

type MatchAllQuery struct {
}

func NewMatchAllQuery(prefix string) *MatchAllQuery {
	return &MatchAllQuery{}
}

type MatchNoneQuery struct {
}

func NewMatchNoneQuery(prefix string) *MatchNoneQuery {
	return &MatchNoneQuery{}
}
