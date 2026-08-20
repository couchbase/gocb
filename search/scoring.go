package search

// Scoring specifies the scoring mode used for a search request. For a hybrid search (a traditional FTS
// query combined with one or more vector queries) a fusion strategy controls how the FTS and vector result sets
// are merged into a single ranked list.
//
// It is created using NewScoringReciprocalRankFusion, NewScoringRelativeScoreFusion or NewScoringNone.
type Scoring interface {
	// isSearchScoring is an unexported marker method that prevents this interface from being implemented
	// outside of this package.
	isSearchScoring()

	Name() string
	Params() map[string]interface{}
}

// ScoringReciprocalRankFusion merges the FTS and vector result sets of a hybrid search by rank rather
// than raw score. It is the recommended score fusion strategy.
type ScoringReciprocalRankFusion struct {
	rankConstant *uint32
	windowSize   *uint32
}

// NewScoringReciprocalRankFusion creates a new ScoringReciprocalRankFusion.
func NewScoringReciprocalRankFusion() *ScoringReciprocalRankFusion {
	return &ScoringReciprocalRankFusion{}
}

// RankConstant specifies the rank constant used when merging the result sets. The server defaults this to 60.
func (f *ScoringReciprocalRankFusion) RankConstant(rankConstant uint32) *ScoringReciprocalRankFusion {
	f.rankConstant = &rankConstant
	return f
}

// WindowSize specifies how many results per list are considered for fusion. The server defaults this to the
// request Limit.
func (f *ScoringReciprocalRankFusion) WindowSize(windowSize uint32) *ScoringReciprocalRankFusion {
	f.windowSize = &windowSize
	return f
}

func (f *ScoringReciprocalRankFusion) isSearchScoring() {}

func (f *ScoringReciprocalRankFusion) Name() string {
	return "rrf"
}

func (f *ScoringReciprocalRankFusion) Params() map[string]interface{} {
	params := make(map[string]interface{})
	if f.rankConstant != nil {
		params["score_rank_constant"] = *f.rankConstant
	}
	if f.windowSize != nil {
		params["score_window_size"] = *f.windowSize
	}
	return params
}

// ScoringRelativeScoreFusion merges the FTS and vector result sets of a hybrid search by normalized
// score rather than rank.
type ScoringRelativeScoreFusion struct {
	windowSize *uint32
}

// NewScoringRelativeScoreFusion creates a new ScoringRelativeScoreFusion.
func NewScoringRelativeScoreFusion() *ScoringRelativeScoreFusion {
	return &ScoringRelativeScoreFusion{}
}

// WindowSize specifies how many results per list are considered for fusion. The server defaults this to the
// request Limit.
func (f *ScoringRelativeScoreFusion) WindowSize(windowSize uint32) *ScoringRelativeScoreFusion {
	f.windowSize = &windowSize
	return f
}

func (f *ScoringRelativeScoreFusion) isSearchScoring() {}

func (f *ScoringRelativeScoreFusion) Name() string {
	return "rsf"
}

func (f *ScoringRelativeScoreFusion) Params() map[string]interface{} {
	params := make(map[string]interface{})
	if f.windowSize != nil {
		params["score_window_size"] = *f.windowSize
	}
	return params
}

// ScoringNone disables scoring. It is not a fusion strategy and works against any server version.
type ScoringNone struct {
}

// NewScoringNone creates a new ScoringNone.
func NewScoringNone() *ScoringNone {
	return &ScoringNone{}
}

func (f *ScoringNone) isSearchScoring() {}

func (f *ScoringNone) Name() string {
	return "none"
}

func (f *ScoringNone) Params() map[string]interface{} {
	return nil
}
