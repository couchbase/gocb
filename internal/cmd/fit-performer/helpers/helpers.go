package helpers

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/run"
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/sdk"
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/sdk/kv"
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/sdk/kv/lookupin"
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/sdk/kv/rangescan"
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/sdk/search"
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/shared"
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/streams"

	"github.com/couchbase/gocb/v2"
)

func Expiry(expiry *shared.Expiry) (time.Duration, error) {
	switch expiryType := expiry.ExpiryType.(type) {
	case *shared.Expiry_RelativeSecs:
		return time.Duration(expiryType.RelativeSecs) * time.Second, nil
	default:
		return 0, errors.New("unsupported expiry type")
	}
}

func ProtocolDuraToSDK(level shared.Durability) gocb.DurabilityLevel {
	switch level {
	case shared.Durability_NONE:
		return gocb.DurabilityLevelNone
	case shared.Durability_MAJORITY:
		return gocb.DurabilityLevelMajority
	case shared.Durability_MAJORITY_AND_PERSIST_TO_ACTIVE:
		return gocb.DurabilityLevelMajorityAndPersistOnMaster
	case shared.Durability_PERSIST_TO_MAJORITY:
		return gocb.DurabilityLevelPersistToMajority
	default:

		return gocb.DurabilityLevelUnknown
		// [else]

	}
}

func SDKDuraToProtocol(level gocb.DurabilityLevel) shared.Durability {
	switch level {
	case gocb.DurabilityLevelNone:
		return shared.Durability_NONE
	case gocb.DurabilityLevelMajority:
		return shared.Durability_MAJORITY
	case gocb.DurabilityLevelPersistToMajority:
		return shared.Durability_PERSIST_TO_MAJORITY
	case gocb.DurabilityLevelMajorityAndPersistOnMaster:
		return shared.Durability_MAJORITY_AND_PERSIST_TO_ACTIVE
	}

	return -1
}

func ScanConsistencyToGocb(sc shared.ScanConsistency) gocb.QueryScanConsistency {
	switch sc {
	case shared.ScanConsistency_REQUEST_PLUS:
		return gocb.QueryScanConsistencyRequestPlus
	case shared.ScanConsistency_NOT_BOUNDED:
		return gocb.QueryScanConsistencyNotBounded
	default:
		panic("unrecognised scan consistency")
	}
}

type bucketTokens map[string][]interface{}
type mutationStateData map[string]*bucketTokens

func ProtoMutationStateToGocb(state *shared.MutationState) (*gocb.MutationState, error) {
	// This is a bit weird but gocb doesn't expose a way to create mutation tokens we generate a JSON representation
	// and then deserialize it to a gocb mutation state.
	var data mutationStateData
	for _, token := range state.Tokens {
		if data == nil {
			data = make(mutationStateData)
		}

		bucketName := token.BucketName
		if (data)[bucketName] == nil {
			tokens := make(bucketTokens)
			(data)[bucketName] = &tokens
		}

		vbID := fmt.Sprintf("%d", token.PartitionId)
		stateToken := (*(data)[bucketName])[vbID]
		if stateToken == nil {
			stateToken = make([]interface{}, 2)
			(*(data)[bucketName])[vbID] = stateToken
		}

		stateToken[0] = uint64(token.SequenceNumber)
		stateToken[1] = fmt.Sprintf("%d", token.PartitionUuid)

	}

	b, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}

	var gocbState *gocb.MutationState
	err = json.Unmarshal(b, &gocbState)
	if err != nil {
		return nil, err
	}

	return gocbState, nil
}

func MakeStreamErrorResult(streamID string, err error) *run.Result {
	return &run.Result{
		Result: &run.Result_Stream{
			Stream: &streams.Signal{
				Signal: &streams.Signal_Error{
					Error: &streams.Error{
						StreamId:  streamID,
						Exception: MapErrorToProto(err),
					},
				},
			},
		},
	}
}

func MakeSearchStreamMetaResult(streamID string, meta *search.SearchMetaData) *run.Result {
	return &run.Result{
		Result: &run.Result_Sdk{
			Sdk: &sdk.Result{
				Result: &sdk.Result_SearchStreamingResult{
					SearchStreamingResult: &search.StreamingSearchResult{
						StreamId: streamID,
						Result: &search.StreamingSearchResult_MetaData{
							MetaData: meta,
						},
					},
				},
			},
		},
	}
}

func MakeSearchStreamFacetsResult(streamID string, facets *search.SearchFacets) *run.Result {
	return &run.Result{
		Result: &run.Result_Sdk{
			Sdk: &sdk.Result{
				Result: &sdk.Result_SearchStreamingResult{
					SearchStreamingResult: &search.StreamingSearchResult{
						StreamId: streamID,
						Result: &search.StreamingSearchResult_Facets{
							Facets: facets,
						},
					},
				},
			},
		},
	}
}

func MakeSearchStreamRowResult(streamID string, row *search.SearchRow) *run.Result {
	return &run.Result{
		Result: &run.Result_Sdk{
			Sdk: &sdk.Result{
				Result: &sdk.Result_SearchStreamingResult{
					SearchStreamingResult: &search.StreamingSearchResult{
						StreamId: streamID,
						Result: &search.StreamingSearchResult_Row{
							Row: row,
						},
					},
				},
			},
		},
	}
}

func MakeStreamCompleteResult(streamID string) *run.Result {
	return &run.Result{
		Result: &run.Result_Stream{
			Stream: &streams.Signal{
				Signal: &streams.Signal_Complete{
					Complete: &streams.Complete{
						StreamId: streamID,
					},
				},
			},
		},
	}
}

func MakeRangeScanSuccessResult(result *rangescan.ScanResult) *run.Result {
	return &run.Result{
		Result: &run.Result_Sdk{
			Sdk: &sdk.Result{
				Result: &sdk.Result_RangeScanResult{
					RangeScanResult: result,
				},
			},
		},
	}
}

func MakeGetAllReplicasSuccessResult(result *kv.GetReplicaResult) *run.Result {
	return &run.Result{
		Result: &run.Result_Sdk{
			Sdk: &sdk.Result{
				Result: &sdk.Result_GetReplicaResult{
					GetReplicaResult: result,
				},
			},
		},
	}
}

func MakeLookupInAllReplicasSuccessResult(result *lookupin.LookupInAllReplicasResult) *run.Result {
	return &run.Result{
		Result: &run.Result_Sdk{
			Sdk: &sdk.Result{
				Result: &sdk.Result_LookupInAllReplicasResult{
					LookupInAllReplicasResult: result,
				},
			},
		},
	}
}
