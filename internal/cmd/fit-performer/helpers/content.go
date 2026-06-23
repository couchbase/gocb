package helpers

import (
	"encoding/json"
	"errors"

	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/shared"
)

func ParseContentAs(as *shared.ContentAs, getContent func(interface{}) error) (*shared.ContentTypes, error) {
	if as == nil {
		var content json.RawMessage
		err := getContent(&content)
		if err != nil {
			return nil, err
		}

		return &shared.ContentTypes{
			Content: &shared.ContentTypes_ContentAsBytes{
				ContentAsBytes: content,
			},
		}, nil
	}
	switch as.As.(type) {
	case *shared.ContentAs_AsString:
		var content string
		err := getContent(&content)
		if err != nil {
			return nil, err
		}

		return &shared.ContentTypes{
			Content: &shared.ContentTypes_ContentAsString{
				ContentAsString: content,
			},
		}, nil
	case *shared.ContentAs_AsByteArray:
		var content []byte
		err := getContent(&content)
		if err != nil {
			return nil, err
		}

		return &shared.ContentTypes{
			Content: &shared.ContentTypes_ContentAsBytes{
				ContentAsBytes: content,
			},
		}, nil
	case *shared.ContentAs_AsJsonObject:
		var content map[string]interface{}
		err := getContent(&content)
		if err != nil {
			return nil, err
		}

		b, err := json.Marshal(content)
		if err != nil {
			return nil, err
		}

		return &shared.ContentTypes{
			Content: &shared.ContentTypes_ContentAsBytes{
				ContentAsBytes: b,
			},
		}, nil
	case *shared.ContentAs_AsJsonArray:
		var content []interface{}
		err := getContent(&content)
		if err != nil {
			return nil, err
		}

		b, err := json.Marshal(content)
		if err != nil {
			return nil, err
		}

		return &shared.ContentTypes{
			Content: &shared.ContentTypes_ContentAsBytes{
				ContentAsBytes: b,
			},
		}, nil
	case *shared.ContentAs_AsBoolean:
		var content bool
		err := getContent(&content)
		if err != nil {
			return nil, err
		}

		return &shared.ContentTypes{
			Content: &shared.ContentTypes_ContentAsBool{
				ContentAsBool: content,
			},
		}, nil
	case *shared.ContentAs_AsInteger:
		var content int64
		err := getContent(&content)
		if err != nil {
			return nil, err
		}

		return &shared.ContentTypes{
			Content: &shared.ContentTypes_ContentAsInt64{
				ContentAsInt64: content,
			},
		}, nil
	case *shared.ContentAs_AsFloatingPoint:
		var content float64
		err := getContent(&content)
		if err != nil {
			return nil, err
		}

		return &shared.ContentTypes{
			Content: &shared.ContentTypes_ContentAsDouble{
				ContentAsDouble: content,
			},
		}, nil
	default:
		return nil, errors.New("unknown content_as type")
	}
}
