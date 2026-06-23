package helpers

import (
	"encoding/json"
	"errors"

	"github.com/couchbase/gocb/v2"
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/sdk/collection/mutatein"

	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/shared"
)

// This file is here because gocbcorex does not support any sort of transcoding so we need
// to pass values in some tests into the gocb transcoder in order to get the []byte value.

func Transcoder(sharedTcoder *shared.Transcoder) (gocb.Transcoder, error) {
	switch sharedTcoder.Transcoder.(type) {
	case *shared.Transcoder_Json:
		return gocb.NewJSONTranscoder(), nil
	case *shared.Transcoder_Legacy:
		return gocb.NewLegacyTranscoder(), nil
	case *shared.Transcoder_RawBinary:
		return gocb.NewRawBinaryTranscoder(), nil
	case *shared.Transcoder_RawJson:
		return gocb.NewRawJSONTranscoder(), nil
	case *shared.Transcoder_RawString:
		return gocb.NewRawStringTranscoder(), nil
	default:
		return nil, errors.New("unknown transcoder")
	}
}

func ContentOrMacro(content *mutatein.ContentOrMacro) (interface{}, error) {
	switch content.ContentOrMacro.(type) {
	case *mutatein.ContentOrMacro_Content:
		return ContentFromShared(content.GetContent())
	case *mutatein.ContentOrMacro_Macro:
		return MacroFromShared(content.GetMacro())
	}
	return "", errors.New("unsupported content type")
}

func ContentFromShared(content *shared.Content) (interface{}, error) {
	switch c := content.Content.(type) {
	case *shared.Content_PassthroughString:
		return c.PassthroughString, nil
	case *shared.Content_ConvertToJson:
		var data interface{}
		err := json.Unmarshal(c.ConvertToJson, &data)
		if err != nil {
			return nil, err
		}

		return data, nil
	case *shared.Content_ByteArray:
		return content.GetByteArray(), nil
	case *shared.Content_Null:
		return nil, nil
	default:
		return "", errors.New("unsupported content type")
	}
}

func MacroFromShared(content mutatein.MutateInMacro) (interface{}, error) {
	switch content.String() {
	case "CAS":
		return gocb.MutationMacroCAS, nil
	case "SEQ_NO":
		return gocb.MutationMacroSeqNo, nil
	case "VALUE_CRC_32C":
		return gocb.MutationMacroValueCRC32c, nil
	}
	return "", errors.New("unsupported macro type")
}
