package gocb

import (
	"encoding/json"
)

type Transcoder interface {
	Decode([]byte, uint32, interface{}) error
	Encode(interface{}) ([]byte, uint32, error)
}

type DefaultTranscoder struct {
}

func (t DefaultTranscoder) Decode(bytes []byte, flags uint32, out interface{}) error {
	// Check for legacy flags
	if flags&cfMask == 0 {
		// Legacy Flags
		if flags == lfJson {
			// Legacy JSON
			flags = cfFmtJson
		} else {
			return clientError{"Unexpected legacy flags value"}
		}
	}

	// Make sure compression is disabled
	if flags&cfCmprMask != cfCmprNone {
		return clientError{"Unexpected value compression"}
	}

	// Normal types of decoding
	if flags&cfFmtMask == cfFmtBinary {
		*(out.(*[]byte)) = bytes
		return nil
	} else if flags&cfFmtMask == cfFmtString {
		*(out.(*string)) = string(bytes)
		return nil
	} else if flags&cfFmtMask == cfFmtJson {
		err := json.Unmarshal(bytes, &out)
		if err != nil {
			return err
		}
		return nil
	} else {
		return clientError{"Unexpected flags value"}
	}
}

func (t DefaultTranscoder) Encode(value interface{}) ([]byte, uint32, error) {
	var bytes []byte
	var flags uint32
	var err error

	switch value.(type) {
	case []byte:
		bytes = value.([]byte)
		flags = cfFmtBinary
	case *[]byte:
		bytes = *value.(*[]byte)
		flags = cfFmtBinary
	case string:
		bytes = []byte(value.(string))
		flags = cfFmtString
	case *string:
		bytes = []byte(*value.(*string))
		flags = cfFmtString
	default:
		bytes, err = json.Marshal(value)
		if err != nil {
			return nil, 0, err
		}
		flags = cfFmtJson
	}

	// No compression supported currently

	return bytes, flags, nil
}
