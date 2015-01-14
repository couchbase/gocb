package gocouchbase

import (
	"encoding/json"
	"fmt"
)

func (b *Bucket) decodeValue(bytes []byte, flags uint32, out interface{}) (interface{}, error) {
	fmt.Printf("Early Flags: %08x\n", flags)

	// Check for legacy flags
	if flags&cfMask == 0 {
		// Legacy Flags
		if flags == lfJson {
			// Legacy JSON
			flags = cfFmtJson
		} else {
			return nil, clientError{"Unexpected legacy flags value"}
		}
	}

	fmt.Printf("Flags: %08x\n", flags)

	// Make sure compression is disabled
	if flags&cfCmprMask != cfCmprNone {
		return nil, clientError{"Unexpected value compression"}
	}

	// If an output object was passed, try to json Unmarshal to it
	if out != nil {
		if flags&cfFmtJson != 0 {
			err := json.Unmarshal(bytes, out)
			if err != nil {
				return nil, clientError{err.Error()}
			}
			return out, nil
		} else {
			return nil, clientError{"Unmarshal target passed, but type does not match."}
		}
	}

	// Normal types of decoding
	if flags&cfFmtMask == cfFmtBinary {
		return bytes, nil
	} else if flags&cfFmtMask == cfFmtString {
		return string(bytes[0:]), nil
	} else if flags&cfFmtMask == cfFmtJson {
		var outVal interface{}
		err := json.Unmarshal(bytes, &outVal)
		if err != nil {
			return nil, clientError{err.Error()}
		}
		return outVal, nil
	} else {
		return nil, clientError{"Unexpected flags value"}
	}
}

func (b *Bucket) encodeValue(value interface{}) ([]byte, uint32, error) {
	var bytes []byte
	var flags uint32
	var err error

	switch value.(type) {
	case []byte:
		bytes = value.([]byte)
		flags = cfFmtBinary
	case string:
		bytes = []byte(value.(string))
		flags = cfFmtString
	default:
		bytes, err = json.Marshal(value)
		if err != nil {
			return nil, 0, clientError{err.Error()}
		}
		flags = cfFmtJson
	}

	// No compression supported currently

	return bytes, flags, nil
}
