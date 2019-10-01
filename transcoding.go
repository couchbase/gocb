package gocb

import (
	"encoding/json"

	gocbcore "github.com/couchbase/gocbcore/v8"
)

// Transcoder provides an interface for transforming Go values to and
// from raw bytes for storage and retreival from Couchbase data storage.
type Transcoder interface {
	// Decodes retrieved bytes into a Go type.
	Decode([]byte, uint32, interface{}) error

	// Encodes a Go type into bytes for storage.
	Encode(interface{}) ([]byte, uint32, error)
}

// JSONSerializer is used a Transcoder for serialization/deserialization of JSON datatype valuess
type JSONSerializer interface {
	// Serialize serializes an interface into bytes.
	Serialize(value interface{}) ([]byte, error)

	// Deserialize deserializes json bytes into an interface.
	Deserialize(bytes []byte, out interface{}) error
}

// JSONTranscoder implements the default transcoding behavior and applies JSON transcoding to all values.
//
// This will apply the following behavior to the value:
// binary ([]byte) -> error.
// default -> JSON value, JSON Flags.
type JSONTranscoder struct {
	serializer JSONSerializer
}

// NewJSONTranscoder returns a new JSONTranscoder initialized to use the specified serializer. If the serializer is
// nil then it will use the DefaultJSONSerializer.
func NewJSONTranscoder(serializer JSONSerializer) *JSONTranscoder {
	if serializer == nil {
		serializer = &DefaultJSONSerializer{}
	}

	return &JSONTranscoder{
		serializer: serializer,
	}
}

// Decode applies JSON transcoding behaviour to decode into a Go type.
func (t *JSONTranscoder) Decode(bytes []byte, flags uint32, out interface{}) error {
	valueType, compression := gocbcore.DecodeCommonFlags(flags)

	// Make sure compression is disabled
	if compression != gocbcore.NoCompression {
		return clientError{message: "unexpected value compression"}
	}

	// Normal types of decoding
	if valueType == gocbcore.BinaryType {
		return configurationError{message: "binary datatype is not supported by JSONTranscoder"}
	} else if valueType == gocbcore.StringType {
		return configurationError{message: "string datatype is not supported by JSONTranscoder"}
	} else if valueType == gocbcore.JsonType {
		err := t.serializer.Deserialize(bytes, &out)
		if err != nil {
			return err
		}
		return nil
	}

	return clientError{message: "unexpected expectedFlags value"}
}

// Encode applies JSON transcoding behaviour to encode a Go type.
func (t *JSONTranscoder) Encode(value interface{}) ([]byte, uint32, error) {
	var bytes []byte
	var flags uint32
	var err error

	switch typeValue := value.(type) {
	case []byte:
		return nil, 0, configurationError{message: "binary data is not supported by JSONTranscoder"}
	case *[]byte:
		return nil, 0, configurationError{message: "binary data is not supported by JSONTranscoder"}
	case json.RawMessage:
		bytes = typeValue
		flags = gocbcore.EncodeCommonFlags(gocbcore.JsonType, gocbcore.NoCompression)
	case *json.RawMessage:
		bytes = *typeValue
		flags = gocbcore.EncodeCommonFlags(gocbcore.JsonType, gocbcore.NoCompression)
	case *interface{}:
		return t.Encode(*typeValue)
	default:
		bytes, err = t.serializer.Serialize(value)
		if err != nil {
			return nil, 0, err
		}
		flags = gocbcore.EncodeCommonFlags(gocbcore.JsonType, gocbcore.NoCompression)
	}

	// No compression supported currently

	return bytes, flags, nil
}

// RawJSONTranscoder implements passthrough behavior of JSON data. This transcoder does not apply any serialization.
// It will forward data across the network without incurring unnecessary parsing costs.
//
// This will apply the following behavior to the value:
// binary ([]byte) -> JSON bytes, JSON expectedFlags.
// string -> JSON bytes, JSON expectedFlags.
// default -> error.
type RawJSONTranscoder struct {
}

// NewRawJSONTranscoder returns a new RawJSONTranscoder.
func NewRawJSONTranscoder() *RawJSONTranscoder {
	return &RawJSONTranscoder{}
}

// Decode applies raw JSON transcoding behaviour to decode into a Go type.
func (t *RawJSONTranscoder) Decode(bytes []byte, flags uint32, out interface{}) error {
	valueType, compression := gocbcore.DecodeCommonFlags(flags)

	// Make sure compression is disabled
	if compression != gocbcore.NoCompression {
		return clientError{message: "unexpected value compression"}
	}

	// Normal types of decoding
	if valueType == gocbcore.BinaryType {
		return clientError{message: "binary datatype is not supported by RawJSONTranscoder"}
	} else if valueType == gocbcore.StringType {
		return clientError{message: "string datatype is not supported by RawJSONTranscoder"}
	} else if valueType == gocbcore.JsonType {
		switch typedOut := out.(type) {
		case *[]byte:
			*typedOut = bytes
			return nil
		case *string:
			*typedOut = string(bytes)
			return nil
		default:
			return clientError{message: "you must encode raw JSON data in a byte array or string"}
		}
	}

	return clientError{message: "unexpected expectedFlags value"}
}

// Encode applies raw JSON transcoding behaviour to encode a Go type.
func (t *RawJSONTranscoder) Encode(value interface{}) ([]byte, uint32, error) {
	var bytes []byte
	var flags uint32

	switch typeValue := value.(type) {
	case []byte:
		bytes = typeValue
		flags = gocbcore.EncodeCommonFlags(gocbcore.JsonType, gocbcore.NoCompression)
	case *[]byte:
		bytes = *typeValue
		flags = gocbcore.EncodeCommonFlags(gocbcore.JsonType, gocbcore.NoCompression)
	case string:
		bytes = []byte(typeValue)
		flags = gocbcore.EncodeCommonFlags(gocbcore.JsonType, gocbcore.NoCompression)
	case *string:
		bytes = []byte(*typeValue)
		flags = gocbcore.EncodeCommonFlags(gocbcore.JsonType, gocbcore.NoCompression)
	case json.RawMessage:
		bytes = typeValue
		flags = gocbcore.EncodeCommonFlags(gocbcore.JsonType, gocbcore.NoCompression)
	case *json.RawMessage:
		bytes = *typeValue
		flags = gocbcore.EncodeCommonFlags(gocbcore.JsonType, gocbcore.NoCompression)
	case *interface{}:
		return t.Encode(*typeValue)
	default:
		return nil, 0, invalidArgumentsError{message: "only binary and string data is supported by RawJSONTranscoder"}
	}

	// No compression supported currently

	return bytes, flags, nil
}

// RawStringTranscoder implements passthrough behavior of raw string data. This transcoder does not apply any serialization.
//
// This will apply the following behavior to the value:
// string -> string bytes, string expectedFlags.
// default -> error.
type RawStringTranscoder struct {
}

// NewRawStringTranscoder returns a new RawStringTranscoder.
func NewRawStringTranscoder() *RawStringTranscoder {
	return &RawStringTranscoder{}
}

// Decode applies raw string transcoding behaviour to decode into a Go type.
func (t *RawStringTranscoder) Decode(bytes []byte, flags uint32, out interface{}) error {
	valueType, compression := gocbcore.DecodeCommonFlags(flags)

	// Make sure compression is disabled
	if compression != gocbcore.NoCompression {
		return clientError{message: "unexpected value compression"}
	}

	// Normal types of decoding
	if valueType == gocbcore.BinaryType {
		return clientError{message: "only string datatype is supported by RawStringTranscoder"}
	} else if valueType == gocbcore.StringType {
		switch typedOut := out.(type) {
		case *string:
			*typedOut = string(bytes)
			return nil
		case *interface{}:
			*typedOut = string(bytes)
			return nil
		default:
			return clientError{message: "you must encode a string in a string or interface"}
		}
	} else if valueType == gocbcore.JsonType {
		return clientError{message: "only string datatype is supported by RawStringTranscoder"}
	}

	return clientError{message: "unexpected expectedFlags value"}
}

// Encode applies raw string transcoding behaviour to encode a Go type.
func (t *RawStringTranscoder) Encode(value interface{}) ([]byte, uint32, error) {
	var bytes []byte
	var flags uint32

	switch typeValue := value.(type) {
	case string:
		bytes = []byte(typeValue)
		flags = gocbcore.EncodeCommonFlags(gocbcore.StringType, gocbcore.NoCompression)
	case *string:
		bytes = []byte(*typeValue)
		flags = gocbcore.EncodeCommonFlags(gocbcore.StringType, gocbcore.NoCompression)
	case *interface{}:
		return t.Encode(*typeValue)
	default:
		return nil, 0, invalidArgumentsError{message: "only raw string data is supported by RawStringTranscoder"}
	}

	// No compression supported currently

	return bytes, flags, nil
}

// RawBinaryTranscoder implements passthrough behavior of raw binary data. This transcoder does not apply any serialization.
//
// This will apply the following behavior to the value:
// binary ([]byte) -> binary bytes, binary expectedFlags.
// default -> error.
type RawBinaryTranscoder struct {
}

// NewRawBinaryTranscoder returns a new RawBinaryTranscoder.
func NewRawBinaryTranscoder() *RawBinaryTranscoder {
	return &RawBinaryTranscoder{}
}

// Decode applies raw binary transcoding behaviour to decode into a Go type.
func (t *RawBinaryTranscoder) Decode(bytes []byte, flags uint32, out interface{}) error {
	valueType, compression := gocbcore.DecodeCommonFlags(flags)

	// Make sure compression is disabled
	if compression != gocbcore.NoCompression {
		return clientError{message: "unexpected value compression"}
	}

	// Normal types of decoding
	if valueType == gocbcore.BinaryType {
		switch typedOut := out.(type) {
		case *[]byte:
			*typedOut = bytes
			return nil
		case *interface{}:
			*typedOut = bytes
			return nil
		default:
			return clientError{message: "you must encode binary in a byte array or interface"}
		}
	} else if valueType == gocbcore.StringType {
		return clientError{message: "only binary datatype is supported by RawBinaryTranscoder"}
	} else if valueType == gocbcore.JsonType {
		return clientError{message: "only binary datatype is supported by RawBinaryTranscoder"}
	}

	return clientError{message: "unexpected expectedFlags value"}
}

// Encode applies raw binary transcoding behaviour to encode a Go type.
func (t *RawBinaryTranscoder) Encode(value interface{}) ([]byte, uint32, error) {
	var bytes []byte
	var flags uint32

	switch typeValue := value.(type) {
	case []byte:
		bytes = typeValue
		flags = gocbcore.EncodeCommonFlags(gocbcore.BinaryType, gocbcore.NoCompression)
	case *[]byte:
		bytes = *typeValue
		flags = gocbcore.EncodeCommonFlags(gocbcore.BinaryType, gocbcore.NoCompression)
	case *interface{}:
		return t.Encode(*typeValue)
	default:
		return nil, 0, invalidArgumentsError{message: "only raw binary data is supported by RawBinaryTranscoder"}
	}

	// No compression supported currently

	return bytes, flags, nil
}

// LegacyTranscoder implements the behaviour for a backward-compatible transcoder. This transcoder implements
// behaviour matching that of gocb v1.
//
// This will apply the following behavior to the value:
// binary ([]byte) -> binary bytes, Binary expectedFlags.
// string -> string bytes, String expectedFlags.
// default -> JSON value, JSON expectedFlags.
type LegacyTranscoder struct {
	serializer JSONSerializer
}

// NewLegacyTranscoder returns a new LegacyTranscoder initialized to use the specified serializer. If the serializer is
// nil then it will use the DefaultJSONSerializer.
func NewLegacyTranscoder(serializer JSONSerializer) *LegacyTranscoder {
	if serializer == nil {
		serializer = &DefaultJSONSerializer{}
	}

	return &LegacyTranscoder{
		serializer: serializer,
	}
}

// Decode applies legacy transcoding behaviour to decode into a Go type.
func (t *LegacyTranscoder) Decode(bytes []byte, flags uint32, out interface{}) error {
	valueType, compression := gocbcore.DecodeCommonFlags(flags)

	// Make sure compression is disabled
	if compression != gocbcore.NoCompression {
		return clientError{message: "unexpected value compression"}
	}

	// Normal types of decoding
	if valueType == gocbcore.BinaryType {
		switch typedOut := out.(type) {
		case *[]byte:
			*typedOut = bytes
			return nil
		case *interface{}:
			*typedOut = bytes
			return nil
		default:
			return clientError{message: "you must encode binary in a byte array or interface"}
		}
	} else if valueType == gocbcore.StringType {
		switch typedOut := out.(type) {
		case *string:
			*typedOut = string(bytes)
			return nil
		case *interface{}:
			*typedOut = string(bytes)
			return nil
		default:
			return clientError{message: "you must encode a string in a string or interface"}
		}
	} else if valueType == gocbcore.JsonType {
		err := t.serializer.Deserialize(bytes, &out)
		if err != nil {
			return err
		}
		return nil
	}

	return clientError{message: "unexpected expectedFlags value"}
}

// Encode applies legacy transcoding behavior to encode a Go type.
func (t *LegacyTranscoder) Encode(value interface{}) ([]byte, uint32, error) {
	var bytes []byte
	var flags uint32
	var err error

	switch typeValue := value.(type) {
	case []byte:
		bytes = typeValue
		flags = gocbcore.EncodeCommonFlags(gocbcore.BinaryType, gocbcore.NoCompression)
	case *[]byte:
		bytes = *typeValue
		flags = gocbcore.EncodeCommonFlags(gocbcore.BinaryType, gocbcore.NoCompression)
	case string:
		bytes = []byte(typeValue)
		flags = gocbcore.EncodeCommonFlags(gocbcore.StringType, gocbcore.NoCompression)
	case *string:
		bytes = []byte(*typeValue)
		flags = gocbcore.EncodeCommonFlags(gocbcore.StringType, gocbcore.NoCompression)
	case json.RawMessage:
		bytes = typeValue
		flags = gocbcore.EncodeCommonFlags(gocbcore.JsonType, gocbcore.NoCompression)
	case *json.RawMessage:
		bytes = *typeValue
		flags = gocbcore.EncodeCommonFlags(gocbcore.JsonType, gocbcore.NoCompression)
	case *interface{}:
		return t.Encode(*typeValue)
	default:
		bytes, err = t.serializer.Serialize(value)
		if err != nil {
			return nil, 0, err
		}
		flags = gocbcore.EncodeCommonFlags(gocbcore.JsonType, gocbcore.NoCompression)
	}

	// No compression supported currently

	return bytes, flags, nil
}

// DefaultJSONSerializer implements the JSONSerializer interface using json.Marshal/Unmarshal.
type DefaultJSONSerializer struct {
}

// Serialize applies the json.Marshal behaviour to serialize a Go type
func (s *DefaultJSONSerializer) Serialize(value interface{}) ([]byte, error) {
	bytes, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}

	return bytes, nil
}

// Deserialize applies the json.Unmarshal behaviour to deserialize into a Go type
func (s *DefaultJSONSerializer) Deserialize(bytes []byte, out interface{}) error {
	err := json.Unmarshal(bytes, &out)
	if err != nil {
		return err
	}

	return nil
}
