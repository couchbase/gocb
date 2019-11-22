package gocb

import (
	"encoding/json"
	"reflect"
	"testing"

	gocbcore "github.com/couchbase/gocbcore/v8"
)

func TestEncode(t *testing.T) {
	byteArray := []byte("something")
	rawString := "something"
	rawMsg := json.RawMessage("hello")
	rawNumber := 22022
	jsonStruct := struct {
		Name string `json:"name"`
	}{Name: "something"}

	jsonValue, err := json.Marshal(jsonStruct)
	if err != nil {
		t.Fatalf("failed to marshal json: %v", err)
	}

	stringValue, err := json.Marshal(rawString)
	if err != nil {
		t.Fatalf("failed to marshal json: %v", err)
	}

	numberValue, err := json.Marshal(rawNumber)
	if err != nil {
		t.Fatalf("failed to marshal json: %v", err)
	}

	var rawInterface interface{} = rawString

	type test struct {
		name          string
		args          interface{}
		expected      []byte
		expectedFlags uint32
		wantErr       bool
	}
	tests := map[Transcoder][]test{
		NewJSONTranscoder(): {
			{
				name:    "byte array",
				args:    byteArray,
				wantErr: true,
			},
			{
				name:    "byte point array",
				args:    &byteArray,
				wantErr: true,
			},
			{
				name:          "string",
				args:          rawString,
				expected:      stringValue,
				expectedFlags: gocbcore.EncodeCommonFlags(gocbcore.JSONType, gocbcore.NoCompression),
				wantErr:       false,
			},
			{
				name:          "string pointer",
				args:          &rawString,
				expected:      stringValue,
				expectedFlags: gocbcore.EncodeCommonFlags(gocbcore.JSONType, gocbcore.NoCompression),
				wantErr:       false,
			},
			{
				name:          "json",
				args:          jsonStruct,
				expected:      jsonValue,
				expectedFlags: gocbcore.EncodeCommonFlags(gocbcore.JSONType, gocbcore.NoCompression),
				wantErr:       false,
			},
			{
				name:          "json pointer",
				args:          &jsonStruct,
				expected:      jsonValue,
				expectedFlags: gocbcore.EncodeCommonFlags(gocbcore.JSONType, gocbcore.NoCompression),
				wantErr:       false,
			},
			{
				name:          "json.RawMessage",
				args:          rawMsg,
				expected:      rawMsg,
				expectedFlags: gocbcore.EncodeCommonFlags(gocbcore.JSONType, gocbcore.NoCompression),
				wantErr:       false,
			},
			{
				name:          "json.RawMessage pointer",
				args:          &rawMsg,
				expected:      rawMsg,
				expectedFlags: gocbcore.EncodeCommonFlags(gocbcore.JSONType, gocbcore.NoCompression),
				wantErr:       false,
			},
			{
				name:          "number",
				args:          rawNumber,
				expected:      numberValue,
				expectedFlags: gocbcore.EncodeCommonFlags(gocbcore.JSONType, gocbcore.NoCompression),
				wantErr:       false,
			},
			{
				name:          "number pointer",
				args:          &rawNumber,
				expected:      numberValue,
				expectedFlags: gocbcore.EncodeCommonFlags(gocbcore.JSONType, gocbcore.NoCompression),
				wantErr:       false,
			},
			{
				name:          "interface",
				args:          rawInterface,
				expected:      stringValue,
				expectedFlags: gocbcore.EncodeCommonFlags(gocbcore.JSONType, gocbcore.NoCompression),
				wantErr:       false,
			},
			{
				name:          "interface pointer",
				args:          &rawInterface,
				expected:      stringValue,
				expectedFlags: gocbcore.EncodeCommonFlags(gocbcore.JSONType, gocbcore.NoCompression),
				wantErr:       false,
			},
		},
		NewLegacyTranscoder(): {
			{
				name:          "byte array",
				args:          byteArray,
				expected:      byteArray,
				expectedFlags: gocbcore.EncodeCommonFlags(gocbcore.BinaryType, gocbcore.NoCompression),
				wantErr:       false,
			},
			{
				name:          "byte point array",
				args:          &byteArray,
				expected:      byteArray,
				expectedFlags: gocbcore.EncodeCommonFlags(gocbcore.BinaryType, gocbcore.NoCompression),
				wantErr:       false,
			},
			{
				name:          "string",
				args:          rawString,
				expected:      []byte(rawString),
				expectedFlags: gocbcore.EncodeCommonFlags(gocbcore.StringType, gocbcore.NoCompression),
				wantErr:       false,
			},
			{
				name:          "string pointer",
				args:          &rawString,
				expected:      []byte(rawString),
				expectedFlags: gocbcore.EncodeCommonFlags(gocbcore.StringType, gocbcore.NoCompression),
				wantErr:       false,
			},
			{
				name:          "json",
				args:          jsonStruct,
				expected:      jsonValue,
				expectedFlags: gocbcore.EncodeCommonFlags(gocbcore.JSONType, gocbcore.NoCompression),
				wantErr:       false,
			},
			{
				name:          "json pointer",
				args:          &jsonStruct,
				expected:      jsonValue,
				expectedFlags: gocbcore.EncodeCommonFlags(gocbcore.JSONType, gocbcore.NoCompression),
				wantErr:       false,
			},
			{
				name:          "json.RawMessage",
				args:          rawMsg,
				expected:      rawMsg,
				expectedFlags: gocbcore.EncodeCommonFlags(gocbcore.JSONType, gocbcore.NoCompression),
				wantErr:       false,
			},
			{
				name:          "json.RawMessage pointer",
				args:          &rawMsg,
				expected:      rawMsg,
				expectedFlags: gocbcore.EncodeCommonFlags(gocbcore.JSONType, gocbcore.NoCompression),
				wantErr:       false,
			},
			{
				name:          "number",
				args:          rawNumber,
				expected:      numberValue,
				expectedFlags: gocbcore.EncodeCommonFlags(gocbcore.JSONType, gocbcore.NoCompression),
				wantErr:       false,
			},
			{
				name:          "number pointer",
				args:          &rawNumber,
				expected:      numberValue,
				expectedFlags: gocbcore.EncodeCommonFlags(gocbcore.JSONType, gocbcore.NoCompression),
				wantErr:       false,
			},
			{
				name:          "interface",
				args:          rawInterface,
				expected:      []byte(rawString),
				expectedFlags: gocbcore.EncodeCommonFlags(gocbcore.StringType, gocbcore.NoCompression),
				wantErr:       false,
			},
			{
				name:          "interface pointer",
				args:          &rawInterface,
				expected:      []byte(rawString),
				expectedFlags: gocbcore.EncodeCommonFlags(gocbcore.StringType, gocbcore.NoCompression),
				wantErr:       false,
			},
		},
		NewRawJSONTranscoder(): {
			{
				name:          "byte array",
				args:          byteArray,
				expected:      byteArray,
				expectedFlags: gocbcore.EncodeCommonFlags(gocbcore.JSONType, gocbcore.NoCompression),
				wantErr:       false,
			},
			{
				name:          "byte point array",
				args:          &byteArray,
				expected:      byteArray,
				expectedFlags: gocbcore.EncodeCommonFlags(gocbcore.JSONType, gocbcore.NoCompression),
				wantErr:       false,
			},
			{
				name:          "string",
				args:          rawString,
				expected:      []byte(rawString),
				expectedFlags: gocbcore.EncodeCommonFlags(gocbcore.JSONType, gocbcore.NoCompression),
				wantErr:       false,
			},
			{
				name:          "string pointer",
				args:          &rawString,
				expected:      []byte(rawString),
				expectedFlags: gocbcore.EncodeCommonFlags(gocbcore.JSONType, gocbcore.NoCompression),
				wantErr:       false,
			},
			{
				name:    "json",
				args:    jsonStruct,
				wantErr: true,
			},
			{
				name:    "json pointer",
				args:    &jsonStruct,
				wantErr: true,
			},
			{
				name:          "json.RawMessage",
				args:          rawMsg,
				expected:      rawMsg,
				expectedFlags: gocbcore.EncodeCommonFlags(gocbcore.JSONType, gocbcore.NoCompression),
				wantErr:       false,
			},
			{
				name:          "json.RawMessage pointer",
				args:          &rawMsg,
				expected:      rawMsg,
				expectedFlags: gocbcore.EncodeCommonFlags(gocbcore.JSONType, gocbcore.NoCompression),
				wantErr:       false,
			},
			{
				name:    "number",
				args:    rawNumber,
				wantErr: true,
			},
			{
				name:    "number pointer",
				args:    &rawNumber,
				wantErr: true,
			},
			{
				name:          "interface",
				args:          rawInterface,
				expected:      []byte(rawString),
				expectedFlags: gocbcore.EncodeCommonFlags(gocbcore.JSONType, gocbcore.NoCompression),
				wantErr:       false,
			},
			{
				name:          "interface pointer",
				args:          &rawInterface,
				expected:      []byte(rawString),
				expectedFlags: gocbcore.EncodeCommonFlags(gocbcore.JSONType, gocbcore.NoCompression),
				wantErr:       false,
			},
		},
		NewRawStringTranscoder(): {
			{
				name:    "byte array",
				args:    byteArray,
				wantErr: true,
			},
			{
				name:    "byte point array",
				args:    &byteArray,
				wantErr: true,
			},
			{
				name:          "string",
				args:          rawString,
				expected:      []byte(rawString),
				expectedFlags: gocbcore.EncodeCommonFlags(gocbcore.StringType, gocbcore.NoCompression),
				wantErr:       false,
			},
			{
				name:          "string pointer",
				args:          &rawString,
				expected:      []byte(rawString),
				expectedFlags: gocbcore.EncodeCommonFlags(gocbcore.StringType, gocbcore.NoCompression),
				wantErr:       false,
			},
			{
				name:    "json",
				args:    jsonStruct,
				wantErr: true,
			},
			{
				name:    "json pointer",
				args:    &jsonStruct,
				wantErr: true,
			},
			{
				name:    "json.RawMessage",
				args:    rawMsg,
				wantErr: true,
			},
			{
				name:    "json.RawMessage pointer",
				args:    &rawMsg,
				wantErr: true,
			},
			{
				name:    "number",
				args:    rawNumber,
				wantErr: true,
			},
			{
				name:    "number pointer",
				args:    &rawNumber,
				wantErr: true,
			},
			{
				name:          "interface",
				args:          rawInterface,
				expected:      []byte(rawString),
				expectedFlags: gocbcore.EncodeCommonFlags(gocbcore.StringType, gocbcore.NoCompression),
				wantErr:       false,
			},
			{
				name:          "interface pointer",
				args:          &rawInterface,
				expected:      []byte(rawString),
				expectedFlags: gocbcore.EncodeCommonFlags(gocbcore.StringType, gocbcore.NoCompression),
				wantErr:       false,
			},
		},
		NewRawBinaryTranscoder(): {
			{
				name:          "byte array",
				args:          byteArray,
				expected:      byteArray,
				expectedFlags: gocbcore.EncodeCommonFlags(gocbcore.BinaryType, gocbcore.NoCompression),
				wantErr:       false,
			},
			{
				name:          "byte point array",
				args:          &byteArray,
				expected:      byteArray,
				expectedFlags: gocbcore.EncodeCommonFlags(gocbcore.BinaryType, gocbcore.NoCompression),
				wantErr:       false,
			},
			{
				name:    "string",
				args:    rawString,
				wantErr: true,
			},
			{
				name:    "string pointer",
				wantErr: true,
			},
			{
				name:    "json",
				args:    jsonStruct,
				wantErr: true,
			},
			{
				name:    "json pointer",
				args:    &jsonStruct,
				wantErr: true,
			},
			{
				name:    "json.RawMessage",
				args:    rawMsg,
				wantErr: true,
			},
			{
				name:    "json.RawMessage pointer",
				args:    &rawMsg,
				wantErr: true,
			},
			{
				name:    "number",
				args:    rawNumber,
				wantErr: true,
			},
			{
				name:    "number pointer",
				args:    &rawNumber,
				wantErr: true,
			},
		},
	}
	for transcoder, transcoderTests := range tests {
		for _, tt := range transcoderTests {
			t.Run(tt.name, func(t *testing.T) {
				actual, flags, err := transcoder.Encode(tt.args)
				name := reflect.ValueOf(transcoder).Type()
				if (err != nil) != tt.wantErr {
					t.Errorf("%s error = %v, wantErr %v", name, err, tt.wantErr)
					return
				}
				if !reflect.DeepEqual(actual, tt.expected) {
					t.Errorf("%s got = %v, want %v", name, actual, tt.expected)
				}
				if flags != tt.expectedFlags {
					t.Errorf("%s got1 = %v, want %v", name, flags, tt.expectedFlags)
				}
			})
		}
	}
}

func TestDecodeJSON(t *testing.T) {
	type jsonType struct {
		Name string `json:"name"`
	}

	jsonStruct := jsonType{Name: "something"}

	jsonValue, err := json.Marshal(jsonStruct)
	if err != nil {
		t.Fatalf("failed to marshal json: %v", err)
	}

	type test struct {
		bytes    []byte
		flags    uint32
		expected jsonType
		wantErr  bool
	}
	tests := map[Transcoder][]test{
		NewJSONTranscoder(): {
			{
				bytes:    jsonValue,
				flags:    0x2000000,
				expected: jsonStruct,
				wantErr:  false,
			},
		},
		NewLegacyTranscoder(): {
			{
				bytes:    jsonValue,
				flags:    0x2000000,
				expected: jsonStruct,
				wantErr:  false,
			},
		},
		NewRawJSONTranscoder(): {
			{
				bytes:   jsonValue,
				flags:   0x2000000,
				wantErr: true,
			},
		},
		NewRawStringTranscoder(): {
			{
				bytes:   jsonValue,
				flags:   0x2000000,
				wantErr: true,
			},
		},
		NewRawBinaryTranscoder(): {
			{
				bytes:   jsonValue,
				flags:   0x2000000,
				wantErr: true,
			},
		},
	}
	for transcoder, transcoderTests := range tests {
		name := reflect.ValueOf(transcoder).Type().String()
		for _, tt := range transcoderTests {
			t.Run(name, func(t *testing.T) {
				var actual jsonType
				err := transcoder.Decode(tt.bytes, tt.flags, &actual)
				if (err != nil) != tt.wantErr {
					t.Errorf("%s error = %v, wantErr %v", name, err, tt.wantErr)
					return
				}
				if !reflect.DeepEqual(actual, tt.expected) {
					t.Errorf("%s got = %v, want %v", name, actual, tt.expected)
				}
			})
		}
	}
}

func TestDecodeJSONInterface(t *testing.T) {
	type jsonType struct {
		Name string `json:"name"`
	}

	jsonStruct := jsonType{Name: "something"}

	jsonValue, err := json.Marshal(jsonStruct)
	if err != nil {
		t.Fatalf("failed to marshal json: %v", err)
	}

	type test struct {
		bytes    []byte
		flags    uint32
		expected jsonType
		wantErr  bool
	}
	tests := map[Transcoder][]test{
		NewJSONTranscoder(): {
			{
				bytes:    jsonValue,
				flags:    0x2000000,
				expected: jsonStruct,
				wantErr:  false,
			},
		},
		NewLegacyTranscoder(): {
			{
				bytes:    jsonValue,
				flags:    0x2000000,
				expected: jsonStruct,
				wantErr:  false,
			},
		},
		NewRawJSONTranscoder(): {
			{
				bytes:   jsonValue,
				flags:   0x2000000,
				wantErr: true,
			},
		},
		NewRawStringTranscoder(): {
			{
				bytes:   jsonValue,
				flags:   0x2000000,
				wantErr: true,
			},
		},
		NewRawBinaryTranscoder(): {
			{
				bytes:   jsonValue,
				flags:   0x2000000,
				wantErr: true,
			},
		},
	}
	for transcoder, transcoderTests := range tests {
		name := reflect.ValueOf(transcoder).Type().String()
		for _, tt := range transcoderTests {
			t.Run(name, func(t *testing.T) {
				str := jsonType{}
				var actual interface{} = &str
				err := transcoder.Decode(tt.bytes, tt.flags, &actual)
				if (err != nil) != tt.wantErr {
					t.Errorf("%s error = %v, wantErr %v", name, err, tt.wantErr)
					return
				}
				if !reflect.DeepEqual(actual, &tt.expected) {
					t.Errorf("%s got = %v, want %v", name, actual, tt.expected)
				}
			})
		}
	}
}

func TestDecodeJSONString(t *testing.T) {
	rawString := "something"

	jsonValue, err := json.Marshal(rawString)
	if err != nil {
		t.Fatalf("failed to marshal json: %v", err)
	}

	type test struct {
		bytes    []byte
		flags    uint32
		expected string
		wantErr  bool
	}
	tests := map[Transcoder][]test{
		NewJSONTranscoder(): {
			{
				bytes:    jsonValue,
				flags:    0x2000000,
				expected: rawString,
				wantErr:  false,
			},
		},
		NewLegacyTranscoder(): {
			{
				bytes:    jsonValue,
				flags:    0x2000000,
				expected: rawString,
				wantErr:  false,
			},
		},
		NewRawJSONTranscoder(): {
			{
				bytes:    jsonValue,
				flags:    0x2000000,
				expected: string(jsonValue),
				wantErr:  false,
			},
		},
		NewRawStringTranscoder(): {
			{
				bytes:   jsonValue,
				flags:   0x2000000,
				wantErr: true,
			},
		},
		NewRawBinaryTranscoder(): {
			{
				bytes:   jsonValue,
				flags:   0x2000000,
				wantErr: true,
			},
		},
	}
	for transcoder, transcoderTests := range tests {
		name := reflect.ValueOf(transcoder).Type().String()
		for _, tt := range transcoderTests {
			t.Run(name, func(t *testing.T) {
				var actual string
				err := transcoder.Decode(tt.bytes, tt.flags, &actual)
				if (err != nil) != tt.wantErr {
					t.Errorf("%s error = %v, wantErr %v", name, err, tt.wantErr)
					return
				}
				if !reflect.DeepEqual(actual, tt.expected) {
					t.Errorf("%s got = %v, want %v", name, actual, tt.expected)
				}
			})
		}
	}
}

func TestDecodeJSONNumber(t *testing.T) {
	rawNumber := 22022

	jsonValue, err := json.Marshal(rawNumber)
	if err != nil {
		t.Fatalf("failed to marshal json: %v", err)
	}

	type test struct {
		bytes    []byte
		flags    uint32
		expected int
		wantErr  bool
	}
	tests := map[Transcoder][]test{
		NewJSONTranscoder(): {
			{
				bytes:    jsonValue,
				flags:    0x2000000,
				expected: rawNumber,
				wantErr:  false,
			},
		},
		NewLegacyTranscoder(): {
			{
				bytes:    jsonValue,
				flags:    0x2000000,
				expected: rawNumber,
				wantErr:  false,
			},
		},
		NewRawJSONTranscoder(): {
			{
				bytes:   jsonValue,
				flags:   0x2000000,
				wantErr: true,
			},
		},
		NewRawStringTranscoder(): {
			{
				bytes:   jsonValue,
				flags:   0x2000000,
				wantErr: true,
			},
		},
		NewRawBinaryTranscoder(): {
			{
				bytes:   jsonValue,
				flags:   0x2000000,
				wantErr: true,
			},
		},
	}
	for transcoder, transcoderTests := range tests {
		name := reflect.ValueOf(transcoder).Type().String()
		for _, tt := range transcoderTests {
			t.Run(name, func(t *testing.T) {
				var actual int
				err := transcoder.Decode(tt.bytes, tt.flags, &actual)
				if (err != nil) != tt.wantErr {
					t.Errorf("%s error = %v, wantErr %v", name, err, tt.wantErr)
					return
				}
				if !reflect.DeepEqual(actual, tt.expected) {
					t.Errorf("%s got = %v, want %v", name, actual, tt.expected)
				}
			})
		}
	}
}

func TestDecodeBinary(t *testing.T) {
	binary := []byte("222222")

	type test struct {
		bytes    []byte
		flags    uint32
		expected []byte
		wantErr  bool
	}
	tests := map[Transcoder][]test{
		NewJSONTranscoder(): {
			{
				bytes:   binary,
				flags:   3 << 24,
				wantErr: true,
			},
		},
		NewLegacyTranscoder(): {
			{
				bytes:    binary,
				flags:    3 << 24,
				expected: binary,
				wantErr:  false,
			},
		},
		NewRawJSONTranscoder(): {
			{
				bytes:   binary,
				flags:   3 << 24,
				wantErr: true,
			},
		},
		NewRawStringTranscoder(): {
			{
				bytes:   binary,
				flags:   3 << 24,
				wantErr: true,
			},
		},
		NewRawBinaryTranscoder(): {
			{
				bytes:    binary,
				flags:    3 << 24,
				expected: binary,
				wantErr:  false,
			},
		},
	}
	for transcoder, transcoderTests := range tests {
		name := reflect.ValueOf(transcoder).Type().String()
		for _, tt := range transcoderTests {
			t.Run(name, func(t *testing.T) {
				var actual []byte
				err := transcoder.Decode(tt.bytes, tt.flags, &actual)
				if (err != nil) != tt.wantErr {
					t.Errorf("%s error = %v, wantErr %v", name, err, tt.wantErr)
					return
				}
				if !reflect.DeepEqual(actual, tt.expected) {
					t.Errorf("%s got = %v, want %v", name, actual, tt.expected)
				}
			})
		}
	}
}

func TestDecodeString(t *testing.T) {
	rawString := "something"

	type test struct {
		bytes    []byte
		flags    uint32
		expected string
		wantErr  bool
	}
	tests := map[Transcoder][]test{
		NewJSONTranscoder(): {
			{
				bytes:   []byte(rawString),
				flags:   4 << 24,
				wantErr: true,
			},
		},
		NewLegacyTranscoder(): {
			{
				bytes:    []byte(rawString),
				flags:    4 << 24,
				expected: rawString,
				wantErr:  false,
			},
		},
		NewRawJSONTranscoder(): {
			{
				bytes:   []byte(rawString),
				flags:   4 << 24,
				wantErr: true,
			},
		},
		NewRawStringTranscoder(): {
			{
				bytes:    []byte(rawString),
				flags:    4 << 24,
				expected: rawString,
				wantErr:  false,
			},
		},
		NewRawBinaryTranscoder(): {
			{
				bytes:   []byte(rawString),
				flags:   4 << 24,
				wantErr: true,
			},
		},
	}
	for transcoder, transcoderTests := range tests {
		name := reflect.ValueOf(transcoder).Type().String()
		for _, tt := range transcoderTests {
			t.Run(name, func(t *testing.T) {
				var actual string
				err := transcoder.Decode(tt.bytes, tt.flags, &actual)
				if (err != nil) != tt.wantErr {
					t.Errorf("%s error = %v, wantErr %v", name, err, tt.wantErr)
					return
				}
				if !reflect.DeepEqual(actual, tt.expected) {
					t.Errorf("%s got = %v, want %v", name, actual, tt.expected)
				}
			})
		}
	}
}
