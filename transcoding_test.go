package gocb

import (
	"encoding/json"
	"reflect"
	"testing"

	gocbcore "github.com/couchbase/gocbcore/v8"
)

var (
	jsonObjStr = []byte("{\"test\":\"value\"}")
	jsonNumStr = []byte("2222")
	jsonStrStr = []byte("Hello World")
)

func TestDefaultEncode(t *testing.T) {
	byteArray := []byte("something")
	stringValue := "something"
	jsonStruct := struct {
		Name string `json:"name"`
	}{Name: "something"}

	jsonValue, err := json.Marshal(jsonStruct)
	if err != nil {
		t.Fatalf("failed to marshal json: %v", err)
	}

	tests := []struct {
		name    string
		args    interface{}
		value   []byte
		flags   uint32
		wantErr bool
	}{
		{
			name:    "byte array",
			args:    byteArray,
			value:   []byte("something"),
			flags:   gocbcore.EncodeCommonFlags(gocbcore.BinaryType, gocbcore.NoCompression),
			wantErr: false,
		},
		{
			name:    "byte point array",
			args:    &byteArray,
			value:   []byte("something"),
			flags:   gocbcore.EncodeCommonFlags(gocbcore.BinaryType, gocbcore.NoCompression),
			wantErr: false,
		},
		{
			name:    "string",
			args:    stringValue,
			value:   []byte(stringValue),
			flags:   gocbcore.EncodeCommonFlags(gocbcore.StringType, gocbcore.NoCompression),
			wantErr: false,
		},
		{
			name:    "string pointer",
			args:    &stringValue,
			value:   []byte(stringValue),
			flags:   gocbcore.EncodeCommonFlags(gocbcore.StringType, gocbcore.NoCompression),
			wantErr: false,
		},
		{
			name:    "json",
			args:    jsonStruct,
			value:   jsonValue,
			flags:   gocbcore.EncodeCommonFlags(gocbcore.JsonType, gocbcore.NoCompression),
			wantErr: false,
		},
		{
			name:    "json pointer",
			args:    &jsonStruct,
			value:   jsonValue,
			flags:   gocbcore.EncodeCommonFlags(gocbcore.JsonType, gocbcore.NoCompression),
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1, err := NewDefaultTranscoder().Encode(tt.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("DefaultEncode() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.value) {
				t.Errorf("DefaultEncode() got = %v, want %v", got, tt.value)
			}
			if got1 != tt.flags {
				t.Errorf("DefaultEncode() got1 = %v, want %v", got1, tt.flags)
			}
		})
	}
}

func testBytesEqual(t *testing.T, left, right []byte) {
	if len(left) != len(right) {
		t.Errorf("Slice lengths do not match")
		return
	}
	for i := range left {
		if left[i] != right[i] {
			t.Errorf("Slice values do not match")
			return
		}
	}
}

func testDecode(t *testing.T, bytes []byte, flags uint32, out interface{}) {
	err := NewDefaultTranscoder().Decode(bytes, flags, out)
	if err != nil {
		t.Errorf("Failed to decode %v", err)
	}
}

func testEncode(t *testing.T, in interface{}) ([]byte, uint32) {
	bytes, flags, err := NewDefaultTranscoder().Encode(in)
	if err != nil {
		t.Errorf("Failed to decode %v", err)
	}
	return bytes, flags
}

func TestDecodeLegacyJson(t *testing.T) {
	var testOut map[string]string
	testDecode(t, jsonObjStr, 0, &testOut)
	if testOut["test"] != "value" {
		t.Errorf("Decoding failed")
	}
}

func TestDecodeJson(t *testing.T) {
	var testOut map[string]string
	testDecode(t, jsonObjStr, 0x2000000, &testOut)
	if testOut["test"] != "value" {
		t.Errorf("Decoding failed")
	}
}

func TestDecodeJsonStruct(t *testing.T) {
	var testOut struct {
		Test string `json:"test"`
	}
	testDecode(t, jsonObjStr, 0x2000000, &testOut)
	if testOut.Test != "value" {
		t.Errorf("Decoding failed")
	}
}

func TestDecodeNumber(t *testing.T) {
	var testOut int
	testDecode(t, jsonNumStr, 0x2000000, &testOut)
	if testOut != 2222 {
		t.Errorf("Decoding failed")
	}
}

func TestDecodeString(t *testing.T) {
	var testOut string
	testDecode(t, jsonStrStr, 0x4000000, &testOut)
	if testOut != "Hello World" {
		t.Errorf("Decoding failed")
	}
}

func TestDecodeBadType(t *testing.T) {
	var testOut string
	err := NewDefaultTranscoder().Decode(jsonNumStr, 0x2000000, &testOut)
	if err == nil {
		t.Errorf("Decoding succeeded but should have failed")
	}
}

func TestDecodeInterface(t *testing.T) {
	var testOut interface{}
	testDecode(t, jsonNumStr, 0x2000000, &testOut)
	switch testOut := testOut.(type) {
	case int:
		if testOut != 2222 {
			t.Errorf("Decoding failed")
		}
	case float64:
		if testOut != 2222 {
			t.Errorf("Decoding failed")
		}
	default:
		t.Errorf("Decoding failed")
	}

	testDecode(t, jsonStrStr, 0x4000000, &testOut)
	switch testOut := testOut.(type) {
	case string:
		if testOut != "Hello World" {
			t.Errorf("Decoding failed")
		}
	default:
		t.Errorf("Decoding failed")
	}
}

func TestEncodeJson(t *testing.T) {
	testIn := make(map[string]string)
	testIn["test"] = "value"
	bytes, flags := testEncode(t, &testIn)
	if flags != 0x2000000 {
		t.Errorf("Bad flags generated")
	}
	testBytesEqual(t, bytes, jsonObjStr)
}

func TestEncodeJsonStruct(t *testing.T) {
	var testIn struct {
		Test string `json:"test"`
	}
	testIn.Test = "value"
	bytes, flags := testEncode(t, &testIn)
	if flags != 0x2000000 {
		t.Errorf("Bad flags generated")
	}
	testBytesEqual(t, bytes, jsonObjStr)
}

func TestEncodeNumber(t *testing.T) {
	testIn := 2222
	bytes, flags := testEncode(t, &testIn)
	if flags != 0x2000000 {
		t.Errorf("Bad flags generated")
	}
	testBytesEqual(t, bytes, jsonNumStr)
}

func TestEncodeString(t *testing.T) {
	testIn := "Hello World"
	bytes, flags := testEncode(t, &testIn)
	if flags != 0x4000000 {
		t.Errorf("Bad flags generated")
	}
	testBytesEqual(t, bytes, jsonStrStr)
}

type MockSerializer struct {
}

func (s *MockSerializer) Serialize(value interface{}) ([]byte, error) {
	return json.Marshal(value)
}

func (s *MockSerializer) Deserialize(bytes []byte, out interface{}) error {
	return json.Unmarshal(bytes, out)
}
