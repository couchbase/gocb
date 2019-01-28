package gocb

import (
	"encoding/json"
	"reflect"
	"testing"

	gocbcore "gopkg.in/couchbase/gocbcore.v8"
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
			got, got1, err := DefaultEncode(tt.args)
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

func TestJsonEncode(t *testing.T) {
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
			flags:   gocbcore.EncodeCommonFlags(gocbcore.JsonType, gocbcore.NoCompression),
			wantErr: false,
		},
		{
			name:    "byte point array",
			args:    &byteArray,
			value:   []byte("something"),
			flags:   gocbcore.EncodeCommonFlags(gocbcore.JsonType, gocbcore.NoCompression),
			wantErr: false,
		},
		{
			name:    "string",
			args:    stringValue,
			value:   []byte(stringValue),
			flags:   gocbcore.EncodeCommonFlags(gocbcore.JsonType, gocbcore.NoCompression),
			wantErr: false,
		},
		{
			name:    "string pointer",
			args:    &stringValue,
			value:   []byte(stringValue),
			flags:   gocbcore.EncodeCommonFlags(gocbcore.JsonType, gocbcore.NoCompression),
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
			got, got1, err := JSONEncode(tt.args)
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
