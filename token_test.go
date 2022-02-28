package gocb

import (
	"encoding/json"
	"strings"

	gocbcore "github.com/couchbase/gocbcore/v10"
)

func (suite *UnitTestSuite) TestMutationState_Add() {
	fakeBucket := &Bucket{}
	fakeBucket.bucketName = "frank"

	fakeToken1 := MutationToken{
		token: gocbcore.MutationToken{
			VbID:   1,
			VbUUID: gocbcore.VbUUID(9),
			SeqNo:  gocbcore.SeqNo(12),
		},
		bucketName: fakeBucket.Name(),
	}
	fakeToken2 := MutationToken{
		token: gocbcore.MutationToken{
			VbID:   2,
			VbUUID: gocbcore.VbUUID(1),
			SeqNo:  gocbcore.SeqNo(22),
		},
		bucketName: fakeBucket.Name(),
	}
	fakeToken3 := MutationToken{
		token: gocbcore.MutationToken{
			VbID:   2,
			VbUUID: gocbcore.VbUUID(4),
			SeqNo:  gocbcore.SeqNo(99),
		},
		bucketName: fakeBucket.Name(),
	}

	state := NewMutationState(fakeToken1, fakeToken2)
	state.Add(fakeToken3)

	bytes, err := json.Marshal(&state)
	if err != nil {
		suite.T().Fatalf("Failed to marshal %v", err)
	}

	if strings.Compare(string(bytes), "{\"frank\":{\"1\":[12,\"9\"],\"2\":[99,\"4\"]}}") != 0 {
		suite.T().Fatalf("Failed to generate correct JSON output %s", bytes)
	}

	// So as to avoid testing on private properties we'll check if unmarshal works by marshaling the result.
	var afterState MutationState
	err = json.Unmarshal(bytes, &afterState)
	if err != nil {
		suite.T().Fatalf("Failed to unmarshal %v", err)
	}

	bytes, err = json.Marshal(&state)
	if err != nil {
		suite.T().Fatalf("Failed to marshal %v", err)
	}

	if strings.Compare(string(bytes), "{\"frank\":{\"1\":[12,\"9\"],\"2\":[99,\"4\"]}}") != 0 {
		suite.T().Fatalf("Failed to generate correct JSON output %s", bytes)
	}
}

func (suite *UnitTestSuite) TestMutationState_toSeachMutationState() {
	fakeBucket := &Bucket{}
	fakeBucket.bucketName = "frank"

	fakeToken1 := MutationToken{
		token: gocbcore.MutationToken{
			VbID:   1,
			VbUUID: gocbcore.VbUUID(9),
			SeqNo:  gocbcore.SeqNo(12),
		},
		bucketName: fakeBucket.Name(),
	}
	fakeToken2 := MutationToken{
		token: gocbcore.MutationToken{
			VbID:   2,
			VbUUID: gocbcore.VbUUID(1),
			SeqNo:  gocbcore.SeqNo(22),
		},
		bucketName: fakeBucket.Name(),
	}

	state := NewMutationState(fakeToken1, fakeToken2)

	searchToken := state.toSearchMutationState("frankindex")

	// What we actually care about is the format once marshaled.
	bytes, err := json.Marshal(&searchToken)
	if err != nil {
		suite.T().Fatalf("Failed to marshal %v", err)
	}

	if strings.Compare(string(bytes), "{\"frankindex\":{\"1/9\":12,\"2/1\":22}}") != 0 {
		suite.T().Fatalf("Failed to generate correct JSON output %s", bytes)
	}
}
