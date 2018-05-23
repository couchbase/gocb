package gocb

import (
	"encoding/json"
	"strings"
	"testing"

	"gopkg.in/couchbase/gocbcore.v7"
)

func TestMutationState_Add(t *testing.T) {
	fakeBucket := &Bucket{
		name: "frank",
	}
	fakeToken1 := MutationToken{
		token: gocbcore.MutationToken{
			VbId:   1,
			VbUuid: gocbcore.VbUuid(9),
			SeqNo:  gocbcore.SeqNo(12),
		},
		bucket: fakeBucket,
	}
	fakeToken2 := MutationToken{
		token: gocbcore.MutationToken{
			VbId:   2,
			VbUuid: gocbcore.VbUuid(1),
			SeqNo:  gocbcore.SeqNo(22),
		},
		bucket: fakeBucket,
	}
	fakeToken3 := MutationToken{
		token: gocbcore.MutationToken{
			VbId:   2,
			VbUuid: gocbcore.VbUuid(4),
			SeqNo:  gocbcore.SeqNo(99),
		},
		bucket: fakeBucket,
	}

	state := NewMutationState(fakeToken1, fakeToken2)
	state.Add(fakeToken3)

	bytes, err := json.Marshal(&state)
	if err != nil {
		t.Fatalf("Failed to marshal %v", err)
	}

	if strings.Compare(string(bytes), "{\"frank\":{\"1\":[12,\"9\"],\"2\":[99,\"4\"]}}") != 0 {
		t.Fatalf("Failed to generate correct JSON output %s", bytes)
	}
}
