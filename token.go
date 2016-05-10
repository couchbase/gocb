package gocb

import (
	"encoding/json"
	"github.com/couchbase/gocb/gocbcore"
	"fmt"
)

type MutationToken struct {
	token gocbcore.MutationToken
	bucket *Bucket
}

type bucketToken struct {
	SeqNo  uint64 `json:"seqno"`
	VbUuid string `json:"vbuuid"`
}
type bucketTokens map[string]*bucketToken
type mutationStateData map[string]*bucketTokens

type MutationState struct {
	data *mutationStateData
}

func NewMutationState(tokens ...MutationToken) *MutationState {
	mt := &MutationState{}
	mt.Add(tokens...)
	return mt
}

func (mt *MutationState) addSingle(token MutationToken) {
	if token.bucket == nil {
		return
	}

	if mt.data == nil {
		data := make(mutationStateData)
		mt.data = &data
	}

	bucketName := token.bucket.name
	if (*mt.data)[bucketName] == nil {
		tokens := make(bucketTokens)
		(*mt.data)[bucketName] = &tokens
	}

	vbId := fmt.Sprintf("%d", token.token.VbId)
	stateToken := (*(*mt.data)[bucketName])[vbId]
	if stateToken == nil {
		stateToken = &bucketToken{}
		(*(*mt.data)[bucketName])[vbId] = stateToken
	}

	stateToken.SeqNo = uint64(token.token.SeqNo)
	stateToken.VbUuid = fmt.Sprintf("%d", token.token.VbUuid)
}

func (mt *MutationState) Add(tokens ...MutationToken) {
	for _, v := range tokens {
		mt.addSingle(v)
	}
}

func (mt *MutationState) MarshalJSON() ([]byte, error) {
	return json.Marshal(mt.data)
}

func (mt *MutationState) UnmarshalJSON(data []byte) error {
	return json.Unmarshal(data, &mt.data)
}