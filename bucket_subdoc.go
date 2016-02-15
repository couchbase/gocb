package gocb

import (
	"encoding/json"
	"github.com/couchbase/gocb/gocbcore"
)

type subDocResult struct {
	path string
	data []byte
	err  error
}

type DocumentFragment struct {
	cas      Cas
	mt       MutationToken
	contents []subDocResult
	pathMap  map[string]int
}

func (frag *DocumentFragment) Cas() Cas {
	return frag.cas
}

func (frag *DocumentFragment) MutationToken() MutationToken {
	return frag.mt
}

func (frag *DocumentFragment) ContentByIndex(idx int, valuePtr interface{}) error {
	res := frag.contents[idx]
	if res.err != nil {
		return res.err
	}
	if valuePtr == nil {
		return nil
	}
	return json.Unmarshal(res.data, valuePtr)
}

func (frag *DocumentFragment) Content(path string, valuePtr interface{}) error {
	if frag.pathMap == nil {
		frag.pathMap = make(map[string]int)
		for i, v := range frag.contents {
			frag.pathMap[v.path] = i
		}
	}
	return frag.ContentByIndex(frag.pathMap[path], valuePtr)
}

func (frag *DocumentFragment) Exists(path string) bool {
	err := frag.Content(path, nil)
	return err == nil
}

type LookupInBuilder struct {
	bucket *Bucket
	name   string
	ops    []gocbcore.SubDocOp
}

func (set *LookupInBuilder) Execute() (*DocumentFragment, error) {
	return set.bucket.lookupIn(set)
}

func (set *LookupInBuilder) Get(path string) *LookupInBuilder {
	op := gocbcore.SubDocOp{
		Op:   gocbcore.SubDocOpGet,
		Path: path,
	}
	set.ops = append(set.ops, op)
	return set
}

func (set *LookupInBuilder) Exists(path string) *LookupInBuilder {
	op := gocbcore.SubDocOp{
		Op:   gocbcore.SubDocOpExists,
		Path: path,
	}
	set.ops = append(set.ops, op)
	return set
}

func (b *Bucket) lookupIn(set *LookupInBuilder) (resOut *DocumentFragment, errOut error) {
	signal := make(chan bool, 1)
	op, err := b.client.SubDocLookup([]byte(set.name), set.ops,
		func(results []gocbcore.SubDocResult, cas gocbcore.Cas, err error) {
			errOut = err

			{
				resSet := &DocumentFragment{}
				resSet.contents = make([]subDocResult, len(results))

				for i, _ := range results {
					resSet.contents[i].path = set.ops[i].Path
					resSet.contents[i].err = results[i].Err
					if results[i].Value != nil {
						resSet.contents[i].data = append([]byte(nil), results[i].Value...)
					}
				}

				resOut = resSet
			}

			signal <- true
		})
	if err != nil {
		return nil, err
	}

	timeoutTmr := gocbcore.AcquireTimer(b.opTimeout)
	select {
	case <-signal:
		gocbcore.ReleaseTimer(timeoutTmr, false)
		return
	case <-timeoutTmr.C:
		gocbcore.ReleaseTimer(timeoutTmr, true)
		if !op.Cancel() {
			<-signal
			return
		}
		return nil, ErrTimeout
	}
}

func (b *Bucket) LookupIn(key string) *LookupInBuilder {
	return &LookupInBuilder{
		bucket: b,
		name:   key,
	}
}

type MutateInBuilder struct {
	bucket *Bucket
	name   string
	cas    gocbcore.Cas
	expiry uint32
	ops    []gocbcore.SubDocOp
}

func (set *MutateInBuilder) Execute() (*DocumentFragment, error) {
	return set.bucket.mutateIn(set)
}

func (set *MutateInBuilder) Insert(path string, value interface{}, createParents bool) *MutateInBuilder {
	op := gocbcore.SubDocOp{
		Op:   gocbcore.SubDocOpDictAdd,
		Path: path,
	}
	op.Value, _ = json.Marshal(value)
	if createParents {
		op.Flags &= gocbcore.SubDocFlagMkDirP
	}
	set.ops = append(set.ops, op)
	return set
}

func (set *MutateInBuilder) Upsert(path string, value interface{}, createParents bool) *MutateInBuilder {
	op := gocbcore.SubDocOp{
		Op:   gocbcore.SubDocOpDictSet,
		Path: path,
	}
	op.Value, _ = json.Marshal(value)
	if createParents {
		op.Flags &= gocbcore.SubDocFlagMkDirP
	}
	set.ops = append(set.ops, op)
	return set
}

func (set *MutateInBuilder) Replace(path string, value interface{}) *MutateInBuilder {
	op := gocbcore.SubDocOp{
		Op:   gocbcore.SubDocOpReplace,
		Path: path,
	}
	op.Value, _ = json.Marshal(value)
	set.ops = append(set.ops, op)
	return set
}

func (set *MutateInBuilder) Remove(path string) *MutateInBuilder {
	op := gocbcore.SubDocOp{
		Op:   gocbcore.SubDocOpDelete,
		Path: path,
	}
	set.ops = append(set.ops, op)
	return set
}

func (set *MutateInBuilder) PushFront(path string, value interface{}, createParents bool) *MutateInBuilder {
	op := gocbcore.SubDocOp{
		Op:   gocbcore.SubDocOpArrayPushFirst,
		Path: path,
	}
	op.Value, _ = json.Marshal(value)
	if createParents {
		op.Flags &= gocbcore.SubDocFlagMkDirP
	}
	set.ops = append(set.ops, op)
	return set
}

func (set *MutateInBuilder) PushBack(path string, value interface{}, createParents bool) *MutateInBuilder {
	op := gocbcore.SubDocOp{
		Op:   gocbcore.SubDocOpArrayPushLast,
		Path: path,
	}
	op.Value, _ = json.Marshal(value)
	if createParents {
		op.Flags &= gocbcore.SubDocFlagMkDirP
	}
	set.ops = append(set.ops, op)
	return set
}

func (set *MutateInBuilder) ArrayInsert(path string, value interface{}) *MutateInBuilder {
	op := gocbcore.SubDocOp{
		Op:   gocbcore.SubDocOpArrayInsert,
		Path: path,
	}
	op.Value, _ = json.Marshal(value)
	set.ops = append(set.ops, op)
	return set
}

func (set *MutateInBuilder) AddUnique(path string, value interface{}, createParents bool) *MutateInBuilder {
	op := gocbcore.SubDocOp{
		Op:   gocbcore.SubDocOpArrayAddUnique,
		Path: path,
	}
	op.Value, _ = json.Marshal(value)
	if createParents {
		op.Flags &= gocbcore.SubDocFlagMkDirP
	}
	set.ops = append(set.ops, op)
	return set
}

func (set *MutateInBuilder) Counter(path string, delta int64, createParents bool) *MutateInBuilder {
	op := gocbcore.SubDocOp{
		Op:   gocbcore.SubDocOpCounter,
		Path: path,
	}
	op.Value, _ = json.Marshal(delta)
	if createParents {
		op.Flags &= gocbcore.SubDocFlagMkDirP
	}
	set.ops = append(set.ops, op)
	return set
}

func (b *Bucket) mutateIn(set *MutateInBuilder) (resOut *DocumentFragment, errOut error) {
	signal := make(chan bool, 1)
	op, err := b.client.SubDocMutate([]byte(set.name), set.ops, set.cas, set.expiry,
		func(results []gocbcore.SubDocResult, cas gocbcore.Cas, mt gocbcore.MutationToken, err error) {
			errOut = err
			if errOut == nil {
				resSet := &DocumentFragment{
					cas: cas,
					mt:  mt,
				}
				resSet.contents = make([]subDocResult, len(results))

				for i, _ := range results {
					resSet.contents[i].path = set.ops[i].Path
					resSet.contents[i].err = results[i].Err
					if results[i].Value != nil {
						resSet.contents[i].data = append([]byte(nil), results[i].Value...)
					}
				}

				resOut = resSet
			}
			signal <- true
		})
	if err != nil {
		return nil, err
	}

	timeoutTmr := gocbcore.AcquireTimer(b.opTimeout)
	select {
	case <-signal:
		gocbcore.ReleaseTimer(timeoutTmr, false)
		return
	case <-timeoutTmr.C:
		gocbcore.ReleaseTimer(timeoutTmr, true)
		if !op.Cancel() {
			<-signal
			return
		}
		return nil, ErrTimeout
	}
}

func (b *Bucket) MutateIn(key string, cas Cas, expiry uint32) *MutateInBuilder {
	return &MutateInBuilder{
		bucket: b,
		name:   key,
		cas:    gocbcore.Cas(cas),
		expiry: expiry,
	}
}
