package gocbcore

import (
	"encoding/binary"
	"sync/atomic"
)

type Cas uint64

type VbUuid uint64
type SeqNo uint64

type MutationToken struct {
	VbUuid VbUuid
	SeqNo SeqNo
}

type PendingOp interface {
	Cancel() bool
}

type multiPendingOp struct {
	ops []PendingOp
}

func (mp *multiPendingOp) Cancel() bool {
	allCancelled := true
	for _, op := range mp.ops {
		if !op.Cancel() {
			allCancelled = false
		}
	}
	return allCancelled
}

func (c *Agent) dispatchOp(req *memdQRequest) (PendingOp, error) {
	err := c.dispatchDirect(req)
	if err != nil {
		return nil, err
	}
	return req, nil
}

type GetCallback func([]byte, uint32, Cas, error)
type UnlockCallback func(Cas, MutationToken, error)
type TouchCallback func(Cas, MutationToken, error)
type RemoveCallback func(Cas, MutationToken, error)
type StoreCallback func(Cas, MutationToken, error)
type CounterCallback func(uint64, Cas, MutationToken, error)
type ObserveCallback func(KeyState, Cas, error)
type ObserveSeqNoCallback func(SeqNo, SeqNo, error)
type GetRandomCallback func([]byte, []byte, uint32, Cas, error)

func (c *Agent) Get(key []byte, cb GetCallback) (PendingOp, error) {
	handler := func(resp *memdResponse, err error) {
		if err != nil {
			cb(nil, 0, 0, err)
			return
		}
		flags := binary.BigEndian.Uint32(resp.Extras[0:])
		cb(resp.Value, flags, Cas(resp.Cas), nil)
	}
	req := &memdQRequest{
		memdRequest: memdRequest{
			Magic:    ReqMagic,
			Opcode:   CmdGet,
			Datatype: 0,
			Cas:      0,
			Extras:   nil,
			Key:      key,
			Value:    nil,
		},
		Callback: handler,
	}
	return c.dispatchOp(req)
}

func (c *Agent) GetAndTouch(key []byte, expiry uint32, cb GetCallback) (PendingOp, error) {
	handler := func(resp *memdResponse, err error) {
		if err != nil {
			cb(nil, 0, 0, err)
			return
		}
		flags := binary.BigEndian.Uint32(resp.Extras[0:])
		cb(resp.Value, flags, Cas(resp.Cas), nil)
	}

	extraBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(extraBuf[0:], expiry)

	req := &memdQRequest{
		memdRequest: memdRequest{
			Magic:    ReqMagic,
			Opcode:   CmdGAT,
			Datatype: 0,
			Cas:      0,
			Extras:   extraBuf,
			Key:      key,
			Value:    nil,
		},
		Callback: handler,
	}
	return c.dispatchOp(req)
}

func (c *Agent) GetAndLock(key []byte, lockTime uint32, cb GetCallback) (PendingOp, error) {
	handler := func(resp *memdResponse, err error) {
		if err != nil {
			cb(nil, 0, 0, err)
			return
		}
		flags := binary.BigEndian.Uint32(resp.Extras[0:])
		cb(resp.Value, flags, Cas(resp.Cas), nil)
	}

	extraBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(extraBuf[0:], lockTime)

	req := &memdQRequest{
		memdRequest: memdRequest{
			Magic:    ReqMagic,
			Opcode:   CmdGetLocked,
			Datatype: 0,
			Cas:      0,
			Extras:   extraBuf,
			Key:      key,
			Value:    nil,
		},
		Callback: handler,
	}
	return c.dispatchOp(req)
}

func (c *Agent) getOneReplica(key []byte, replicaIdx int, cb GetCallback) (PendingOp, error) {
	if replicaIdx <= 0 {
		panic("Replica number must be greater than 0")
	}

	handler := func(resp *memdResponse, err error) {
		if err != nil {
			cb(nil, 0, 0, err)
			return
		}
		flags := binary.BigEndian.Uint32(resp.Extras[0:])
		cb(resp.Value, flags, Cas(resp.Cas), nil)
	}

	req := &memdQRequest{
		memdRequest: memdRequest{
			Magic:    ReqMagic,
			Opcode:   CmdGetReplica,
			Datatype: 0,
			Cas:      0,
			Extras:   nil,
			Key:      key,
			Value:    nil,
		},
		Callback:   handler,
		ReplicaIdx: replicaIdx,
	}
	return c.dispatchOp(req)
}

func (c *Agent) getAnyReplica(key []byte, cb GetCallback) (PendingOp, error) {
	opRes := &multiPendingOp{}

	var cbCalled uint32
	handler := func(value []byte, flags uint32, cas Cas, err error) {
		if atomic.CompareAndSwapUint32(&cbCalled, 0, 1) {
			// Cancel all other commands if possible.
			opRes.Cancel()
			// Dispatch Callback
			cb(value, flags, cas, err)
		}
	}

	// Dispatch a getReplica for each replica server
	numReplicas := c.NumReplicas()
	for repIdx := 1; repIdx <= numReplicas; repIdx++ {
		op, err := c.getOneReplica(key, repIdx, handler)
		if err == nil {
			opRes.ops = append(opRes.ops, op)
		}
	}

	// If we have no pending ops, no requests were successful
	if len(opRes.ops) == 0 {
		return nil, &agentError{"No replicas available"}
	}

	return opRes, nil
}

func (c *Agent) GetReplica(key []byte, replicaIdx int, cb GetCallback) (PendingOp, error) {
	if replicaIdx > 0 {
		return c.getOneReplica(key, replicaIdx, cb)
	} else if replicaIdx == 0 {
		return c.getAnyReplica(key, cb)
	} else {
		panic("Replica number must not be less than 0.")
	}
}

func (c *Agent) Touch(key []byte, cas Cas, expiry uint32, cb TouchCallback) (PendingOp, error) {
	handler := func(resp *memdResponse, err error) {
		if err != nil {
			cb(0, MutationToken{}, err)
			return
		}

		mutToken := MutationToken{}
		if len(resp.Extras) >= 16 {
			mutToken.VbUuid = VbUuid(binary.BigEndian.Uint64(resp.Extras[0:]))
			mutToken.SeqNo = SeqNo(binary.BigEndian.Uint64(resp.Extras[8:]))
		}

		cb(Cas(resp.Cas), mutToken, nil)
	}

	extraBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(extraBuf[0:], expiry)

	req := &memdQRequest{
		memdRequest: memdRequest{
			Magic:    ReqMagic,
			Opcode:   CmdTouch,
			Datatype: 0,
			Cas:      uint64(cas),
			Extras:   extraBuf,
			Key:      key,
			Value:    nil,
		},
		Callback: handler,
	}
	return c.dispatchOp(req)
}

func (c *Agent) Unlock(key []byte, cas Cas, cb UnlockCallback) (PendingOp, error) {
	handler := func(resp *memdResponse, err error) {
		if err != nil {
			cb(0, MutationToken{}, err)
			return
		}

		mutToken := MutationToken{}
		if len(resp.Extras) >= 16 {
			mutToken.VbUuid = VbUuid(binary.BigEndian.Uint64(resp.Extras[0:]))
			mutToken.SeqNo = SeqNo(binary.BigEndian.Uint64(resp.Extras[8:]))
		}

		cb(Cas(resp.Cas), mutToken, nil)
	}

	req := &memdQRequest{
		memdRequest: memdRequest{
			Magic:    ReqMagic,
			Opcode:   CmdUnlockKey,
			Datatype: 0,
			Cas:      0,
			Extras:   nil,
			Key:      key,
			Value:    nil,
		},
		Callback: handler,
	}
	return c.dispatchOp(req)
}

func (c *Agent) Remove(key []byte, cas Cas, cb RemoveCallback) (PendingOp, error) {
	handler := func(resp *memdResponse, err error) {
		if err != nil {
			cb(0, MutationToken{}, err)
			return
		}

		mutToken := MutationToken{}
		if len(resp.Extras) >= 16 {
			mutToken.VbUuid = VbUuid(binary.BigEndian.Uint64(resp.Extras[0:]))
			mutToken.SeqNo = SeqNo(binary.BigEndian.Uint64(resp.Extras[8:]))
		}

		cb(Cas(resp.Cas), mutToken, nil)
	}

	req := &memdQRequest{
		memdRequest: memdRequest{
			Magic:    ReqMagic,
			Opcode:   CmdDelete,
			Datatype: 0,
			Cas:      uint64(cas),
			Extras:   nil,
			Key:      key,
			Value:    nil,
		},
		Callback: handler,
	}
	return c.dispatchOp(req)
}

func (c *Agent) store(opcode CommandCode, key, value []byte, flags uint32, cas Cas, expiry uint32, cb StoreCallback) (PendingOp, error) {
	handler := func(resp *memdResponse, err error) {
		if err != nil {
			cb(0, MutationToken{}, err)
			return
		}

		mutToken := MutationToken{}
		if len(resp.Extras) >= 16 {
			mutToken.VbUuid = VbUuid(binary.BigEndian.Uint64(resp.Extras[0:]))
			mutToken.SeqNo = SeqNo(binary.BigEndian.Uint64(resp.Extras[8:]))
		}

		cb(Cas(resp.Cas), mutToken, nil)
	}

	extraBuf := make([]byte, 8)
	binary.BigEndian.PutUint32(extraBuf[0:], flags)
	binary.BigEndian.PutUint32(extraBuf[4:], expiry)
	req := &memdQRequest{
		memdRequest: memdRequest{
			Magic:    ReqMagic,
			Opcode:   opcode,
			Datatype: 0,
			Cas:      uint64(cas),
			Extras:   extraBuf,
			Key:      key,
			Value:    value,
		},
		Callback: handler,
	}
	return c.dispatchOp(req)
}

func (c *Agent) Add(key, value []byte, flags uint32, expiry uint32, cb StoreCallback) (PendingOp, error) {
	return c.store(CmdAdd, key, value, flags, 0, expiry, cb)
}

func (c *Agent) Set(key, value []byte, flags uint32, expiry uint32, cb StoreCallback) (PendingOp, error) {
	return c.store(CmdSet, key, value, flags, 0, expiry, cb)
}

func (c *Agent) Replace(key, value []byte, flags uint32, cas Cas, expiry uint32, cb StoreCallback) (PendingOp, error) {
	return c.store(CmdReplace, key, value, flags, cas, expiry, cb)
}

func (c *Agent) adjoin(opcode CommandCode, key, value []byte, cb StoreCallback) (PendingOp, error) {
	handler := func(resp *memdResponse, err error) {
		if err != nil {
			cb(0, MutationToken{}, err)
			return
		}

		mutToken := MutationToken{}
		if len(resp.Extras) >= 16 {
			mutToken.VbUuid = VbUuid(binary.BigEndian.Uint64(resp.Extras[0:]))
			mutToken.SeqNo = SeqNo(binary.BigEndian.Uint64(resp.Extras[8:]))
		}

		cb(Cas(resp.Cas), mutToken, nil)
	}

	req := &memdQRequest{
		memdRequest: memdRequest{
			Magic:    ReqMagic,
			Opcode:   opcode,
			Datatype: 0,
			Cas:      0,
			Extras:   nil,
			Key:      key,
			Value:    value,
		},
		Callback: handler,
	}
	return c.dispatchOp(req)
}

func (c *Agent) Append(key, value []byte, cb StoreCallback) (PendingOp, error) {
	return c.adjoin(CmdAppend, key, value, cb)
}

func (c *Agent) Prepend(key, value []byte, cb StoreCallback) (PendingOp, error) {
	return c.adjoin(CmdPrepend, key, value, cb)
}

func (c *Agent) counter(opcode CommandCode, key []byte, delta, initial uint64, expiry uint32, cb CounterCallback) (PendingOp, error) {
	handler := func(resp *memdResponse, err error) {
		if err != nil {
			cb(0, 0, MutationToken{}, err)
			return
		}

		if len(resp.Value) != 8 {
			cb(0, 0, MutationToken{}, agentError{"Failed to parse returned value"})
			return
		}
		intVal := binary.BigEndian.Uint64(resp.Value)

		mutToken := MutationToken{}
		if len(resp.Extras) >= 16 {
			mutToken.VbUuid = VbUuid(binary.BigEndian.Uint64(resp.Extras[0:]))
			mutToken.SeqNo = SeqNo(binary.BigEndian.Uint64(resp.Extras[8:]))
		}

		cb(intVal, Cas(resp.Cas), mutToken, nil)
	}

	extraBuf := make([]byte, 20)
	binary.BigEndian.PutUint64(extraBuf[0:], delta)
	binary.BigEndian.PutUint64(extraBuf[8:], initial)
	binary.BigEndian.PutUint32(extraBuf[16:], expiry)

	req := &memdQRequest{
		memdRequest: memdRequest{
			Magic:    ReqMagic,
			Opcode:   opcode,
			Datatype: 0,
			Cas:      0,
			Extras:   extraBuf,
			Key:      key,
			Value:    nil,
		},
		Callback: handler,
	}
	return c.dispatchOp(req)
}

func (c *Agent) Increment(key []byte, delta, initial uint64, expiry uint32, cb CounterCallback) (PendingOp, error) {
	return c.counter(CmdIncrement, key, delta, initial, expiry, cb)
}

func (c *Agent) Decrement(key []byte, delta, initial uint64, expiry uint32, cb CounterCallback) (PendingOp, error) {
	return c.counter(CmdDecrement, key, delta, initial, expiry, cb)
}

func (c *Agent) Observe(key []byte, replicaIdx int, cb ObserveCallback) (PendingOp, error) {
	handler := func(resp *memdResponse, err error) {
		if err != nil {
			cb(0, 0, err)
			return
		}

		if len(resp.Value) < 4 {
			cb(0, 0, agentError{"Failed to parse response data"})
			return
		}
		keyLen := int(binary.BigEndian.Uint16(resp.Value[2:]))

		if len(resp.Value) != 2+2+keyLen+1+8 {
			cb(0, 0, agentError{"Failed to parse response data"})
			return
		}
		keyState := KeyState(resp.Value[2+2+keyLen])
		cas := binary.BigEndian.Uint64(resp.Value[2+2+keyLen+1:])

		cb(keyState, Cas(cas), nil)
	}

	vbId := c.KeyToVbucket(key)

	valueBuf := make([]byte, 2+2+len(key))
	binary.BigEndian.PutUint16(valueBuf[0:], vbId)
	binary.BigEndian.PutUint16(valueBuf[2:], uint16(len(key)))
	copy(valueBuf[4:], key)

	req := &memdQRequest{
		memdRequest: memdRequest{
			Magic:    ReqMagic,
			Opcode:   CmdObserve,
			Datatype: 0,
			Cas:      0,
			Extras:   nil,
			Key:      nil,
			Value:    valueBuf,
			Vbucket:  vbId,
		},
		ReplicaIdx: replicaIdx,
		Callback:   handler,
	}
	return c.dispatchOp(req)
}

func (c *Agent) ObserveSeqNo(key []byte, vbUuid VbUuid, replicaIdx int, cb ObserveSeqNoCallback) (PendingOp, error) {
	handler := func(resp *memdResponse, err error) {
		if err != nil {
			cb(0, 0, err)
			return
		}

		if len(resp.Value) < 1 {
			cb(0, 0, agentError{"Failed to parse response data"})
			return
		}

		formatType := resp.Value[0]
		if formatType == 0 {
			// Normal
			if len(resp.Value) < 27 {
				cb(0, 0, agentError{"Failed to parse response data (type=0)"})
				return
			}

			//vbId := binary.BigEndian.Uint16(resp.Value[1:])
			//vbUuid := binary.BigEndian.Uint64(resp.Value[3:])
			persistSeqNo := binary.BigEndian.Uint64(resp.Value[11:])
			currentSeqNo := binary.BigEndian.Uint64(resp.Value[19:])

			cb(SeqNo(currentSeqNo), SeqNo(persistSeqNo), nil)
			return
		} else if formatType == 1 {
			// Hard Failover
			if len(resp.Value) < 43 {
				cb(0, 0, agentError{"Failed to parse response data (type=1)"})
				return
			}

			//vbId := binary.BigEndian.Uint16(resp.Value[1:])
			//newVbUuid := binary.BigEndian.Uint64(resp.Value[3:])
			//persistSeqNo := binary.BigEndian.Uint64(resp.Value[11:])
			//currentSeqNo := binary.BigEndian.Uint64(resp.Value[19:])
			//vbUuid := binary.BigEndian.Uint64(resp.Value[27:])
			lastSeqNo := binary.BigEndian.Uint64(resp.Value[35:])

			cb(SeqNo(lastSeqNo), SeqNo(lastSeqNo), nil)
			return
		} else {
			cb(0, 0, agentError{"Failed to parse response format type"})
			return
		}
	}

	vbId := c.KeyToVbucket(key)

	valueBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(valueBuf[0:], uint64(vbUuid))

	req := &memdQRequest{
		memdRequest: memdRequest{
			Magic:    ReqMagic,
			Opcode:   CmdObserveSeqNo,
			Datatype: 0,
			Cas:      0,
			Extras:   nil,
			Key:      nil,
			Value:    valueBuf,
			Vbucket:  vbId,
		},
		ReplicaIdx: replicaIdx,
		Callback:   handler,
	}
	return c.dispatchOp(req)
}

func (c *Agent) GetRandom(cb GetRandomCallback) (PendingOp, error) {
	handler := func(resp *memdResponse, err error) {
		if err != nil {
			cb(nil, nil, 0, 0, err)
			return
		}
		flags := binary.BigEndian.Uint32(resp.Extras[0:])
		cb(resp.Key, resp.Value, flags, Cas(resp.Cas), nil)
	}
	req := &memdQRequest{
		memdRequest: memdRequest{
			Magic:    ReqMagic,
			Opcode:   CmdGetRandom,
			Datatype: 0,
			Cas:      0,
			Extras:   nil,
			Key:      nil,
			Value:    nil,
		},
		Callback: handler,
	}
	return c.dispatchOp(req)
}

func (c *Agent) SetMeta(key, value, extra []byte, flags, expiry uint32, cas, revseqno uint64, cb StoreCallback) (PendingOp, error) {
	handler := func(resp *memdResponse, err error) {
		if err != nil {
			cb(0, MutationToken{}, err)
			return
		}

		mutToken := MutationToken{}
		if len(resp.Extras) >= 16 {
			mutToken.VbUuid = VbUuid(binary.BigEndian.Uint64(resp.Extras[0:]))
			mutToken.SeqNo = SeqNo(binary.BigEndian.Uint64(resp.Extras[8:]))
		}

		cb(Cas(resp.Cas), mutToken, nil)
	}

	extraBuf := make([]byte, 26 + len(extra))
	binary.BigEndian.PutUint32(extraBuf[0:], flags)
	binary.BigEndian.PutUint32(extraBuf[4:], expiry)
	binary.BigEndian.PutUint64(extraBuf[8:], revseqno)
	binary.BigEndian.PutUint64(extraBuf[16:], cas)
	binary.BigEndian.PutUint16(extraBuf[24:], uint16(len(extra)))
	copy(extraBuf[26:], extra)
	req := &memdQRequest{
		memdRequest: memdRequest{
			Magic:    ReqMagic,
			Opcode:   CmdSetMeta,
			Datatype: 0,
			Cas:      0,
			Extras:   extraBuf,
			Key:      key,
			Value:    value,
		},
		Callback: handler,
	}
	return c.dispatchOp(req)
}

func (c *Agent) DeleteMeta(key, extra []byte, flags, expiry uint32, cas, revseqno uint64, cb RemoveCallback) (PendingOp, error) {
	handler := func(resp *memdResponse, err error) {
		if err != nil {
			cb(0, MutationToken{}, err)
			return
		}

		mutToken := MutationToken{}
		if len(resp.Extras) >= 16 {
			mutToken.VbUuid = VbUuid(binary.BigEndian.Uint64(resp.Extras[0:]))
			mutToken.SeqNo = SeqNo(binary.BigEndian.Uint64(resp.Extras[8:]))
		}

		cb(Cas(resp.Cas), mutToken, nil)
	}

	extraBuf := make([]byte, 26 + len(extra))
	binary.BigEndian.PutUint32(extraBuf[0:], flags)
	binary.BigEndian.PutUint32(extraBuf[4:], expiry)
	binary.BigEndian.PutUint64(extraBuf[8:], revseqno)
	binary.BigEndian.PutUint64(extraBuf[16:], cas)
	binary.BigEndian.PutUint16(extraBuf[24:], uint16(len(extra)))
	copy(extraBuf[26:], extra)
	req := &memdQRequest{
		memdRequest: memdRequest{
			Magic:    ReqMagic,
			Opcode:   CmdSetMeta,
			Datatype: 0,
			Cas:      0,
			Extras:   extraBuf,
			Key:      key,
			Value:    nil,
		},
		Callback: handler,
	}
	return c.dispatchOp(req)
}
