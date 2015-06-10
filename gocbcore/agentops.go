package gocbcore

import (
	"encoding/binary"
	"sync/atomic"
)

type Cas uint64

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
type UnlockCallback func(Cas, error)
type TouchCallback func(Cas, error)
type RemoveCallback func(Cas, error)
type StoreCallback func(Cas, error)
type CounterCallback func(uint64, Cas, error)
type ObserveCallback func(KeyState, Cas, error)

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
			cb(0, err)
			return
		}
		cb(Cas(resp.Cas), nil)
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
			cb(0, err)
			return
		}
		cb(Cas(resp.Cas), nil)
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
			cb(0, err)
			return
		}
		cb(Cas(resp.Cas), nil)
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
			cb(0, err)
			return
		}
		cb(Cas(resp.Cas), nil)
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
			cb(0, err)
			return
		}
		cb(Cas(resp.Cas), nil)
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
			cb(0, 0, err)
			return
		}

		if len(resp.Value) != 8 {
			cb(0, 0, agentError{"Failed to parse returned value"})
			return
		}
		intVal := binary.BigEndian.Uint64(resp.Value)

		cb(intVal, Cas(resp.Cas), nil)
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
