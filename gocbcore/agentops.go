package gocbcore

import (
	"encoding/binary"
)

type PendingOp interface {
	Cancel() bool
}

func (c *Agent) dispatchOp(req *memdQRequest) (PendingOp, error) {
	err := c.dispatchDirect(req)
	if err != nil {
		return nil, err
	}
	return req, nil
}

type GetCallback func([]byte, uint32, uint64, error)
type UnlockCallback func(uint64, error)
type TouchCallback func(uint64, error)
type RemoveCallback func(uint64, error)
type StoreCallback func(uint64, error)
type CounterCallback func(uint64, uint64, error)

func (c *Agent) Get(key []byte, cb GetCallback) (PendingOp, error) {
	handler := func(resp *memdResponse, err error) {
		if err != nil {
			cb(nil, 0, 0, err)
			return
		}
		flags := binary.BigEndian.Uint32(resp.Extras[0:])
		cb(resp.Value, flags, resp.Cas, nil)
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
		cb(resp.Value, flags, resp.Cas, nil)
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
		cb(resp.Value, flags, resp.Cas, nil)
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

func (c *Agent) GetReplica(key []byte, replicaIdx int, cb GetCallback) (PendingOp, error) {
	if replicaIdx <= 0 {
		panic("Replica number must be greater than 0")
	}

	handler := func(resp *memdResponse, err error) {
		if err != nil {
			cb(nil, 0, 0, err)
			return
		}
		flags := binary.BigEndian.Uint32(resp.Extras[0:])
		cb(resp.Value, flags, resp.Cas, nil)
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

func (c *Agent) Touch(key []byte, expiry uint32, cb TouchCallback) (PendingOp, error) {
	// This seems odd, but this is how it's done in Node.js
	return c.GetAndTouch(key, expiry, func(value []byte, flags uint32, cas uint64, err error) {
		cb(cas, err)
	})
}

func (c *Agent) Unlock(key []byte, cas uint64, cb UnlockCallback) (PendingOp, error) {
	handler := func(resp *memdResponse, err error) {
		if err != nil {
			cb(0, err)
			return
		}
		cb(resp.Cas, nil)
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

func (c *Agent) Remove(key []byte, cas uint64, cb RemoveCallback) (PendingOp, error) {
	handler := func(resp *memdResponse, err error) {
		if err != nil {
			cb(0, err)
			return
		}
		cb(resp.Cas, nil)
	}

	req := &memdQRequest{
		memdRequest: memdRequest{
			Magic:    ReqMagic,
			Opcode:   CmdDelete,
			Datatype: 0,
			Cas:      cas,
			Extras:   nil,
			Key:      key,
			Value:    nil,
		},
		Callback: handler,
	}
	return c.dispatchOp(req)
}

func (c *Agent) store(opcode CommandCode, key, value []byte, flags uint32, cas uint64, expiry uint32, cb StoreCallback) (PendingOp, error) {
	handler := func(resp *memdResponse, err error) {
		if err != nil {
			cb(0, err)
			return
		}
		cb(resp.Cas, nil)
	}

	extraBuf := make([]byte, 8)
	binary.BigEndian.PutUint32(extraBuf[0:], flags)
	binary.BigEndian.PutUint32(extraBuf[4:], expiry)
	req := &memdQRequest{
		memdRequest: memdRequest{
			Magic:    ReqMagic,
			Opcode:   opcode,
			Datatype: 0,
			Cas:      cas,
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

func (c *Agent) Replace(key, value []byte, flags uint32, cas uint64, expiry uint32, cb StoreCallback) (PendingOp, error) {
	return c.store(CmdReplace, key, value, flags, cas, expiry, cb)
}

func (c *Agent) adjoin(opcode CommandCode, key, value []byte, cb StoreCallback) (PendingOp, error) {
	handler := func(resp *memdResponse, err error) {
		if err != nil {
			cb(0, err)
			return
		}
		cb(resp.Cas, nil)
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
			cb(0, 0, ErrAuthInvalidReturn)
			return
		}
		intVal := binary.BigEndian.Uint64(resp.Value)

		cb(intVal, resp.Cas, nil)
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
