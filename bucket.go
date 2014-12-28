package couchbase

import "encoding/json"
import "time"
import "fmt"
import "errors"

type Bucket struct {
	manager *BucketManager
	Client  *BaseConnection
}

func (b *Bucket) SetOperationTimeout(val time.Duration) {
	b.Client.SetOperationTimeout(val)
}
func (b *Bucket) GetOperationTimeout() time.Duration {
	return b.Client.GetOperationTimeout()
}

func (b *Bucket) decodeValue(bytes []byte, flags uint32, out interface{}) (interface{}, error) {
	fmt.Printf("Early Flags: %08x\n", flags)

	// Check for legacy flags
	if flags&CF_MASK == 0 {
		// Legacy Flags
		if flags == LF_JSON {
			// Legacy JSON
			flags = CF_FMT_JSON
		} else {
			return nil, errors.New("Unexpected legacy flags value")
		}
	}

	fmt.Printf("Flags: %08x\n", flags)

	// Make sure compression is disabled
	if flags&CF_CMPR_MASK != CF_CMPR_NONE {
		return nil, errors.New("Unexpected value compression")
	}

	// If an output object was passed, try to json Unmarshal to it
	if out != nil {
		if flags&CF_FMT_JSON != 0 {
			err := json.Unmarshal(bytes, out)
			if err != nil {
				return nil, err
			}
			return out, nil
		} else {
			return nil, errors.New("Unmarshal target passed, but type does not match.")
		}
	}

	// Normal types of decoding
	if flags&CF_FMT_MASK == CF_FMT_BINARY {
		return bytes, nil
	} else if flags&CF_FMT_MASK == CF_FMT_STRING {
		return string(bytes[0:]), nil
	} else if flags&CF_FMT_MASK == CF_FMT_JSON {
		var outVal interface{}
		err := json.Unmarshal(bytes, &outVal)
		if err != nil {
			return nil, err
		}
		return outVal, nil
	} else {
		return nil, errors.New("Unexpected flags value")
	}
}

func (b *Bucket) encodeValue(value interface{}) ([]byte, uint32, error) {
	var bytes []byte
	var flags uint32
	var err error

	switch value.(type) {
	case []byte:
		bytes = value.([]byte)
		flags = CF_FMT_BINARY
	case string:
		bytes = []byte(value.(string))
		flags = CF_FMT_STRING
	default:
		bytes, err = json.Marshal(value)
		if err != nil {
			return nil, 0, err
		}
		flags = CF_FMT_JSON
	}

	// No compression supported currently

	return bytes, flags, nil
}

func (b *Bucket) Get(key string, valuePtr interface{}) (interface{}, uint64, error) {
	bytes, flags, cas, err := b.Client.Get([]byte(key))
	if err != nil {
		return nil, 0, err
	}

	value, err := b.decodeValue(bytes, flags, valuePtr)
	if err != nil {
		return nil, 0, err
	}

	return value, cas, nil
}

func (b *Bucket) GetAndTouch(key string, valuePtr interface{}, expiry uint32) (interface{}, uint64, error) {
	bytes, flags, cas, err := b.Client.GetAndTouch([]byte(key), expiry)
	if err != nil {
		return nil, 0, err
	}

	value, err := b.decodeValue(bytes, flags, valuePtr)
	if err != nil {
		return nil, 0, err
	}

	return value, cas, nil
}

func (b *Bucket) GetAndLock(key string, valuePtr interface{}, lockTime uint32) (interface{}, uint64, error) {
	bytes, flags, cas, err := b.Client.GetAndLock([]byte(key), lockTime)
	if err != nil {
		return nil, 0, err
	}

	value, err := b.decodeValue(bytes, flags, valuePtr)
	if err != nil {
		return nil, 0, err
	}

	return value, cas, nil
}

func (b *Bucket) Unlock(key string, cas uint64) (uint64, error) {
	return b.Client.Unlock([]byte(key), cas)
}

func (b *Bucket) GetReplica(key string, valuePtr interface{}, replicaIdx int) (interface{}, uint64, error) {
	bytes, flags, cas, err := b.Client.GetReplica([]byte(key), replicaIdx)
	if err != nil {
		return nil, 0, err
	}

	value, err := b.decodeValue(bytes, flags, valuePtr)
	if err != nil {
		return nil, 0, err
	}

	return value, cas, nil
}

func (b *Bucket) Touch(key string, expiry uint32) (uint64, error) {
	return b.Client.Touch([]byte(key), expiry)
}

func (b *Bucket) Remove(key string, cas uint64) (uint64, error) {
	return b.Client.Remove([]byte(key), cas)
}

func (b *Bucket) Upsert(key string, value interface{}) (uint64, error) {
	bytes, flags, err := b.encodeValue(value)
	if err != nil {
		return 0, err
	}

	return b.Client.Set([]byte(key), bytes, flags)
}

func (b *Bucket) Insert(key string, value interface{}) (uint64, error) {
	bytes, flags, err := b.encodeValue(value)
	if err != nil {
		return 0, err
	}

	return b.Client.Add([]byte(key), bytes, flags)
}

func (b *Bucket) Replace(key string, value interface{}, cas uint64) (uint64, error) {
	bytes, flags, err := b.encodeValue(value)
	if err != nil {
		return 0, err
	}

	return b.Client.Replace([]byte(key), bytes, flags, cas)
}

func (b *Bucket) Append(key, value string) (uint64, error) {
	return b.Client.Append([]byte(key), []byte(value))
}

func (b *Bucket) Prepend(key, value string) (uint64, error) {
	return b.Client.Prepend([]byte(key), []byte(value))
}

func (b *Bucket) Counter(key string, delta, initial int64, expiry uint32) (uint64, uint64, error) {
	realInitial := uint64(0xFFFFFFFFFFFFFFFF)
	if initial > 0 {
		realInitial = uint64(initial)
	}

	if delta > 0 {
		return b.Client.Increment([]byte(key), uint64(delta), realInitial, expiry)
	} else if delta < 0 {
		return b.Client.Decrement([]byte(key), uint64(-delta), realInitial, expiry)
	} else {
		panic("Delta must be non-zero")
	}
}

func (b *Bucket) Manager() *BucketManager {
	if b.manager == nil {
		b.manager = &BucketManager{}
	}
	return b.manager
}
