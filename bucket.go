package couchbase

import "encoding/json"
import "time"
import "fmt"

// An interface representing a single bucket within a cluster.
type Bucket struct {
	manager *BucketManager
	Client  *BaseConnection
}

// Sets the timeout period for any CRUD operations
func (b *Bucket) SetOperationTimeout(val time.Duration) {
	b.Client.SetOperationTimeout(val)
}

// Retrieves the timeout period for any CRUD operations.
func (b *Bucket) GetOperationTimeout() time.Duration {
	return b.Client.GetOperationTimeout()
}

func (b *Bucket) decodeValue(bytes []byte, flags uint32, out interface{}) (interface{}, Error) {
	fmt.Printf("Early Flags: %08x\n", flags)

	// Check for legacy flags
	if flags&cfMask == 0 {
		// Legacy Flags
		if flags == lfJson {
			// Legacy JSON
			flags = cfFmtJson
		} else {
			return nil, clientError{"Unexpected legacy flags value"}
		}
	}

	fmt.Printf("Flags: %08x\n", flags)

	// Make sure compression is disabled
	if flags&cfCmprMask != cfCmprNone {
		return nil, clientError{"Unexpected value compression"}
	}

	// If an output object was passed, try to json Unmarshal to it
	if out != nil {
		if flags&cfFmtJson != 0 {
			err := json.Unmarshal(bytes, out)
			if err != nil {
				return nil, clientError{err.Error()}
			}
			return out, nil
		} else {
			return nil, clientError{"Unmarshal target passed, but type does not match."}
		}
	}

	// Normal types of decoding
	if flags&cfFmtMask == cfFmtBinary {
		return bytes, nil
	} else if flags&cfFmtMask == cfFmtString {
		return string(bytes[0:]), nil
	} else if flags&cfFmtMask == cfFmtJson {
		var outVal interface{}
		err := json.Unmarshal(bytes, &outVal)
		if err != nil {
			return nil, clientError{err.Error()}
		}
		return outVal, nil
	} else {
		return nil, clientError{"Unexpected flags value"}
	}
}

func (b *Bucket) encodeValue(value interface{}) ([]byte, uint32, Error) {
	var bytes []byte
	var flags uint32
	var err error

	switch value.(type) {
	case []byte:
		bytes = value.([]byte)
		flags = cfFmtBinary
	case string:
		bytes = []byte(value.(string))
		flags = cfFmtString
	default:
		bytes, err = json.Marshal(value)
		if err != nil {
			return nil, 0, clientError{err.Error()}
		}
		flags = cfFmtJson
	}

	// No compression supported currently

	return bytes, flags, nil
}

// Retrieves a document from the bucket
func (b *Bucket) Get(key string, valuePtr interface{}) (interface{}, uint64, Error) {
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

// Retrieves a document and simultaneously updates its expiry time.
func (b *Bucket) GetAndTouch(key string, valuePtr interface{}, expiry uint32) (interface{}, uint64, Error) {
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

// Locks a document for a period of time, providing exclusive RW access to it.
func (b *Bucket) GetAndLock(key string, valuePtr interface{}, lockTime uint32) (interface{}, uint64, Error) {
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

// Unlocks a document which was locked with GetAndLock.
func (b *Bucket) Unlock(key string, cas uint64) (uint64, Error) {
	return b.Client.Unlock([]byte(key), cas)
}

// Returns the value of a particular document from a replica server.
func (b *Bucket) GetReplica(key string, valuePtr interface{}, replicaIdx int) (interface{}, uint64, Error) {
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

// Touches a document, specifying a new expiry time for it.
func (b *Bucket) Touch(key string, expiry uint32) (uint64, Error) {
	return b.Client.Touch([]byte(key), expiry)
}

// Removes a document from the bucket.
func (b *Bucket) Remove(key string, cas uint64) (uint64, Error) {
	return b.Client.Remove([]byte(key), cas)
}

// Inserts or replaces a document in the bucket.
func (b *Bucket) Upsert(key string, value interface{}, expiry uint32) (uint64, Error) {
	bytes, flags, err := b.encodeValue(value)
	if err != nil {
		return 0, err
	}

	return b.Client.Set([]byte(key), bytes, flags, expiry)
}

// Inserts a new document to the bucket.
func (b *Bucket) Insert(key string, value interface{}, expiry uint32) (uint64, Error) {
	bytes, flags, err := b.encodeValue(value)
	if err != nil {
		return 0, err
	}

	return b.Client.Add([]byte(key), bytes, flags, expiry)
}

// Replaces a document in the bucket.
func (b *Bucket) Replace(key string, value interface{}, cas uint64, expiry uint32) (uint64, Error) {
	bytes, flags, err := b.encodeValue(value)
	if err != nil {
		return 0, err
	}

	return b.Client.Replace([]byte(key), bytes, flags, cas, expiry)
}

// Appends a string value to a document.
func (b *Bucket) Append(key, value string) (uint64, Error) {
	return b.Client.Append([]byte(key), []byte(value))
}

// Prepends a string value to a document.
func (b *Bucket) Prepend(key, value string) (uint64, Error) {
	return b.Client.Prepend([]byte(key), []byte(value))
}

// Performs an atomic addition or subtraction for an integer document.
func (b *Bucket) Counter(key string, delta, initial int64, expiry uint32) (uint64, uint64, Error) {
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

// Performs a view query and returns a list of rows or an error.
func (b *Bucket) PerformViewQuery(queryObj ViewQuery) ([]interface{}, Error) {
	return nil, nil
}

// Returns an interface allowing management operations to be performed on the bucket.
func (b *Bucket) Manager() *BucketManager {
	if b.manager == nil {
		b.manager = &BucketManager{}
	}
	return b.manager
}
