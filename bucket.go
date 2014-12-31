package couchbase

import "encoding/json"
import "time"
import "fmt"

// An interface representing a single bucket within a cluster.
type Bucket struct {
	manager *BucketManager
	client  *CbIoRouter
}

// Sets the timeout period for any CRUD operations
func (b *Bucket) SetOperationTimeout(val time.Duration) {
	b.client.SetOperationTimeout(val)
}

// Retrieves the timeout period for any CRUD operations.
func (b *Bucket) GetOperationTimeout() time.Duration {
	return b.client.GetOperationTimeout()
}

func (b *Bucket) decodeValue(bytes []byte, flags uint32, out interface{}) (interface{}, error) {
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

func (b *Bucket) encodeValue(value interface{}) ([]byte, uint32, error) {
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
func (b *Bucket) Get(key string, valuePtr interface{}) (interface{}, uint64, error) {
	bytes, flags, cas, err := b.client.Get([]byte(key))
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
func (b *Bucket) GetAndTouch(key string, valuePtr interface{}, expiry uint32) (interface{}, uint64, error) {
	bytes, flags, cas, err := b.client.GetAndTouch([]byte(key), expiry)
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
func (b *Bucket) GetAndLock(key string, valuePtr interface{}, lockTime uint32) (interface{}, uint64, error) {
	bytes, flags, cas, err := b.client.GetAndLock([]byte(key), lockTime)
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
func (b *Bucket) Unlock(key string, cas uint64) (uint64, error) {
	return b.client.Unlock([]byte(key), cas)
}

// Returns the value of a particular document from a replica server.
func (b *Bucket) GetReplica(key string, valuePtr interface{}, replicaIdx int) (interface{}, uint64, error) {
	bytes, flags, cas, err := b.client.GetReplica([]byte(key), replicaIdx)
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
func (b *Bucket) Touch(key string, expiry uint32) (uint64, error) {
	return b.client.Touch([]byte(key), expiry)
}

// Removes a document from the bucket.
func (b *Bucket) Remove(key string, cas uint64) (uint64, error) {
	return b.client.Remove([]byte(key), cas)
}

// Inserts or replaces a document in the bucket.
func (b *Bucket) Upsert(key string, value interface{}, expiry uint32) (uint64, error) {
	bytes, flags, err := b.encodeValue(value)
	if err != nil {
		return 0, err
	}

	return b.client.Set([]byte(key), bytes, flags, expiry)
}

// Inserts a new document to the bucket.
func (b *Bucket) Insert(key string, value interface{}, expiry uint32) (uint64, error) {
	bytes, flags, err := b.encodeValue(value)
	if err != nil {
		return 0, err
	}

	return b.client.Add([]byte(key), bytes, flags, expiry)
}

// Replaces a document in the bucket.
func (b *Bucket) Replace(key string, value interface{}, cas uint64, expiry uint32) (uint64, error) {
	bytes, flags, err := b.encodeValue(value)
	if err != nil {
		return 0, err
	}

	return b.client.Replace([]byte(key), bytes, flags, cas, expiry)
}

// Appends a string value to a document.
func (b *Bucket) Append(key, value string) (uint64, error) {
	return b.client.Append([]byte(key), []byte(value))
}

// Prepends a string value to a document.
func (b *Bucket) Prepend(key, value string) (uint64, error) {
	return b.client.Prepend([]byte(key), []byte(value))
}

// Performs an atomic addition or subtraction for an integer document.
func (b *Bucket) Counter(key string, delta, initial int64, expiry uint32) (uint64, uint64, error) {
	realInitial := uint64(0xFFFFFFFFFFFFFFFF)
	if initial > 0 {
		realInitial = uint64(initial)
	}

	if delta > 0 {
		return b.client.Increment([]byte(key), uint64(delta), realInitial, expiry)
	} else if delta < 0 {
		return b.client.Decrement([]byte(key), uint64(-delta), realInitial, expiry)
	} else {
		panic("Delta must be non-zero")
	}
}

// Performs a view query and returns a list of rows or an error.
func (b *Bucket) PerformViewQuery(queryObj ViewQuery) ([]interface{}, error) {
	return nil, nil
}

// Returns an interface allowing management operations to be performed on the bucket.
func (b *Bucket) Manager() *BucketManager {
	if b.manager == nil {
		b.manager = &BucketManager{}
	}
	return b.manager
}

func (b *Bucket) GetIoRouter() *CbIoRouter {
	return b.client
}
