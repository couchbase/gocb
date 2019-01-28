package gocb

import (
	"encoding/json"

	"github.com/pkg/errors"
	"gopkg.in/couchbase/gocbcore.v8"
)

// Result is the base type for the return types of operations
type Result struct {
	cas            Cas
	expiration     uint32
	withExpiration bool
}

// Cas returns the cas of the result.
func (d *Result) Cas() Cas {
	return d.cas
}

// HasExpiration verifies whether or not the result has an expiration value.
func (d *Result) HasExpiration() bool {
	return d.withExpiration
}

// Expiration returns the expiration value for the result.
func (d *Result) Expiration() uint32 {
	return d.expiration
}

// GetResult is the return type of Get operations.
type GetResult struct {
	Result
	flags    uint32
	contents []byte
}

// Content assigns the value of the result into the valuePtr using default decoding.
func (d *GetResult) Content(valuePtr interface{}) error {
	return DefaultDecode(d.contents, d.flags, valuePtr)
}

// Decode assigns the value of the result into the valuePtr using the decode function
// specified.
func (d *GetResult) Decode(valuePtr interface{}, decode Decode) error {
	if decode == nil {
		decode = DefaultDecode
	}
	return decode(d.contents, d.flags, valuePtr)
}

func (d *GetResult) fromSubDoc(ops []LookupInOp, result *LookupInResult, ignorePathErrors bool) error {
	content := make(map[string]interface{})
	if len(ops) == 1 && ops[0].op.Path == "" {
		// This is a special case where the subdoc was a sole fulldoc.
		d.contents = result.contents[0].data
		return nil
	}

	var errs projectionErrors
	for i, op := range ops {
		err := result.contents[i].err
		if err != nil {
			kvErr, ok := err.(kvError)
			if !ok {
				// this shouldn't happen, if it does then let's just bail.
				return err
			}

			if !(IsSubdocPathNotFoundError(err) && ignorePathErrors) {
				errs.errors = append(errs.errors, kvErr)
			}
		}
		parts := d.pathParts(op.op.Path)
		d.set(parts, content, result.contents[i].data)
	}

	if len(errs.errors) > 0 {
		return errs
	}

	bytes, err := json.Marshal(content)
	if err != nil {
		return errors.Wrap(err, "could not unmarshal result contents") // TODO
	}
	d.contents = bytes

	return nil
}

type subdocPath struct {
	path    string
	elem    int
	isArray bool
}

func (d *GetResult) pathParts(pathStr string) []subdocPath {
	pathLen := len(pathStr)
	var elemIdx int
	var i int
	var paths []subdocPath

	for i < pathLen {
		ch := pathStr[i]
		i++

		if ch == '[' {
			// opening of an array
			isArr := false
			arrayStart := i

			for i < pathLen {
				arrCh := pathStr[i]
				if arrCh == ']' {
					isArr = true
					i++
					break
				} else if arrCh == '.' {
					i++
					break
				}
				i++
			}

			if isArr {
				paths = append(paths, subdocPath{path: pathStr[elemIdx : arrayStart-1], isArray: true})
			} else {
				paths = append(paths, subdocPath{path: pathStr[elemIdx:i], isArray: false})
			}
			elemIdx = i

			if i < pathLen && pathStr[i] == '.' {
				i++
				elemIdx = i
			}
		} else if ch == '.' {
			paths = append(paths, subdocPath{path: pathStr[elemIdx : i-1]})
			elemIdx = i
		}
	}

	if elemIdx != i {
		// this should only ever be an object as an array would have ended in [...]
		paths = append(paths, subdocPath{path: pathStr[elemIdx:i]})
	}

	return paths
}

// thing,false , animals,true , anotherthing,false .thing

func (d *GetResult) set(paths []subdocPath, content interface{}, value interface{}) interface{} {
	path := paths[0]
	if len(paths) == 1 {
		if path.isArray {
			arr := make([]interface{}, 0)
			arr = append(arr, value)
			content.(map[string]interface{})[path.path] = arr
		} else {
			if _, ok := content.([]interface{}); ok {
				elem := make(map[string]interface{})
				elem[path.path] = value
				content = append(content.([]interface{}), elem)
			} else {
				content.(map[string]interface{})[path.path] = value
			}
		}
		return content
	}

	if path.isArray {
		// TODO: in the future consider an array of arrays
		if cMap, ok := content.(map[string]interface{}); ok {
			cMap[path.path] = make([]interface{}, 0)
			cMap[path.path] = d.set(paths[1:], cMap[path.path], value)
			return content
		}
	} else {
		if arr, ok := content.([]interface{}); ok {
			m := make(map[string]interface{})
			m[path.path] = make(map[string]interface{})
			content = append(arr, m)
			d.set(paths[1:], m[path.path], value)
			return content
		}
		cMap, ok := content.(map[string]interface{})
		if !ok {
			// this isn't possible but the linter won't play nice without it
		}
		cMap[path.path] = make(map[string]interface{})
		return d.set(paths[1:], cMap[path.path], value)
	}

	return content
}

// LookupInResult is the return type for LookupIn.
type LookupInResult struct {
	Result
	contents []lookupInPartial
	pathMap  map[string]int
}

type lookupInPartial struct {
	data json.RawMessage
	err  error
}

func (pr *lookupInPartial) as(valuePtr interface{}) error {
	if pr.err != nil {
		return pr.err
	}

	if valuePtr == nil {
		return nil
	}

	if valuePtr, ok := valuePtr.(*[]byte); ok {
		*valuePtr = pr.data
		return nil
	}

	return json.Unmarshal(pr.data, valuePtr)
}

func (pr *lookupInPartial) exists() bool {
	err := pr.as(nil)
	return err == nil
}

// ContentAt retrieves the value of the operation by its index. The index is the position of
// the operation as it was added to the builder.
func (lir *LookupInResult) ContentAt(idx int, valuePtr interface{}) error {
	return lir.contents[idx].as(valuePtr)
}

// Exists verifies that the item at idx exists.
func (lir *LookupInResult) Exists(idx int) bool {
	return lir.contents[idx].exists()
}

// ExistsResult is the return type of Exist operations.
type ExistsResult struct {
	Result
	keyState gocbcore.KeyState
}

// Exists returns whether or not the document exists.
func (d *ExistsResult) Exists() bool {
	return d.keyState != gocbcore.KeyStateNotFound && d.keyState != gocbcore.KeyStateDeleted
}

// MutationResult is the return type of any store related operations. It contains Cas and mutation tokens.
type MutationResult struct {
	Result
	mt MutationToken
}

// MutationToken returns the mutation token belonging to an operation.
func (mr MutationResult) MutationToken() MutationToken {
	return mr.mt
}

// MutateInResult is the return type of any mutate in related operations.
// It contains Cas, mutation tokens and any returned content.
type MutateInResult struct {
	MutationResult
	contents []mutateInPartial
}

type mutateInPartial struct {
	data json.RawMessage
}

func (pr *mutateInPartial) as(valuePtr interface{}) error {
	if valuePtr == nil {
		return nil
	}

	if valuePtr, ok := valuePtr.(*[]byte); ok {
		*valuePtr = pr.data
		return nil
	}

	return json.Unmarshal(pr.data, valuePtr)
}

// ContentAt retrieves the value of the operation by its index. The index is the position of
// the operation as it was added to the builder.
func (mir MutateInResult) ContentAt(idx int, valuePtr interface{}) error {
	return mir.contents[idx].as(valuePtr)
}

// CounterResult is the return type of counter operations.
type CounterResult struct {
	MutationResult
	content uint64
}

// MutationToken returns the mutation token belonging to an operation.
func (mr CounterResult) MutationToken() MutationToken {
	return mr.mt
}

// Cas returns the Cas value for a document following an operation.
func (mr CounterResult) Cas() Cas {
	return mr.cas
}

// Content returns the new value for the counter document.
func (mr CounterResult) Content() uint64 {
	return mr.content
}
