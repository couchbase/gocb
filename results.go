package gocb

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/couchbase/gocbcore/v8"
	"github.com/pkg/errors"
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
	transcoder Transcoder
	flags      uint32
	contents   []byte
}

// Content assigns the value of the result into the valuePtr using default decoding.
func (d *GetResult) Content(valuePtr interface{}) error {
	return d.transcoder.Decode(d.contents, d.flags, valuePtr)
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
	serializer Serializer
	contents   []lookupInPartial
	pathMap    map[string]int
}

type lookupInPartial struct {
	data json.RawMessage
	err  error
}

func (pr *lookupInPartial) as(valuePtr interface{}, serializer Serializer) error {
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

	return serializer.Deserialize(pr.data, valuePtr)
}

func (pr *lookupInPartial) exists() bool {
	err := pr.as(nil, nil)
	return err == nil
}

// ContentAt retrieves the value of the operation by its index. The index is the position of
// the operation as it was added to the builder.
func (lir *LookupInResult) ContentAt(idx int, valuePtr interface{}) error {
	if idx > len(lir.contents) {
		return errors.New("the supplied index was invalid")
	}
	return lir.contents[idx].as(valuePtr, lir.serializer)
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

type streamingResultCb func(decoder *json.Decoder, t json.Token) (rowsHit bool, err error)

type streamingResult struct {
	stream      io.ReadCloser
	closed      bool
	decoder     *json.Decoder
	allRowsRead bool
	hasRows     bool
	attributeCb streamingResultCb
}

func newStreamingResults(stream io.ReadCloser, attributeCb streamingResultCb) (*streamingResult, error) {
	dec := json.NewDecoder(stream)

	// read the opening { to prevent the decoder from trying to read the entire response into memory
	t, err := dec.Token()
	if err != nil {
		bodyErr := stream.Close()
		if bodyErr != nil {
			logDebugf("Failed to close socket (%s)", bodyErr.Error())
		}
		return nil, err
	}
	delim, ok := t.(json.Delim)
	if !ok {
		bodyErr := stream.Close()
		if bodyErr != nil {
			logDebugf("Failed to close socket (%s)", bodyErr.Error())
		}
		return nil, errors.New("could not read response body, no data found")
	}
	if delim != '{' {
		bodyErr := stream.Close()
		if bodyErr != nil {
			logDebugf("Failed to close socket (%s)", bodyErr.Error())
		}
		return nil, errors.New("could not read response body, opening token should have been { but found: " + string(delim))
	}

	return &streamingResult{
		decoder:     dec,
		stream:      stream,
		attributeCb: attributeCb,
	}, nil
}

func (r *streamingResult) NextBytes() ([]byte, error) {
	if !r.hasRows {
		return nil, nil
	}

	// At this point the decoder is in the results array
	if r.decoder.More() {
		var raw json.RawMessage
		err := r.decoder.Decode(&raw)
		if err != nil {
			return nil, errors.Wrap(err, "failed to decode result")
		}

		return raw, nil
	}

	// read the array close token to complete reading of the results
	t, err := r.decoder.Token()
	if err != nil {
		if err == io.EOF {
			return nil, nil
		}
		return nil, err
	}
	if delim, ok := t.(json.Delim); !ok || delim != ']' {
		return nil, fmt.Errorf("expected results closing token to be ] but was %s", string(t.(json.Delim)))
	}

	r.allRowsRead = true
	return nil, nil

}

func (r *streamingResult) Close() error {
	var finalReadErr error
	// if we haven't read all of the rows then we can't read the remaining attributes
	if r.allRowsRead {
		// don't just return error here, we need to close the stream first
		finalReadErr = r.readAttributes()
	}
	r.closed = true
	// We always need to close but we'll let any error from decoding take precedent
	if r.stream == nil {
		return finalReadErr
	}
	err := r.stream.Close()
	if finalReadErr != nil {
		return finalReadErr
	}
	return err
}

func (r *streamingResult) Closed() bool {
	return r.closed
}

func (r *streamingResult) HasRows() bool {
	return r.hasRows
}

func (r *streamingResult) Token() (json.Token, error) {
	if r.decoder.More() {
		t, err := r.decoder.Token()
		if err != nil {
			return nil, err
		}

		return t, nil
	}

	return nil, nil
}

func (r *streamingResult) readAttributes() error {
	for r.decoder.More() {
		t, err := r.decoder.Token()
		if err != nil {
			return err
		}

		rowsHit, err := r.attributeCb(r.decoder, t)
		if err != nil {
			return err
		}
		if rowsHit {
			r.hasRows = true
			return nil
		}
	}

	return nil
}

// GetReplicaResult is the return type of GetReplica operations.
type GetReplicaResult struct {
	GetResult
	isMaster bool
}

// IsMaster returns whether or not this result came from the active server.
func (r *GetReplicaResult) IsMaster() bool {
	return r.isMaster
}

// GetAllReplicasResult is the return type of GetAllReplica operations.
type GetAllReplicasResult struct {
	ctx         context.Context
	provider    kvProvider
	opts        gocbcore.GetOneReplicaOptions
	err         error
	closed      bool
	cancel      context.CancelFunc
	maxReplicas int
	transcoder  Transcoder
}

// Next fetches the new replica.
func (r *GetAllReplicasResult) Next(valuePtr *GetReplicaResult) bool {
	if r.err != nil || r.closed {
		return false
	}

	if r.opts.ReplicaIdx > r.maxReplicas {
		r.closed = true
		return false
	}

	waitCh := make(chan bool)
	var op gocbcore.PendingOp
	var err error
	if r.opts.ReplicaIdx == 0 {
		op, err = r.provider.GetEx(gocbcore.GetOptions{
			Key:            r.opts.Key,
			CollectionName: r.opts.CollectionName,
			ScopeName:      r.opts.ScopeName,
		}, func(res *gocbcore.GetResult, err error) {
			if err != nil {
				r.err = maybeEnhanceKVErr(err, string(r.opts.Key), false)
				waitCh <- false
				return
			}
			*valuePtr = GetReplicaResult{
				GetResult: GetResult{
					Result: Result{
						cas: Cas(res.Cas),
					},
					transcoder: r.transcoder,
					contents:   res.Value,
					flags:      res.Flags,
				},
				isMaster: true,
			}
			waitCh <- true
		})
	} else {
		op, err = r.provider.GetOneReplicaEx(r.opts, func(res *gocbcore.GetReplicaResult, err error) {
			if err != nil {
				r.err = maybeEnhanceKVErr(err, string(r.opts.Key), false)
				waitCh <- false
				return
			}
			*valuePtr = GetReplicaResult{
				GetResult: GetResult{
					Result: Result{
						cas: Cas(res.Cas),
					},
					transcoder: r.transcoder,
					contents:   res.Value,
					flags:      res.Flags,
				},
			}
			waitCh <- true
		})
	}
	if err != nil {
		r.err = err
		return false
	}
	r.opts.ReplicaIdx++

	select {
	case <-r.ctx.Done():
		if op.Cancel() {
			ctxErr := r.ctx.Err()
			if ctxErr == context.DeadlineExceeded {
				r.err = timeoutError{}
			} else {
				r.err = ctxErr
			}
			return false
		}

		return <-waitCh
	case success := <-waitCh:
		return success
	}
}

// Close ends the stream and returns any errors that occurred.
func (r *GetAllReplicasResult) Close() error {
	if r.cancel != nil {
		r.cancel()
	}
	r.closed = true
	return r.err
}
