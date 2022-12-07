package gocb

import (
	"context"
	"encoding/hex"
	"errors"
	"io"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/couchbase/gocbcore/v10"
)

const (
	rangeScanDefaultItemLimit  = 50
	rangeScanDefaultBytesLimit = 15000
)

type rangeScanOpManager struct {
	err error
	ctx context.Context

	span          RequestSpan
	transcoder    Transcoder
	timeout       time.Duration
	deadline      time.Time
	retryStrategy *retryStrategyWrapper
	impersonate   string

	cancelCh chan struct{}
	dataCh   chan *ScanResultItem
	streams  map[uint16]*rangeScanStream

	agent       kvProvider
	createdTime time.Time
	meter       *meterWrapper
	tracer      RequestTracer

	defaultRetryStrategy *retryStrategyWrapper
	defaultTranscoder    Transcoder
	defaultTimeout       time.Duration

	collectionName string
	scopeName      string
	bucketName     string

	rangeOptions          *gocbcore.RangeScanCreateRangeScanConfig
	samplingOptions       *gocbcore.RangeScanCreateRandomSamplingConfig
	vBucketToSnapshotOpts map[uint16]gocbcore.RangeScanCreateSnapshotRequirements

	numVbuckets int
	keysOnly    bool
	sort        ScanSort
	itemLimit   uint32
	byteLimit   uint32

	result *ScanResult

	cancelled uint32
}

func (m *rangeScanOpManager) getTimeout() time.Duration {
	if m.timeout > 0 {
		return m.timeout
	}

	return m.defaultTimeout
}

func (m *rangeScanOpManager) SetTimeout(timeout time.Duration) {
	m.timeout = timeout
}

func (m *rangeScanOpManager) SetItemLimit(limit uint32) {
	if limit == 0 {
		limit = rangeScanDefaultItemLimit
	}
	m.itemLimit = limit
}

func (m *rangeScanOpManager) SetByteLimit(limit uint32) {
	if limit == 0 {
		limit = rangeScanDefaultBytesLimit
	}
	m.byteLimit = limit
}

func (m *rangeScanOpManager) SetResult(result *ScanResult) {
	m.result = result
}

func (m *rangeScanOpManager) SetTranscoder(transcoder Transcoder) {
	if transcoder == nil {
		transcoder = m.defaultTranscoder
	}
	m.transcoder = transcoder
}

func (m *rangeScanOpManager) SetRetryStrategy(retryStrategy RetryStrategy) {
	wrapper := m.defaultRetryStrategy
	if retryStrategy != nil {
		wrapper = newRetryStrategyWrapper(retryStrategy)
	}
	m.retryStrategy = wrapper
}

func (m *rangeScanOpManager) SetImpersonate(user string) {
	m.impersonate = user
}

func (m *rangeScanOpManager) SetContext(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}
	m.ctx = ctx
}

func (m *rangeScanOpManager) Finish() {
	m.span.End()

	m.meter.ValueRecord(meterValueServiceKV, "range_scan", m.createdTime)
}

func (m *rangeScanOpManager) TraceSpanContext() RequestSpanContext {
	return m.span.Context()
}

func (m *rangeScanOpManager) TraceSpan() RequestSpan {
	return m.span
}

func (m *rangeScanOpManager) CollectionName() string {
	return m.collectionName
}

func (m *rangeScanOpManager) ScopeName() string {
	return m.scopeName
}

func (m *rangeScanOpManager) BucketName() string {
	return m.bucketName
}

func (m *rangeScanOpManager) Transcoder() Transcoder {
	return m.transcoder
}

func (m *rangeScanOpManager) RangeOptions() *gocbcore.RangeScanCreateRangeScanConfig {
	return m.rangeOptions
}

func (m *rangeScanOpManager) SamplingOptions() *gocbcore.RangeScanCreateRandomSamplingConfig {
	return m.samplingOptions
}

func (m *rangeScanOpManager) SnapshotOptions(vbID uint16) *gocbcore.RangeScanCreateSnapshotRequirements {
	opts, ok := m.vBucketToSnapshotOpts[vbID]
	if !ok {
		return nil
	}

	return &opts
}

func (m *rangeScanOpManager) KeysOnly() bool {
	return m.keysOnly
}

func (m *rangeScanOpManager) CheckReadyForOp() error {
	if m.err != nil {
		return m.err
	}

	timeout := m.getTimeout()
	if timeout == 0 {
		return errors.New("range scan op manager had no timeout specified")
	}

	m.deadline = time.Now().Add(timeout)

	return nil
}

func (m *rangeScanOpManager) EnhanceErr(err error) error {
	return maybeEnhanceKVErr(err, m.bucketName, m.scopeName, m.collectionName, "scan")
}

func (m *rangeScanOpManager) Deadline() time.Time {
	return m.deadline
}

func (m *rangeScanOpManager) Timeout() time.Duration {
	return m.getTimeout()
}

func (m *rangeScanOpManager) RetryStrategy() *retryStrategyWrapper {
	return m.retryStrategy
}

func (m *rangeScanOpManager) Impersonate() string {
	return m.impersonate
}

func (m *rangeScanOpManager) Context() context.Context {
	return m.ctx
}

// Cancel will trigger all underlying streams to cancel themselves, the read loop
// inside of Scan will handle calling Finish on the span and tidying up.
func (m *rangeScanOpManager) Cancel() {
	m.cancel(ErrRequestCanceled)
}

func (m *rangeScanOpManager) cancel(err error) {
	if atomic.CompareAndSwapUint32(&m.cancelled, 0, 1) {
		m.result.setErr(err)
		close(m.cancelCh)
	}
}

func (m *rangeScanOpManager) DataCh() chan *ScanResultItem {
	return m.dataCh
}

func (m *rangeScanOpManager) getNextItemSorted() *ScanResultItem {
	var lowestKey string
	var lowestVbID uint16
	for vbID, stream := range m.streams {
		peeked := stream.Peek()
		if peeked == nil {
			delete(m.streams, vbID)
			continue
		}

		if lowestKey == "" || peeked.id < lowestKey {
			lowestKey = peeked.id
			lowestVbID = vbID
		}
	}

	if lowestKey == "" {
		return nil
	}

	return m.streams[lowestVbID].Take()
}

func (m *rangeScanOpManager) getNextItem() *ScanResultItem {
	for vbID, stream := range m.streams {
		peeked := stream.Peek()
		if peeked == nil {
			delete(m.streams, vbID)
			continue
		}

		return stream.Take()
	}

	return nil
}

func (m *rangeScanOpManager) Scan() (*ScanResult, error) {
	var limit uint64
	if m.SamplingOptions() != nil {
		limit = m.SamplingOptions().Samples
	}
	var numItems uint64

	// Keep a track of the result object so that we can tell it any errors that occur.
	r := &ScanResult{
		resultChan: m.DataCh(),
		cancelFn:   m.Cancel,
	}
	m.SetResult(r)

	for vbucket := 0; vbucket < m.numVbuckets; vbucket++ {
		stream := m.newRangeScanStream(uint16(vbucket))
		m.streams[uint16(vbucket)] = stream
		go func(stream *rangeScanStream) {
			// This may seem a little unusual but calling end only once scan has returned allows us to
			// avoid a lot of races that would otherwise be an issue.
			stream.Scan()
			stream.End()
		}(stream)
	}

	for _, stream := range m.streams {
		select {
		case <-stream.createdCh:
			// This stream is ready to go.
		case <-m.cancelCh:
			return nil, m.result.Err()
		}
	}

	go func() {
		for {
			var item *ScanResultItem
			if m.sort == ScanSortNone {
				item = m.getNextItem()
			} else {
				item = m.getNextItemSorted()
			}
			// If we're doing a sampling scan then we need to only write data into the channel
			// if we haven't seen the number of items that the user requested. Otherwise
			// we need to cancel the streams and iterate over them until they close.
			if item != nil && (limit == 0 || numItems < limit) {
				numItems++
				m.dataCh <- item
			}
			if limit > 0 && numItems == limit {
				m.cancel(nil)
			}

			if len(m.streams) == 0 {
				m.Finish()
				close(m.dataCh)
				return
			}
		}
	}()

	return r, nil
}

func (c *Collection) newRangeScanOpManager(scanType ScanType, numVbuckets int, agent kvProvider,
	parentSpan RequestSpan, consistentWith *MutationState, keysOnly bool, sort ScanSort) (*rangeScanOpManager, error) {
	var tracectx RequestSpanContext
	if parentSpan != nil {
		tracectx = parentSpan.Context()
	}

	span := c.tracer.RequestSpan(tracectx, "range_scan")
	span.SetAttribute(spanAttribDBNameKey, c.bucket.Name())
	span.SetAttribute(spanAttribDBCollectionNameKey, c.Name())
	span.SetAttribute(spanAttribDBScopeNameKey, c.ScopeName())
	span.SetAttribute(spanAttribServiceKey, "kv_scan")
	span.SetAttribute(spanAttribOperationKey, "range_scan")
	span.SetAttribute(spanAttribDBSystemKey, spanAttribDBSystemValue)
	span.SetAttribute("num_partitions", numVbuckets)
	span.SetAttribute("without_content", keysOnly)

	var rangeOptions *gocbcore.RangeScanCreateRangeScanConfig
	var samplingOptions *gocbcore.RangeScanCreateRandomSamplingConfig

	setRangeScanOpts := func(st RangeScan) error {
		if st.To == nil {
			st.To = ScanTermMaximum()
		}
		if st.From == nil {
			st.From = ScanTermMinimum()
		}

		span.SetAttribute("scan_type", "range")
		span.SetAttribute("from_term", st.From.Term)
		span.SetAttribute("to_term", st.To.Term)
		var err error
		rangeOptions, err = st.toCore()
		if err != nil {
			return err
		}

		return nil
	}

	setSamplingScanOpts := func(st SamplingScan) error {
		if st.Seed == 0 {
			st.Seed = rand.Uint64() // #nosec G404
		}
		span.SetAttribute("scan_type", "sampling")
		span.SetAttribute("limit", st.Limit)
		span.SetAttribute("seed", st.Seed)
		var err error
		samplingOptions, err = st.toCore()
		if err != nil {
			return err
		}

		return nil
	}

	var err error
	switch st := scanType.(type) {
	case RangeScan:
		if err := setRangeScanOpts(st); err != nil {
			return nil, err
		}
	case *RangeScan:
		if err := setRangeScanOpts(*st); err != nil {
			return nil, err
		}
	case SamplingScan:
		if err := setSamplingScanOpts(st); err != nil {
			return nil, err
		}
	case *SamplingScan:
		if err := setSamplingScanOpts(*st); err != nil {
			return nil, err
		}
	default:
		err = makeInvalidArgumentsError("only RangeScan and SamplingScan are supported for ScanType")
	}

	vBucketToSnapshotOpts := make(map[uint16]gocbcore.RangeScanCreateSnapshotRequirements)
	if consistentWith != nil {
		for _, token := range consistentWith.tokens {
			vBucketToSnapshotOpts[uint16(token.PartitionID())] = gocbcore.RangeScanCreateSnapshotRequirements{
				VbUUID: gocbcore.VbUUID(token.PartitionUUID()),
				SeqNo:  gocbcore.SeqNo(token.SequenceNumber()),
			}
		}
	}

	m := &rangeScanOpManager{
		err: err,

		span:        span,
		createdTime: time.Now(),
		meter:       c.meter,
		tracer:      c.tracer,

		dataCh:   make(chan *ScanResultItem),
		cancelCh: make(chan struct{}),
		streams:  make(map[uint16]*rangeScanStream, numVbuckets),

		numVbuckets:          numVbuckets,
		agent:                agent,
		defaultTimeout:       c.timeoutsConfig.KVScanTimeout,
		defaultTranscoder:    c.transcoder,
		defaultRetryStrategy: c.retryStrategyWrapper,
		collectionName:       c.Name(),
		scopeName:            c.ScopeName(),
		bucketName:           c.Bucket().Name(),

		rangeOptions:          rangeOptions,
		samplingOptions:       samplingOptions,
		vBucketToSnapshotOpts: vBucketToSnapshotOpts,
		keysOnly:              keysOnly,
		sort:                  sort,
	}

	return m, nil
}

type rangeScanStream struct {
	// This is a bit lazy, but it saves us copying all the information into 1024 more places.
	opm       *rangeScanOpManager
	buffer    chan ScanResultItem
	vbID      uint16
	peeked    *ScanResultItem
	span      RequestSpan
	createdCh chan struct{}
}

func (m *rangeScanOpManager) newRangeScanStream(vbID uint16) *rangeScanStream {
	span := m.tracer.RequestSpan(m.span.Context(), "range_scan_partition")
	span.SetAttribute("partition_id", vbID)

	return &rangeScanStream{
		opm:       m,
		buffer:    make(chan ScanResultItem),
		vbID:      vbID,
		span:      span,
		createdCh: make(chan struct{}),
	}
}

func (rss *rangeScanStream) Take() *ScanResultItem {
	peeked := rss.peeked
	rss.peeked = nil
	return peeked
}

func (rss *rangeScanStream) Peek() *ScanResultItem {
	if rss.peeked != nil {
		return rss.peeked
	}

	select {
	case peeked, hasMore := <-rss.buffer:
		if !hasMore {
			return nil
		}
		rss.peeked = &peeked
		return rss.peeked
	case <-rss.opm.cancelCh:
		return nil
	}
}

func (rss *rangeScanStream) End() {
	rss.span.End()
	close(rss.buffer)
}

func (rss *rangeScanStream) Scan() {
	var lastTermSeen []byte
	rangeOpts := rss.opm.RangeOptions()
	samplingOpts := rss.opm.SamplingOptions()
	ctx := rss.opm.Context()
	var firstCreateDone bool
	for {
		if rangeOpts != nil && len(lastTermSeen) > 0 {
			rangeOpts.Start = lastTermSeen
		}

		scanUUID, err := rss.create(ctx, rangeOpts, samplingOpts)
		if err != nil {
			err = rss.opm.EnhanceErr(err)
			if errors.Is(err, gocbcore.ErrDocumentNotFound) {
				if !firstCreateDone {
					close(rss.createdCh)
				}
				logDebugf("Ignoring vbid %d as no documents exist for that vbucket", rss.vbID)
				return
			}

			// We only signal to cancel the entire stream if this is a range scan.
			if rangeOpts == nil {
				if !firstCreateDone {
					close(rss.createdCh)
				}
			} else {
				// We don't close the created channel here, because we don't want to signal a successful create
				// call to the stream manager.
				rss.opm.cancel(err)
			}
			return
		}
		if !firstCreateDone {
			close(rss.createdCh)
		}
		firstCreateDone = true

		// We only apply context to the initial create stream request, after that we consider the stream active
		// and context cancellation no longer applies.
		ctx = context.Background()

		// We've created the stream so now loop continue until the stream is complete or cancelled.
		for {
			items, isComplete, err := rss.scanContinue(scanUUID)
			if err != nil {
				err = rss.opm.EnhanceErr(err)
				// If the error is NMV or EOF then we should recreate the stream from the last known item.
				// Breaking here without calling cancel will trigger us to reloop rather than call Cancel on
				// the stream and then return.
				if errors.Is(err, gocbcore.ErrNotMyVBucket) || errors.Is(err, io.EOF) {
					break
				}

				rss.opm.cancel(err)
				break
			}

			if len(items) > 0 {
				for _, item := range items {
					var expiry time.Time
					if item.Expiry > 0 {
						expiry = time.Unix(int64(item.Expiry), 0)
					}
					select {
					case <-rss.opm.cancelCh:
						if !isComplete {
							rss.cancel(scanUUID)
						}
						return
					case rss.buffer <- ScanResultItem{
						Result: Result{
							cas: Cas(item.Cas),
						},
						transcoder: rss.opm.Transcoder(),
						id:         string(item.Key),
						flags:      item.Flags,
						contents:   item.Value,
						expiryTime: expiry,
						keysOnly:   rss.opm.KeysOnly(),
					}:
					}
				}

				lastTermSeen = items[len(items)-1].Key
			}

			if isComplete {
				return
			}
		}

		select {
		case <-rss.opm.cancelCh:
			rss.cancel(scanUUID)
			return
		default:
		}
	}
}

func (rss *rangeScanStream) create(ctx context.Context, rangeOpts *gocbcore.RangeScanCreateRangeScanConfig,
	samplingOpts *gocbcore.RangeScanCreateRandomSamplingConfig) (uuidOut []byte, errOut error) {
	span := rss.opm.tracer.RequestSpan(rss.span.Context(), "range_scan_create")
	defer span.End()
	span.SetAttribute("without_content", rss.opm.KeysOnly())
	if samplingOpts != nil {
		span.SetAttribute("scan_type", "sampling")
		span.SetAttribute("limit", samplingOpts.Samples)
		span.SetAttribute("seed", samplingOpts.Seed)
	} else if rangeOpts != nil {
		span.SetAttribute("scan_type", "range")
		span.SetAttribute("from_term", string(rangeOpts.Start))
		span.SetAttribute("to_term", string(rangeOpts.End))
		span.SetAttribute("from_exclusive", len(rangeOpts.ExclusiveStart) > 0)
		span.SetAttribute("to_exclusive", len(rangeOpts.ExclusiveEnd) > 0)
	}

	opMan := newAsyncOpManager(ctx)
	opMan.SetCancelCh(rss.opm.cancelCh)

	err := opMan.Wait(rss.opm.agent.RangeScanCreate(rss.vbID, gocbcore.RangeScanCreateOptions{
		RetryStrategy:  rss.opm.RetryStrategy(),
		Deadline:       time.Now().Add(rss.opm.Timeout()),
		CollectionName: rss.opm.CollectionName(),
		ScopeName:      rss.opm.ScopeName(),
		KeysOnly:       rss.opm.KeysOnly(),
		Range:          rangeOpts,
		Sampling:       samplingOpts,
		Snapshot:       rss.opm.SnapshotOptions(rss.vbID),
		User:           rss.opm.Impersonate(),
		TraceContext:   span.Context(),
	}, func(result *gocbcore.RangeScanCreateResult, err error) {
		if err != nil {
			errOut = err
			opMan.Reject()
			return
		}

		uuidOut = result.ScanUUUID
		opMan.Resolve()
	}))
	if err != nil {
		errOut = err
	}

	return
}

func (rss *rangeScanStream) scanContinue(scanUUID []byte) (itemsOut []gocbcore.RangeScanItem, completeOut bool, errOut error) {
	span := rss.opm.tracer.RequestSpan(rss.span.Context(), "range_scan_continue")
	defer span.End()

	span.SetAttribute("item_limit", rss.opm.itemLimit)
	span.SetAttribute("byte_limit", rss.opm.byteLimit)
	span.SetAttribute("time_limit", 0)

	opm := newAsyncOpManager(context.Background())
	opm.SetCancelCh(rss.opm.cancelCh)

	var items []gocbcore.RangeScanItem
	span.SetAttribute("range_scan_id", "0x"+hex.EncodeToString(scanUUID))

	err := opm.Wait(rss.opm.agent.RangeScanContinue(scanUUID, rss.vbID, gocbcore.RangeScanContinueOptions{
		RetryStrategy: rss.opm.RetryStrategy(),
		User:          rss.opm.Impersonate(),
		TraceContext:  span.Context(),
		MaxCount:      rss.opm.itemLimit,
		MaxBytes:      rss.opm.byteLimit,
	}, func(coreItems []gocbcore.RangeScanItem) {
		items = append(items, coreItems...)
	}, func(result *gocbcore.RangeScanContinueResult, err error) {
		if err != nil {
			errOut = err
			opm.Reject()
			return
		}

		itemsOut = items
		if result.Complete {
			completeOut = true
			opm.Resolve()
			return
		}
		if result.More {
			opm.Resolve()
			return
		}
		logInfof("Received a range scan action that did not meet what we expected")
		opm.Resolve()
	}))
	if err != nil {
		errOut = err
	}

	return
}

func (rss *rangeScanStream) cancel(scanUUID []byte) {
	opMan := newAsyncOpManager(context.Background())
	span := rss.opm.tracer.RequestSpan(rss.span.Context(), "range_scan_cancel")
	defer span.End()

	span.SetAttribute("range_scan_id", "0x"+hex.EncodeToString(scanUUID))

	err := opMan.Wait(rss.opm.agent.RangeScanCancel(scanUUID, rss.vbID, gocbcore.RangeScanCancelOptions{
		RetryStrategy: rss.opm.RetryStrategy(),
		Deadline:      time.Now().Add(rss.opm.Timeout()),
		User:          rss.opm.Impersonate(),
		TraceContext:  rss.span.Context(),
	}, func(result *gocbcore.RangeScanCancelResult, err error) {
		if err != nil {
			logDebugf("Failed to cancel scan 0x%s: %v", hex.EncodeToString(scanUUID), err)
			opMan.Reject()
			return
		}

		opMan.Resolve()
	}))
	if err != nil {
		return
	}
}
