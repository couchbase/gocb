package gocb

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"github.com/couchbase/gocbcore/v10"
	"github.com/stretchr/testify/mock"
)

type mockConfigSnapshotProvider struct {
	snapshot *mockConfigSnapshot
}

func (p *mockConfigSnapshotProvider) WaitForConfigSnapshot(ctx context.Context, deadline time.Time) (coreConfigSnapshot, error) {
	return p.snapshot, nil
}

type mockConfigSnapshot struct {
	revID            int64
	numVbuckets      int
	numReplicas      int
	serverToVbuckets map[int][]uint16
}

func (p *mockConfigSnapshot) RevID() int64 {
	return p.revID
}

func (p *mockConfigSnapshot) NumVbuckets() (int, error) {
	return p.numVbuckets, nil
}

func (p *mockConfigSnapshot) NumReplicas() (int, error) {
	return p.numReplicas, nil
}

func (p *mockConfigSnapshot) NumServers() (int, error) {
	return len(p.serverToVbuckets), nil
}

func (p *mockConfigSnapshot) VbucketsOnServer(index int) ([]uint16, error) {
	vbuckets, ok := p.serverToVbuckets[index]
	if !ok {
		return nil, errors.New(fmt.Sprintf("could not find server with index %d", index))
	}

	return vbuckets, nil
}

func newMockConfigSnapshot(numVbuckets int, numServers int) *mockConfigSnapshot {
	serverToVbuckets := make(map[int][]uint16)

	// Divide the vbuckets across the nodes
	remainingVbucketCount := numVbuckets
	for serverIndex := 0; serverIndex < numServers; serverIndex++ {
		lowerBound := numVbuckets - remainingVbucketCount
		upperBound := lowerBound + remainingVbucketCount/(numServers-serverIndex)
		for vbID := uint16(lowerBound); vbID < uint16(upperBound); vbID++ {
			remainingVbucketCount--
			serverToVbuckets[serverIndex] = append(serverToVbuckets[serverIndex], vbID)
		}
	}
	return &mockConfigSnapshot{
		numVbuckets:      numVbuckets,
		serverToVbuckets: serverToVbuckets,
	}
}

func (suite *UnitTestSuite) TestScanAllScansTmpFailAtCreate() {
	test := func(scan ScanType) (*ScanResult, error) {
		start := time.Now()

		opts := &ScanOptions{
			IDsOnly: true,
			Timeout: 1 * time.Millisecond,
		}

		provider := makeRangeScanProvider(func(args mock.Arguments) {
			cb := args.Get(2).(gocbcore.RangeScanCreateCallback)

			if time.Now().Sub(start) > opts.Timeout {
				cb(nil, ErrTimeout)
				return
			}
			cb(nil, ErrTemporaryFailure)
		})

		snap := newMockConfigSnapshot(8, 1)

		agent := &kvProviderCore{agent: provider, snapshotProvider: &mockConfigSnapshotProvider{snapshot: snap}}
		col := suite.collection("mock", "", "", agent)

		return agent.Scan(col, scan, opts)
	}

	suite.Run("Sampling", func() {
		scan := SamplingScan{
			Limit: 10,
			Seed:  5,
		}

		res, err := test(scan)
		suite.Require().NoError(err)

		ids := suite.iterateRangeScan(res)

		suite.Assert().Empty(ids)
		suite.Require().NoError(res.Err())
	})

	suite.Run("Range", func() {
		scan := NewRangeScanForPrefix("hi")

		_, err := test(scan)
		suite.Require().ErrorIs(err, ErrTimeout)
	})
}

func (suite *UnitTestSuite) TestScanAllScansEmpty() {
	test := func(scan ScanType) {
		provider := new(mockKvProviderCoreProvider)
		provider.
			On(
				"RangeScanCreate",
				mock.AnythingOfType("uint16"),
				mock.AnythingOfType("gocbcore.RangeScanCreateOptions"),
				mock.AnythingOfType("gocbcore.RangeScanCreateCallback"),
			).
			Run(func(args mock.Arguments) {
				cb := args.Get(2).(gocbcore.RangeScanCreateCallback)

				cb(nil, gocbcore.ErrDocumentNotFound)
			}).
			Return(new(mockPendingOp), nil)

		opts := &ScanOptions{
			IDsOnly: true,
		}

		snap := newMockConfigSnapshot(8, 1)

		agent := &kvProviderCore{agent: provider, snapshotProvider: &mockConfigSnapshotProvider{snapshot: snap}}
		col := suite.collection("mock", "", "", agent)

		res, err := agent.Scan(col, scan, opts)
		suite.Require().Nil(err, err)

		ids := make(map[string]struct{})
		for {
			d := res.Next()
			if d == nil {
				break
			}

			ids[d.ID()] = struct{}{}
		}

		suite.Assert().Empty(ids)
		suite.Require().Nil(res.Err())
	}

	suite.Run("Sampling", func() {
		scan := SamplingScan{
			Limit: 10,
			Seed:  5,
		}

		test(scan)
	})

	suite.Run("Range", func() {
		scan := NewRangeScanForPrefix("hi")

		test(scan)
	})
}

type rangeScanCreateResult struct {
	keysOnly bool
	items    []gocbcore.RangeScanItem

	continueRunFunc func(gocbcore.RangeScanContinueDataCallback, gocbcore.RangeScanContinueActionCallback)

	wasCancelled bool
}

func (r *rangeScanCreateResult) ScanUUID() []byte {
	return []byte("scanuuid")
}

func (r *rangeScanCreateResult) KeysOnly() bool {
	return r.keysOnly
}

func (r *rangeScanCreateResult) RangeScanContinue(opts gocbcore.RangeScanContinueOptions, dataCb gocbcore.RangeScanContinueDataCallback, actionCb gocbcore.RangeScanContinueActionCallback) (gocbcore.PendingOp, error) {
	time.AfterFunc(1*time.Microsecond, func() {
		if r.continueRunFunc == nil {
			dataCb(r.items)
			actionCb(&gocbcore.RangeScanContinueResult{
				Complete: true,
			}, nil)
		} else {
			r.continueRunFunc(dataCb, actionCb)
		}
	})

	return &mockPendingOp{}, nil
}

func (r *rangeScanCreateResult) RangeScanCancel(opts gocbcore.RangeScanCancelOptions, cb gocbcore.RangeScanCancelCallback) (gocbcore.PendingOp, error) {
	time.AfterFunc(1*time.Microsecond, func() {
		r.wasCancelled = true
		cb(&gocbcore.RangeScanCancelResult{}, nil)
	})

	return &mockPendingOp{}, nil
}

func (suite *UnitTestSuite) TestScanFirstCreateFailsUnknownError() {
	expectedErr := errors.New("unknown error")
	test := func(scan ScanType) (*ScanResult, error) {
		var calls uint32
		provider := makeRangeScanProvider(func(args mock.Arguments) {
			cb := args.Get(2).(gocbcore.RangeScanCreateCallback)

			if atomic.AddUint32(&calls, 1) == 1 {
				cb(nil, expectedErr)
				return
			}

			cb(&rangeScanCreateResult{
				items: []gocbcore.RangeScanItem{
					{
						Key: []byte(uuid.NewString()[:6]),
					},
				},
			}, nil)
		})

		snap := newMockConfigSnapshot(4, 1)

		agent := &kvProviderCore{agent: provider, snapshotProvider: &mockConfigSnapshotProvider{snapshot: snap}}
		col := suite.collection("mock", "", "", agent)

		opts := &ScanOptions{
			IDsOnly: true,
		}

		return agent.Scan(col, scan, opts)
	}

	suite.Run("Sampling", func() {
		scan := SamplingScan{
			Limit: 10,
			Seed:  5,
		}

		res, err := test(scan)
		suite.Require().NoError(err)

		ids := suite.iterateRangeScan(res)

		suite.Assert().Len(ids, 3)
		suite.Require().NoError(res.Err())
	})

	suite.Run("Range", func() {
		scan := NewRangeScanForPrefix("hi")

		_, err := test(scan)
		suite.Require().ErrorIs(err, expectedErr)
	})
}

func (suite *UnitTestSuite) TestScanSecondCreateFailsUnknownError() {
	expectedErr := errors.New("unknown error")
	test := func(scan ScanType) (*ScanResult, error) {
		var calls uint32
		provider := makeRangeScanProvider(func(args mock.Arguments) {
			cb := args.Get(2).(gocbcore.RangeScanCreateCallback)

			if atomic.AddUint32(&calls, 1) == 2 {
				cb(nil, expectedErr)
				return
			}

			cb(&rangeScanCreateResult{
				items: []gocbcore.RangeScanItem{
					{
						Key: []byte(uuid.NewString()[:6]),
					},
				},
			}, nil)
			return
		})

		snap := newMockConfigSnapshot(4, 1)

		agent := &kvProviderCore{agent: provider, snapshotProvider: &mockConfigSnapshotProvider{snapshot: snap}}
		col := suite.collection("mock", "", "", agent)

		opts := &ScanOptions{
			IDsOnly: true,
		}

		return agent.Scan(col, scan, opts)
	}

	suite.Run("Sampling", func() {
		scan := SamplingScan{
			Limit: 10,
			Seed:  5,
		}

		res, err := test(scan)
		suite.Require().NoError(err)

		ids := suite.iterateRangeScan(res)

		suite.Assert().Len(ids, 3)
		suite.Require().NoError(res.Err())
	})

	suite.Run("Range", func() {
		scan := NewRangeScanForPrefix("hi")

		res, err := test(scan)
		suite.Require().NoError(err)

		ids := suite.iterateRangeScan(res)

		suite.Assert().Len(ids, 1)
		suite.Require().ErrorIs(expectedErr, res.Err())
	})
}

func (suite *UnitTestSuite) TestScanNMV() {
	lastTermSeen := "lasttermseen"
	keyAfter := "keyafter"
	test := func(scan ScanType) (map[string]struct{}, string, error) {
		var firstTermAfter string
		var calls uint32
		var vbID int16 = -1
		var createCalls uint32
		provider := makeRangeScanProvider(func(args mock.Arguments) {
			thisVBID := int16(args.Get(0).(uint16))
			opts := args.Get(1).(gocbcore.RangeScanCreateOptions)
			cb := args.Get(2).(gocbcore.RangeScanCreateCallback)

			if atomic.AddUint32(&calls, 1) == 2 || thisVBID == vbID {
				vbID = thisVBID
				if createCalls == 0 {
					createCalls++
					var contCalls uint32
					cb(&rangeScanCreateResult{
						continueRunFunc: func(dataCb gocbcore.RangeScanContinueDataCallback, actionCb gocbcore.RangeScanContinueActionCallback) {
							contCalls++
							if contCalls == 1 {
								dataCb([]gocbcore.RangeScanItem{
									{
										Key: []byte(uuid.NewString()[:6]),
									},
									{
										Key: []byte(lastTermSeen),
									},
								})
								actionCb(&gocbcore.RangeScanContinueResult{
									More: true,
								}, nil)
							} else {
								actionCb(nil, gocbcore.ErrNotMyVBucket)
							}
						},
					}, nil)
				} else {
					if opts.Range != nil {
						firstTermAfter = string(opts.Range.Start)
					}
					cb(&rangeScanCreateResult{
						continueRunFunc: func(dataCb gocbcore.RangeScanContinueDataCallback, actionCb gocbcore.RangeScanContinueActionCallback) {
							dataCb([]gocbcore.RangeScanItem{
								{
									Key: []byte(keyAfter),
								},
							})
							actionCb(&gocbcore.RangeScanContinueResult{
								Complete: true,
							}, nil)
						},
					}, nil)
				}
				return
			}

			cb(&rangeScanCreateResult{
				items: []gocbcore.RangeScanItem{
					{
						Key: []byte(uuid.NewString()[:6]),
					},
				},
			}, nil)
			return
		})

		snap := newMockConfigSnapshot(4, 1)

		agent := &kvProviderCore{agent: provider, snapshotProvider: &mockConfigSnapshotProvider{snapshot: snap}}
		col := suite.collection("mock", "", "", agent)

		opts := &ScanOptions{
			IDsOnly: true,
		}

		res, err := agent.Scan(col, scan, opts)
		if err != nil {
			return nil, "", err
		}

		ids := suite.iterateRangeScan(res)

		return ids, firstTermAfter, res.Err()
	}

	suite.Run("Sampling", func() {
		scan := SamplingScan{
			Limit: 10,
			Seed:  5,
		}

		ids, _, err := test(scan)
		suite.Require().NoError(err)

		// 3 vbuckets will yield 1 item,  the other will yield 2 items on the first continue, then 1 further on the
		// continue after the NMV.
		suite.Assert().Len(ids, 6)
	})

	suite.Run("Range", func() {
		scan := NewRangeScanForPrefix("hi")

		ids, firstTermAfter, err := test(scan)
		suite.Require().NoError(err)

		// 3 vbuckets will yield 1 item,  the other will yield 2 items on the first continue, then 1 further on the
		// continue after the NMV.
		suite.Assert().Len(ids, 6)
		// A NMV error should trigger a new create with the start being the last term seen.
		suite.Assert().Equal(lastTermSeen, firstTermAfter)
	})
}

func (suite *UnitTestSuite) iterateRangeScan(res *ScanResult) map[string]struct{} {
	ids := make(map[string]struct{})
	for {
		d := res.Next()
		if d == nil {
			break
		}

		suite.Assert().NotEmpty(d.ID())

		ids[d.ID()] = struct{}{}
	}

	return ids
}

func makeRangeScanProvider(createCb func(args mock.Arguments)) *mockKvProviderCoreProvider {
	provider := new(mockKvProviderCoreProvider)
	provider.
		On(
			"RangeScanCreate",
			mock.AnythingOfType("uint16"),
			mock.AnythingOfType("gocbcore.RangeScanCreateOptions"),
			mock.AnythingOfType("gocbcore.RangeScanCreateCallback"),
		).
		Run(createCb).
		Return(new(mockPendingOp), nil)

	return provider
}

func (suite *UnitTestSuite) TestRangeScanLoadBalancer() {
	vbucketServers := []int{0, 0, 1, 1, 2, 2}
	serverToVbucketMap := make(map[int][]uint16)
	for vbucket, server := range vbucketServers {
		serverToVbucketMap[server] = append(serverToVbucketMap[server], uint16(vbucket))
	}

	suite.Run("selecting 3 vbuckets from 3-node cluster with equal number of vbuckets in each gives one vbucket from each node", func() {
		balancer := newRangeScanLoadBalancer(serverToVbucketMap, 0)

		var selectedNodes []int
		for i := 0; i < 3; i++ {
			vbucket, ok := balancer.selectVbucket()
			suite.Require().True(ok)
			suite.Require().Equal(vbucketServers[vbucket.id], vbucket.server)
			suite.Assert().NotContains(selectedNodes, vbucket.server)
			selectedNodes = append(selectedNodes, vbucket.server)
		}
	})

	suite.Run("the selected vbucket should come from the least busy node", func() {
		balancer := newRangeScanLoadBalancer(serverToVbucketMap, 0)

		// Nodes 0 and 1 have one scan in-progress each. Node 2 has no active scans
		balancer.scanStarting(rangeScanVbucket{id: 0, server: 0})
		balancer.scanStarting(rangeScanVbucket{id: 2, server: 1})

		vbucket, ok := balancer.selectVbucket()
		suite.Require().True(ok)
		suite.Require().Equal(vbucketServers[vbucket.id], vbucket.server)
		suite.Assert().Equal(vbucket.server, 2)
	})

	suite.Run("when there are no retries each vbucket should be returned once", func() {
		balancer := newRangeScanLoadBalancer(serverToVbucketMap, 0)

		// select all vbuckets
		ch := make(chan uint16)
		var count atomic.Uint32
		for i := 0; i <= len(vbucketServers); i++ {
			go func() {
				vbucket, ok := balancer.selectVbucket()
				if ok {
					ch <- vbucket.id
				}

				if count.Add(1) == uint32(len(vbucketServers)+1) {
					close(ch)
				}
			}()
		}

		var selectedVbuckets []uint16
		for vbId := range ch {
			suite.Assert().NotContains(selectedVbuckets, vbId)
			selectedVbuckets = append(selectedVbuckets, vbId)
		}
		suite.Assert().Equal(len(vbucketServers), len(selectedVbuckets))
	})

	suite.Run("retried vbucket id will be returned twice", func() {
		balancer := newRangeScanLoadBalancer(serverToVbucketMap, 0)

		var retriedVbucket uint16 = 3
		var selectedVbuckets []uint16

		retried := false
		for {
			vbucket, ok := balancer.selectVbucket()
			if !ok {
				break
			}
			if vbucket.id == retriedVbucket {
				if retried {
					suite.Assert().Contains(selectedVbuckets, vbucket.id)
				} else {
					balancer.retryScan(vbucket)
					suite.Assert().NotContains(selectedVbuckets, vbucket.id)
				}
				retried = true
			} else {
				suite.Assert().NotContains(selectedVbuckets, vbucket.id)
			}
			selectedVbuckets = append(selectedVbuckets, vbucket.id)
		}
	})
}
