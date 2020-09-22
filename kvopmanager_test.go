package gocb

import (
	"testing"
	"time"
)

func (suite *IntegrationTestSuite) TestKvOpManagerTimeouts() {
	type tCase struct {
		name                      string
		timeout                   time.Duration
		durabilityLevel           DurabilityLevel
		expectedDurabilityTimeout time.Duration
		expectedDeadline          time.Time
	}

	testCases := []tCase{
		{
			name:                      "timeout",
			timeout:                   3000 * time.Millisecond,
			expectedDurabilityTimeout: 0,
			expectedDeadline:          time.Now().Add(3000 * time.Millisecond),
		},
		{
			name:                      "timeout, with durability level majority",
			timeout:                   3000 * time.Millisecond,
			durabilityLevel:           DurabilityLevelMajority,
			expectedDurabilityTimeout: time.Duration(float64(3000*time.Millisecond) * 0.9),
			expectedDeadline:          time.Now().Add(3000 * time.Millisecond),
		},
		{
			name:                      "timeout, with durability level persist to majority",
			timeout:                   3000 * time.Millisecond,
			durabilityLevel:           DurabilityLevelPersistToMajority,
			expectedDurabilityTimeout: time.Duration(float64(3000*time.Millisecond) * 0.9),
			expectedDeadline:          time.Now().Add(3000 * time.Millisecond),
		},
		{
			name:                      "timeout, with durability level majority and persist master",
			timeout:                   3000 * time.Millisecond,
			durabilityLevel:           DurabilityLevelMajorityAndPersistOnMaster,
			expectedDurabilityTimeout: time.Duration(float64(3000*time.Millisecond) * 0.9),
			expectedDeadline:          time.Now().Add(3000 * time.Millisecond),
		},
		{
			name:                      "low timeout",
			timeout:                   1000 * time.Millisecond,
			expectedDurabilityTimeout: 0,
			expectedDeadline:          time.Now().Add(1000 * time.Millisecond),
		},
		{
			name:                      "low timeout, with durability level majority",
			timeout:                   1000 * time.Millisecond,
			durabilityLevel:           DurabilityLevelMajority,
			expectedDurabilityTimeout: durabilityTimeoutFloor,
			expectedDeadline:          time.Now().Add(durabilityTimeoutFloor),
		},
		{
			name:                      "low timeout, with durability level persist to majority",
			timeout:                   1000 * time.Millisecond,
			durabilityLevel:           DurabilityLevelPersistToMajority,
			expectedDurabilityTimeout: durabilityTimeoutFloor,
			expectedDeadline:          time.Now().Add(durabilityTimeoutFloor),
		},
		{
			name:                      "low timeout, with durability level majority and persist master",
			timeout:                   1000 * time.Millisecond,
			durabilityLevel:           DurabilityLevelMajorityAndPersistOnMaster,
			expectedDurabilityTimeout: durabilityTimeoutFloor,
			expectedDeadline:          time.Now().Add(durabilityTimeoutFloor),
		},
		// Edge timeouts mean that the timeout set is above the durable floor but after applying the adaptive
		// algorithm the value will be below and require coercion.
		{
			name:                      "edge timeout",
			timeout:                   1600 * time.Millisecond,
			expectedDurabilityTimeout: 0,
			expectedDeadline:          time.Now().Add(1600 * time.Millisecond),
		},
		{
			name:                      "edge timeout, with durability level majority",
			timeout:                   1600 * time.Millisecond,
			durabilityLevel:           DurabilityLevelMajority,
			expectedDurabilityTimeout: durabilityTimeoutFloor,
			expectedDeadline:          time.Now().Add(1600 * time.Millisecond),
		},
		{
			name:                      "edge timeout, with durability level persist to majority",
			timeout:                   1600 * time.Millisecond,
			durabilityLevel:           DurabilityLevelPersistToMajority,
			expectedDurabilityTimeout: durabilityTimeoutFloor,
			expectedDeadline:          time.Now().Add(1600 * time.Millisecond),
		},
		{
			name:                      "edge timeout, with durability level majority and persist master",
			timeout:                   1600 * time.Millisecond,
			durabilityLevel:           DurabilityLevelMajorityAndPersistOnMaster,
			expectedDurabilityTimeout: durabilityTimeoutFloor,
			expectedDeadline:          time.Now().Add(1600 * time.Millisecond),
		},
		{
			name:                      "no timeout",
			timeout:                   globalCollection.timeoutsConfig.KVTimeout,
			expectedDurabilityTimeout: 0,
			expectedDeadline:          time.Now().Add(globalCollection.timeoutsConfig.KVTimeout),
		},
		{
			name:                      "no timeout, with durability level majority",
			timeout:                   0,
			durabilityLevel:           DurabilityLevelMajority,
			expectedDurabilityTimeout: time.Duration(float64(globalCollection.timeoutsConfig.KVTimeout) * 0.9),
			expectedDeadline:          time.Now().Add(globalCollection.timeoutsConfig.KVTimeout),
		},
		{
			name:                      "no timeout, with durability level persist to majority",
			timeout:                   0,
			durabilityLevel:           DurabilityLevelPersistToMajority,
			expectedDurabilityTimeout: time.Duration(float64(globalCollection.timeoutsConfig.KVDurableTimeout) * 0.9),
			expectedDeadline:          time.Now().Add(globalCollection.timeoutsConfig.KVDurableTimeout),
		},
		{
			name:                      "no timeout, with durability level majority and persist master",
			timeout:                   0,
			durabilityLevel:           DurabilityLevelMajorityAndPersistOnMaster,
			expectedDurabilityTimeout: time.Duration(float64(globalCollection.timeoutsConfig.KVDurableTimeout) * 0.9),
			expectedDeadline:          time.Now().Add(globalCollection.timeoutsConfig.KVDurableTimeout),
		},
	}

	for _, tc := range testCases {
		suite.T().Run(tc.name, func(tt *testing.T) {
			mgr := globalCollection.newKvOpManager("test", nil)
			mgr.SetTimeout(tc.timeout)
			mgr.SetDuraOptions(0, 0, tc.durabilityLevel)

			deadline := mgr.Deadline()
			duraTimeout := mgr.DurabilityTimeout()

			diff := deadline.Sub(tc.expectedDeadline)
			if diff > 5*time.Millisecond || diff < -5*time.Millisecond {
				tt.Logf("Expected deadline to be %s but was %s, not within 5ms delta",
					tc.expectedDeadline.String(), deadline.String())
				tt.Fail()
			}

			if tc.expectedDurabilityTimeout != duraTimeout {
				tt.Logf("Expected durable timeout to be %s but was %s",
					tc.expectedDurabilityTimeout.String(), duraTimeout.String())
				tt.Fail()
			}
		})
	}
}
