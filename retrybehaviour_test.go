package gocb

import (
	"testing"
	"time"
)

func TestDelayRetryBehaviorCanRetry(t *testing.T) {
	behav := StandardDelayRetryBehavior(10, 1000, 1*time.Second, LinearDelayFunction)

	var retries uint
	if !behav.CanRetry(retries) {
		t.Log("TestDelayRetryBehaviorLinear should have been able to retry but couldn't")
		t.Fail()
	}

	retries = 9
	if !behav.CanRetry(retries) {
		t.Log("TestDelayRetryBehaviorLinear should have been able to retry but couldn't")
		t.Fail()
	}

	retries = 10
	if behav.CanRetry(retries) {
		t.Log("TestDelayRetryBehaviorLinear shouldn't have been able to retry but could")
		t.Fail()
	}
}

func TestDelayRetryBehaviorLinear(t *testing.T) {
	behav := StandardDelayRetryBehavior(10, 2, 500*time.Millisecond, LinearDelayFunction)

	testNextInterval(t, behav, 1, 2*time.Millisecond, "TestDelayRetryBehaviorLinear")
	testNextInterval(t, behav, 5, 10*time.Millisecond, "TestDelayRetryBehaviorLinear")
	testNextInterval(t, behav, 10, 20*time.Millisecond, "TestDelayRetryBehaviorLinear")
	testNextInterval(t, behav, 1000, 500*time.Millisecond, "TestDelayRetryBehaviorLinear")
}

func TestDelayRetryBehaviorExponential(t *testing.T) {
	behav := StandardDelayRetryBehavior(10, 2, 500*time.Millisecond, ExponentialDelayFunction)

	testNextInterval(t, behav, 1, 2*time.Millisecond, "TestDelayRetryBehaviorExponential")
	testNextInterval(t, behav, 5, 32*time.Millisecond, "TestDelayRetryBehaviorExponential")
	testNextInterval(t, behav, 10, 500*time.Millisecond, "TestDelayRetryBehaviorExponential")
}

func testNextInterval(t *testing.T, behav *DelayRetryBehavior, retries uint, expected time.Duration, testName string) {
	interval := behav.NextInterval(retries)
	if interval != expected {
		t.Logf("%s expected interval of %v but was %v", testName, expected, interval)
		t.Fail()
	}
}
