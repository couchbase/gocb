package gocbcore

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"
)

type zombieLogEntry struct {
	connectionID string
	operationID  string
	endpoint     string
	duration     time.Duration
	serviceType  string
}

type zombieLogItem struct {
	ConnectionID     string `json:"c"`
	OperationID      string `json:"i"`
	Endpoint         string `json:"r"`
	ServerDurationUs uint64 `json:"d"`
	ServiceType      string `json:"s"`
}

type zombieLogService struct {
	Service string          `json:"service"`
	Count   int             `json:"count"`
	Top     []zombieLogItem `json:"top"`
}

func (agent *Agent) zombieLogger(interval time.Duration, sampleSize int) {
	lastTick := time.Now()

	for {
		// We tick every 1 second to make sure that the goroutines
		// are cleaned up promptly after agent shutdown.
		<-time.After(1 * time.Second)

		routingInfo := agent.routingInfo.Get()
		if routingInfo == nil {
			// If the routing info is gone, the agent shut down and we should close
			return
		}

		if time.Now().Sub(lastTick) < interval {
			continue
		}

		lastTick = lastTick.Add(interval)

		// Preallocate space to copy the ops into...
		oldOps := make([]*zombieLogEntry, sampleSize)

		agent.zombieLock.Lock()
		// Escape early if we have no ops to log...
		if len(agent.zombieOps) == 0 {
			agent.zombieLock.Unlock()
			return
		}

		// Copy out our ops so we can cheaply print them out without blocking
		// our ops from actually being recorded in other goroutines (which would
		// effectively slow down the op pipeline for logging).

		oldOps = oldOps[0:len(agent.zombieOps)]
		copy(oldOps, agent.zombieOps)
		agent.zombieOps = agent.zombieOps[:0]

		agent.zombieLock.Unlock()

		jsonData := zombieLogService{
			Service: "kv",
		}

		for i := len(oldOps) - 1; i >= 0; i-- {
			op := oldOps[i]

			jsonData.Top = append(jsonData.Top, zombieLogItem{
				OperationID:      op.operationID,
				ConnectionID:     op.connectionID,
				Endpoint:         op.endpoint,
				ServerDurationUs: uint64(op.duration / time.Microsecond),
				ServiceType:      op.serviceType,
			})
		}

		jsonData.Count = len(jsonData.Top)

		jsonBytes, err := json.Marshal(jsonData)
		if err != nil {
			logDebugf("Failed to generate zombie logging JSON: %s", err)
		}

		logWarnf("Orphaned responses observed:\n %s", jsonBytes)
	}
}

func (agent *Agent) recordZombieResponse(resp *memdQResponse, client *memdClient) {
	entry := &zombieLogEntry{
		connectionID: client.connId,
		operationID:  fmt.Sprintf("0x%x", resp.Opaque),
		endpoint:     client.Address(),
		duration:     0,
		serviceType:  fmt.Sprintf("kv:%s", getCommandName(resp.Opcode)),
	}

	if resp.FrameExtras != nil && resp.FrameExtras.HasSrvDuration {
		entry.duration = resp.FrameExtras.SrvDuration
	}

	agent.zombieLock.RLock()
	if len(agent.zombieOps) == cap(agent.zombieOps) && entry.duration < agent.zombieOps[0].duration {
		// we are at capacity and we are faster than the fastest slow op
		agent.zombieLock.RUnlock()
		return
	}
	agent.zombieLock.RUnlock()

	agent.zombieLock.Lock()
	if len(agent.zombieOps) == cap(agent.zombieOps) && entry.duration < agent.zombieOps[0].duration {
		// we are at capacity and we are faster than the fastest slow op
		agent.zombieLock.Unlock()
		return
	}

	l := len(agent.zombieOps)
	i := sort.Search(l, func(i int) bool { return entry.duration < agent.zombieOps[i].duration })

	// i represents the slot where it should be inserted

	if len(agent.zombieOps) < cap(agent.zombieOps) {
		if i == l {
			agent.zombieOps = append(agent.zombieOps, entry)
		} else {
			agent.zombieOps = append(agent.zombieOps, nil)
			copy(agent.zombieOps[i+1:], agent.zombieOps[i:])
			agent.zombieOps[i] = entry
		}
	} else {
		if i == 0 {
			agent.zombieOps[i] = entry
		} else {
			copy(agent.zombieOps[0:i-1], agent.zombieOps[1:i])
			agent.zombieOps[i-1] = entry
		}
	}

	agent.zombieLock.Unlock()
}
