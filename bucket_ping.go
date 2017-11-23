package gocb

import (
	"encoding/json"
	"github.com/google/uuid"
	"gopkg.in/couchbase/gocbcore.v7"
	"sync"
	"time"
)

func diagServiceString(service ServiceType) string {
	switch service {
	case MemdService:
		return "kv"
	case CapiService:
		return "view"
	case MgmtService:
		return "mgmt"
	case N1qlService:
		return "n1ql"
	case FtsService:
		return "fts"
	case CbasService:
		return "cbas"
	}
	return "?"
}

type PingServiceEntry struct {
	Service  ServiceType
	Endpoint string
	Success  bool
	Latency  time.Duration
}

type PingReport struct {
	services []PingServiceEntry
}

type jsonPingServiceEntry struct {
	Remote    string `json:"remote"`
	LatencyUs uint64 `json:"latency_us"`
	Success   bool   `json:"success"`
}

type jsonPingReport struct {
	Version   int                               `json:"version"`
	Id        string                            `json:"id"`
	ConfigRev int                               `json:"config_rev"`
	Sdk       string                            `json:"sdk"`
	Services  map[string][]jsonPingServiceEntry `json:"services"`
}

func (report *PingReport) MarshalJSON() ([]byte, error) {
	jsonReport := jsonPingReport{
		Version:  1,
		Id:       uuid.New().String(),
		Services: make(map[string][]jsonPingServiceEntry),
	}

	for _, service := range report.services {
		serviceStr := diagServiceString(service.Service)
		jsonReport.Services[serviceStr] = append(jsonReport.Services[serviceStr], jsonPingServiceEntry{
			Remote:    service.Endpoint,
			LatencyUs: uint64(service.Latency / time.Nanosecond),
		})
	}

	return json.Marshal(&jsonReport)
}

func (b *Bucket) pingKv() (pingsOut []gocbcore.PingResult, errOut error) {
	signal := make(chan bool, 1)

	op, err := b.client.Ping(func(results []gocbcore.PingResult) {
		pingsOut = make([]gocbcore.PingResult, len(results))
		for pingIdx, ping := range results {
			// We rewrite the cancelled errors into timeout errors here.
			if ping.Error == gocbcore.ErrCancelled {
				ping.Error = ErrTimeout
			}
			pingsOut[pingIdx] = ping
		}
		signal <- true
	})
	if err != nil {
		return nil, err
	}

	timeoutTmr := gocbcore.AcquireTimer(b.opTimeout)
	select {
	case <-signal:
		gocbcore.ReleaseTimer(timeoutTmr, false)
		return
	case <-timeoutTmr.C:
		gocbcore.ReleaseTimer(timeoutTmr, true)
		if !op.Cancel() {
			<-signal
			return
		}
		return nil, ErrTimeout
	}
}

func (b *Bucket) Ping(services []ServiceType) (*PingReport, error) {
	numServices := 0
	waitCh := make(chan error, 10)
	report := &PingReport{}
	var reportLock sync.Mutex

	if services == nil {
		services = []ServiceType{
			MemdService,
			CapiService,
			N1qlService,
		}
	}

	for _, serviceType := range services {
		switch serviceType {
		case MemdService:
			numServices++
			go func() {
				pings, err := b.pingKv()
				if err != nil {
					logWarnf("Failed to ping KV for report: %s", err)
					waitCh <- nil
					return
				}

				reportLock.Lock()
				// We intentionally ignore errors here and simply include
				// any non-error pings that we have received.  Note that
				// gocbcore's ping command, when cancelled, still returns
				// any pings that had occurred before the operation was
				// cancelled and then marks the rest as errors.
				for _, ping := range pings {
					wasSuccess := true
					if ping.Error != nil {
						wasSuccess = false
					}

					report.services = append(report.services, PingServiceEntry{
						Service:  MemdService,
						Endpoint: ping.Endpoint,
						Success:  wasSuccess,
						Latency:  ping.Latency,
					})
				}
				reportLock.Unlock()
				waitCh <- nil
			}()
		case CapiService:
			// View Service is not currently supported as a ping target
		case N1qlService:
			numServices++
			go func() {
				startTime := time.Now()

				res, err := b.ExecuteN1qlQuery(NewN1qlQuery("SELECT 1"), nil)
				if err == nil {
					err = res.Close()
				}

				queryLatency := time.Now().Sub(startTime)

				reportLock.Lock()
				if err != nil {
					report.services = append(report.services, PingServiceEntry{
						Service:  N1qlService,
						Endpoint: "oops",
						Success:  false,
					})
				} else {
					report.services = append(report.services, PingServiceEntry{
						Service:  N1qlService,
						Endpoint: res.SourceEndpoint(),
						Success:  true,
						Latency:  queryLatency,
					})
				}
				reportLock.Unlock()
				waitCh <- nil
			}()
		}
	}

	for i := 0; i < numServices; i++ {
		<-waitCh
	}

	return report, nil
}
