package gocb

import (
	"encoding/json"
	"sync"
	"time"

	gocbcore "github.com/couchbase/gocbcore/v8"
	"github.com/google/uuid"
)

// PingServiceEntry represents a single entry in a ping report.
type PingServiceEntry struct {
	RemoteAddr string
	State      string
	Latency    time.Duration
	Scope      string
	ID         string
	Detail     string
}

// PingResult encapsulates the details from a executed ping operation.
type PingResult struct {
	Services  map[ServiceType][]PingServiceEntry
	ConfigRev int64
	ID        string
}

type jsonPingServiceEntry struct {
	Remote    string `json:"remote"`
	LatencyUs uint64 `json:"latency_us"`
	Scope     string `json:"scope,omitempty"`
	ID        string `json:"id,omitempty"`
	State     string `json:"state"`
	Detail    string `json:"detail"`
}

type jsonPingReport struct {
	Version   int                               `json:"version"`
	ID        string                            `json:"id"`
	Sdk       string                            `json:"sdk"`
	Services  map[string][]jsonPingServiceEntry `json:"services"`
	ConfigRev int64                             `json:"config_rev"`
}

// MarshalJSON generates a JSON representation of this ping report.
func (report *PingResult) MarshalJSON() ([]byte, error) {
	jsonReport := jsonPingReport{
		Version:  1,
		ID:       report.ID,
		Sdk:      Identifier() + " " + "gocbcore/" + gocbcore.Version(),
		Services: make(map[string][]jsonPingServiceEntry),
	}

	for key, serviceType := range report.Services {
		serviceStr := diagServiceString(key)
		if _, ok := jsonReport.Services[serviceStr]; !ok {
			jsonReport.Services[serviceStr] = make([]jsonPingServiceEntry, 0)
		}
		for _, service := range serviceType {
			jsonReport.Services[serviceStr] = append(jsonReport.Services[serviceStr], jsonPingServiceEntry{
				Remote:    service.RemoteAddr,
				LatencyUs: uint64(service.Latency / time.Nanosecond),
				State:     service.State,
				Scope:     service.Scope,
				ID:        service.ID,
				Detail:    service.Detail,
			})
		}
	}

	return json.Marshal(&jsonReport)
}

func (jsonReport *jsonPingReport) toReport() *PingResult {
	report := &PingResult{
		ID: jsonReport.ID,
	}

	for key, jsonServices := range jsonReport.Services {
		serviceType := diagStringService(key)
		if _, ok := report.Services[serviceType]; !ok {
			report.Services[serviceType] = make([]PingServiceEntry, 0)
		}
		for _, jsonService := range jsonServices {
			report.Services[serviceType] = append(report.Services[serviceType], PingServiceEntry{
				RemoteAddr: jsonService.Remote,
				Latency:    time.Duration(jsonService.LatencyUs) * time.Nanosecond,
				State:      jsonService.State,
				Scope:      jsonService.Scope,
				ID:         jsonService.ID,
				Detail:     jsonService.Detail,
			})
		}
	}

	return report
}

func (b *Bucket) pingKv(provider kvProvider) (pingsOut *gocbcore.PingKvResult, errOut error) {
	signal := make(chan bool, 1)

	op, err := provider.PingKvEx(gocbcore.PingKvOptions{}, func(result *gocbcore.PingKvResult, err error) {
		if err != nil {
			errOut = maybeEnhanceKVErr(err, b.Name(), "", "", "")
			signal <- true
			return
		}

		pingsOut = result
		signal <- true
	})
	if err != nil {
		return nil, err
	}

	timeoutTmr := gocbcore.AcquireTimer(b.sb.KvTimeout)
	select {
	case <-signal:
		gocbcore.ReleaseTimer(timeoutTmr, false)
		return
	case <-timeoutTmr.C:
		gocbcore.ReleaseTimer(timeoutTmr, true)
		op.Cancel(ErrAmbiguousTimeout)
		<-signal
		return
	}
}

// PingOptions are the options available to the Ping operation.
type PingOptions struct {
	ServiceTypes []ServiceType
	ReportID     string
}

// Ping will ping a list of services and verify they are active and
// responding in an acceptable period of time.
//
// Volatile: This API is subject to change at any time.
func (b *Bucket) Ping(opts *PingOptions) (*PingResult, error) {
	if opts == nil {
		opts = &PingOptions{}
	}

	numServices := 0
	waitCh := make(chan error, 10)
	report := &PingResult{
		Services: make(map[ServiceType][]PingServiceEntry),
	}
	var reportLock sync.Mutex
	services := opts.ServiceTypes

	report.ID = opts.ReportID
	if report.ID == "" {
		report.ID = uuid.New().String()
	}

	if services == nil {
		services = []ServiceType{
			KeyValueService,
			QueryService,
			SearchService,
			AnalyticsService,
		}
	}

	httpReq := func(service ServiceType, url string) (time.Duration, string, error) {
		startTime := time.Now()

		cli := b.sb.getCachedClient()
		provider, err := cli.getHTTPProvider()
		if err != nil {
			return 0, "", err
		}

		timeout := 60 * time.Second
		if service == QueryService {
			timeout = b.sb.QueryTimeout
		} else if service == SearchService {
			timeout = b.sb.SearchTimeout
		} else if service == AnalyticsService {
			timeout = b.sb.AnalyticsTimeout
		}

		req := gocbcore.HTTPRequest{
			Method:  "GET",
			Path:    url,
			Service: gocbcore.ServiceType(service),
			Timeout: timeout,
		}

		resp, err := provider.DoHTTPRequest(&req)
		if err != nil {
			return 0, req.Endpoint, err
		}

		err = resp.Body.Close()
		if err != nil {
			logDebugf("Failed to close http request: %s", err)
		}

		pingLatency := time.Now().Sub(startTime)

		return pingLatency, req.Endpoint, err
	}

	for _, serviceType := range services {
		switch serviceType {
		case KeyValueService:
			numServices++
			go func() {
				cli := b.sb.getCachedClient()
				provider, err := cli.getKvProvider()
				if err != nil {
					logWarnf("Failed to get KV provider for report: %s", err)
					waitCh <- nil
					return
				}

				pings, err := b.pingKv(provider)
				if err != nil {
					logWarnf("Failed to ping KV for report: %s", err)
					waitCh <- nil
					return
				}

				reportLock.Lock()
				report.ConfigRev = pings.ConfigRev
				report.Services[KeyValueService] = make([]PingServiceEntry, 0)
				// We intentionally ignore errors here and simply include
				// any non-error pings that we have received.  Note that
				// gocbcore's ping command, when cancelled, still returns
				// any pings that had occurred before the operation was
				// cancelled and then marks the rest as errors.
				for _, ping := range pings.Services {
					state := "ok"
					detail := ""
					if ping.Error != nil {
						state = "error"
						detail = ping.Error.Error()
					}

					report.Services[KeyValueService] = append(report.Services[KeyValueService], PingServiceEntry{
						RemoteAddr: ping.Endpoint,
						State:      state,
						Latency:    ping.Latency,
						Scope:      ping.Scope,
						ID:         ping.ID,
						Detail:     detail,
					})
				}
				reportLock.Unlock()
				waitCh <- nil
			}()
		case CapiService:
			// View Service is not currently supported as a ping target
		case QueryService:
			numServices++
			go func() {
				pingLatency, endpoint, err := httpReq(QueryService, "/admin/ping")

				reportLock.Lock()
				report.Services[QueryService] = make([]PingServiceEntry, 0)
				if err != nil {
					report.Services[QueryService] = append(report.Services[QueryService], PingServiceEntry{
						RemoteAddr: endpoint,
						State:      "error",
						Detail:     err.Error(),
					})
				} else {
					report.Services[QueryService] = append(report.Services[QueryService], PingServiceEntry{
						RemoteAddr: endpoint,
						State:      "ok",
						Latency:    pingLatency,
					})
				}
				reportLock.Unlock()

				waitCh <- nil
			}()
		case SearchService:
			numServices++
			go func() {
				pingLatency, endpoint, err := httpReq(SearchService, "/api/ping")

				reportLock.Lock()
				report.Services[SearchService] = make([]PingServiceEntry, 0)
				if err != nil {
					report.Services[SearchService] = append(report.Services[SearchService], PingServiceEntry{
						RemoteAddr: endpoint,
						State:      "error",
						Detail:     err.Error(),
					})
				} else {
					report.Services[SearchService] = append(report.Services[SearchService], PingServiceEntry{
						RemoteAddr: endpoint,
						State:      "ok",
						Latency:    pingLatency,
					})
				}
				reportLock.Unlock()

				waitCh <- nil
			}()
		case AnalyticsService:
			numServices++
			go func() {
				pingLatency, endpoint, err := httpReq(AnalyticsService, "/admin/ping")

				reportLock.Lock()
				report.Services[AnalyticsService] = make([]PingServiceEntry, 0)
				if err != nil {
					report.Services[AnalyticsService] = append(report.Services[AnalyticsService], PingServiceEntry{
						RemoteAddr: endpoint,
						State:      "error",
						Detail:     err.Error(),
					})
				} else {
					report.Services[AnalyticsService] = append(report.Services[AnalyticsService], PingServiceEntry{
						RemoteAddr: endpoint,
						State:      "ok",
						Latency:    pingLatency,
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
