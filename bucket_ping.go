package gocb

import (
	"encoding/json"
	"sync"
	"time"

	gocbcore "github.com/couchbase/gocbcore/v8"
	"github.com/google/uuid"
)

// EndpointPingReport represents a single entry in a ping report.
type EndpointPingReport struct {
	ID        string
	Local     string
	Remote    string
	State     PingState
	Error     string
	Namespace string
	Latency   time.Duration
}

// PingResult encapsulates the details from a executed ping operation.
type PingResult struct {
	ID       string
	Services map[ServiceType][]EndpointPingReport

	sdk string
}

type jsonEndpointPingReport struct {
	ID        string `json:"id,omitempty"`
	Local     string `json:"local,omitempty"`
	Remote    string `json:"remote,omitempty"`
	State     string `json:"state,omitempty"`
	Error     string `json:"error,omitempty"`
	Namespace string `json:"namespace,omitempty"`
	LatencyUs uint64 `json:"latency_us"`
}

type jsonPingReport struct {
	Version  int                                 `json:"version"`
	SDK      string                              `json:"sdk,omitempty"`
	ID       string                              `json:"id,omitempty"`
	Services map[string][]jsonEndpointPingReport `json:"services,omitempty"`
}

// MarshalJSON generates a JSON representation of this ping report.
func (report *PingResult) MarshalJSON() ([]byte, error) {
	jsonReport := jsonPingReport{
		Version:  2,
		SDK:      report.sdk,
		ID:       report.ID,
		Services: make(map[string][]jsonEndpointPingReport),
	}

	for serviceType, serviceInfo := range report.Services {
		serviceStr := serviceTypeToString(serviceType)
		if _, ok := jsonReport.Services[serviceStr]; !ok {
			jsonReport.Services[serviceStr] = make([]jsonEndpointPingReport, 0)
		}

		for _, service := range serviceInfo {
			jsonReport.Services[serviceStr] = append(jsonReport.Services[serviceStr], jsonEndpointPingReport{
				ID:        service.ID,
				Local:     service.Local,
				Remote:    service.Remote,
				State:     pingStateToString(service.State),
				Error:     service.Error,
				Namespace: service.Namespace,
				LatencyUs: uint64(service.Latency / time.Nanosecond),
			})
		}
	}

	return json.Marshal(&jsonReport)
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
func (b *Bucket) Ping(opts *PingOptions) (*PingResult, error) {
	if opts == nil {
		opts = &PingOptions{}
	}

	numServices := 0
	waitCh := make(chan error, 10)
	report := &PingResult{
		sdk:      Identifier() + " " + "gocbcore/" + gocbcore.Version(),
		Services: make(map[ServiceType][]EndpointPingReport),
	}
	var reportLock sync.Mutex
	services := opts.ServiceTypes

	report.ID = opts.ReportID
	if report.ID == "" {
		report.ID = uuid.New().String()
	}

	if services == nil {
		services = []ServiceType{
			ServiceTypeKeyValue,
			ServiceTypeQuery,
			ServiceTypeSearch,
			ServiceTypeAnalytics,
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
		if service == ServiceTypeQuery {
			timeout = b.sb.QueryTimeout
		} else if service == ServiceTypeSearch {
			timeout = b.sb.SearchTimeout
		} else if service == ServiceTypeAnalytics {
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
		case ServiceTypeKeyValue:
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
				report.Services[ServiceTypeKeyValue] = make([]EndpointPingReport, 0)
				// We intentionally ignore errors here and simply include
				// any non-error pings that we have received.  Note that
				// gocbcore's ping command, when cancelled, still returns
				// any pings that had occurred before the operation was
				// cancelled and then marks the rest as errors.
				for _, ping := range pings.Services {
					state := PingStateOk
					detail := ""
					if ping.Error != nil {
						state = PingStateError
						detail = ping.Error.Error()
					}

					report.Services[ServiceTypeKeyValue] = append(report.Services[ServiceTypeKeyValue], EndpointPingReport{
						Remote:    ping.Endpoint,
						State:     state,
						Latency:   ping.Latency,
						Namespace: ping.Scope,
						ID:        ping.ID,
						Error:     detail,
					})
				}
				reportLock.Unlock()
				waitCh <- nil
			}()
		case ServiceTypeViews:
			// View Service is not currently supported as a ping target
		case ServiceTypeQuery:
			numServices++
			go func() {
				pingLatency, endpoint, err := httpReq(ServiceTypeQuery, "/admin/ping")

				reportLock.Lock()
				report.Services[ServiceTypeQuery] = make([]EndpointPingReport, 0)
				if err != nil {
					report.Services[ServiceTypeQuery] = append(report.Services[ServiceTypeQuery], EndpointPingReport{
						Remote: endpoint,
						State:  PingStateError,
						Error:  err.Error(),
					})
				} else {
					report.Services[ServiceTypeQuery] = append(report.Services[ServiceTypeQuery], EndpointPingReport{
						Remote:  endpoint,
						State:   PingStateOk,
						Latency: pingLatency,
					})
				}
				reportLock.Unlock()

				waitCh <- nil
			}()
		case ServiceTypeSearch:
			numServices++
			go func() {
				pingLatency, endpoint, err := httpReq(ServiceTypeSearch, "/api/ping")

				reportLock.Lock()
				report.Services[ServiceTypeSearch] = make([]EndpointPingReport, 0)
				if err != nil {
					report.Services[ServiceTypeSearch] = append(report.Services[ServiceTypeSearch], EndpointPingReport{
						Remote: endpoint,
						State:  PingStateError,
						Error:  err.Error(),
					})
				} else {
					report.Services[ServiceTypeSearch] = append(report.Services[ServiceTypeSearch], EndpointPingReport{
						Remote:  endpoint,
						State:   PingStateOk,
						Latency: pingLatency,
					})
				}
				reportLock.Unlock()

				waitCh <- nil
			}()
		case ServiceTypeAnalytics:
			numServices++
			go func() {
				pingLatency, endpoint, err := httpReq(ServiceTypeAnalytics, "/admin/ping")

				reportLock.Lock()
				report.Services[ServiceTypeAnalytics] = make([]EndpointPingReport, 0)
				if err != nil {
					report.Services[ServiceTypeAnalytics] = append(report.Services[ServiceTypeAnalytics], EndpointPingReport{
						Remote: endpoint,
						State:  PingStateError,
						Error:  err.Error(),
					})
				} else {
					report.Services[ServiceTypeAnalytics] = append(report.Services[ServiceTypeAnalytics], EndpointPingReport{
						Remote:  endpoint,
						State:   PingStateOk,
						Latency: pingLatency,
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
