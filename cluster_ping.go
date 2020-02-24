package gocb

import (
	"sync"
	"time"

	"github.com/couchbase/gocbcore/v8"
	"github.com/google/uuid"
)

// Ping will ping a list of services and verify they are active and
// responding in an acceptable period of time.
func (c *Cluster) Ping(opts *PingOptions) (*PingResult, error) {
	if opts == nil {
		opts = &PingOptions{}
	}

	services := opts.ServiceTypes
	if services == nil {
		services = []ServiceType{
			ServiceTypeQuery,
			ServiceTypeSearch,
			ServiceTypeAnalytics,
		}
	} else {
		for _, svc := range services {
			if svc == ServiceTypeKeyValue {
				return nil, invalidArgumentsError{
					message: "keyvalue service is not a valid service type for cluster level ping",
				}
			}
			if svc == ServiceTypeViews {
				return nil, invalidArgumentsError{
					message: "view service is not a valid service type for cluster level ping",
				}
			}
		}
	}

	numServices := 0
	waitCh := make(chan error, 10)
	report := &PingResult{
		sdk:      Identifier() + " " + "gocbcore/" + gocbcore.Version(),
		Services: make(map[ServiceType][]EndpointPingReport),
	}
	var reportLock sync.Mutex

	report.ID = opts.ReportID
	if report.ID == "" {
		report.ID = uuid.New().String()
	}

	httpReq := func(service ServiceType, url string) (time.Duration, string, error) {
		startTime := time.Now()

		provider, err := c.getHTTPProvider()
		if err != nil {
			return 0, "", err
		}

		timeout := 60 * time.Second
		if service == ServiceTypeQuery {
			timeout = c.sb.QueryTimeout
		} else if service == ServiceTypeSearch {
			timeout = c.sb.SearchTimeout
		} else if service == ServiceTypeAnalytics {
			timeout = c.sb.AnalyticsTimeout
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

		return pingLatency, resp.Endpoint, err
	}

	for _, serviceType := range services {
		switch serviceType {
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
