package gocbcore

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type configStreamBlock struct {
	Bytes []byte
}

func (i *configStreamBlock) UnmarshalJSON(data []byte) error {
	i.Bytes = make([]byte, len(data))
	copy(i.Bytes, data)
	return nil
}

func hostnameFromUri(uri string) string {
	uriInfo, err := url.Parse(uri)
	if err != nil {
		panic("Failed to parse URI to hostname!")
	}
	return strings.Split(uriInfo.Host, ":")[0]
}

func (c *Agent) httpLooper(firstCfgFn func(*cfgBucket, error)) {
	waitPeriod := 20 * time.Second
	maxConnPeriod := 10 * time.Second
	var iterNum uint64 = 1
	iterSawConfig := false
	seenNodes := make(map[string]uint64)
	isFirstTry := true

	logDebugf("HTTP Looper starting.")
	for {
		routingInfo := c.routingInfo.get()
		if routingInfo == nil {
			// Shutdown the looper if the agent is shutdown
			break
		}

		var pickedSrv string
		for _, srv := range routingInfo.mgmtEpList {
			if seenNodes[srv] >= iterNum {
				continue
			}
			pickedSrv = srv
			break
		}

		if pickedSrv == "" {
			logDebugf("Pick Failed.")
			// All servers have been visited during this iteration
			if isFirstTry {
				logDebugf("Could not find any alive http hosts.")
				firstCfgFn(nil, &agentError{"Failed to connect to all specified hosts."})
				return
			} else {
				if !iterSawConfig {
					logDebugf("Looper waiting...")
					// Wait for a period before trying again if there was a problem...
					<-time.After(waitPeriod)
				}
				logDebugf("Looping again.")
				// Go to next iteration and try all servers again
				iterNum++
				iterSawConfig = false
				continue
			}
		}

		logDebugf("Http Picked: %s.", pickedSrv)

		seenNodes[pickedSrv] = iterNum

		hostname := hostnameFromUri(pickedSrv)

		logDebugf("HTTP Hostname: %s.", pickedSrv)

		// HTTP request time!
		uri := fmt.Sprintf("%s/pools/default/bs/%s", pickedSrv, c.bucket)

		logDebugf("Requesting config from: %s.", uri)

		req, err := http.NewRequest("GET", uri, nil)
		if err != nil {
			logDebugf("Failed to build HTTP config request. %v", err)
			continue
		}

		req.SetBasicAuth(c.bucket, c.password)

		resp, err := c.httpCli.Do(req)
		if err != nil {
			logDebugf("Failed to connect to host. %v", err)
			continue
		}

		if resp.StatusCode != 200 {
			if resp.StatusCode == 401 {
				logDebugf("Failed to connect to host, bad auth.")
				firstCfgFn(nil, &memdError{StatusAuthError})
				return
			}
			logDebugf("Failed to connect to host, unexpected status code: %v.", resp.StatusCode)
			continue
		}

		logDebugf("Connected.")

		// Autodisconnect eventually
		go func() {
			<-time.After(maxConnPeriod)
			logDebugf("Auto DC!")
			resp.Body.Close()
		}()

		dec := json.NewDecoder(resp.Body)
		configBlock := new(configStreamBlock)
		for {
			err := dec.Decode(configBlock)
			if err != nil {
				resp.Body.Close()
				break
			}

			logDebugf("Got Block.")

			bkCfg, err := parseConfig(configBlock.Bytes, hostname)
			if err != nil {
				resp.Body.Close()
				break
			}

			logDebugf("Got Config.")

			iterSawConfig = true
			if isFirstTry {
				logDebugf("HTTP Config Init")
				firstCfgFn(bkCfg, nil)
				isFirstTry = false
			} else {
				logDebugf("HTTP Config Update")
				c.updateConfig(bkCfg)
			}
		}

		logDebugf("HTTP, Setting %s to iter %d", pickedSrv, iterNum)
	}
}
