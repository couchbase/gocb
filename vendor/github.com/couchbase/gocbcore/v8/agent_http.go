package gocbcore

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
)

// HttpRequest contains the description of an HTTP request to perform.
type HttpRequest struct {
	Service     ServiceType
	Method      string
	Endpoint    string
	Path        string
	Username    string
	Password    string
	Body        []byte
	Context     context.Context
	Headers     map[string]string
	ContentType string
}

// HttpResponse encapsulates the response from an HTTP request.
type HttpResponse struct {
	Endpoint   string
	StatusCode int
	Body       io.ReadCloser
}

func injectJsonCreds(body []byte, creds []UserPassPair) []byte {
	var props map[string]json.RawMessage
	err := json.Unmarshal(body, &props)
	if err == nil {
		if _, ok := props["creds"]; ok {
			// Early out if the user has already passed a set of credentials.
			return body
		}

		jsonCreds, err := json.Marshal(creds)
		if err == nil {
			props["creds"] = json.RawMessage(jsonCreds)

			newBody, err := json.Marshal(props)
			if err == nil {
				return newBody
			}
		}
	}

	return body
}

func (agent *Agent) getMgmtEp() (string, error) {
	mgmtEps := agent.MgmtEps()
	if len(mgmtEps) == 0 {
		return "", ErrNoMgmtService
	}
	return mgmtEps[rand.Intn(len(mgmtEps))], nil
}

func (agent *Agent) getCapiEp() (string, error) {
	capiEps := agent.CapiEps()
	if len(capiEps) == 0 {
		return "", ErrNoMgmtService
	}
	return capiEps[rand.Intn(len(capiEps))], nil
}

func (agent *Agent) getN1qlEp() (string, error) {
	n1qlEps := agent.N1qlEps()
	if len(n1qlEps) == 0 {
		return "", ErrNoN1qlService
	}
	return n1qlEps[rand.Intn(len(n1qlEps))], nil
}

func (agent *Agent) getFtsEp() (string, error) {
	ftsEps := agent.FtsEps()
	if len(ftsEps) == 0 {
		return "", ErrNoFtsService
	}
	return ftsEps[rand.Intn(len(ftsEps))], nil
}

func (agent *Agent) getCbasEp() (string, error) {
	cbasEps := agent.CbasEps()
	if len(cbasEps) == 0 {
		return "", ErrNoCbasService
	}
	return cbasEps[rand.Intn(len(cbasEps))], nil
}

// DoHttpRequest will perform an HTTP request against one of the HTTP
// services which are available within the SDK.
func (agent *Agent) DoHttpRequest(req *HttpRequest) (*HttpResponse, error) {
	if req.Service == MemdService {
		return nil, ErrInvalidService
	}

	// Identify an endpoint to use for the request
	endpoint := req.Endpoint
	if endpoint == "" {
		var err error
		switch req.Service {
		case MgmtService:
			endpoint, err = agent.getMgmtEp()
		case CapiService:
			endpoint, err = agent.getCapiEp()
		case N1qlService:
			endpoint, err = agent.getN1qlEp()
		case FtsService:
			endpoint, err = agent.getFtsEp()
		case CbasService:
			endpoint, err = agent.getCbasEp()
		}
		if err != nil {
			return nil, err
		}

		req.Endpoint = endpoint
	}

	// Generate a request URI
	reqUri := endpoint + req.Path

	// Create a new request
	hreq, err := http.NewRequest(req.Method, reqUri, nil)
	if err != nil {
		return nil, err
	}
	// hreq.WithContext will panic if ctx is nil so make absolutely sure it isn't
	if req.Context == nil {
		req.Context = context.Background()
	}
	hreq = hreq.WithContext(req.Context)

	body := req.Body

	// Inject credentials into the request
	if req.Username != "" || req.Password != "" {
		hreq.SetBasicAuth(req.Username, req.Password)
	} else {
		creds, err := agent.auth.Credentials(AuthCredsRequest{
			Service:  req.Service,
			Endpoint: endpoint,
		})
		if err != nil {
			return nil, err
		}

		if req.Service == N1qlService || req.Service == CbasService ||
			req.Service == FtsService {
			// Handle service which support multi-bucket authentication using
			// injection into the body of the request.
			if len(creds) == 1 {
				hreq.SetBasicAuth(creds[0].Username, creds[0].Password)
			} else {
				body = injectJsonCreds(body, creds)
			}
		} else {
			if len(creds) != 1 {
				return nil, ErrInvalidCredentials
			}

			hreq.SetBasicAuth(creds[0].Username, creds[0].Password)
		}
	}

	hreq.Body = ioutil.NopCloser(bytes.NewReader(body))

	if req.ContentType != "" {
		hreq.Header.Set("Content-Type", req.ContentType)
	} else {
		hreq.Header.Set("Content-Type", "application/json")
	}
	for key, val := range req.Headers {
		hreq.Header.Set(key, val)
	}

	hresp, err := agent.httpCli.Do(hreq)
	if err != nil {
		return nil, err
	}

	respOut := HttpResponse{
		Endpoint:   endpoint,
		StatusCode: hresp.StatusCode,
		Body:       hresp.Body,
	}

	return &respOut, nil
}
