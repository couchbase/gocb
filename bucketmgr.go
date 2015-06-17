package gocb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
)

type View struct {
	Map    string `json:"map,omitempty"`
	Reduce string `json:"reduce,omitempty"`
}

func (v View) hasReduce() bool {
	return v.Reduce != ""
}

type DesignDocument struct {
	Name         string          `json:"-"`
	Views        map[string]View `json:"views,omitempty"`
	SpatialViews map[string]View `json:"spatial,omitempty"`
}

type BucketManager struct {
	bucket   *Bucket
	username string
	password string
}

func (bm *BucketManager) capiRequest(method, uri, contentType string, body io.Reader) (*http.Response, error) {
	if contentType == "" && body != nil {
		panic("Content-type must be specified for non-null body.")
	}

	viewEp, err := bm.bucket.getViewEp()
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest(method, viewEp+uri, body)
	if contentType != "" {
		req.Header.Add("Content-Type", contentType)
	}
	if err != nil {
		return nil, err
	}

	req.SetBasicAuth(bm.username, bm.password)
	return bm.bucket.client.HttpClient().Do(req)
}

func (bm *BucketManager) mgmtRequest(method, uri, contentType string, body io.Reader) (*http.Response, error) {
	if contentType == "" && body != nil {
		panic("Content-type must be specified for non-null body.")
	}

	mgmtEp, err := bm.bucket.getMgmtEp()
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest(method, mgmtEp+uri, body)
	if contentType != "" {
		req.Header.Add("Content-Type", contentType)
	}
	if err != nil {
		return nil, err
	}

	req.SetBasicAuth(bm.username, bm.password)
	return bm.bucket.client.HttpClient().Do(req)
}

func (bm *BucketManager) Flush() error {
	reqUri := fmt.Sprintf("/pools/default/%s/controller/doFlush", bm.bucket.name)
	resp, err := bm.mgmtRequest("POST", reqUri, "", nil)
	if err != nil {
		return err
	}

	if resp.StatusCode != 200 {
		data, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		resp.Body.Close()
		return clientError{string(data)}
	}
	return nil
}

func (bm *BucketManager) GetDesignDocument(name string) (*DesignDocument, error) {
	reqUri := fmt.Sprintf("/_design/%s", name)

	resp, err := bm.capiRequest("GET", reqUri, "", nil)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		data, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
		resp.Body.Close()
		return nil, clientError{string(data)}
	}

	ddocObj := DesignDocument{}
	jsonDec := json.NewDecoder(resp.Body)
	err = jsonDec.Decode(&ddocObj)
	if err != nil {
		return nil, err
	}

	ddocObj.Name = name
	return &ddocObj, nil
}

func (bm *BucketManager) GetDesignDocuments() ([]*DesignDocument, error) {
	reqUri := fmt.Sprintf("/pools/default/buckets/%s/ddocs", bm.bucket.name)

	resp, err := bm.mgmtRequest("GET", reqUri, "", nil)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		data, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
		resp.Body.Close()
		return nil, clientError{string(data)}
	}

	var ddocsObj struct {
		Rows []struct {
			Doc struct {
				Meta struct {
					Id string
				}
				Json DesignDocument
			}
		}
	}
	jsonDec := json.NewDecoder(resp.Body)
	err = jsonDec.Decode(&ddocsObj)
	if err != nil {
		return nil, err
	}

	var ddocs []*DesignDocument
	for _, ddocData := range ddocsObj.Rows {
		ddoc := &ddocData.Doc.Json
		ddoc.Name = ddocData.Doc.Meta.Id[8:]
		ddocs = append(ddocs, ddoc)
	}

	return ddocs, nil
}

func (bm *BucketManager) InsertDesignDocument(ddoc *DesignDocument) error {
	oldDdoc, _ := bm.GetDesignDocument(ddoc.Name)
	if oldDdoc != nil {
		return clientError{"Design document already exists"}
	}
	return bm.UpsertDesignDocument(ddoc)
}

func (bm *BucketManager) UpsertDesignDocument(ddoc *DesignDocument) error {
	reqUri := fmt.Sprintf("/_design/%s", ddoc.Name)

	data, err := json.Marshal(&ddoc)
	if err != nil {
		return err
	}

	resp, err := bm.capiRequest("PUT", reqUri, "application/json", bytes.NewReader(data))
	if err != nil {
		return err
	}

	if resp.StatusCode != 201 {
		data, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		resp.Body.Close()
		return clientError{string(data)}
	}

	return nil
}

func (bm *BucketManager) RemoveDesignDocument(name string) error {
	reqUri := fmt.Sprintf("/_design/%s", name)

	resp, err := bm.capiRequest("DELETE", reqUri, "", nil)
	if err != nil {
		return err
	}

	if resp.StatusCode != 200 {
		data, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		resp.Body.Close()
		return clientError{string(data)}
	}

	return nil
}
