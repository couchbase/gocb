package gocb

import (
	"fmt"
	"io/ioutil"
	"testing"
	"time"
)

type testBeer struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	Type string `json:"type"`
}

func assertAnalyticsBeers(t *testing.T, res AnalyticsResults, beers []testBeer) {
	var row testBeer
	i := 0
	for res.Next(&row) {
		if row.ID != beers[i].ID {
			t.Logf("Beers not equal, expected %s but was %s", row.ID, beers[i].ID)
			t.Fail()
			return
		}
		i++
	}
	if err := res.Close(); err != nil {
		t.Logf("Error closing rows: %v", err)
		t.Fail()
	}
}

func assertAnalyticsMetric(t *testing.T, metric uint, expected uint, metricName string) {
	if metric != expected {
		t.Logf("Expected %s to be %d, was %d", metricName, expected, metric)
		t.Fail()
	}
}

func TestAnalyticsQueryExecution(t *testing.T) {
	var beers []testBeer
	err := loadTestDataset("beer", &beers)
	if err != nil {
		t.Fatal(err)
	}
	ep := "http://testip:8095"
	opts := make(map[string]interface{})
	q1 := NewAnalyticsQuery("SELECT id, name, `type` FROM `allbeers` WHERE `type`='beer' ORDER BY id ASC")

	response, err := ioutil.ReadFile("testdata/beerAnalyticsResponse.json")
	if err != nil {
		t.Fatal(err)
	}

	creds := []UserPassPair{}

	client := newMockHTTPClient(t, fmt.Sprintf("%s/analytics/service", ep), "POST", response)
	opts["statement"] = q1

	res, err := globalCluster.executeAnalyticsQuery(globalBucket.tracer.StartSpan("test").Context(), ep, opts, creds, 100*time.Millisecond, client)
	if err != nil {
		t.Fatal(err)
	}
	assertAnalyticsBeers(t, res, beers[1:])
	metrics := res.Metrics()
	assertAnalyticsMetric(t, metrics.ErrorCount, 0, "ErrorCount")
	assertAnalyticsMetric(t, metrics.MutationCount, 0, "MutationCount")
	assertAnalyticsMetric(t, metrics.ResultCount, 10, "ResultCount")
	assertAnalyticsMetric(t, metrics.SortCount, 0, "SortCount")
	assertAnalyticsMetric(t, metrics.WarningCount, 0, "WarningCount")
	assertAnalyticsMetric(t, metrics.ResultSize, 997, "ResultSize")
	assertAnalyticsMetric(t, metrics.ProcessedObjects, 10, "ProcessedObjects")
}

func TestAnalyticsQuery(t *testing.T) {
	qTxt := "SELECT id, name, `type` FROM `allbeers` WHERE `type`='brewery'"
	clientId := "12345678910"
	q := NewAnalyticsQuery(qTxt).
		Pretty(true).Priority(true).ServerSideTimeout(1 * time.Minute).ContextId(clientId)

	if q.options["statement"] != qTxt {
		t.Logf("Expected statement to be %s but was %v", qTxt, q.options["statement"])
		t.Fail()
	}
	if q.options["pretty"] != true {
		t.Logf("Expected pretty to be true but was %v", q.options["pretty"])
		t.Fail()
	}
	if q.options["priority"] != -1 {
		t.Logf("Expected pretty to be -1 but was %v", q.options["priority"])
		t.Fail()
	}
	if q.options["timeout"] != (1 * time.Minute).String() {
		t.Logf("Expected timeout to be 1 minute but was %v", q.options["timeout"])
		t.Fail()
	}
	if q.options["client_context_id"] != clientId {
		t.Logf("Expected client_context_id to be %s but was %v", clientId, q.options["client_context_id"])
		t.Fail()
	}

	q.Priority(false)
	if _, ok := q.options["priority"]; ok {
		t.Logf("Expected priority to be missing but was %v", q.options["priority"])
		t.Fail()
	}

	q.Deferred(true)
	if q.options["mode"] != "async" {
		t.Logf("Expected async to be %s but was %v", "async", q.options["mode"])
		t.Fail()
	}
}
