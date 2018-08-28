package gocb

import (
	"testing"
	"time"
)

// type testBeer struct {
// 	ID   string `json:"id"`
// 	Name string `json:"name"`
// 	Type string `json:"type"`
// }

// func createBeerDataset(t *testing.T) []testBeer {
// 	var beers []testBeer
// 	err := loadTestDataset("beer", &beers)
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	for _, beer := range beers {
// 		_, err := globalBucket.Upsert(beer.ID, beer, 0)
// 		if err != nil {
// 			t.Fatal(err)
// 		}
// 	}

// 	return beers
// }

// func dropBeerDataset(t *testing.T) {
// 	creds, err := globalCluster.auth.Credentials(AuthCredsRequest{
// 		Service: MgmtService,
// 	})
// 	if err != nil {
// 		logErrorf("Couldn't flush beer dataset %v", err)
// 	}

// 	err = globalBucket.Manager(creds[0].Username, creds[0].Password).Flush()
// 	if err != nil {
// 		logErrorf("Couldn't flush beer dataset %v", err)
// 	}
// }

// func createAnalyticsDataset(t *testing.T) {
// 	_, err := globalBucket.ExecuteAnalyticsQuery(
// 		NewAnalyticsQuery("CREATE DATASET allbeers on `default`;"),
// 		nil,
// 	)
// 	if err != nil {
// 		t.Fatal(err)
// 	}

// 	_, err = globalBucket.ExecuteAnalyticsQuery(
// 		NewAnalyticsQuery("CONNECT LINK Local"),
// 		nil,
// 	)
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// }

// func dropAnalyticsDataset(t *testing.T) {
// 	_, err := globalBucket.ExecuteAnalyticsQuery(
// 		NewAnalyticsQuery("DISCONNECT LINK Local;"),
// 		nil,
// 	)
// 	if err != nil {
// 		t.Logf("Couldn't drop analytics dataset %v", err)
// 	}

// 	_, err = globalBucket.ExecuteAnalyticsQuery(
// 		NewAnalyticsQuery("DROP DATASET allbeers;"),
// 		nil,
// 	)
// 	if err != nil {
// 		t.Logf("Couldn't drop analytics dataset %v", err)
// 	}
// }

// func assertBeers(t *testing.T, res AnalyticsResults, beers []testBeer) {
// 	var row testBeer
// 	i := 0
// 	for res.Next(&row) {
// 		if row.ID != beers[i].ID {
// 			t.Logf("Beers not equal, expected %s but was %s", row.ID, beers[i].ID)
// 			t.Fail()
// 			return
// 		}
// 		i++
// 	}
// 	if err := res.Close(); err != nil {
// 		t.Logf("Error closing rows: %v", err)
// 		t.Fail()
// 	}
// }

// func assertMetric(t *testing.T, metric uint, expected uint, metricName string) {
// 	if metric != expected {
// 		t.Logf("Expected %s to be %d, was %d", metricName, expected, metric)
// 		t.Fail()
// 	}
// }

// func TestAnalyticsQueryExecution(t *testing.T) {
// 	dropBeerDataset(t)
// 	beers := createBeerDataset(t)
// 	createAnalyticsDataset(t)
// 	defer dropBeerDataset(t)
// 	defer dropAnalyticsDataset(t)

// 	time.Sleep(500 * time.Millisecond)

// 	q1 := NewAnalyticsQuery("SELECT id, name, `type` FROM `allbeers` WHERE `type`='beer' ORDER BY id ASC")
// 	res, err := globalBucket.ExecuteAnalyticsQuery(q1, nil)
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	assertBeers(t, res, beers[1:])
// 	metrics := res.Metrics()
// 	assertMetric(t, metrics.ErrorCount, 0, "ErrorCount")
// 	assertMetric(t, metrics.MutationCount, 0, "MutationCount")
// 	assertMetric(t, metrics.ProcessedObjects, 10, "ProcessedObjects")
// 	assertMetric(t, metrics.ResultCount, 9, "ResultCount")
// 	assertMetric(t, metrics.SortCount, 0, "SortCount")
// 	assertMetric(t, metrics.WarningCount, 0, "WarningCount")
// 	if metrics.ResultSize == 0 {
// 		t.Logf("Expected ResultSize to be greater than 0, was %d", metrics.ResultSize)
// 		t.Fail()
// 	}

// 	qPositionalParam := NewAnalyticsQuery("SELECT id, name, `type` FROM `allbeers` WHERE `type`=$1 ORDER BY id ASC")
// 	res, err = globalBucket.ExecuteAnalyticsQuery(qPositionalParam, []interface{}{"beer"})
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	assertBeers(t, res, beers[1:])
// 	metrics = res.Metrics()
// 	assertMetric(t, metrics.ErrorCount, 0, "ErrorCount")
// 	assertMetric(t, metrics.MutationCount, 0, "MutationCount")
// 	assertMetric(t, metrics.ProcessedObjects, 10, "ProcessedObjects")
// 	assertMetric(t, metrics.ResultCount, 9, "ResultCount")
// 	assertMetric(t, metrics.SortCount, 0, "SortCount")
// 	assertMetric(t, metrics.WarningCount, 0, "WarningCount")
// 	if metrics.ResultSize == 0 {
// 		t.Logf("Expected ResultSize to be greater than 0, was %d", metrics.ResultSize)
// 		t.Fail()
// 	}

// 	qNamedParam := NewAnalyticsQuery("SELECT id, name, `type` FROM `allbeers` WHERE `type`=$param ORDER BY id ASC")
// 	params := make(map[string]interface{})
// 	params["param"] = "beer"
// 	res, err = globalBucket.ExecuteAnalyticsQuery(qNamedParam, params)
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	assertBeers(t, res, beers[1:])
// 	metrics = res.Metrics()
// 	assertMetric(t, metrics.ErrorCount, 0, "ErrorCount")
// 	assertMetric(t, metrics.MutationCount, 0, "MutationCount")
// 	assertMetric(t, metrics.ProcessedObjects, 10, "ProcessedObjects")
// 	assertMetric(t, metrics.ResultCount, 9, "ResultCount")
// 	assertMetric(t, metrics.SortCount, 0, "SortCount")
// 	assertMetric(t, metrics.WarningCount, 0, "WarningCount")
// 	if metrics.ResultSize == 0 {
// 		t.Logf("Expected ResultSize to be greater than 0, was %d", metrics.ResultSize)
// 		t.Fail()
// 	}
// }

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
}
