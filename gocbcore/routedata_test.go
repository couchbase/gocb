package gocbcore

import (
	"testing"
)

func TestRouteDataPtr(t *testing.T) {
	var rd routeDataPtr
	if rd.get() != nil {
		t.Errorf("Route Data should start with nil")
	}
}
