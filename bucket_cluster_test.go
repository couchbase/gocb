package gocb

// Get the mock first!
import (
	"fmt"
	"testing"
)

func TestBadConnstr(t *testing.T) {
	_, err := Connect(fmt.Sprintf("couchbase://host.com/bucket"))
	if err == nil {
		t.Fatal("Explicit bucket should fail for Connect")
	}

	_, err = Connect("blah://bad_conn_str")
	if err == nil {
		t.Fatal("Connection should fail with bad scheme!")
	}
	_, err = Connect("couchbase://host.com?bootstrap_on=dummy")
	if err == nil {
		t.Fatal("Connection should fail with bad 'bootstrap_on' option")
	}
}

func TestBootstrapOn(t *testing.T) {
	connstr := "couchbase://foo.com,bar.com,baz.com"
	c, err := Connect(connstr)
	if err != nil {
		t.Fatalf("Multi-host connection string failed: %v", err)
	}
	if len(c.agentConfig.HttpAddrs) != 3 || len(c.agentConfig.MemdAddrs) != 3 {
		t.Fatal("Wrong number of hosts for http/memcached")
	}

	// Use http only
	c, err = Connect("couchbase://foo.com,bar.com,baz.com?bootstrap_on=http")
	if err != nil {
		t.Fatalf("bootstrap_on=http: %v", err)
	}
	if len(c.agentConfig.HttpAddrs) != 3 {
		t.Fatalf("HttpHosts is not 3 (%v)", c.agentConfig.HttpAddrs)
	}
	if len(c.agentConfig.MemdAddrs) != 0 {
		t.Fatalf("MemcachedHosts is not 0: %v", c.agentConfig.MemdAddrs)
	}

	c, err = Connect("couchbase://foo.com,bar.com,baz.com?bootstrap_on=cccp")
	if err != nil {
		t.Fatalf("bootstrap_on=cccp: %v", err)
	}
	if len(c.agentConfig.MemdAddrs) != 3 {
		t.Fatalf("Expected 3 hosts in memcached: %v", c.agentConfig.MemdAddrs)
	}
	if len(c.agentConfig.HttpAddrs) != 0 {
		t.Fatalf("Expected 0 hosts in http: %v", c.agentConfig.HttpAddrs)
	}

	// Should fail if there are no hosts
	c, err = Connect("couchbase://foo.com:12000?bootstrap_on=http")
	if err == nil {
		t.Fatal("Expected failure with explicit http without http hosts")
	}
	c, err = Connect("http://foo.com:9000?bootstrap_on=cccp")
	if err == nil {
		t.Fatal("Expected failure with explicit cccp without cccp hosts")
	}

}
