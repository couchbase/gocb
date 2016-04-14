package gocb

import (
	"runtime"
	"testing"
)

func (s *connSpec) findHost(hostname string, scheme connSpecScheme) (a *connSpecAddr) {
	if scheme == csInvalid || scheme.IsHTTP() {
		for _, host := range s.HttpHosts {
			if hostname == host.Host {
				return host
			}
		}
	}
	if scheme == csInvalid || scheme.IsMCD() {
		for _, host := range s.MemcachedHosts {
			if hostname == host.Host {
				return host
			}
		}
	}
	return nil
}

// Check if the host is present in both formats in its default form
func (s *connSpec) hasBoth(hostname string) bool {
	if tmphost := s.findHost(hostname, csPlainHttp); tmphost == nil || tmphost.Port != uint16(csPlainHttp.DefaultPort()) {
		return false
	}
	if tmphost := s.findHost(hostname, csPlainMcd); tmphost == nil || tmphost.Port != uint16(csPlainMcd.DefaultPort()) {
		return false
	}
	return true
}

func parseOrDie(connstr string, t *testing.T) (cs connSpec) {
	_, _, line, _ := runtime.Caller(1)
	cs, err := parseConnSpec(connstr)
	if err != nil {
		t.Fatalf("(called from line %d) Failed to parse %s: %v", line, connstr, err)
	}
	return cs
}

func TestParseBasic(t *testing.T) {
	cs := parseOrDie("couchbase://1.2.3.4", t)
	if len(cs.HttpHosts) != 1 || len(cs.MemcachedHosts) != 1 {
		t.Fatalf("Expected 1 memcached and 1 http host!")
	}

	if tmphost := cs.findHost("1.2.3.4", csPlainMcd); tmphost.Port != 11210 {
		t.Fatalf("Wrong memcached port found (%d)", tmphost.Port)
	}

	if tmphost := cs.findHost("1.2.3.4", csPlainHttp); tmphost.Port != 8091 {
		t.Fatalf("Wrong HTTP port found!")
	}

	cs, err := parseConnSpec("blah://foo.com")
	if err == nil {
		t.Fatalf("Expected error for bad scheme")
	}

	cs = parseOrDie("couchbase://", t)
	if len(cs.MemcachedHosts) < 1 {
		t.Fatalf("Scheme-only spec should inject localhost by default!")
	}
	if tmphost := cs.findHost("127.0.0.1", csPlainMcd); tmphost.Port != uint16(csPlainMcd.DefaultPort()) {
		t.Fatalf("Bad implicit parsing of hostless connspec")
	}

	cs = parseOrDie("couchbase://?", t)
	if len(cs.Options) != 0 {
		t.Fatalf("Options should be 0 with couchbase://?")
	}
	if cs.findHost("127.0.0.1", csPlainMcd) == nil {
		t.Fatalf("Parsed couchbase://? with '?' as host!")
	}

	cs = parseOrDie("1.2.3.4", t)
	if !cs.hasBoth("1.2.3.4") {
		t.Fatalf("Expected one Mcd and one Htp entry for schemeless host!")
	}

	cs = parseOrDie("1.2.3.4:8091", t)
	if !cs.hasBoth("1.2.3.4") {
		t.Fatalf("implicit xxx:8091 should contain both host types!")
	}

	cs, err = parseConnSpec("1.2.3.4:999")
	if err == nil {
		t.Fatalf("Expected error with non-default port without scheme")
	}
}

func TestParseHosts(t *testing.T) {
	cs := parseOrDie("couchbase://foo.com,bar.com,baz.com", t)
	if len(cs.MemcachedHosts) != 3 || len(cs.HttpHosts) != 3 {
		t.Fatalf("Memcached and HTTP hosts not at expected count")
	}

	// Parse using legacy format
	cs, err := parseConnSpec("couchbase://foo.com:8091")
	if err == nil {
		t.Fatalf("Expected error for couchbase://XXX:8091")
	}

	cs = parseOrDie("couchbase://foo.com:4444", t)
	if tmphost := cs.findHost("foo.com", csPlainMcd); tmphost.Port != 4444 {
		t.Fatalf("couchbase://foo.com:4444 - want explicit mcd port")
	}
	if tmphost := cs.findHost("foo.com", csPlainHttp); tmphost != nil {
		t.Fatalf("Derived HTTP port found when none should be present")
	}

	cs = parseOrDie("couchbases://foo.com:4444", t)
	if tmphost := cs.findHost("foo.com", csPlainMcd); tmphost.Port != 4444 {
		t.Fatalf("Explicit SSL port not set!")
	}
	if tmphost := cs.findHost("foo.com", csPlainHttp); tmphost != nil {
		t.Fatalf("HTTP Host found with ssl explicit port")
	}

	cs = parseOrDie("couchbases://", t)
	if tmphost := cs.findHost("127.0.0.1", csPlainMcd); tmphost.Port != uint16(csSslMcd.DefaultPort()) {
		t.Fatal("Implicit couchbases scheme didn't set SSL/Mcd port!")
	}

	if tmphost := cs.findHost("127.0.0.1", csPlainHttp); tmphost.Port != uint16(csSslHttp.DefaultPort()) {
		t.Fatal("Implicit couchbases scheme didn't set SSL/HTTP port")
	}

	cs = parseOrDie("couchbase://foo.com,bar.com:4444", t)
	if tmphost := cs.findHost("bar.com", csPlainMcd); tmphost == nil || tmphost.Port != 4444 {
		t.Fatalf("Couldn't find explicit mcd host!")
	}
	if tmphost := cs.findHost("bar.com", csPlainHttp); tmphost != nil {
		t.Fatalf("can't derive http host with non-default mcd port")
	}
	if !cs.hasBoth("foo.com") {
		t.Fatalf("Portless host is not found in both mcd and http")
	}

	// Check old-style delimiters
	cs = parseOrDie("couchbase://foo.com;bar.com;baz.com", t)
	if !cs.hasBoth("foo.com") || !cs.hasBoth("bar.com") || !cs.hasBoth("baz.com") {
		t.Fatalf("Failed to parse all hosts in the spec!")
	}
}

func TestParseBucket(t *testing.T) {
	cs := parseOrDie("couchbase://foo.com/user", t)
	if cs.Bucket != "user" {
		t.Fatalf("Wrong bucket name. Expected user. Got %s", cs.Bucket)
	}

	cs = parseOrDie("couchbase://foo.com/user/", t)
	if cs.Bucket != "user/" {
		t.Fatalf("Wrong bucket name! (exp: user/, got: %s", cs.Bucket)
	}

	cs = parseOrDie("couchbase:///default", t)
	if cs.Bucket != "default" {
		t.Fatalf("Expected 'default' bucket without host. Got %s", cs.Bucket)
	}

	cs = parseOrDie("couchbase:///default?", t)
	if cs.Bucket != "default" {
		t.Fatalf("Expected 'default' bucket wihtout host. Got %s", cs.Bucket)
	}

	cs = parseOrDie("couchbase:///%2FUsers%2F?", t)
	if cs.Bucket != "/Users/" {
		t.Fatalf("Expected URL-encoding escaping. Got %s", cs.Bucket)
	}
}

func TestOptionsPassthrough(t *testing.T) {
	cs := parseOrDie("couchbase:///?foo=bar", t)
	if cs.Options.Get("foo") != "bar" || len(cs.Options) != 1 {
		t.Fatalf("Couldn't match options (///?)")
	}

	cs = parseOrDie("couchbase://?foo=bar", t)
	if cs.Options.Get("foo") != "bar" || len(cs.Options) != 1 {
		t.Fatalf("Couldn't match options (//?)")
	}

	/*
		cs, err := parseConnSpec("couchbase://?foo")
		if err == nil {
			t.Fatalf("Expected error for value-less option!")
		}
	*/

	cs = parseOrDie("couchbase://?foo=fooval&bar=barval", t)
	if cs.Options.Get("foo") != "fooval" || cs.Options.Get("bar") != "barval" {
		t.Fatalf("Missing options")
	}
	if len(cs.Options) != 2 {
		t.Fatalf("Too many options!")
	}

	cs = parseOrDie("couchbase://?foo=fooval&bar=barval&", t)
	if cs.Options.Get("foo") != "fooval" || cs.Options.Get("bar") != "barval" || len(cs.Options) != 2 {
		t.Fatal("Didn't get expected options!")
	}

	// TODO: bootstrap_on
}
