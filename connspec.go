package gocouchbase

import "fmt"
import "regexp"
import "strconv"

// A single address stored within a connection string
type connSpecAddr struct {
	Host string
	Port int
}

// A parsed connection string
type connSpec struct {
	Scheme  string
	Hosts   []connSpecAddr
	Bucket  string
	Options map[string]string
}

// Parses a connection string into a structure more easily consumed by the library.
func parseConnSpec(connStr string) connSpec {
	var out connSpec
	out.Options = map[string]string{}

	partMatcher := regexp.MustCompile(`((.*):\/\/)?([^\/?]*)(\/([^\?]*))?(\?(.*))?`)
	hostMatcher := regexp.MustCompile(`([^;\,\:]+)(:([0-9]*))?(;\,)?`)
	kvMatcher := regexp.MustCompile(`([^=]*)=([^&?]*)[&?]?`)
	parts := partMatcher.FindStringSubmatch(connStr)

	if parts[2] != "" {
		out.Scheme = parts[2]
	}

	if parts[3] != "" {
		hosts := hostMatcher.FindAllStringSubmatch(parts[3], -1)
		for _, hostInfo := range hosts {
			port := 0
			if hostInfo[3] != "" {
				port, _ = strconv.Atoi(hostInfo[3])
			}

			out.Hosts = append(out.Hosts, connSpecAddr{
				Host: hostInfo[1],
				Port: port,
			})
		}
	}

	if parts[5] != "" {
		out.Bucket = parts[5]
	}

	if parts[7] != "" {
		kvs := kvMatcher.FindAllStringSubmatch(parts[7], -1)
		for _, kvInfo := range kvs {
			out.Options[kvInfo[1]] = kvInfo[2]
		}
	}

	return out
}

// Guesses a list of memcached hosts based on a connection string specification structure.
func csGetMemdHosts(spec connSpec) []connSpecAddr {
	var out []connSpecAddr
	for _, host := range spec.Hosts {
		memdHost := connSpecAddr{
			Host: host.Host,
			Port: 0,
		}

		fmt.Printf("Host parse: %s:%d", host.Host, host.Port)

		if host.Port == 0 {
			if spec.Scheme != "couchbases" {
				memdHost.Port = 11210
			} else {
				memdHost.Port = 11207
			}
		} else if host.Port == 8091 {
			memdHost.Port = 11210
		} else if host.Port == 18091 {
			memdHost.Port = 11207
		}

		if memdHost.Port != 0 {
			out = append(out, memdHost)
		}
	}
	return out
}
