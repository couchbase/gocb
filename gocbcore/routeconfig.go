package gocbcore

import (
	"fmt"
	"strings"
)

type routeConfig struct {
	revId uint

	kvServerList []string
	capiEpList   []string
	mgmtEpList   []string
	vbMap        [][]int

	source *cfgBucket
}

func buildRouteConfig(bk *cfgBucket, useSsl bool) *routeConfig {
	var kvServerList []string
	var capiEpList []string
	var mgmtEpList []string

	if bk.NodesExt != nil {
		var kvPort uint16
		for _, node := range bk.NodesExt {
			if !useSsl {
				kvPort = node.Services.Kv
			} else {
				kvPort = node.Services.KvSsl
			}

			// Hostname blank means to use the same one as was connected to
			if node.Hostname == "" {
				node.Hostname = bk.SourceHostname
			}

			kvServerList = append(kvServerList, fmt.Sprintf("%s:%d", node.Hostname, kvPort))

			if !useSsl {
				capiEpList = append(capiEpList, fmt.Sprintf("http://%s:%d/%s", node.Hostname, node.Services.Capi, bk.Name))
				mgmtEpList = append(mgmtEpList, fmt.Sprintf("http://%s:%d", node.Hostname, node.Services.Mgmt))
			} else {
				capiEpList = append(capiEpList, fmt.Sprintf("https://%s:%d/%s", node.Hostname, node.Services.CapiSsl, bk.Name))
				mgmtEpList = append(mgmtEpList, fmt.Sprintf("https://%s:%d", node.Hostname, node.Services.MgmtSsl))
			}
		}
	} else {
		if useSsl {
			panic("Received config without nodesExt while SSL is enabled.")
		}

		kvServerList = bk.VBucketServerMap.ServerList

		for _, node := range bk.Nodes {
			// Slice off the UUID as Go's HTTP client cannot handle being passed URL-Encoded path values.
			capiEp := strings.SplitN(node.CouchAPIBase, "%2B", 2)[0]

			// Add this node to the list of nodes.
			capiEpList = append(capiEpList, capiEp)
			mgmtEpList = append(mgmtEpList, fmt.Sprintf("http://%s", node.Hostname))
		}
	}

	return &routeConfig{
		revId:        0,
		kvServerList: kvServerList,
		capiEpList:   capiEpList,
		mgmtEpList:   mgmtEpList,
		vbMap:        bk.VBucketServerMap.VBucketMap,
		source:       bk,
	}
}
