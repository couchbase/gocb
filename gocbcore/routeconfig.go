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
	n1qlEpList   []string
	vbMap        [][]int

	source *cfgBucket
}

func buildRouteConfig(bk *cfgBucket, useSsl bool) *routeConfig {
	var kvServerList []string
	var capiEpList []string
	var mgmtEpList []string
	var n1qlEpList []string

	if bk.NodesExt != nil {
		for _, node := range bk.NodesExt {
			// Hostname blank means to use the same one as was connected to
			if node.Hostname == "" {
				node.Hostname = bk.SourceHostname
			}

			if !useSsl {
				if node.Services.Kv > 0 {
					kvServerList = append(kvServerList, fmt.Sprintf("%s:%d", node.Hostname, node.Services.Kv))
				}
				if node.Services.Capi > 0 {
					capiEpList = append(capiEpList, fmt.Sprintf("http://%s:%d/%s", node.Hostname, node.Services.Capi, bk.Name))
				}
				if node.Services.Mgmt > 0 {
					mgmtEpList = append(mgmtEpList, fmt.Sprintf("http://%s:%d", node.Hostname, node.Services.Mgmt))
				}
				if node.Services.N1ql > 0 {
					n1qlEpList = append(n1qlEpList, fmt.Sprintf("http://%s:%d", node.Hostname, node.Services.N1ql))
				}
			} else {
				if node.Services.KvSsl > 0 {
					kvServerList = append(kvServerList, fmt.Sprintf("%s:%d", node.Hostname, node.Services.KvSsl))
				}
				if node.Services.CapiSsl > 0 {
					capiEpList = append(capiEpList, fmt.Sprintf("https://%s:%d/%s", node.Hostname, node.Services.CapiSsl, bk.Name))
				}
				if node.Services.MgmtSsl > 0 {
					mgmtEpList = append(mgmtEpList, fmt.Sprintf("https://%s:%d", node.Hostname, node.Services.MgmtSsl))
				}
				if node.Services.N1qlSsl > 0 {
					n1qlEpList = append(n1qlEpList, fmt.Sprintf("https://%s:%d", node.Hostname, node.Services.N1qlSsl))
				}
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
		n1qlEpList:   n1qlEpList,
		vbMap:        bk.VBucketServerMap.VBucketMap,
		source:       bk,
	}
}
