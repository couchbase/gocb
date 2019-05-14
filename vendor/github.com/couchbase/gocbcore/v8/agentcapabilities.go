package gocbcore

import "sync/atomic"

func (agent *Agent) buildClusterCapabilities(bk *cfgBucket) ClusterCapability {
	if bk.ClusterCapabilitiesVer == nil || len(bk.ClusterCapabilitiesVer) == 0 || bk.ClusterCapabilities == nil {
		return 0
	}

	var agentCapabilities ClusterCapability
	if bk.ClusterCapabilitiesVer[0] == 1 {
		for category, catCapabilities := range bk.ClusterCapabilities {
			switch category {
			case "n1ql":
				for _, capability := range catCapabilities {
					switch capability {
					case "enhancedPreparedStatements":
						agentCapabilities |= ClusterCapabilityEnhancedPreparedStatements
					}
				}
			}
		}
	}

	return agentCapabilities
}

func (agent *Agent) updateClusterCapabilities(bk *cfgBucket) {
	agentCapabilities := agent.buildClusterCapabilities(bk)
	if agentCapabilities == 0 {
		return
	}

	atomic.StoreUint32(&agent.clusterCapabilities, uint32(agentCapabilities))
}

// SupportsClusterCapability returns whether or not the cluster supports a given capability.
func (agent *Agent) SupportsClusterCapability(capability ClusterCapability) bool {
	capabilities := ClusterCapability(atomic.LoadUint32(&agent.clusterCapabilities))

	return capabilities&capability != 0
}
