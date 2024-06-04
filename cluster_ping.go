package gocb

// Ping will ping a list of services and verify they are active and
// responding in an acceptable period of time.
func (c *Cluster) Ping(opts *PingOptions) (*PingResult, error) {
	if opts == nil {
		opts = &PingOptions{}
	}

	provider, err := c.getDiagnosticsProvider()
	if err != nil {
		return nil, err
	}

	return provider.Ping(opts)
}
