package gocb

// CertificateAuthenticator is included for backwards compatibility only.
// Deprecated: Use CertAuthenticator instead.
type CertificateAuthenticator struct {
	CertAuthenticator
}

// Credentials returns the credentials for a particular service.
func (ca CertificateAuthenticator) Credentials(req AuthCredsRequest) ([]UserPassPair, error) {
	return ca.CertAuthenticator.Credentials(req)
}
