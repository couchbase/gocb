package gocb

import (
	"crypto/tls"

	gocbcore "github.com/couchbase/gocbcore/v8"
)

// UserPassPair represents a username and password pair.
type UserPassPair gocbcore.UserPassPair

// AuthCredsRequest encapsulates the data for a credential request
// from the new Authenticator interface.
// UNCOMMITTED
type AuthCredsRequest struct {
	Service  ServiceType
	Endpoint string
}

// AuthCertRequest encapsulates the data for a certificate request
// from the new Authenticator interface.
// UNCOMMITTED
type AuthCertRequest struct {
	Service  ServiceType
	Endpoint string
}

// Authenticator provides an interface to authenticate to each service.  Note that
// only authenticators implemented here are stable, and support for custom
// authenticators is considered volatile.
type Authenticator interface {
	SupportsTLS() bool
	SupportsNonTLS() bool
	Certificate(req AuthCertRequest) (*tls.Certificate, error)
	Credentials(req AuthCredsRequest) ([]UserPassPair, error)
}

// PasswordAuthenticator implements an Authenticator which uses an RBAC username and password.
type PasswordAuthenticator struct {
	Username string
	Password string
}

// SupportsTLS returns whether this authenticator can authenticate a TLS connection
func (ra PasswordAuthenticator) SupportsTLS() bool {
	return true
}

// SupportsNonTLS returns whether this authenticator can authenticate a non-TLS connection
func (ra PasswordAuthenticator) SupportsNonTLS() bool {
	return true
}

// Certificate returns the certificate to use when connecting to a specified server
func (ra PasswordAuthenticator) Certificate(req AuthCertRequest) (*tls.Certificate, error) {
	return nil, nil
}

// Credentials returns the credentials for a particular service.
func (ra PasswordAuthenticator) Credentials(req AuthCredsRequest) ([]UserPassPair, error) {
	return []UserPassPair{{
		Username: ra.Username,
		Password: ra.Password,
	}}, nil
}

// CertificateAuthenticator implements an Authenticator which can be used with certificate authentication.
type CertificateAuthenticator struct {
	ClientCertificate *tls.Certificate
}

// SupportsTLS returns whether this authenticator can authenticate a TLS connection
func (ca CertificateAuthenticator) SupportsTLS() bool {
	return true
}

// SupportsNonTLS returns whether this authenticator can authenticate a non-TLS connection
func (ca CertificateAuthenticator) SupportsNonTLS() bool {
	return false
}

// Certificate returns the certificate to use when connecting to a specified server
func (ca CertificateAuthenticator) Certificate(req AuthCertRequest) (*tls.Certificate, error) {
	return ca.ClientCertificate, nil
}

// Credentials returns the credentials for a particular service.
func (ca CertificateAuthenticator) Credentials(req AuthCredsRequest) ([]UserPassPair, error) {
	return []UserPassPair{{
		Username: "",
		Password: "",
	}}, nil
}

func getSingleCredential(auth Authenticator, req AuthCredsRequest) (UserPassPair, error) {
	creds, err := auth.Credentials(req)
	if err != nil {
		return UserPassPair{}, err
	}

	if len(creds) != 1 {
		return UserPassPair{}, gocbcore.ErrInvalidCredentials
	}

	return creds[0], nil
}

type coreAuthWrapper struct {
	auth Authenticator
}

func (auth *coreAuthWrapper) SupportsTLS() bool {
	return auth.auth.SupportsTLS()
}

func (auth *coreAuthWrapper) SupportsNonTLS() bool {
	return auth.auth.SupportsNonTLS()
}

func (auth *coreAuthWrapper) Certificate(req gocbcore.AuthCertRequest) (*tls.Certificate, error) {
	return auth.auth.Certificate(AuthCertRequest{
		Service:  ServiceType(req.Service),
		Endpoint: req.Endpoint,
	})
}

func (auth *coreAuthWrapper) Credentials(req gocbcore.AuthCredsRequest) ([]gocbcore.UserPassPair, error) {
	creds, err := auth.auth.Credentials(AuthCredsRequest{
		Service:  ServiceType(req.Service),
		Endpoint: req.Endpoint,
	})
	if err != nil {
		return nil, err
	}

	coreCreds := make([]gocbcore.UserPassPair, len(creds))
	for credIdx, userPass := range creds {
		coreCreds[credIdx] = gocbcore.UserPassPair(userPass)
	}
	return coreCreds, nil
}
