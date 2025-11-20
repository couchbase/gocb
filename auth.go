package gocb

import (
	"crypto/tls"

	gocbcore "github.com/couchbase/gocbcore/v10"
)

// UserPassPair represents a username and password pair.
// VOLATILE: This API is subject to change at any time.
type UserPassPair gocbcore.UserPassPair

// AuthCredsRequest encapsulates the data for a credential request
// from the new Authenticator interface.
// VOLATILE: This API is subject to change at any time.
type AuthCredsRequest struct {
	Service  ServiceType
	Endpoint string
}

// AuthCertRequest encapsulates the data for a certificate request
// from the new Authenticator interface.
// VOLATILE: This API is subject to change at any time.
type AuthCertRequest struct {
	Service  ServiceType
	Endpoint string
}

// Authenticator provides an interface to authenticate to each service.  Note that
// only authenticators implemented via the SDK are stable.
type Authenticator interface {
	// VOLATILE: This API is subject to change at any time.
	SupportsTLS() bool

	// VOLATILE: This API is subject to change at any time.
	SupportsNonTLS() bool

	// VOLATILE: This API is subject to change at any time.
	Certificate(req AuthCertRequest) (*tls.Certificate, error)

	// VOLATILE: This API is subject to change at any time.
	Credentials(req AuthCredsRequest) ([]UserPassPair, error)

	// VOLATILE: This API is subject to change at any time.
	JWT(req AuthCredsRequest) (JWT, error)

	// VOLATILE: This API is subject to change at any time.
	DefaultSaslMechanisms(tlsEnabled bool) []SaslMechanism
}

// JWT represents a JWT token.
type JWT = string

// PasswordAuthenticator implements an Authenticator which uses an RBAC username and password.
type PasswordAuthenticator struct {
	Username string
	Password string
}

// SupportsTLS returns whether this authenticator can authenticate a TLS connection.
// VOLATILE: This API is subject to change at any time.
func (ra PasswordAuthenticator) SupportsTLS() bool {
	return true
}

// SupportsNonTLS returns whether this authenticator can authenticate a non-TLS connection.
// VOLATILE: This API is subject to change at any time.
func (ra PasswordAuthenticator) SupportsNonTLS() bool {
	return true
}

// Certificate returns the certificate to use when connecting to a specified server.
// VOLATILE: This API is subject to change at any time.
func (ra PasswordAuthenticator) Certificate(req AuthCertRequest) (*tls.Certificate, error) {
	return nil, nil
}

// Credentials returns the credentials for a particular service.
// VOLATILE: This API is subject to change at any time.
func (ra PasswordAuthenticator) Credentials(req AuthCredsRequest) ([]UserPassPair, error) {
	return []UserPassPair{{
		Username: ra.Username,
		Password: ra.Password,
	}}, nil
}

func (ra PasswordAuthenticator) JWT(req AuthCredsRequest) (JWT, error) {
	return "", nil
}

func (ra PasswordAuthenticator) DefaultSaslMechanisms(tlsEnabled bool) []SaslMechanism {
	if tlsEnabled {
		return []SaslMechanism{ScramSha512SaslMechanism, ScramSha256SaslMechanism, ScramSha1SaslMechanism}
	}

	return []SaslMechanism{PlainSaslMechanism}
}

// CertificateAuthenticator implements an Authenticator which can be used with certificate authentication.
type CertificateAuthenticator struct {
	ClientCertificate *tls.Certificate
}

// SupportsTLS returns whether this authenticator can authenticate a TLS connection.
// VOLATILE: This API is subject to change at any time.
func (ca CertificateAuthenticator) SupportsTLS() bool {
	return true
}

// SupportsNonTLS returns whether this authenticator can authenticate a non-TLS connection.
// VOLATILE: This API is subject to change at any time.
func (ca CertificateAuthenticator) SupportsNonTLS() bool {
	return false
}

// Certificate returns the certificate to use when connecting to a specified server.
// VOLATILE: This API is subject to change at any time.
func (ca CertificateAuthenticator) Certificate(req AuthCertRequest) (*tls.Certificate, error) {
	return ca.ClientCertificate, nil
}

// Credentials returns the credentials for a particular service.
// VOLATILE: This API is subject to change at any time.
func (ca CertificateAuthenticator) Credentials(req AuthCredsRequest) ([]UserPassPair, error) {
	return []UserPassPair{{
		Username: "",
		Password: "",
	}}, nil
}

func (ca CertificateAuthenticator) JWT(req AuthCredsRequest) (JWT, error) {
	return "", nil
}

func (ca CertificateAuthenticator) DefaultSaslMechanisms(tlsEnabled bool) []SaslMechanism {
	return []SaslMechanism{}
}

type JWTAuthenticator struct {
	Token string
}

// SupportsTLS returns whether this authenticator can authenticate a TLS connection.
// VOLATILE: This API is subject to change at any time.
func (ja JWTAuthenticator) SupportsTLS() bool {
	return true
}

// SupportsNonTLS returns whether this authenticator can authenticate a non-TLS connection.
// VOLATILE: This API is subject to change at any time.
func (ja JWTAuthenticator) SupportsNonTLS() bool {
	return false
}

// Certificate returns the certificate to use when connecting to a specified server.
// VOLATILE: This API is subject to change at any time.
func (ja JWTAuthenticator) Certificate(req AuthCertRequest) (*tls.Certificate, error) {
	return nil, nil
}

// Credentials returns the credentials for a particular service.
// VOLATILE: This API is subject to change at any time.
func (ja JWTAuthenticator) Credentials(req AuthCredsRequest) ([]UserPassPair, error) {
	return []UserPassPair{{
		Username: "",
		Password: "",
	}}, nil
}

func (ja JWTAuthenticator) JWT(req AuthCredsRequest) (JWT, error) {
	return ja.Token, nil
}

func (ja JWTAuthenticator) DefaultSaslMechanisms(tlsEnabled bool) []SaslMechanism {
	return []SaslMechanism{OAuthBearerSaslMechasnism}
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

func (auth *coreAuthWrapper) JWT(req gocbcore.AuthCredsRequest) (gocbcore.JWT, error) {
	return auth.auth.JWT(AuthCredsRequest{
		Service:  ServiceType(req.Service),
		Endpoint: req.Endpoint,
	})
}

func (auth *coreAuthWrapper) DefaultAuthMechanisms(tlsEnabled bool) []gocbcore.AuthMechanism {
	mechs := auth.auth.DefaultSaslMechanisms(tlsEnabled)
	var coreMechs []gocbcore.AuthMechanism
	for _, mech := range mechs {
		coreMechs = append(coreMechs, gocbcore.AuthMechanism(mech))
	}

	return coreMechs
}
