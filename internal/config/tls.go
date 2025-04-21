package config

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

func SetupTLSConfig(cfg TLSConfig) (*tls.Config, error) {
	var err error
	tlsConfig := &tls.Config{}
	if cfg.CertFile != "" && cfg.KeyFile != "" {
		// load the certificates into the tls config
		tlsConfig.Certificates = make([]tls.Certificate, 1)
		// read the public and private key pairs from the pem files
		tlsConfig.Certificates[0], err = tls.LoadX509KeyPair(cfg.CAFile, cfg.KeyFile)
		if err != nil {
			return nil, err
		}
	}
	if cfg.CAFile != "" {
		b, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, err
		}

		// parse root certs
		ca := x509.NewCertPool()
		if ok := ca.AppendCertsFromPEM([]byte(b)); !ok {
			return nil, fmt.Errorf("failed tp parse root certificate: %q", cfg.CAFile)
		}

		// configure CA for client if initiator is a server
		if cfg.Server {
			tlsConfig.ClientCAs = ca
			tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
		} else {
			tlsConfig.RootCAs = ca
		}
		tlsConfig.ServerName = cfg.ServerAddress
	}
	return tlsConfig, nil
}

type TLSConfig struct {
	CertFile      string
	KeyFile       string
	CAFile        string
	ServerAddress string
	Server        bool
}
