package tls_client

import (
	"errors"
	"fmt"
	"net"
	"slices"
	"strings"

	http "github.com/bogdanfinn/fhttp"
	tls "github.com/bogdanfinn/utls"
	"github.com/tam7t/hpkp"
)

var DefaultBadPinHandler = func(req *http.Request) {
	fmt.Println("this is the default bad pin handler")
}

var ErrBadPinDetected = errors.New("bad ssl pin detected")

type certificatePinner struct {
	certificatePins map[string][]string
	storage         *hpkp.MemStorage
}

type CertificatePinner interface {
	Pin(conn *tls.UConn, host string) error
}

func NewCertificatePinner(certificatePins map[string][]string) (CertificatePinner, error) {
	pinner := &certificatePinner{
		certificatePins: certificatePins,
		storage:         hpkp.NewMemStorage(),
	}

	err := pinner.init()
	if err != nil {
		return nil, fmt.Errorf("failed to instantiate certificate pinner: %w", err)
	}

	return pinner, nil
}

func (cp *certificatePinner) init() error {
	normalized := make(map[string][]string, len(cp.certificatePins))
	for host, pinsByHost := range cp.certificatePins {
		includeSubdomains := strings.HasPrefix(host, "*.")
		if includeSubdomains {
			host = strings.TrimPrefix(host, "*.")
		}
		var err error
		host, err = canonicalPinHost(host)
		if err != nil {
			return err
		}
		if includeSubdomains && net.ParseIP(host) != nil {
			return errors.New("certificate pin wildcard requires a DNS name")
		}

		if prior, ok := normalized[host]; ok {
			pinnedHost := cp.storage.Lookup(host)
			if !slices.Equal(prior, pinsByHost) || pinnedHost.IncludeSubDomains != includeSubdomains {
				return errors.New("conflicting certificate pins for a canonical host")
			}
			continue
		}
		pins := slices.Clone(pinsByHost)
		normalized[host] = pins
		cp.storage.Add(host, &hpkp.Header{
			Permanent:         true,
			Sha256Pins:        pins,
			IncludeSubDomains: includeSubdomains,
		})
	}
	cp.certificatePins = normalized
	return nil
}

func canonicalPinHost(host string) (string, error) {
	if address := net.ParseIP(host); address != nil {
		return address.String(), nil
	}
	host = strings.ToLower(strings.TrimSuffix(host, "."))
	if host == "" || strings.ContainsAny(host, "*:\\/?#@[] \t\r\n\x00") {
		return "", errors.New("invalid certificate pin host")
	}
	for _, label := range strings.Split(host, ".") {
		if label == "" {
			return "", errors.New("invalid certificate pin host")
		}
	}
	return host, nil
}

func (cp *certificatePinner) Pin(conn *tls.UConn, host string) error {
	validPin := false

	if len(cp.certificatePins) == 0 {
		return nil
	}

	var err error
	host, err = canonicalPinHost(host)
	if err != nil {
		return err
	}
	pinnedHost := cp.storage.Lookup(host)

	if pinnedHost == nil {
		// host is not pinned, we treat it as valid
		return nil
	}

	var actualPins []string

	for _, peerCert := range conn.ConnectionState().PeerCertificates {
		peerPin := hpkp.Fingerprint(peerCert)
		actualPins = append(actualPins, peerPin)

		if pinnedHost.Matches(peerPin) {
			validPin = true
		}
	}

	if !validPin {
		return fmt.Errorf("%w, found pins: %v", ErrBadPinDetected, actualPins)
	}

	return nil
}
