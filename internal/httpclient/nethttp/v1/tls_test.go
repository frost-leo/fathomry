/**
 * fathomry
 * Copyright (C) 2026  Frost Leo
 * SPDX-License-Identifier: GPL-3.0-or-later
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package nethttp

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/conformance"
	"github.com/frost-leo/fathomry/internal/resource"
)

func mutualPeer(t *testing.T, h2 bool) (*httptest.Server, OptionsV1, tls.Certificate, *x509.CertPool) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	root := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "synthetic-root"}, IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour)}
	der, err := x509.CreateCertificate(rand.Reader, root, root, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	authority, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(authority)
	issue := func(serial int64, usage x509.ExtKeyUsage) (tls.Certificate, string, string) {
		private, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		template := &x509.Certificate{SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: "synthetic-leaf"},
			NotBefore: root.NotBefore, NotAfter: root.NotAfter, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{usage},
			IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}}
		encoded, err := x509.CreateCertificate(rand.Reader, template, authority, &private.PublicKey, key)
		if err != nil {
			t.Fatal(err)
		}
		secret, err := x509.MarshalPKCS8PrivateKey(private)
		if err != nil {
			t.Fatal(err)
		}
		certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: encoded})
		keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: secret})
		certificate, err := tls.X509KeyPair(certPEM, keyPEM)
		if err != nil {
			t.Fatal(err)
		}
		return certificate, string(certPEM), string(keyPEM)
	}
	serverCert, _, _ := issue(2, x509.ExtKeyUsageServerAuth)
	clientCert, certPEM, keyPEM := issue(3, x509.ExtKeyUsageClientAuth)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.TLS == nil || len(request.TLS.PeerCertificates) != 1 {
			t.Error("client certificate not verified")
		}
		_, _ = io.WriteString(writer, "mutual")
	}))
	server.TLS = &tls.Config{Certificates: []tls.Certificate{serverCert}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: roots}
	server.EnableHTTP2 = h2
	server.StartTLS()
	t.Cleanup(server.Close)
	return server, OptionsV1{Name: "mutual", HTTP1: !h2, HTTP2: h2, RootCAPEM: string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})),
		ClientCertPEM: certPEM, ClientKeyPEM: keyPEM}, clientCert, roots
}

func TestTLSCertificatesAndNativeClientCertificateCallback(t *testing.T) {
	eachProtocol(t, func(t *testing.T, h2 bool) {
		for _, native := range []bool{false, true} {
			t.Run(map[bool]string{false: "configured", true: "native"}[native], func(t *testing.T) {
				server, options, certificate, roots := mutualPeer(t, h2)
				var callbacks atomic.Int64
				if native {
					options.RootCAPEM, options.ClientCertPEM, options.ClientKeyPEM = "", "", ""
					options.Native.TLS = &tls.Config{RootCAs: roots, GetClientCertificate: func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
						callbacks.Add(1)
						return &certificate, nil
					}}
				}
				f := bindFixture(t, options, 1)
				receipt, err := f.client.Do(deadline(t), deadline(t), correlation("mutual"), newRequest(t, "GET", server.URL, nil))
				if err != nil {
					t.Fatal(err)
				}
				result := settle(t, f, receipt)
				if !result.Outcome.Value.Complete() || string(result.Outcome.Value.DataCopy()) != "mutual" || native && callbacks.Load() != 1 {
					t.Fatal("native client authentication failed")
				}
			})
		}
	})
}

func TestCertificateAndHostnameFailuresPreserveNativeCause(t *testing.T) {
	for _, hostname := range []bool{false, true} {
		t.Run(map[bool]string{false: "trust", true: "hostname"}[hostname], func(t *testing.T) {
			server, options := newPeer(t, false, func(http.ResponseWriter, *http.Request) { t.Error("unverified TLS reached HTTP") })
			if hostname {
				options.ServerName = "wrong.invalid"
			} else {
				options.RootCAPEM = ""
				options.Native.TLS = &tls.Config{RootCAs: x509.NewCertPool()}
			}
			f := bindFixture(t, options, 1)
			receipt, err := f.client.Do(deadline(t), deadline(t), correlation("trust"), newRequest(t, "GET", server.URL, nil))
			if err == nil {
				t.Fatal("unverified TLS accepted")
			}
			result := settle(t, f, receipt)
			if hostname {
				conformance.Cause[x509.HostnameError](t, result.Err(), func(x509.HostnameError) bool { return true })
			} else {
				conformance.Cause[x509.UnknownAuthorityError](t, result.Err(), func(x509.UnknownAuthorityError) bool { return true })
			}
		})
	}
}

func TestNativeCallbackCapacityAndShutdownRemainBounded(t *testing.T) {
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	server, options := newPeer(t, false, func(http.ResponseWriter, *http.Request) {})
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM([]byte(options.RootCAPEM)) {
		t.Fatal("bad test roots")
	}
	var entered atomic.Int64
	options.RootCAPEM = ""
	options.MaxConnections, options.MaxActive = 1, 1
	options.Timeout = 50 * time.Millisecond
	options.Native.TLS = &tls.Config{RootCAs: roots, VerifyConnection: func(tls.ConnectionState) error { entered.Add(1); <-release; return nil }}
	f := bindFixture(t, options, 1)
	for index := range 6 {
		receipt, err := f.client.Do(deadline(t), deadline(t), correlation("callback"), newRequest(t, "GET", server.URL, nil))
		if err == nil {
			t.Fatal("blocked native verification completed")
		}
		if receipt != nil {
			settle(t, f, receipt)
		}
		if index == 5 && entered.Load() > 4 {
			t.Fatal("native callbacks exceeded the declared source bound")
		}
	}
	cleanup, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if err := f.assembly.Close(cleanup); !errors.Is(err, resource.ErrIncomplete) {
		t.Fatal("blocked callback lost its owner", err)
	}
	unblock()
	f.cleanupCause = context.DeadlineExceeded
	// Historical cleanup interruption remains inspectable after actual release.
	if err := f.assembly.Close(deadline(t)); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("cleanup history was erased", err)
	}
}
