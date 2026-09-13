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

package otel

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
	"log"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
)

type certificates struct {
	root, certificate, key string
	pair                   tls.Certificate
}

func testCertificates(t *testing.T, usage x509.ExtKeyUsage) certificates {
	t.Helper()
	rootKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	root := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "owned-test-root"}, IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour)}
	rootDER, err := x509.CreateCertificate(rand.Reader, root, root, &rootKey.PublicKey, rootKey)
	if err != nil {
		t.Fatal(err)
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	leaf := &x509.Certificate{SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "owned-test-leaf"}, ExtKeyUsage: []x509.ExtKeyUsage{usage},
		KeyUsage: x509.KeyUsageDigitalSignature, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, NotBefore: root.NotBefore, NotAfter: root.NotAfter}
	der, err := x509.CreateCertificate(rand.Reader, leaf, root, &key.PublicKey, rootKey)
	if err != nil {
		t.Fatal(err)
	}
	private, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	result := certificates{root: string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: rootDER})),
		certificate: string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})),
		key:         string(pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: private}))}
	result.pair, err = tls.X509KeyPair([]byte(result.certificate), []byte(result.key))
	if err != nil {
		t.Fatal(err)
	}
	return result
}
func TestExportExplicitMTLSAndCertificateRefusal(t *testing.T) {
	serverCert := testCertificates(t, x509.ExtKeyUsageServerAuth)
	clientCert := testCertificates(t, x509.ExtKeyUsageClientAuth)
	clientRoots := x509.NewCertPool()
	clientRoots.AppendCertsFromPEM([]byte(clientCert.root))
	var calls atomic.Int32
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		_, _ = io.Copy(io.Discard, request.Body)
		if request.TLS == nil || len(request.TLS.VerifiedChains) == 0 {
			writer.WriteHeader(403)
			return
		}
		calls.Add(1)
		writer.Header().Set("Content-Type", "application/x-protobuf")
	}))
	server.Config.ErrorLog = log.New(io.Discard, "", 0)
	server.TLS = &tls.Config{Certificates: []tls.Certificate{serverCert.pair}, ClientCAs: clientRoots, ClientAuth: tls.RequireAndVerifyClientCert, MinVersion: tls.VersionTLS12}
	server.StartTLS()
	t.Cleanup(server.Close)
	for _, name := range []string{"valid", "missing-client-certificate", "wrong-ca"} {
		t.Run(name, func(t *testing.T) {
			before := calls.Load()
			profile := &TLSV1{CA: serverCert.root, Certificate: clientCert.certificate, Key: clientCert.key}
			if name == "missing-client-certificate" {
				profile.Certificate, profile.Key = "", ""
			}
			if name == "wrong-ca" {
				profile.CA = clientCert.root
			}
			fixture := newFixture(t, OptionsV1{LogsEndpoint: server.URL + "/v1/logs", TLS: profile}, nil)
			if result := emitOne(t, fixture, "tls", "message"); result.Err() != nil {
				t.Fatal(result.Err())
			}
			result := flushOne(t, fixture)
			if name == "valid" {
				if result.Err() != nil || calls.Load() != before+1 {
					t.Fatal("mTLS not established")
				}
			} else {
				if !errors.Is(result.Err(), ErrExport) || calls.Load() != before {
					t.Fatal("unverified certificate accepted")
				}
				if name == "wrong-ca" {
					var original *tls.CertificateVerificationError
					if !errors.As(result.Err(), &original) {
						t.Fatal("native TLS cause lost")
					}
				}
			}
		})
	}
}
func TestCanceledCloseCanFinishWithoutNativeShutdownFalseSuccess(t *testing.T) {
	var calls atomic.Int32
	fixture := newFixture(t, OptionsV1{}, http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		_, _ = io.Copy(io.Discard, request.Body)
		calls.Add(1)
		writer.Header().Set("Content-Type", "application/x-protobuf")
	}))
	emitOne(t, fixture, "record", "message")
	ctx, cancel := context.WithCancelCause(context.Background())
	cause := errors.New("close-cause")
	cancel(cause)
	if err := fixture.assembly.Close(ctx); !errors.Is(err, cause) {
		t.Fatal("canceled cleanup cause lost")
	}
	if calls.Load() != 0 {
		t.Fatal("canceled cleanup exported")
	}
	if err := fixture.assembly.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatal("later close did not actually export accepted record")
	}
	if _, err := fixture.client.Flush(context.Background(), fault.Correlation{Call: "late"}); err == nil {
		t.Fatal("closed source admitted")
	}
}
