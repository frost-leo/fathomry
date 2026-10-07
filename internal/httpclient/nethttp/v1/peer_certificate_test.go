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
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestPeerCertificateResidenceHasNativePayloadCharge(t *testing.T) {
	rootKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	rootTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "synthetic peer root"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	rootDER, err := x509.CreateCertificate(rand.Reader, rootTemplate, rootTemplate, &rootKey.PublicKey, rootKey)
	if err != nil {
		t.Fatal(err)
	}
	root, err := x509.ParseCertificate(rootDER)
	if err != nil {
		t.Fatal(err)
	}
	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	extensionDER, err := asn1.Marshal(bytes.Repeat([]byte{0x41}, 240<<10))
	if err != nil {
		t.Fatal(err)
	}
	leafTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "synthetic large peer"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		BasicConstraintsValid: true, KeyUsage: x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
		ExtraExtensions: []pkix.Extension{{Id: asn1.ObjectIdentifier{1, 3, 6, 1, 4, 1, 57264, 117}, Value: extensionDER}},
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leafTemplate, root, &leafKey.PublicKey, rootKey)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(leafDER)
	if err != nil || leaf.CheckSignatureFrom(root) != nil {
		t.Fatal("large test certificate is invalid", err)
	}
	if len(leafDER) >= 256<<10 {
		t.Fatal("test leaf no longer fits selected native certificate-message bound")
	}
	var received atomic.Int64
	peer := httptest.NewUnstartedServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		received.Add(1)
		writer.Header().Set("Content-Length", "1")
		_, _ = io.WriteString(writer, "x")
	}))
	peer.TLS = &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{leafDER}, PrivateKey: leafKey}}, MinVersion: tls.VersionTLS13}
	peer.StartTLS()
	t.Cleanup(peer.Close)
	roots := x509.NewCertPool()
	roots.AddCert(root)
	var handshakes, peerDERBytes atomic.Int64
	options := OptionsV1{
		Name: "peer-residence", HTTP1: true, MaxActive: 1, MaxConnections: 1,
		MaxRequestBytes: 1, MaxResponseBytes: 1, MaxHeaderBytes: 1024, MaxExchanges: 1,
		Native: NativeOptionsV1{TLS: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS13,
			VerifyConnection: func(state tls.ConnectionState) error {
				handshakes.Add(1)
				var size int64
				for _, certificate := range state.PeerCertificates {
					size += int64(len(certificate.Raw))
				}
				peerDERBytes.Store(size)
				return nil
			},
		}},
	}
	prepared, err := PrepareV1(options)
	if err != nil {
		t.Fatal(err)
	}
	f := bindFixture(t, options, 1)
	for _, name := range []string{"first", "reuse"} {
		receipt, err := f.client.Do(deadline(t), deadline(t), correlation(name), newRequest(t, "GET", peer.URL, nil))
		if err != nil {
			t.Fatal(err)
		}
		result := settle(t, f, receipt)
		if result.Err() != nil || !result.Outcome.Value.Complete() || string(result.Outcome.Value.DataCopy()) != "x" {
			t.Fatal("bounded HTTP result did not complete", result.Err())
		}
	}
	status := f.assembly.Snapshot().Sources[0]
	f.client.owner.mu.Lock()
	sockets := len(f.client.owner.sockets)
	f.client.owner.mu.Unlock()
	if received.Load() != 2 || handshakes.Load() != 1 || sockets != 1 || status.Usage.Active != 0 || f.inbox.Usage().Outstanding != 0 {
		t.Fatal("test did not establish idle native reuse after root and evidence release")
	}
	if peerDERBytes.Load() != int64(len(leafDER)) {
		t.Fatal("native verified peer payload differs from the generated certificate")
	}
	if declared := prepared.Metadata().SourceBytes; declared < peerDERBytes.Load() {
		t.Fatalf("source declaration %d is below its one verified idle connection's retained peer DER payload %d; parsed objects/cache keys/transport buffers are additional, not measured RSS", declared, peerDERBytes.Load())
	}
}
