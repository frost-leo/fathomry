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

package minio

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"io"
	"log"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/resource"
)

func TestExplicitTLSRootsAndSSEHeader(t *testing.T) {
	server, options := newPeer(t)
	secure := httptest.NewUnstartedServer(http.HandlerFunc(server.serve))
	secure.Config.ErrorLog = log.New(io.Discard, "", 0)
	secure.StartTLS()
	t.Cleanup(secure.Close)
	options.Endpoint = secure.URL
	options.Plaintext = false
	options.RootCAPEM = string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: secure.Certificate().Raw}))
	t.Setenv("HTTPS_PROXY", "http://127.0.0.1:1")
	t.Setenv("SSL_CERT_FILE", "/nonexistent/fathomry-fixture-ca")
	fixture := bindFixture(t, options, 4)
	receipt, err := fixture.client.Put(deadline(t), deadline(t), correlation("tls-sse"), WriteRequest{Key: "owned/tls", Size: 3, Encryption: "SSE-S3"}, bytes.NewReader([]byte("tls")))
	result := settle(t, receipt, err)
	if result.Err() != nil || string(server.content("owned/tls")) != "tls" {
		t.Fatal("explicit TLS failed", result.Err())
	}
	server.mu.Lock()
	found := false
	for _, request := range server.requests {
		if request.method == "PUT" && request.header.Get("X-Amz-Server-Side-Encryption") == "AES256" {
			found = true
		}
	}
	server.mu.Unlock()
	if !found {
		t.Fatal("SSE-S3 request weakened")
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	certificate, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	options.RootCAPEM = string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate}))
	selected, err := Select(options)
	if err != nil {
		t.Fatal(err)
	}
	selected = resource.WithLimits(selected, LimitsV1(options))
	assembly, err := resource.Assemble(deadline(t), deadline(t), "untrusted", selected)
	if !errors.Is(err, ErrConnect) || assembly == nil {
		t.Fatal("untrusted TLS endpoint accepted or cleanup owner lost")
	}
	if err := assembly.Close(deadline(t)); err != nil {
		t.Fatal(err)
	}
}
