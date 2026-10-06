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

package redis

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
	"fmt"
	"math/big"
	"net"
	"strings"
	"testing"
	"time"
)

func TestPublicTLSMutualTLSAndMaintenanceTrust(t *testing.T) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "redis.test"}, DNSNames: []string{"redis.test"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), BasicConstraintsValid: true, IsCA: true,
		KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	certificate, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(certPEM)
	listener, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{certificate}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: roots})
	if err != nil {
		t.Fatal(err)
	}
	address := servePeer(t, listener, func(_ net.Conn, _ []string) string { return "+OK\r\n" })
	value := testSettings(address)
	value.Plaintext = false
	value.RootCAPEM = string(certPEM)
	value.ServerName = "redis.test"
	value.CertificatePEM = string(certPEM)
	value.PrivateKeyPEM = string(keyPEM)
	value.MaintenanceMode = "enabled"
	owner, deps := openTest(t, value)
	if _, err := owner.Client().Cache().Execute(testContext(t), command(t, Cache, "PING")); err != nil {
		t.Fatal(err)
	}
	ack(t, deps.Evidence, 1)
	value.ServerName = "wrong.test"
	refused, evidence := openTest(t, value)
	result, err := refused.Client().Cache().Execute(testContext(t), command(t, Cache, "PING"))
	var verification *tls.CertificateVerificationError
	if !errors.As(err, &verification) || result.Replies()[0].HasValue() {
		t.Fatal("TLS failure lost/no command output boundary")
	}
	ack(t, evidence.Evidence, 1)
	stats, err := refused.Client().Stats(testContext(t))
	if err != nil || stats.Sockets != 0 {
		t.Fatal("failed TLS retained socket")
	}
}

func TestControlledExtensionAndDedicatedOnlyCommand(t *testing.T) {
	address := peer(t, func(_ net.Conn, _ []string) string { return "+OK\r\n" })
	value := testSettings(address)
	value.AdminCommands = []string{"JSON.GET", "HIMPORT"}
	owner, deps := openTest(t, value)
	module := command(t, Cache, "JSON.GET", "owned", "$").WithKeyPosition(1)
	if _, err := owner.Client().Cache().Execute(testContext(t), module); err != nil {
		t.Fatal(err)
	}
	ack(t, deps.Evidence, 1)
	imported := command(t, Cache, "HIMPORT", "owned", "synthetic")
	if _, err := owner.Client().Cache().Execute(testContext(t), imported); !errors.Is(err, ErrAuthority) {
		t.Fatal("dedicated-only command escaped")
	}
	ack(t, deps.Evidence, 1)
	_, err := owner.Client().Cache().Dedicated(testContext(t), testContext(t), "owned", func(ctx context.Context, session *Session) error {
		_, err := session.Execute(ctx, imported)
		ack(t, deps.Evidence, 1)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	ack(t, deps.Evidence, 1)
}

func FuzzPublicReplyAndCorrelation(f *testing.F) {
	f.Add([]byte(""), "empty")
	f.Add([]byte("a\x00\xffb"), "binary")
	f.Add([]byte("reply"), "bad\ncorrelation")
	f.Fuzz(func(t *testing.T, data []byte, id string) {
		if len(data) > 4096 || len(id) > 512 {
			return
		}
		address := peer(t, func(_ net.Conn, _ []string) string { return fmt.Sprintf("$%d\r\n%s\r\n", len(data), data) })
		owner, deps := openTest(t, testSettings(address))
		identified, err := owner.Client().WithID(id)
		if err != nil {
			if !errors.Is(err, ErrInput) {
				t.Fatal("invalid correlation identity")
			}
			return
		}
		output, err := identified.Cache().Execute(testContext(t), command(t, Cache, "GET", "owned"))
		if err != nil {
			t.Fatal(err)
		}
		reply := output.Replies()[0]
		text, ok := reply.Value().Text()
		if !reply.HasValue() || !ok || text != string(data) || output.Attribution().ID != id {
			t.Fatal("frozen binary/correlation changed")
		}
		bytes, _ := reply.Value().Bytes()
		if len(bytes) > 0 {
			bytes[0] ^= 0xff
		}
		text, _ = output.Replies()[0].Value().Text()
		if text != string(data) {
			t.Fatal("reply alias")
		}
		if strings.Contains(fmt.Sprintf("%v", output), string(data)) && len(data) > 32 {
			t.Fatal("diagnostic payload leak")
		}
		ack(t, deps.Evidence, 1)
	})
}
