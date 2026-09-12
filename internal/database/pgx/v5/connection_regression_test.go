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

package pgx

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"runtime"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

func TestStartupFailureRevokesReturnedConfigCallbacks(t *testing.T) {
	prior := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(prior)
	var escaped atomic.Int32
	for attempt := 0; attempt < 100; attempt++ {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		options := testOptions()
		_, port, _ := net.SplitHostPort(listener.Addr().String())
		number, _ := strconv.Atoi(port)
		options.Port = uint16(number)
		served := make(chan struct{})
		go func() {
			defer close(served)
			socket, err := listener.Accept()
			if err != nil {
				return
			}
			_ = socket.SetDeadline(time.Now().Add(time.Second))
			backend := pgproto3.NewBackend(socket, socket)
			_, _ = backend.ReceiveStartupMessage()
			_ = socket.Close()
		}()
		config, err := nativeConfig(defaults(options))
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		connection, err := connect(ctx, config, options.RootCAPEM)
		cancel()
		if connection != nil {
			t.Fatal("unexpected successful startup")
		}
		var native *pgconn.ConnectError
		if !errors.As(err, &native) {
			t.Fatal("missing ConnectError")
		}
		// Mutating a returned error config cannot redirect retired native cleanup.
		native.Config.DialFunc = func(context.Context, string, string) (net.Conn, error) {
			escaped.Add(1)
			return nil, errors.New("review callback observed after constructor return")
		}
		runtime.Gosched()
		_ = listener.Close()
		<-served
	}
	if escaped.Load() != 0 {
		t.Fatalf("native async cleanup invoked replaced returned-error callback %d times after connect returned", escaped.Load())
	}
}

func TestNativeCertificateCauseCannotMutateFutureTrust(t *testing.T) {
	rootPublic, rootPrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	rootTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(50), Subject: pkix.Name{CommonName: "review-expired-root"},
		NotBefore: time.Now().Add(-2 * time.Hour), NotAfter: time.Now().Add(-time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	rootDER, err := x509.CreateCertificate(rand.Reader, rootTemplate, rootTemplate, rootPublic, rootPrivate)
	if err != nil {
		t.Fatal(err)
	}
	root, err := x509.ParseCertificate(rootDER)
	if err != nil {
		t.Fatal(err)
	}
	leafPublic, leafPrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	leafTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(51), Subject: pkix.Name{CommonName: "review-leaf"}, DNSNames: []string{"fixture.invalid"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leafTemplate, root, leafPublic, rootPrivate)
	if err != nil {
		t.Fatal(err)
	}
	peer := newProtocolPeer(t, true)
	peer.tls.Certificates = []tls.Certificate{{Certificate: [][]byte{leafDER}, PrivateKey: leafPrivate}}
	peer.roots = string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: rootDER}))
	config, err := nativeConfig(defaults(peer.options()))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	first, err := connect(ctx, config, peer.roots)
	if first != nil {
		_ = first.close(ctx)
		t.Fatal("expired CA unexpectedly accepted")
	}
	var invalid x509.CertificateInvalidError
	if !errors.As(err, &invalid) || invalid.Reason != x509.Expired || invalid.Cert.Subject.CommonName != "review-expired-root" {
		t.Fatal("expired root was not exposed as the native error cause")
	}
	invalid.Cert.NotAfter = time.Now().Add(time.Hour)
	second, err := connect(ctx, config, peer.roots)
	if second != nil {
		_ = second.close(ctx)
	}
	if err == nil {
		t.Fatal("mutating returned native root certificate cause made a later source connection trust an expired CA")
	}
}

func TestUnspecifiedAddressRefusalAndNativeCancellationCounterexample(t *testing.T) {
	peer := newProtocolPeer(t, false)
	options := peer.options()
	options.Address = "0.0.0.0"
	if _, err := Select(options); !errors.Is(err, ErrInput) {
		t.Fatal("unspecified destination was not rejected before construction")
	}
	config, err := nativeConfig(defaults(peer.options()))
	if err != nil {
		t.Fatal(err)
	}
	// Deliberately bypass the validated input boundary to reproduce why the
	// unspecified dial target cannot authorize cancellation to its resolved peer.
	config.Host = options.Address
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	native, err := connect(ctx, config, peer.roots)
	if err != nil {
		t.Fatal("counterexample could not enter native connection", err)
	}
	defer native.close(ctx)
	if err := native.native.PgConn().CancelRequest(ctx); !errors.Is(err, ErrState) {
		t.Fatal("defective unspecified destination no longer distinguishes the cancellation-address mismatch")
	}
}
