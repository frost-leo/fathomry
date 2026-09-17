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

package temporal_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/orchestration/temporal/v1"
	"github.com/frost-leo/fathomry/internal/resource"
	"go.temporal.io/api/workflowservice/v1"
	sdk "go.temporal.io/sdk/client"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
)

type reviewNamespaceHeaders struct{ override atomic.Bool }

func (headers *reviewNamespaceHeaders) GetHeaders(context.Context) (map[string]string, error) {
	result := map[string]string{"x-review-fixture": "allowed"}
	if headers.override.Load() {
		result["temporal-namespace"] = "foreign-namespace"
	}
	return result, nil
}

func TestReviewClientHeadersCannotRetargetNamespace(t *testing.T) {
	headers := &reviewNamespaceHeaders{}
	var calls atomic.Int32
	observed := make(chan string, 2)
	fixture := newRuntimeFixture(t, 1, temporal.RuntimeOptions{HeadersProvider: headers}, nil,
		func(_ *temporal.OptionsV1, _ *resource.Limits, peer *rpcServer) {
			peer.intercept = func(ctx context.Context, request any, _ *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
				if input, ok := request.(*workflowservice.CountWorkflowExecutionsRequest); ok {
					calls.Add(1)
					md, _ := metadata.FromIncomingContext(ctx)
					if md.Get("x-review-fixture")[0] != "allowed" || input.Namespace != "test" {
						return nil, errors.New("normal headers or namespace changed")
					}
					observed <- md.Get("temporal-namespace")[0]
				}
				return next(ctx, request)
			}
		})
	executions, inbox := executionBinding(t, fixture)
	if _, err := executions.CountWorkflow(context.Background(), fault.Correlation{Call: "normal-header"}, &workflowservice.CountWorkflowExecutionsRequest{Namespace: "test"}); err != nil {
		t.Fatal("normal headers control failed", err)
	}
	receiveExecution(t, inbox)
	if actual := <-observed; actual != "test" || calls.Load() != 1 {
		t.Fatalf("normal namespace metadata=%q calls=%d", actual, calls.Load())
	}
	headers.override.Store(true)
	_, err := executions.CountWorkflow(context.Background(), fault.Correlation{Call: "foreign-header"}, &workflowservice.CountWorkflowExecutionsRequest{Namespace: "test"})
	receiveExecution(t, inbox)
	if err == nil || calls.Load() != 1 {
		var actual string
		select {
		case actual = <-observed:
		default:
		}
		t.Fatalf("foreign namespace routing header not rejected: error=%v calls=%d observed namespace=%q", err, calls.Load(), actual)
	}
}

func TestReviewTLSCallbackMustEndBeforeSourceRollback(t *testing.T) {
	certificateFixture := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	certificate := certificateFixture.TLS.Certificates[0]
	certificateFixture.Close()
	root, err := x509.ParseCertificate(certificate.Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.CreateCertificate(rand.Reader, &x509.Certificate{SerialNumber: big.NewInt(1),
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, KeyUsage: x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}, root, &leafKey.PublicKey, certificate.PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	certificate = tls.Certificate{Certificate: [][]byte{leaf, root.Raw}, PrivateKey: leafKey}
	for _, hook := range []string{"verify-connection", "root-ca-constraint"} {
		for _, block := range []bool{false, true} {
			name := hook + "/normal-handshake"
			if block {
				name = hook + "/blocked-callback"
			}
			t.Run(name, func(t *testing.T) {
				listener, err := net.Listen("tcp", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				server := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{certificate}, MinVersion: tls.VersionTLS12})))
				workflowservice.RegisterWorkflowServiceServer(server, &rpcServer{})
				serverDone := make(chan struct{})
				go func() { defer close(serverDone); _ = server.Serve(listener) }()
				t.Cleanup(func() { server.Stop(); _ = listener.Close(); <-serverDone })
				entered, release, callbackDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
				var enteredOnce, releaseOnce, callbackOnce sync.Once
				defer releaseOnce.Do(func() { close(release) })
				probe := func() error {
					enteredOnce.Do(func() { close(entered) })
					defer callbackOnce.Do(func() { close(callbackDone) })
					if block {
						<-release
					}
					return nil
				}
				clientTLS := &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: true,
					VerifyConnection: func(tls.ConnectionState) error { return probe() }}
				if hook == "root-ca-constraint" {
					clientTLS.InsecureSkipVerify = false
					clientTLS.VerifyConnection = nil
					clientTLS.RootCAs = x509.NewCertPool()
					clientTLS.RootCAs.AddCertWithConstraint(root, func([]*x509.Certificate) error { return probe() })
				}
				selected, err := temporal.SelectWithRuntime(temporal.OptionsV1{Name: "review-tls", Endpoint: listener.Addr().String(), Namespace: "test", ConnectTimeout: 300 * time.Millisecond},
					temporal.RuntimeOptions{ConnectionOptions: sdk.ConnectionOptions{TLS: clientTLS}})
				if err != nil {
					t.Fatal(err)
				}
				type outcome struct {
					assembly *resource.Assembly
					err      error
				}
				result := make(chan outcome, 1)
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				go func() {
					assembly, err := resource.Assemble(ctx, ctx, "review-tls", selected)
					result <- outcome{assembly: assembly, err: err}
				}()
				select {
				case <-entered:
				case <-ctx.Done():
					t.Fatal("TLS callback did not enter")
				}
				var actual outcome
				early := false
				earlyReleased := false
				if block {
					select {
					case actual = <-result:
						early = true
						if actual.assembly != nil {
							for _, source := range actual.assembly.Snapshot().Sources {
								earlyReleased = earlyReleased || source.Quiescent && source.Released
							}
						}
					case <-time.After(time.Second):
					}
				}
				releaseOnce.Do(func() { close(release) })
				if !early {
					select {
					case actual = <-result:
					case <-ctx.Done():
						t.Fatal("assembly did not finish after callback release")
					}
				}
				select {
				case <-callbackDone:
				case <-ctx.Done():
					t.Fatal("TLS callback did not end")
				}
				if actual.assembly == nil {
					t.Fatal("assembly outcome missing")
				}
				if err := actual.assembly.Close(ctx); err != nil {
					t.Fatal("source cleanup failed", err)
				}
				if !block && actual.err != nil {
					t.Fatal("normal TLS control failed", actual.err)
				}
				if earlyReleased {
					t.Fatal("assembly returned failed TLS source as quiescent/released while native TLS callback was still blocked")
				}
			})
		}
	}
}

func TestReviewOwnedTLSKeepsNativeMTLSCredentials(t *testing.T) {
	fixture := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	certificate := fixture.TLS.Certificates[0]
	fixture.Close()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{certificate}, ClientAuth: tls.RequireAnyClientCert, MinVersion: tls.VersionTLS12})))
	workflowservice.RegisterWorkflowServiceServer(server, &rpcServer{})
	joined := make(chan struct{})
	go func() { defer close(joined); _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close(); <-joined })
	t.Run("native-sdk-mtls-control", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		client, err := sdk.DialContext(ctx, sdk.Options{HostPort: listener.Addr().String(), Namespace: "test",
			Credentials:       sdk.NewMTLSCredentials(certificate),
			ConnectionOptions: sdk.ConnectionOptions{TLS: &tls.Config{InsecureSkipVerify: true, MinVersion: tls.VersionTLS12}}})
		if client != nil {
			client.Close()
		}
		if err != nil {
			t.Fatal("native SDK mTLS control failed", err)
		}
	})
	for _, injected := range []bool{false, true} {
		name := "static-certificates-control"
		if injected {
			name = "native-mtls-credentials"
		}
		t.Run(name, func(t *testing.T) {
			runtime := temporal.RuntimeOptions{ConnectionOptions: sdk.ConnectionOptions{TLS: &tls.Config{InsecureSkipVerify: true, MinVersion: tls.VersionTLS12}}}
			if injected {
				runtime.Credentials = sdk.NewMTLSCredentials(certificate)
			} else {
				runtime.ConnectionOptions.TLS.Certificates = []tls.Certificate{certificate}
			}
			selected, err := temporal.SelectWithRuntime(temporal.OptionsV1{Name: "review-mtls", Endpoint: listener.Addr().String(), Namespace: "test", ConnectTimeout: 500 * time.Millisecond}, runtime)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			assembly, err := resource.Assemble(ctx, ctx, "review-mtls", selected)
			if assembly != nil {
				if cleanupErr := assembly.Close(ctx); cleanupErr != nil {
					t.Fatal("mTLS fixture cleanup failed", cleanupErr)
				}
			}
			if err != nil {
				t.Fatal("equivalent native mTLS profile did not connect", err)
			}
		})
	}
}
