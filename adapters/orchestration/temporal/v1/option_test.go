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

package temporal

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"testing"

	"github.com/frost-leo/fathomry/adapters/v1"
	sdk "go.temporal.io/sdk/client"
	nativelog "go.temporal.io/sdk/log"
	"google.golang.org/grpc"
)

type nilLogger struct{ nativelog.Logger }

type tlsContainerPlugin struct {
	sdk.PluginBase
	configured, constructed int
	key                     *struct{}
	roots                   *x509.CertPool
	leaf                    *x509.Certificate
	der, ech                []byte
	retained                *tls.Config
}

func (*tlsContainerPlugin) Name() string { return "tls-containers" }
func (plugin *tlsContainerPlugin) ConfigureClient(_ context.Context, options sdk.PluginConfigureClientOptions) error {
	trust := options.ClientOptions.ConnectionOptions.TLS
	if trust == nil || len(trust.NextProtos) != 1 || trust.NextProtos[0] != "h2" ||
		trust.CipherSuites[0] != tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256 || trust.CurvePreferences[0] != tls.X25519 ||
		string(trust.Certificates[0].Certificate[0]) != "certificate" || string(trust.Certificates[0].SignedCertificateTimestamps[0]) != "sct" ||
		trust.Certificates[0].SupportedSignatureAlgorithms[0] != tls.PSSWithSHA256 || trust.NameToCertificate["named"] == nil ||
		string(trust.NameToCertificate["named"].Certificate[0]) != "named-certificate" ||
		string(trust.EncryptedClientHelloKeys[0].Config) != "ech-config" || trust.EncryptedClientHelloKeys[0].SendAsRetry {
		return errors.New("prepared TLS option containers changed")
	}
	if trust.RootCAs != plugin.roots || trust.Certificates[0].PrivateKey != plugin.key || trust.Certificates[0].Leaf != plugin.leaf ||
		&trust.Certificates[0].Certificate[0][0] != &plugin.der[0] || &trust.EncryptedClientHelloConfigList[0] != &plugin.ech[0] {
		return errors.New("borrowed TLS material or runtime object was replaced")
	}
	plugin.configured++
	trust.NextProtos[0] = "configured"
	trust.Certificates[0].Certificate[0] = []byte("configured-certificate")
	trust.NameToCertificate["named"].Certificate[0] = []byte("configured-name")
	trust.EncryptedClientHelloKeys[0].Config = []byte("configured-ech")
	plugin.retained = trust
	return nil
}
func (plugin *tlsContainerPlugin) NewClient(ctx context.Context, options sdk.PluginNewClientOptions, next func(context.Context, sdk.PluginNewClientOptions) error) error {
	trust := options.ClientOptions.ConnectionOptions.TLS
	if trust == nil || trust.NextProtos[0] != "configured" || string(trust.Certificates[0].Certificate[0]) != "configured-certificate" ||
		string(trust.NameToCertificate["named"].Certificate[0]) != "configured-name" {
		return errors.New("native accepted TLS configuration was not preserved")
	}
	plugin.constructed++
	trust.NextProtos[0] = "new-client-snapshot"
	trust.Certificates[0].Certificate[0] = []byte("new-client-certificate")
	return next(ctx, options)
}

func TestPreparationFreezesTLSContainersButBorrowsImmutableMaterial(t *testing.T) {
	key, roots, leaf := &struct{}{}, x509.NewCertPool(), &x509.Certificate{}
	der, ech := []byte("certificate"), []byte("ech-client-config")
	named := &tls.Certificate{Certificate: [][]byte{[]byte("named-certificate")}}
	trust := &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots, NextProtos: []string{"h2"},
		CipherSuites: []uint16{tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256}, CurvePreferences: []tls.CurveID{tls.X25519},
		Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf,
			SupportedSignatureAlgorithms: []tls.SignatureScheme{tls.PSSWithSHA256}, SignedCertificateTimestamps: [][]byte{[]byte("sct")}}},
		NameToCertificate: map[string]*tls.Certificate{"named": named}, EncryptedClientHelloConfigList: ech,
		EncryptedClientHelloKeys: []tls.EncryptedClientHelloKey{{Config: []byte("ech-config"), PrivateKey: []byte("ech-private")}}}
	plugin := &tlsContainerPlugin{key: key, roots: roots, leaf: leaf, der: der, ech: ech}
	settings := Settings{Name: "tls-copy", Endpoint: "127.0.0.1:1", Namespace: "test", Lazy: true,
		MaxActive: 1, MaxRequestBytes: 1024, MaxResponseBytes: 1024, InnerEvidenceCapacity: 4}
	prepared, err := Prepare(settings, NativeOptions{ConnectionOptions: sdk.ConnectionOptions{TLS: trust}, Plugins: []sdk.Plugin{plugin}})
	if err != nil {
		t.Fatal(err)
	}
	if plugin.configured != 0 || plugin.constructed != 0 {
		t.Fatal("offline preparation invoked TLS plugin")
	}
	base, err := Prepare(settings, NativeOptions{})
	if err != nil || prepared.metadata.SourceBytes <= base.metadata.SourceBytes {
		t.Fatal("owned TLS container charge missing", err)
	}
	trust.NextProtos[0] = "caller"
	trust.CipherSuites[0] = tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384
	trust.CurvePreferences[0] = tls.CurveP256
	trust.Certificates[0].Certificate[0] = []byte("caller-certificate")
	trust.Certificates[0].SignedCertificateTimestamps[0] = []byte("caller-sct")
	trust.Certificates[0].SupportedSignatureAlgorithms[0] = tls.PKCS1WithSHA256
	named.Certificate[0] = []byte("caller-named-certificate")
	delete(trust.NameToCertificate, "named")
	trust.EncryptedClientHelloKeys[0] = tls.EncryptedClientHelloKey{Config: []byte("caller-ech"), SendAsRetry: true}
	trust.EncryptedClientHelloConfigList = []byte("caller-config-list")

	policy, err := prepared.Policy()
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		ctx, cancel := context.WithCancel(context.Background())
		runtime, err := adapters.New(ctx, policy.Runtime)
		if err != nil {
			t.Fatal(err)
		}
		evidence, _ := adapters.NewInbox[Result](policy.Evidence)
		workers, _ := adapters.NewInbox[WorkerResult](policy.Workers)
		tasks, _ := adapters.NewInbox[TaskResult](policy.Tasks)
		owner, err := prepared.Open(ctx, Dependencies{Runtime: runtime, Evidence: evidence, Workers: workers, Tasks: tasks})
		if owner != nil {
			if closeErr := owner.Close(context.Background()); closeErr != nil {
				t.Error(closeErr)
			}
		}
		cancel()
		if closeErr := runtime.Close(context.Background()); closeErr != nil {
			t.Error(closeErr)
		}
		if err != nil {
			t.Fatal("frozen TLS source construction", err)
		}
		plugin.retained.NextProtos[0] = "retained-caller-snapshot"
		plugin.retained.Certificates[0].Certificate[0] = []byte("retained-certificate")
	}
	if plugin.configured != 2 || plugin.constructed != 2 {
		t.Fatal("reusable frozen preparation did not construct both native sources")
	}
}

func TestPreparationCopiesInputsAndRejectsTypedNil(t *testing.T) {
	const method = "/temporal.api.workflowservice.v1.WorkflowService/CountWorkflowExecutions"
	settings := Settings{Name: "prepared", Endpoint: "127.0.0.1:7233", Namespace: "test", Plaintext: true, RPCs: []string{method}}
	before, _ := json.Marshal(settings)
	prepared, err := Prepare(settings, NativeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	after, _ := json.Marshal(settings)
	if string(before) != string(after) {
		t.Fatal("preparation mutated input")
	}
	settings.RPCs[0] = "invalid"
	first := prepared.Settings()
	if first.RPCs[0] != method {
		t.Fatal("prepared settings share caller slice")
	}
	first.RPCs[0] = "changed"
	if prepared.Settings().RPCs[0] != method {
		t.Fatal("effective settings share published slice")
	}
	var typedNil *nilLogger
	if _, err := Prepare(prepared.Settings(), NativeOptions{Logger: typedNil}); !errors.Is(err, ErrInput) {
		t.Fatal("typed nil accepted", err)
	}
	if _, err := Prepare(prepared.Settings(), NativeOptions{ConnectionOptions: sdk.ConnectionOptions{DialOptions: []grpc.DialOption{grpc.WithDisableRetry()}}}); !errors.Is(err, ErrInput) {
		t.Fatal("opaque dial override accepted", err)
	}
	if _, err := Prepare(prepared.Settings(), NativeOptions{DataConverter: (*blockingConverter)(nil)}); !errors.Is(err, ErrInput) {
		t.Fatal("typed nil converter accepted", err)
	}
}

func FuzzPrepareSettings(f *testing.F) {
	f.Add("127.0.0.1:7233", "namespace", uint32(1), int64(0))
	f.Add("dns://127.0.0.1:53/localhost:7233", "namespace", uint32(0), int64(1000))
	f.Add("passthrough:///localhost:7233", "namespace", uint32(2), int64(-1))
	f.Fuzz(func(t *testing.T, endpoint, namespace string, version uint32, workerBytes int64) {
		if len(endpoint) > 4096 || len(namespace) > 4096 {
			return
		}
		input := Settings{Name: "fuzz", Endpoint: endpoint, Namespace: namespace, Version: version, Plaintext: true, WorkerWorkBytes: workerBytes}
		before, _ := json.Marshal(input)
		prepared, err := Prepare(input, NativeOptions{})
		after, _ := json.Marshal(input)
		if string(before) != string(after) {
			t.Fatal("input mutation")
		}
		if err == nil {
			effective := prepared.Settings()
			if effective.Endpoint != endpoint || effective.Namespace != namespace {
				t.Fatal("source identity changed")
			}
			if _, err := Prepare(effective, NativeOptions{}); err != nil {
				t.Fatal("effective settings not reusable", err)
			}
		}
	})
}
