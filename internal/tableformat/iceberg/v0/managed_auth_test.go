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

package iceberg

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
)

type controlledAuth struct {
	block            atomic.Bool
	entered, release chan struct{}
}

func (a *controlledAuth) AuthHeader() (string, string, error) {
	return "Authorization", "Bearer synthetic", nil
}
func (a *controlledAuth) AuthHeaderWithContext(context.Context) (string, string, error) {
	if a.block.Load() {
		close(a.entered)
		<-a.release
	}
	return a.AuthHeader()
}

var _ rest.ContextAuthManager = (*controlledAuth)(nil)

func TestManagedAuthIsOfflineAndActuallyJoined(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/config" {
			_, _ = w.Write([]byte(`{"defaults":{}}`))
			return
		}
		w.WriteHeader(404)
	}))
	defer server.Close()
	settings := ManagedSettings{Name: "auth", CatalogURI: server.URL, Warehouse: "fixture", Namespace: []string{"isolated"}, Location: "s3://fixture/owned/", Plaintext: true, StorageEndpoint: server.URL, StorageRegion: "test", StorageAccessKey: "test", StorageSecretKey: "test", Permissions: Grants{Read: true}}
	auth := &controlledAuth{entered: make(chan struct{}), release: make(chan struct{})}
	prepared, err := PrepareManaged(settings, ManagedNativeOptions{Auth: auth})
	if err != nil {
		t.Fatal(err)
	}
	selection, err := prepared.Select()
	if err != nil {
		t.Fatal(err)
	}
	selection = resource.WithLimits(selection, prepared.Limits())
	assembly, err := resource.Assemble(deadline(t), deadline(t), "auth", selection)
	if err != nil {
		t.Fatal(err)
	}
	defer assembly.Close(context.Background())
	inbox, _ := invocation.NewInbox[ManagedResult](1, prepared.Budget().EvidenceBytes)
	client, err := BindManaged(assembly, selection, inbox)
	if err != nil {
		t.Fatal(err)
	}
	auth.block.Store(true)
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	cause := errors.New("owner canceled")
	receipt, err := client.Use(ctx, correlation("auth"), func(s *ManagedSession) error { _, err := s.CheckTableExists([]string{"isolated", "data"}); return err })
	if err != nil {
		t.Fatal(err)
	}
	<-auth.entered
	cancel(cause)
	short, stop := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer stop()
	if _, err = receipt.WaitReleased(short); err == nil {
		t.Fatal("blocked auth falsely joined")
	}
	close(auth.release)
	result, err := receipt.WaitReleased(deadline(t))
	if err != nil || !errors.Is(result.Err(), cause) {
		t.Fatal("cause or join lost", err)
	}
	delivery, err := inbox.Next(deadline(t))
	if err != nil {
		t.Fatal(err)
	}
	_ = delivery.Release()
}
func TestManagedOAuthExplicitAudienceAndDefaultScope(t *testing.T) {
	var tokens atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/oauth/tokens" {
			tokens.Add(1)
			_ = r.ParseForm()
			if r.Form.Get("scope") != "catalog" || r.Form.Get("audience") != "fixture" || r.Form.Get("resource") != "tableformat" || r.Form.Get("client_id") != "client" || r.Form.Get("client_secret") != "secret" {
				t.Error("OAuth semantics changed")
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"access_token":"synthetic","token_type":"Bearer","expires_in":60}`))
			return
		}
		if r.Header.Get("Authorization") != "Bearer synthetic" {
			t.Error("token absent")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"defaults":{}}`))
	}))
	defer server.Close()
	settings := ManagedSettings{Name: "oauth", CatalogURI: server.URL, Warehouse: "fixture", Namespace: []string{"isolated"}, Location: "s3://fixture/owned/", Plaintext: true, StorageEndpoint: server.URL, StorageRegion: "test", StorageAccessKey: "test", StorageSecretKey: "test", OAuthCredential: "client:secret", OAuthAudience: "fixture", OAuthResource: "tableformat"}
	prepared, err := PrepareManaged(settings, ManagedNativeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if tokens.Load() != 0 || prepared.Settings().OAuthURI != server.URL+"/v1/oauth/tokens" || prepared.Settings().OAuthScope != "catalog" {
		t.Fatal("offline/effective OAuth")
	}
	selection, err := prepared.Select()
	if err != nil {
		t.Fatal(err)
	}
	selection = resource.WithLimits(selection, prepared.Limits())
	assembly, err := resource.Assemble(deadline(t), deadline(t), "oauth", selection)
	if err != nil {
		t.Fatal("OAuth setup", causes(err))
	}
	if err = assembly.Close(deadline(t)); err != nil {
		t.Fatal(err)
	}
	if tokens.Load() != 1 {
		t.Fatal("token cache not shared")
	}
}
func TestManagedTLSVerifierTailRetainsSource(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(`{"defaults":{}}`)) }))
	defer server.Close()
	roots := x509.NewCertPool()
	roots.AddCert(server.Certificate())
	entered, release := make(chan struct{}), make(chan struct{})
	config := &tls.Config{RootCAs: roots, VerifyConnection: func(tls.ConnectionState) error { close(entered); <-release; return nil }}
	settings := ManagedSettings{Name: "tls", CatalogURI: server.URL, Warehouse: "fixture", Namespace: []string{"isolated"}, Location: "s3://fixture/owned/", StorageEndpoint: server.URL, StorageRegion: "test", StorageAccessKey: "test", StorageSecretKey: "test", Timeout: 50 * time.Millisecond}
	prepared, err := PrepareManaged(settings, ManagedNativeOptions{TLS: config})
	if err != nil {
		t.Fatal(err)
	}
	value, err := newManagedSource(context.Background(), prepared)
	if err != nil {
		t.Fatal(err)
	}
	checked := make(chan error, 1)
	go func() { checked <- value.Check(context.Background()) }()
	<-entered
	err = <-checked
	if err == nil {
		t.Fatal("timed handshake unexpectedly succeeded")
	}
	released := make(chan resource.ReleaseResult, 1)
	go func() { released <- value.Release(context.Background()) }()
	select {
	case <-released:
		t.Fatal("source released before verifier return")
	case <-time.After(10 * time.Millisecond):
	}
	close(release)
	select {
	case result := <-released:
		if !result.Released {
			t.Error("actual verifier termination not joined")
		}
	case <-time.After(time.Second):
		t.Fatal("verifier cleanup stalled")
	}
}
func TestManagedCredentialPrefixRejectsAliases(t *testing.T) {
	_, options := newPeer(t)
	settings := managedSettingsFor(options)
	settings.VendedCredentials = true
	prepared, err := PrepareManaged(settings, ManagedNativeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	owner := &managedSource{prepared: prepared}
	for _, prefix := range []string{"s3://other/owned/", "s3://fixture/outside/", "s3://fixture/owned/%2e%2e/", "s3://fixture/owned/table"} {
		err := owner.validateCredentials(context.Background(), "s3://fixture/owned/table-other/metadata.json", []rest.StorageCredential{{Prefix: prefix}})
		if err == nil {
			t.Error("credential authority expanded", fmt.Sprintf("%T", err))
		}
	}
	if err = owner.validateCredentials(context.Background(), "s3://fixture/owned/table/metadata.json", []rest.StorageCredential{{Prefix: "s3://fixture/owned/table/"}}); err != nil {
		t.Fatal("valid scoped credential refused")
	}
}
