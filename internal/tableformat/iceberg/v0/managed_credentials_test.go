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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	sdk "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/encryption"
	"github.com/apache/iceberg-go/table"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/frost-leo/fathomry/internal/resource"
)

func TestManagedTLSCertificateCachesDoNotAliasCaller(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer server.Close()
	configuration := server.TLS.Clone()
	configuration.Certificates[0].Leaf = server.Certificate()
	configuration.Certificates[0].SupportedSignatureAlgorithms = []tls.SignatureScheme{tls.PSSWithSHA256}
	cloned := cloneManagedTLS(configuration)
	if cloned.Certificates[0].Leaf != nil {
		t.Fatal("parsed certificate cache aliases caller instead of reparsing frozen DER")
	}
	configuration.Certificates[0].SupportedSignatureAlgorithms[0] = tls.Ed25519
	configuration.Certificates[0].Certificate[0][0] ^= 1
	if cloned.Certificates[0].SupportedSignatureAlgorithms[0] != tls.PSSWithSHA256 || cloned.Certificates[0].Certificate[0][0] == configuration.Certificates[0].Certificate[0][0] {
		t.Fatal("certificate containers alias caller")
	}
}

func TestManagedSigV4CatalogAndStorageCredentialsAreSeparate(t *testing.T) {
	peer, options := newPeer(t)
	var catalogRequests, storageRequests atomic.Int32
	peer.hook = func(w http.ResponseWriter, r *http.Request) bool {
		if strings.HasPrefix(r.URL.Path, "/catalog/") {
			catalogRequests.Add(1)
			if !strings.Contains(r.Header.Get("Authorization"), "Credential=catalog-only/") || !strings.Contains(r.Header.Get("Authorization"), "/us-east-1/execute-api/aws4_request") || r.Header.Get("X-Amz-Security-Token") != "catalog-session" {
				t.Error("explicit catalog SigV4 identity was not used")
			}
		} else {
			storageRequests.Add(1)
			if !strings.Contains(r.Header.Get("Authorization"), "Credential=test-access/") || strings.Contains(r.Header.Get("Authorization"), "catalog-only") || r.Header.Get("X-Amz-Security-Token") != "" {
				t.Error("catalog credentials crossed into storage")
			}
		}
		return false
	}
	settings := managedSettingsFor(options)
	settings.SigV4, settings.SigningRegion, settings.SigningService = true, "us-east-1", "execute-api"
	provider := aws.CredentialsProviderFunc(func(ctx context.Context) (aws.Credentials, error) {
		if ctx.Err() != nil {
			return aws.Credentials{}, ctx.Err()
		}
		return aws.Credentials{AccessKeyID: "catalog-only", SecretAccessKey: "synthetic-secret", SessionToken: "catalog-session"}, nil
	})
	f := bindManagedNativeFixture(t, settings, ManagedNativeOptions{CatalogCredentials: provider}, 1)
	f.run(t, "signed-profile", func(s *ManagedSession) error {
		if _, err := s.CheckTableExists([]string{"isolated", "absent"}); err != nil {
			return err
		}
		_, err := s.CreateFile(options.Location+"signed", "", func(file encryption.EncryptedOutputFile) error {
			_, err := file.Write([]byte("signature-isolation"))
			return err
		})
		if err != nil {
			return err
		}
		return s.ReadFile(options.Location+"signed", nil, func(file encryption.EncryptedInputFile) error {
			value, err := io.ReadAll(file)
			if err == nil && string(value) != "signature-isolation" {
				return fmt.Errorf("file bytes changed")
			}
			return err
		})
	})
	if catalogRequests.Load() < 2 || storageRequests.Load() < 3 {
		t.Fatal("signature isolation did not execute both transports")
	}
}

func TestManagedOAuthUsesIndependentFrozenTLS(t *testing.T) {
	var verifications, tokens atomic.Int32
	issuer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tokens.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"access_token":"scoped-token","token_type":"Bearer","expires_in":60}`))
	}))
	defer issuer.Close()
	peer, options := newPeer(t)
	peer.hook = func(w http.ResponseWriter, r *http.Request) bool {
		if r.Header.Get("Authorization") != "Bearer scoped-token" {
			t.Error("OAuth token not applied to catalog request")
		}
		return false
	}
	roots := x509.NewCertPool()
	roots.AddCert(issuer.Certificate())
	oauthTLS := &tls.Config{RootCAs: roots, VerifyConnection: func(tls.ConnectionState) error {
		verifications.Add(1)
		return nil
	}}
	settings := managedSettingsFor(options)
	settings.OAuthCredential, settings.OAuthURI = "client:synthetic", issuer.URL+"/token"
	prepared, err := PrepareManaged(settings, ManagedNativeOptions{OAuthTLS: oauthTLS})
	if err != nil {
		t.Fatal(err)
	}
	if tokens.Load() != 0 || verifications.Load() != 0 {
		t.Fatal("TLS/token callbacks ran before admission")
	}
	oauthTLS.RootCAs = x509.NewCertPool()
	oauthTLS.VerifyConnection = func(tls.ConnectionState) error { return fmt.Errorf("mutated caller TLS") }
	selection, err := prepared.Select()
	if err != nil {
		t.Fatal(err)
	}
	selection = resource.WithLimits(selection, prepared.Limits())
	assembly, err := resource.Assemble(deadline(t), deadline(t), "oauth-tls", selection)
	if err != nil {
		t.Fatal("independent OAuth TLS failed", causes(err))
	}
	if err = assembly.Close(deadline(t)); err != nil {
		t.Fatal(err)
	}
	if tokens.Load() != 1 || verifications.Load() != 1 {
		t.Fatal("frozen OAuth TLS profile not used exactly once")
	}
}

func TestManagedExpiredVendedCredentialsRefreshBeforeStorageIO(t *testing.T) {
	peer, options := newPeer(t)
	meta, err := table.NewMetadata(testSchema(), sdk.UnpartitionedSpec, table.UnsortedSortOrder, options.Location+"data", nil)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(meta)
	peer.persist("data", raw)
	var refreshes, storageReads atomic.Int32
	credential := func(key string, expiry time.Time) map[string]any {
		return map[string]any{"prefix": options.Location, "config": map[string]string{
			"s3.access-key-id": key, "s3.secret-access-key": "synthetic-secret", "s3.session-token-expires-at-ms": strconv.FormatInt(expiry.UnixMilli(), 10),
			"s3.endpoint": "http://unselected.invalid", "s3.region": "unselected-region",
		}}
	}
	peer.hook = func(w http.ResponseWriter, r *http.Request) bool {
		switch {
		case strings.HasSuffix(r.URL.Path, "/config"):
			writeJSON(w, map[string]any{"defaults": map[string]string{"prefix": "warehouse"}, "endpoints": []string{
				"GET /v1/{prefix}/namespaces/{namespace}/tables/{table}",
				"GET /v1/{prefix}/namespaces/{namespace}/tables/{table}/credentials",
			}})
			return true
		case strings.HasSuffix(r.URL.Path, "/credentials"):
			refreshes.Add(1)
			writeJSON(w, map[string]any{"storage-credentials": []any{credential("renewed", time.Now().Add(time.Hour))}})
			return true
		case strings.HasSuffix(r.URL.Path, "/tables/data"):
			peer.mu.Lock()
			reply := peer.tableResponse("data", peer.tables["data"])
			peer.mu.Unlock()
			reply["storage-credentials"] = []any{credential("expired", time.Now().Add(-time.Hour))}
			writeJSON(w, reply)
			return true
		case strings.HasPrefix(r.URL.Path, "/fixture/"):
			storageReads.Add(1)
			if !strings.Contains(r.Header.Get("Authorization"), "Credential=renewed/") || !strings.Contains(r.Header.Get("Authorization"), "/us-east-1/s3/aws4_request") {
				t.Error("expired credentials or unselected region reached storage")
			}
		}
		return false
	}
	settings := managedSettingsFor(options)
	settings.StorageAccessKey, settings.StorageSecretKey = "", ""
	settings.VendedCredentials = true
	f := bindManagedFixture(t, settings, 1)
	for _, name := range []string{"first", "new-family"} {
		f.run(t, name, func(s *ManagedSession) error { _, err := s.LoadTable([]string{"isolated", "data"}); return err })
	}
	if refreshes.Load() != 2 || storageReads.Load() < 4 {
		t.Fatal("fresh family did not renew credentials with its own context")
	}
}
