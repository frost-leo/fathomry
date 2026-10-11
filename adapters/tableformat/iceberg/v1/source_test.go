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

package iceberg_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	iceberg "github.com/frost-leo/fathomry/adapters/tableformat/iceberg/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
)

func testContext(t testing.TB) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	return ctx
}
func publicSettings(endpoint string) iceberg.Settings {
	return iceberg.Settings{Name: "public", CatalogURI: endpoint + "/catalog", CatalogPrefix: "warehouse", Warehouse: "test", Namespace: []string{"isolated"}, Location: "s3://fixture/owned/", Plaintext: true, StorageEndpoint: endpoint, StorageRegion: "us-east-1", StorageAccessKey: "private-key-canary", StorageSecretKey: "private-secret-canary", Permissions: iceberg.Grants{Read: true}}
}
func openPublic(t testing.TB, settings iceberg.Settings) (*iceberg.Owner, *adapters.Inbox[iceberg.Result], *adapters.Runtime) {
	return openPublicNative(t, settings, iceberg.NativeOptions{})
}
func openPublicNative(t testing.TB, settings iceberg.Settings, native iceberg.NativeOptions) (*iceberg.Owner, *adapters.Inbox[iceberg.Result], *adapters.Runtime) {
	t.Helper()
	policy, err := iceberg.Recommend(settings)
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := adapters.New(context.Background(), policy.Runtime)
	if err != nil {
		t.Fatal(err)
	}
	inbox, err := adapters.NewInbox[iceberg.Result](policy.Evidence)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := iceberg.Open(testContext(t), settings, iceberg.Dependencies{Runtime: runtime, Evidence: inbox, Native: native})
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if owner != nil {
			if err := owner.Close(ctx); err != nil || !owner.ShutdownComplete() {
				t.Error("owner did not close", err)
			}
		}
		if err := runtime.Close(ctx); err != nil {
			t.Error(err)
		}
		for state, _ := inbox.Inspect(); state.Outstanding > 0; state, _ = inbox.Inspect() {
			delivery, err := inbox.NextReleased(ctx)
			if err != nil {
				t.Error(err)
				break
			}
			if err = delivery.Ack(); err != nil {
				t.Error(err)
				break
			}
		}
	})
	if err != nil {
		t.Fatal("open failed", err)
	}
	return owner, inbox, runtime
}
func settlePublic(t testing.TB, receipt *adapters.Receipt[iceberg.Result], err error) adapters.Snapshot[iceberg.Result] {
	t.Helper()
	if err != nil || receipt == nil {
		t.Fatal("admission failed", err)
	}
	value, err := receipt.WaitReleased(testContext(t))
	if err != nil {
		t.Fatal(err)
	}
	return value
}
func ackPublic(t testing.TB, inbox *adapters.Inbox[iceberg.Result]) {
	t.Helper()
	delivery, err := inbox.NextReleased(testContext(t))
	if err != nil {
		t.Fatal(err)
	}
	if err = delivery.Ack(); err != nil {
		t.Fatal(err)
	}
}
func TestPublicOwnerScopeEvidenceAndPrivacy(t *testing.T) {
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.URL.Path == "/catalog/v1/config" {
			_, _ = w.Write([]byte(`{"defaults":{"prefix":"warehouse"}}`))
			return
		}
		http.Error(w, "private-payload-canary", 403)
	}))
	defer server.Close()
	settings := publicSettings(server.URL)
	before := requests.Load()
	prepared, err := iceberg.Prepare(settings, iceberg.NativeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	settings.Namespace[0] = "changed"
	if prepared.Settings().Namespace[0] != "isolated" || requests.Load() != before {
		t.Fatal("prepare aliases input or performs I/O")
	}
	if text := fmt.Sprintf("%+v %#v", settings, prepared); strings.Contains(text, "canary") || strings.Contains(text, server.URL) {
		t.Fatal("secret diagnostic")
	}
	owner, inbox, runtime := openPublic(t, prepared.Settings())
	var escaped *iceberg.Session
	receipt, err := owner.Client().Use(testContext(t), func(s *iceberg.Session) error { escaped = s; _, _, err := s.RemotePlanningCapabilities(); return err })
	value := settlePublic(t, receipt, err)
	if value.Err() != nil {
		t.Fatal(value.Err())
	}
	result, ok := value.ValueCopy()
	if !ok || !result.Released() || result.Attribution().SourceID == "" {
		t.Fatal("missing ownership evidence")
	}
	after := requests.Load()
	if _, _, err = escaped.RemotePlanningCapabilities(); err == nil || requests.Load() != after {
		t.Fatal("escaped session survived")
	}
	policy, _ := iceberg.Recommend(prepared.Settings())
	for i := 1; i < policy.Evidence.Capacity-1; i++ {
		receipt, err = owner.Client().Use(testContext(t), func(*iceberg.Session) error { return nil })
		if value := settlePublic(t, receipt, err); value.Err() != nil {
			t.Fatal(value.Err())
		}
	}
	called := false
	if _, err = owner.Client().Use(testContext(t), func(*iceberg.Session) error { called = true; return nil }); !errors.Is(err, adapters.ErrEvidence) || called {
		t.Fatal("independent evidence bypass", err)
	}
	for i := 0; i < policy.Evidence.Capacity-1; i++ {
		ackPublic(t, inbox)
	}
	if err = owner.Close(testContext(t)); err != nil || !owner.ShutdownComplete() {
		t.Fatal("source cleanup", err)
	}
	if state, _ := runtime.Inspect(); state.Active != 0 {
		t.Fatal("source work retained after release")
	}
}
func TestPublicMissingEvidencePrecedesNativeFactory(t *testing.T) {
	settings := publicSettings("http://127.0.0.1:1")
	policy, err := iceberg.Recommend(settings)
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := adapters.New(testContext(t), policy.Runtime)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close(context.Background())
	called := false
	_, err = iceberg.Open(testContext(t), settings, iceberg.Dependencies{Runtime: runtime, Native: iceberg.NativeOptions{Transport: func(*http.Transport) (http.RoundTripper, func() error, error) { called = true; return nil, nil, nil }}})
	if err == nil || called {
		t.Fatal("native callback ran without evidence")
	}
}
