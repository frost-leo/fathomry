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

package trino_test

import (
	"context"
	"encoding/pem"
	"net/http"
	"testing"
	"time"

	trino "github.com/frost-leo/fathomry/adapters/sqlengine/trino/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
)

func boundedContext(t testing.TB) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func (peer *wirePeer) settings() trino.Settings {
	return trino.Settings{Name: "fixture", Endpoint: peer.server.URL, User: "fixture", Plaintext: true,
		Catalog: "test_catalog", Schema: "test_schema", Writes: true, Maintenance: true,
		MaxActive: 2, MaxSQLBytes: 64 << 10, MaxParameters: 128, MaxRows: 256, MaxColumns: 16,
		MaxPageBytes: 16 << 10, MaxResultBytes: 64 << 10, MaxPages: 64, MaxWireBytes: 1 << 20,
		Timeout: 5 * time.Second, CleanupTimeout: time.Second}
}

func openPublic(t testing.TB, options trino.Settings, capacity int) (*trino.Owner, *adapters.Inbox[trino.Result], *adapters.Runtime) {
	t.Helper()
	policy, err := trino.Recommend(options)
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := adapters.New(context.Background(), policy.Runtime)
	if err != nil {
		t.Fatal(err)
	}
	if capacity != 0 {
		policy.Evidence.Capacity = capacity
		policy.Evidence.MaxBytes *= int64(capacity)
	}
	inbox, err := adapters.NewInbox[trino.Result](policy.Evidence)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := trino.Open(boundedContext(t), options, trino.Dependencies{Runtime: runtime, Evidence: inbox})
	if owner != nil {
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := owner.Close(ctx); err != nil || !owner.ShutdownComplete() {
				t.Error("source cleanup was not joined", err)
			}
			if err := runtime.Close(ctx); err != nil {
				t.Error("public runtime cleanup", err)
			}
			for status, _ := inbox.Inspect(); status.Outstanding > 0; status, _ = inbox.Inspect() {
				delivery, err := inbox.NextReleased(ctx)
				if err != nil {
					t.Error("evidence cleanup", err)
					break
				}
				if err := delivery.Ack(); err != nil {
					t.Error("evidence acknowledgement", err)
					break
				}
			}
		})
	}
	if err != nil || owner == nil {
		t.Fatal("public source open", err)
	}
	return owner, inbox, runtime
}

func releasedEvidence(t testing.TB, inbox *adapters.Inbox[trino.Result]) (adapters.Delivery[trino.Result], adapters.Snapshot[trino.Result]) {
	t.Helper()
	delivery, err := inbox.NextReleased(boundedContext(t))
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := delivery.Receipt()
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := receipt.WaitReleased(boundedContext(t))
	if err != nil {
		t.Fatal(err)
	}
	if !snapshot.Info().Released || !snapshot.Info().Resolved {
		t.Fatal("released evidence has unfinished ownership")
	}
	return delivery, snapshot
}

func acknowledge(t testing.TB, inbox *adapters.Inbox[trino.Result]) adapters.Snapshot[trino.Result] {
	t.Helper()
	delivery, snapshot := releasedEvidence(t, inbox)
	if err := delivery.Ack(); err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func TestReadinessFailureRetainsPublicOwner(t *testing.T) {
	peer := newWirePeer(t, func(http.ResponseWriter, *http.Request, []byte) { t.Error("readiness dispatched application work") })
	peer.onReady = func(writer http.ResponseWriter, _ *http.Request) { writePage(t, writer, map[string]any{"id": "ready"}) }
	policy, err := trino.Recommend(peer.settings())
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := adapters.New(context.Background(), policy.Runtime)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.Close(context.Background()) })
	inbox, err := adapters.NewInbox[trino.Result](policy.Evidence)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := trino.Open(boundedContext(t), peer.settings(), trino.Dependencies{Runtime: runtime, Evidence: inbox})
	if err == nil || owner == nil || peer.readiness.Load() != 1 || peer.posts.Load() != 0 {
		t.Fatal("failed readiness lost its owner or claimed valid initialization")
	}
	if err := owner.Close(boundedContext(t)); err != nil || !owner.ShutdownComplete() {
		t.Fatal("failed acquisition owner could not join cleanup", err)
	}
	if result := owner.Release(boundedContext(t)); !result.Complete || result.Err != nil {
		t.Fatal("completed failed acquisition is not idempotently releasable")
	}
	snapshot := acknowledge(t, inbox)
	if snapshot.Primary() == nil {
		t.Fatal("source evidence erased failed readiness")
	}
	if state, _ := runtime.Inspect(); state.Active != 0 || state.WorkBytes != 0 {
		t.Fatal("failed source retains public work after confirmed release")
	}
}

func TestPublicObservationsAreDetached(t *testing.T) {
	peer := newWirePeer(t, func(http.ResponseWriter, *http.Request, []byte) { t.Error("observation dispatched application SQL") })
	owner, _, _ := openPublic(t, peer.settings(), 0)
	original := owner.Info()
	if original.Revision == "" || original.FormatVersion != 1 || original.Name != "fixture" {
		t.Fatal("resolved preparation identity missing")
	}
	if len(original.Provenance) != 0 && len(original.Provenance[0].Fields) != 0 {
		original.Provenance[0].Fields[0] = "changed"
		if owner.Info().Provenance[0].Fields[0] == "changed" || owner.Handle().Info().Provenance[0].Fields[0] == "changed" {
			t.Fatal("source provenance aliases caller-owned copy")
		}
	}
	profile, err := owner.Client().Profile(boundedContext(t))
	if err != nil || profile.ServiceVersion.Kind != "observed" || profile.ServiceVersion.Value != "483" || profile.SDKMode != "trino-direct" || len(profile.Options) == 0 {
		t.Fatal("coordinator readiness observation unavailable", err)
	}
	profile.Options[0].Value = "changed"
	current, err := owner.Client().Profile(boundedContext(t))
	if err != nil || current.Options[0].Value == "changed" || peer.readiness.Load() != 1 || peer.posts.Load() != 0 {
		t.Fatal("profile copy aliases state or performs hidden readiness", err)
	}
	build, err := trino.Build()
	if err != nil || len(build.SDKs) != 1 {
		t.Fatal("selected SDK observation missing", err)
	}
	build.SDKs[0].Path.Value = "changed"
	newBuild, err := trino.Build()
	if err != nil || newBuild.SDKs[0].Path.Value == "changed" {
		t.Fatal("build observation aliases mutable copies", err)
	}
}

func TestPublicVerifiedTLSAndExplicitAuthentication(t *testing.T) {
	for _, mode := range []string{"basic", "bearer"} {
		t.Run(mode, func(t *testing.T) {
			verify := func(request *http.Request) {
				if request.TLS == nil || request.Header.Get("X-Trino-User") != "fixture" {
					t.Error("public authenticated route lost TLS or explicit user")
				}
				if mode == "basic" {
					user, password, ok := request.BasicAuth()
					if !ok || user != "fixture" || password != "password_canary" {
						t.Error("explicit Basic credentials changed")
					}
				} else if request.Header.Get("Authorization") != "Bearer token_canary" {
					t.Error("explicit Bearer credential changed")
				}
			}
			peer := makeWirePeer(t, true, func(writer http.ResponseWriter, request *http.Request, _ []byte) {
				verify(request)
				writePage(t, writer, map[string]any{"id": "secure"})
			})
			peer.onReady = func(writer http.ResponseWriter, request *http.Request) {
				verify(request)
				writePage(t, writer, map[string]any{"id": "ready", "columns": []any{wireColumn("version", "varchar", "varchar", longArgument(2147483647))}, "data": [][]string{{"483"}}})
			}
			options := peer.settings()
			options.Plaintext = false
			options.RootCAPEM = string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: peer.server.Certificate().Raw}))
			if mode == "basic" {
				options.Password = "password_canary"
			} else {
				options.BearerToken = "token_canary"
			}
			owner, inbox, _ := openPublic(t, options, 0)
			value, err := owner.Client().Query(boundedContext(t), boundedContext(t), trino.Statement{SQL: "SELECT secure"})
			if err != nil || !value.Complete() || peer.readiness.Load() != 1 || peer.posts.Load() != 1 {
				t.Fatal("verified public TLS route failed", err)
			}
			acknowledge(t, inbox)
			options.Endpoint = "http://127.0.0.1:1"
			options.Plaintext = true
			if trino.Validate(options) == nil {
				t.Fatal("authenticated plaintext source accepted")
			}
		})
	}
}
