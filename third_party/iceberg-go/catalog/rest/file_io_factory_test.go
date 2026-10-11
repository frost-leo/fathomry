// fathomry
// Copyright (C) 2026  Frost Leo
// SPDX-License-Identifier: GPL-3.0-or-later
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

package rest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/semaphore"
)

func TestTableValidatorRunsOnNativeRefreshAndRetainsRefusal(t *testing.T) {
	metadata, err := table.NewMetadata(iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64}), iceberg.UnpartitionedSpec, table.UnsortedSortOrder, "hook://bucket/table", nil)
	require.NoError(t, err)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/v1/config" {
			_, _ = w.Write([]byte(`{"defaults":{}}`))
			return
		}
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"metadata": metadata, "metadata-location": "hook://bucket/table/metadata.json"}))
	}))
	defer server.Close()
	refusal := errors.New("metadata-object-refused")
	calls := 0
	cat, err := NewCatalog(t.Context(), "validated", server.URL, WithFileIOFactory(func(context.Context, string, iceberg.Properties) (iceio.IO, error) { return iceio.LocalFS{}, nil }), WithTableValidator(func(ctx context.Context, tbl *table.Table) error {
		calls++
		require.Equal(t, "hook://bucket/table/metadata.json", tbl.MetadataLocation())
		_, err := tbl.FS(ctx)
		require.NoError(t, err)
		if calls == 2 {
			return refusal
		}
		return nil
	}))
	require.NoError(t, err)
	defer cat.Close()
	tbl, err := cat.LoadTable(t.Context(), table.Identifier{"ns", "tbl"})
	require.NoError(t, err)
	require.ErrorIs(t, tbl.Refresh(t.Context()), refusal)
	require.Equal(t, 2, calls)
}

func TestFileIOFactoryOverridesRegistryAndFreezesProperties(t *testing.T) {
	props := iceberg.Properties{"key": "value"}
	filesystem := &closeTrackingIO{IO: iceio.LocalFS{}, closeCount: new(atomic.Int32)}
	var calls int
	factory := func(ctx context.Context, location string, received iceberg.Properties) (iceio.IO, error) {
		calls++
		require.Equal(t, "no-registry://bucket/table", location)
		require.Equal(t, "value", received["key"])
		received["key"] = "mutated"
		return filesystem, nil
	}
	for range 2 {
		got, err := loadFileIO(t.Context(), factory, "no-registry://bucket/table", props)
		require.NoError(t, err)
		require.Same(t, filesystem, got)
	}
	require.Equal(t, 2, calls)
	require.Equal(t, "value", props["key"])
}

func TestFileIOFactoryFailureNeverFallsBack(t *testing.T) {
	want := errors.New("factory refusal")
	_, err := loadFileIO(t.Context(), func(context.Context, string, iceberg.Properties) (iceio.IO, error) {
		return nil, want
	}, "file:///allowed-by-native-registry", nil)
	require.ErrorIs(t, err, want)
	for _, filesystem := range []iceio.IO{nil, (*closeTrackingIO)(nil)} {
		_, err = loadFileIO(t.Context(), func(context.Context, string, iceberg.Properties) (iceio.IO, error) {
			return filesystem, nil
		}, "file:///allowed-by-native-registry", nil)
		require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
	}
	ctx, cancel := context.WithCancelCause(t.Context())
	cancel(want)
	_, err = loadFileIO(ctx, func(context.Context, string, iceberg.Properties) (iceio.IO, error) {
		t.Fatal("canceled construction invoked factory")
		return nil, nil
	}, "file:///allowed-by-native-registry", nil)
	require.ErrorIs(t, err, want)
}

func TestFileIOFactoryCatalogOptionSurvivesConfigAndRefresh(t *testing.T) {
	metadata, err := table.NewMetadata(iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64}),
		iceberg.UnpartitionedSpec, table.UnsortedSortOrder, "hook://bucket/table",
		iceberg.Properties{"table-property": "table", "layer": "metadata"})
	require.NoError(t, err)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/v1/config":
			_, _ = w.Write([]byte(`{"defaults":{"catalog-property":"catalog"},"overrides":{}}`))
		case "/v1/namespaces/ns/tables/tbl":
			require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
				"metadata": metadata, "metadata-location": "hook://bucket/table/metadata.json",
				"config": map[string]string{"table-response": "response", "layer": "response"},
			}))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	var calls int
	cat, err := NewCatalog(t.Context(), "managed", server.URL, WithFileIOFactory(
		func(ctx context.Context, location string, props iceberg.Properties) (iceio.IO, error) {
			calls++
			require.Equal(t, "hook://bucket/table/metadata.json", location)
			require.Equal(t, "catalog", props["catalog-property"])
			require.Equal(t, "table", props["table-property"])
			require.Equal(t, "response", props["table-response"])
			require.Equal(t, "response", props["layer"])
			return iceio.LocalFS{}, nil
		}))
	require.NoError(t, err)
	defer cat.Close()
	tbl, err := cat.LoadTable(t.Context(), table.Identifier{"ns", "tbl"})
	require.NoError(t, err)
	require.Zero(t, calls, "load remains lazy")
	_, err = tbl.FS(t.Context())
	require.NoError(t, err)
	require.NoError(t, tbl.Refresh(t.Context()))
	_, err = tbl.FS(t.Context())
	require.NoError(t, err)
	require.Equal(t, 2, calls)
}

func TestFileIOFactoryVendedRefreshUsesRenewedProperties(t *testing.T) {
	now := time.Now()
	var keys []string
	refresher := &vendedCredentialRefresher{
		mu: semaphore.NewWeighted(1), location: "hook://bucket/table", nowFunc: func() time.Time { return now },
		props: iceberg.Properties{iceio.S3AccessKeyID: "initial", keyS3TokenExpiresAtMs: strconv.FormatInt(now.Add(time.Minute).UnixMilli(), 10)},
		fetchCreds: func(context.Context, []string) (iceberg.Properties, error) {
			return iceberg.Properties{iceio.S3AccessKeyID: "renewed", keyS3TokenExpiresAtMs: strconv.FormatInt(now.Add(time.Hour).UnixMilli(), 10)}, nil
		},
		fileIOFactory: func(ctx context.Context, location string, props iceberg.Properties) (iceio.IO, error) {
			keys = append(keys, props[iceio.S3AccessKeyID])
			props[iceio.S3AccessKeyID] = "factory-mutation"
			return iceio.LocalFS{}, nil
		},
	}
	_, err := refresher.loadFS(t.Context())
	require.NoError(t, err)
	now = now.Add(2 * time.Minute)
	_, err = refresher.loadFS(t.Context())
	require.NoError(t, err)
	require.Equal(t, []string{"initial", "renewed"}, keys)
	require.Equal(t, "initial", refresher.props[iceio.S3AccessKeyID])
}

func TestFileIOFactoryRemotePlanPrefixesAndLeaseClose(t *testing.T) {
	var locations, keys []string
	var closes atomic.Int32
	planIO := planIOFromCredentialsWithFactory([]StorageCredential{
		{Prefix: "hook://bucket/data/", Config: iceberg.Properties{iceio.S3AccessKeyID: "data"}},
		{Prefix: "hook://bucket/delete/", Config: iceberg.Properties{iceio.S3AccessKeyID: "delete"}},
	}, "hook://bucket/metadata.json", iceberg.Properties{"base": "retained"},
		func(ctx context.Context, location string, props iceberg.Properties) (iceio.IO, error) {
			locations = append(locations, location)
			keys = append(keys, props[iceio.S3AccessKeyID])
			require.Equal(t, "retained", props["base"])
			return &closeTrackingIO{IO: iceio.LocalFS{}, closeCount: &closes}, nil
		})
	filesystem, err := planIO.Load(t.Context())
	require.NoError(t, err)
	prefixIO := filesystem.(*prefixScopedIO)
	_, err = prefixIO.filesystemFor("hook://bucket/data/part.parquet")
	require.NoError(t, err)
	_, err = prefixIO.filesystemFor("hook://bucket/delete/part.parquet")
	require.NoError(t, err)
	require.Equal(t, []string{"hook://bucket/data/part.parquet", "hook://bucket/delete/part.parquet"}, locations)
	require.Equal(t, []string{"data", "delete"}, keys)
	require.NoError(t, planIO.Close())
	require.NoError(t, planIO.Close())
	require.Equal(t, int32(2), closes.Load())
	_, err = planIO.Load(t.Context())
	require.Error(t, err)
}

func TestFileIOFactoryRefreshCachesCredentialsNotContexts(t *testing.T) {
	var contexts []context.Context
	var refreshes int
	refresher := &vendedCredentialRefresher{
		mu: semaphore.NewWeighted(1), location: "hook://bucket/table",
		props: iceberg.Properties{iceio.S3AccessKeyID: "original"},
		fetchCreds: func(context.Context, []string) (iceberg.Properties, error) {
			refreshes++
			return nil, errors.New("unexpected refresh")
		},
		fileIOFactory: func(ctx context.Context, location string, props iceberg.Properties) (iceio.IO, error) {
			contexts = append(contexts, ctx)
			require.Equal(t, "original", props[iceio.S3AccessKeyID])
			return iceio.LocalFS{}, nil
		},
	}
	first, cancel := context.WithCancel(t.Context())
	_, err := refresher.loadFS(first)
	require.NoError(t, err)
	cancel()
	_, err = refresher.loadFS(t.Context())
	require.NoError(t, err)
	require.Len(t, contexts, 2)
	require.ErrorIs(t, contexts[0].Err(), context.Canceled)
	require.NoError(t, contexts[1].Err())
	require.Zero(t, refreshes)
}

func TestFileIOFactoryPlanAcquisitionsHaveIndependentContexts(t *testing.T) {
	var contexts []context.Context
	planIO := planIOFromCredentialsWithFactory([]StorageCredential{
		{Prefix: "hook://bucket/", Config: iceberg.Properties{iceio.S3AccessKeyID: "plan"}},
	}, "hook://bucket/metadata.json", nil, func(ctx context.Context, location string, props iceberg.Properties) (iceio.IO, error) {
		contexts = append(contexts, ctx)
		return iceio.LocalFS{}, nil
	})
	defer planIO.Close()
	first, cancel := context.WithCancel(t.Context())
	filesystem, err := planIO.Load(first)
	require.NoError(t, err)
	_, err = filesystem.(*prefixScopedIO).filesystemFor("hook://bucket/data.parquet")
	require.NoError(t, err)
	cancel()
	filesystem, err = planIO.Load(t.Context())
	require.NoError(t, err)
	_, err = filesystem.(*prefixScopedIO).filesystemFor("hook://bucket/data.parquet")
	require.NoError(t, err)
	require.Len(t, contexts, 2)
	require.ErrorIs(t, contexts[0].Err(), context.Canceled)
	require.NoError(t, contexts[1].Err())
}

func TestStorageCredentialValidatorReceivesIndependentEntries(t *testing.T) {
	credentials := []StorageCredential{
		{Prefix: "hook://outside/", Config: iceberg.Properties{"key": "outside"}},
		{Prefix: "hook://inside/", Config: iceberg.Properties{"key": "inside"}},
	}
	want := errors.New("outside prefix")
	cat := &Catalog{storageCredentialValidator: func(ctx context.Context, location string, got []StorageCredential) error {
		require.Equal(t, "hook://inside/file", location)
		require.Len(t, got, 2, "nonmatching credentials must also reach policy")
		got[0].Prefix = "mutated"
		got[0].Config["key"] = "mutated"
		return want
	}}
	require.ErrorIs(t, cat.validateStorageCredentials(t.Context(), "hook://inside/file", credentials), want)
	require.Equal(t, "hook://outside/", credentials[0].Prefix)
	require.Equal(t, "outside", credentials[0].Config["key"])
}

func TestStorageCredentialValidatorRejectsPlanBeforeIOAndCancels(t *testing.T) {
	want := errors.New("vending disabled")
	var cancellations, factoryCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/v1/config":
			_, _ = w.Write([]byte(`{"defaults":{},"overrides":{},"endpoints":["POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/plan","GET /v1/{prefix}/namespaces/{namespace}/tables/{table}/plan/{plan-id}","DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table}/plan/{plan-id}"]}`))
		case r.Method == http.MethodDelete:
			cancellations.Add(1)
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodPost:
			_, _ = w.Write([]byte(`{"status":"completed","plan-id":"p1","storage-credentials":[{"prefix":"hook://outside/","config":{"key":"private"}}]}`))
		case r.Method == http.MethodGet:
			_, _ = w.Write([]byte(`{"status":"completed","storage-credentials":[{"prefix":"hook://outside/","config":{"key":"private"}}]}`))
		}
	}))
	defer server.Close()
	cat, err := NewCatalog(t.Context(), "managed", server.URL,
		WithFileIOFactory(func(context.Context, string, iceberg.Properties) (iceio.IO, error) {
			factoryCalls.Add(1)
			return nil, errors.New("must not be called")
		}),
		WithStorageCredentialValidator(func(ctx context.Context, location string, credentials []StorageCredential) error {
			require.Empty(t, location)
			require.Len(t, credentials, 1)
			return want
		}))
	require.NoError(t, err)
	defer cat.Close()
	ident := table.Identifier{"ns", "tbl"}
	_, err = cat.PlanTableScan(t.Context(), ident, PlanTableScanRequest{})
	require.ErrorIs(t, err, want)
	_, err = cat.FetchPlanningResult(t.Context(), ident, "p2", FetchPlanningResultOptions{})
	require.ErrorIs(t, err, want)
	require.Equal(t, int32(2), cancellations.Load())
	require.Zero(t, factoryCalls.Load())
}

func TestCatalogLoggerIsSourceLocal(t *testing.T) {
	previous := slog.Default()
	var output bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&output, nil))
	var options options
	WithLogger(logger)(&options)
	require.Same(t, logger, options.logger)
	resolveEndpointsWithLogger([]string{"not-a-valid-endpoint"}, false, options.logger)
	require.Contains(t, output.String(), "dropping unparseable")
	require.Same(t, previous, slog.Default())
}
