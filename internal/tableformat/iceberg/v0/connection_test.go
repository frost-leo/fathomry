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
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	native "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
	"github.com/google/uuid"
)

type testPeer struct {
	mu          sync.Mutex
	options     OptionsV1
	namespace   bool
	tables      map[string][]byte
	locations   map[string]string
	objects     map[string][]byte
	requests    int
	commits     int
	loads       int
	hook        func(http.ResponseWriter, *http.Request) bool
	afterCommit func(http.ResponseWriter, *http.Request, []byte) bool
}

func newPeer(t *testing.T) (*testPeer, OptionsV1) {
	t.Helper()
	peer := &testPeer{tables: map[string][]byte{}, locations: map[string]string{}, objects: map[string][]byte{}}
	server := httptest.NewServer(http.HandlerFunc(peer.serve))
	t.Cleanup(server.Close)
	options := OptionsV1{Name: "lake", CatalogURI: server.URL + "/catalog", CatalogPrefix: "warehouse", Warehouse: "test",
		Namespace: "isolated", Location: "s3://fixture/owned/", Plaintext: true, StorageEndpoint: server.URL, StorageRegion: "us-east-1",
		StorageAccessKey: "test-access", StorageSecretKey: "test-secret", Writes: true, Timeout: 15 * time.Second}
	peer.options = options
	return peer, options
}
func (peer *testPeer) serve(writer http.ResponseWriter, request *http.Request) {
	peer.mu.Lock()
	peer.requests++
	hook := peer.hook
	peer.mu.Unlock()
	if hook != nil && hook(writer, request) {
		return
	}
	if strings.HasPrefix(request.URL.Path, "/fixture") {
		peer.serveObjects(writer, request)
		return
	}
	if request.URL.Path == "/catalog/v1/config" {
		writeJSON(writer, map[string]any{"defaults": map[string]string{"prefix": "warehouse"}})
		return
	}
	route := strings.TrimPrefix(request.URL.Path, "/catalog/v1/warehouse/namespaces")
	if route == "" && request.Method == "POST" {
		peer.mu.Lock()
		defer peer.mu.Unlock()
		if peer.namespace {
			catalogError(writer, 409)
			return
		}
		peer.namespace = true
		writeJSON(writer, map[string]any{"namespace": []string{"isolated"}, "properties": map[string]string{}})
		return
	}
	if route == "/isolated" && request.Method == "DELETE" {
		peer.mu.Lock()
		defer peer.mu.Unlock()
		if len(peer.tables) > 0 {
			catalogError(writer, 409)
			return
		}
		peer.namespace = false
		writer.WriteHeader(204)
		return
	}
	if route == "/isolated/tables" {
		switch request.Method {
		case "POST":
			var payload struct {
				Name       string
				Location   string
				Schema     *native.Schema
				Properties native.Properties
			}
			if json.NewDecoder(request.Body).Decode(&payload) != nil {
				catalogError(writer, 400)
				return
			}
			peer.mu.Lock()
			defer peer.mu.Unlock()
			if peer.tables[payload.Name] != nil {
				catalogError(writer, 409)
				return
			}
			metadata, err := table.NewMetadata(payload.Schema, native.UnpartitionedSpec, table.UnsortedSortOrder, payload.Location, payload.Properties)
			if err != nil {
				catalogError(writer, 400)
				return
			}
			peer.tables[payload.Name], err = json.Marshal(metadata)
			if err != nil {
				catalogError(writer, 500)
				return
			}
			peer.persist(payload.Name, peer.tables[payload.Name])
			writeJSON(writer, peer.tableResponse(payload.Name, peer.tables[payload.Name]))
			return
		case "GET":
			peer.mu.Lock()
			defer peer.mu.Unlock()
			names := make([]string, 0, len(peer.tables))
			for name := range peer.tables {
				names = append(names, name)
			}
			slices.Sort(names)
			identifiers := []map[string]any{}
			for _, name := range names {
				identifiers = append(identifiers, map[string]any{"namespace": []string{"isolated"}, "name": name})
			}
			writeJSON(writer, map[string]any{"identifiers": identifiers})
			return
		}
	}
	name := strings.TrimPrefix(route, "/isolated/tables/")
	if name == route {
		catalogError(writer, 404)
		return
	}
	switch request.Method {
	case "GET":
		peer.mu.Lock()
		defer peer.mu.Unlock()
		raw := peer.tables[name]
		if raw == nil {
			catalogError(writer, 404)
			return
		}
		peer.loads++
		writeJSON(writer, peer.tableResponse(name, raw))
	case "DELETE":
		peer.mu.Lock()
		defer peer.mu.Unlock()
		delete(peer.tables, name)
		writer.WriteHeader(204)
	case "POST":
		var payload struct {
			Requirements table.Requirements
			Updates      table.Updates
		}
		if json.NewDecoder(request.Body).Decode(&payload) != nil {
			catalogError(writer, 400)
			return
		}
		peer.mu.Lock()
		peer.commits++
		meta, err := table.ParseMetadataBytes(peer.tables[name])
		if err != nil {
			peer.mu.Unlock()
			catalogError(writer, 404)
			return
		}
		for _, requirement := range payload.Requirements {
			if err = requirement.Validate(meta); err != nil {
				peer.mu.Unlock()
				catalogError(writer, 409)
				return
			}
		}
		meta, err = table.UpdateTableMetadata(meta, payload.Updates, peer.options.Location+name+"/metadata/old.json")
		if err != nil {
			peer.mu.Unlock()
			catalogError(writer, 400)
			return
		}
		raw, err := json.Marshal(meta)
		if err != nil {
			peer.mu.Unlock()
			catalogError(writer, 500)
			return
		}
		peer.persist(name, raw)
		body, _ := json.Marshal(peer.tableResponse(name, raw))
		after := peer.afterCommit
		peer.mu.Unlock()
		if after != nil && after(writer, request, body) {
			return
		}
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write(body)
	default:
		catalogError(writer, 404)
	}
}
func (peer *testPeer) tableResponse(name string, raw []byte) map[string]any {
	return map[string]any{"metadata": json.RawMessage(raw), "metadata-location": peer.locations[name]}
}

func (peer *testPeer) persist(name string, raw []byte) {
	peer.tables[name] = bytes.Clone(raw)
	location := peer.options.Location + name + "/metadata/" + uuid.NewString() + ".json"
	peer.locations[name] = location
	peer.objects[strings.TrimPrefix(location, "s3://fixture/")] = bytes.Clone(raw)
}
func writeJSON(writer http.ResponseWriter, value any) {
	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(value)
}
func catalogError(writer http.ResponseWriter, status int) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(map[string]any{"error": map[string]any{"message": "fixture-native-canary", "type": "FixtureError", "code": status}})
}
func (peer *testPeer) serveObjects(writer http.ResponseWriter, request *http.Request) {
	key := strings.TrimPrefix(request.URL.Path, "/fixture/")
	if request.Method == "HEAD" && (request.URL.Path == "/fixture/" || request.URL.Path == "/fixture") {
		writer.WriteHeader(200)
		return
	}
	peer.mu.Lock()
	defer peer.mu.Unlock()
	switch request.Method {
	case "PUT":
		if _, exists := peer.objects[key]; exists && request.Header.Get("If-None-Match") == "*" {
			writer.WriteHeader(412)
			return
		}
		raw, err := io.ReadAll(request.Body)
		if err != nil {
			writer.WriteHeader(400)
			return
		}
		peer.objects[key] = raw
		hash := sha256.Sum256(raw)
		writer.Header().Set("ETag", strconv.Quote(hex.EncodeToString(hash[:])))
	case "GET", "HEAD":
		raw, exists := peer.objects[key]
		if !exists {
			writer.Header().Set("Content-Type", "application/xml")
			writer.WriteHeader(404)
			_, _ = io.WriteString(writer, "<Error><Code>NoSuchKey</Code></Error>")
			return
		}
		hash := sha256.Sum256(raw)
		writer.Header().Set("ETag", strconv.Quote(hex.EncodeToString(hash[:])))
		writer.Header().Set("Last-Modified", time.Unix(1700000000, 0).UTC().Format(http.TimeFormat))
		writer.Header().Set("Content-Type", "application/octet-stream")
		if match := request.Header.Get("If-Match"); match != "" && match != strconv.Quote(hex.EncodeToString(hash[:])) {
			writer.WriteHeader(412)
			return
		}
		if request.Method == "GET" && request.Header.Get("Range") != "" {
			var start, end int
			if _, err := fmt.Sscanf(request.Header.Get("Range"), "bytes=%d-%d", &start, &end); err != nil || start < 0 || end < start || end >= len(raw) {
				writer.WriteHeader(416)
				return
			}
			writer.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(raw)))
			raw = raw[start : end+1]
			writer.Header().Set("Content-Length", strconv.Itoa(len(raw)))
			writer.WriteHeader(206)
		} else {
			writer.Header().Set("Content-Length", strconv.Itoa(len(raw)))
		}
		if request.Method == "GET" {
			_, _ = writer.Write(raw)
		}
	default:
		writer.WriteHeader(405)
	}
}

type fixture struct {
	client   *Client
	assembly *resource.Assembly
	selected resource.Selection[Source]
	inbox    *invocation.Inbox[Result]
}

func deadline(t testing.TB) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	return ctx
}
func correlation(name string) fault.Correlation { return fault.Correlation{Call: name} }
func bindFixture(t *testing.T, options OptionsV1, capacity int) fixture {
	t.Helper()
	selected, err := Select(options)
	if err != nil {
		t.Fatal(err)
	}
	selected = resource.WithLimits(selected, LimitsV1(options))
	assembly, err := resource.Assemble(deadline(t), deadline(t), "test", selected)
	if err != nil {
		if assembly != nil {
			_ = assembly.Close(deadline(t))
		}
		t.Fatalf("assembly: %v; causes: %s", err, causes(err))
	}
	inbox, err := invocation.NewInbox[Result](capacity, int64(capacity)*defaults(options).evidenceBytes())
	if err != nil {
		t.Fatal(err)
	}
	client, err := Bind(assembly, selected, inbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	f := fixture{client, assembly, selected, inbox}
	t.Cleanup(func() {
		drain(t, f)
		if err := assembly.Close(deadline(t)); err != nil {
			t.Error(err)
		}
	})
	return f
}
func drain(t testing.TB, f fixture) {
	t.Helper()
	for f.inbox.Usage().Outstanding > 0 {
		delivery, err := f.inbox.Next(deadline(t))
		if err != nil {
			t.Error(err)
			return
		}
		if _, err = delivery.Receipt().WaitReleased(deadline(t)); err != nil {
			t.Error(err)
			return
		}
		if err = delivery.Release(); err != nil {
			t.Error(err)
			return
		}
	}
}
func settle(t testing.TB, receipt *invocation.Receipt[Result], err error) invocation.Result[Result] {
	t.Helper()
	if err != nil {
		t.Fatalf("setup: %v; %s", err, causes(err))
	}
	result, err := receipt.WaitReleased(deadline(t))
	if err != nil {
		t.Fatal(err)
	}
	return result
}
func requireOK(t testing.TB, result invocation.Result[Result]) Result {
	t.Helper()
	if result.Err() != nil {
		t.Fatalf("operation: %v; %s", result.Err(), causes(result.Err()))
	}
	return result.Outcome.Value
}

// Only synthetic fixture failures use this helper. Real-service tests never
// print native causes, because SDK errors can contain endpoints or credentials.
func causes(err error) string {
	if err == nil {
		return ""
	}
	var buffer bytes.Buffer
	var visit func(error)
	visit = func(err error) {
		if err == nil {
			return
		}
		fmt.Fprintln(&buffer, err.Error())
		switch wrapped := err.(type) {
		case interface{ Unwrap() []error }:
			for _, child := range wrapped.Unwrap() {
				visit(child)
			}
		case interface{ Unwrap() error }:
			visit(wrapped.Unwrap())
		}
	}
	visit(err)
	return buffer.String()
}
