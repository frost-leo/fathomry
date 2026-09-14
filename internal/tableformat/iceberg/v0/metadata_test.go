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
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"

	native "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"github.com/frost-leo/fathomry/internal/resource"
)

func TestRegressionBoundaryPositiveControls(t *testing.T) {
	t.Run("selected-prefix-and-empty-list", func(t *testing.T) {
		_, options := newPeer(t)
		f := bindFixture(t, options, 4)
		receipt, err := f.client.ListTables(deadline(t), correlation("valid-empty"))
		result := requireOK(t, settle(t, receipt, err))
		if !result.Complete() || len(result.NamesCopy()) != 0 {
			t.Fatal("valid empty list was not complete")
		}
		receipt, err = f.client.InspectTable(deadline(t), correlation("valid-missing"), "data")
		if result := settle(t, receipt, err); !errors.Is(result.Err(), catalog.ErrNoSuchTable) {
			t.Fatalf("well-formed 404 did not retain native identity: %v", result.Err())
		}
	})
	t.Run("correct-namespace-and-metadata", func(t *testing.T) {
		_, options := newPeer(t)
		f := bindFixture(t, options, 4)
		setupTable(t, f)
		receipt, err := f.client.InspectTable(deadline(t), correlation("valid-metadata"), "data")
		result := requireOK(t, settle(t, receipt, err))
		if !result.Complete() || !result.Reloaded() {
			t.Fatal("valid metadata was not complete")
		}
	})
	t.Run("canonical-forbidden-prefix-rejected", func(t *testing.T) {
		peer, options := newPeer(t)
		peer.hook = func(writer http.ResponseWriter, request *http.Request) bool {
			if strings.HasSuffix(request.URL.Path, "/config") {
				_, _ = writer.Write([]byte(`{"overrides":{"prefix":"unauthorized"}}`))
				return true
			}
			return false
		}
		selected, err := Select(options)
		if err != nil {
			t.Fatal(err)
		}
		selected = resource.WithLimits(selected, LimitsV1(options))
		assembly, err := resource.Assemble(deadline(t), deadline(t), "control", selected)
		if assembly != nil {
			_ = assembly.Close(deadline(t))
		}
		if !errors.Is(err, ErrAuthority) {
			t.Fatalf("canonical forbidden prefix not rejected: %v", err)
		}
	})
	t.Run("canonical-forbidden-metadata-rejected", func(t *testing.T) {
		peer, options := newPeer(t)
		f := bindFixture(t, options, 4)
		setupTable(t, f)
		peer.mu.Lock()
		var metadata map[string]any
		_ = json.Unmarshal(peer.tables["data"], &metadata)
		metadata["location"] = "s3://fixture/not-authorized/data"
		unsafe, _ := json.Marshal(metadata)
		peer.persist("data", unsafe)
		peer.mu.Unlock()
		receipt, err := f.client.InspectTable(deadline(t), correlation("invalid-location"), "data")
		if result := settle(t, receipt, err); !errors.Is(result.Err(), ErrAuthority) {
			t.Fatalf("canonical forbidden metadata not rejected: %v", result.Err())
		}
	})
}

func TestRegressionConfigCasingCannotChangePrefix(t *testing.T) {
	for _, body := range []string{
		`{"overrides":{},"Overrides":{"prefix":"wrong"}}`,
		`{"overrides":{"prefix":"wrong"},"overrides":{}}`,
		`{"Overrides":{"prefix":"wrong"}}`,
	} {
		peer, options := newPeer(t)
		peer.hook = func(writer http.ResponseWriter, request *http.Request) bool {
			if strings.HasSuffix(request.URL.Path, "/config") {
				_, _ = writer.Write([]byte(body))
				return true
			}
			return false
		}
		selected, err := Select(options)
		if err != nil {
			t.Fatal(err)
		}
		selected = resource.WithLimits(selected, LimitsV1(options))
		assembly, err := resource.Assemble(deadline(t), deadline(t), "rejected", selected)
		if assembly != nil {
			_ = assembly.Close(deadline(t))
		}
		if !errors.Is(err, ErrProtocol) {
			t.Fatal("ambiguous configuration reply accepted")
		}
	}
}

func TestRegressionNullSchemaDoesNotPanic(t *testing.T) {
	options := OptionsV1{CatalogURI: "http://127.0.0.1:1234/catalog", Namespace: "isolated", Location: "s3://fixture/owned/"}
	metadata, err := table.NewMetadata(testSchema(), native.UnpartitionedSpec, table.UnsortedSortOrder, options.Location+"data", tableProperties(defaults(options)))
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(metadata)
	var payload map[string]any
	_ = json.Unmarshal(raw, &payload)
	payload["schemas"] = []any{nil}
	body, _ := json.Marshal(map[string]any{"metadata": payload, "metadata-location": options.Location + "data/metadata/current.json"})
	request, _ := http.NewRequest("GET", options.CatalogURI+"/v1/warehouse/namespaces/isolated/tables/data", nil)
	defer func() {
		if panicked := recover(); panicked != nil {
			t.Errorf("malformed Catalog metadata panics instead of rejecting: %v", panicked)
		}
	}()
	if err := validateResponse(defaults(options), request, 200, body); err == nil {
		t.Fatal("malformed Catalog metadata accepted")
	}
}

func TestRegressionMalformedCatalogListsAreNotComplete(t *testing.T) {
	for _, body := range []string{`{}`, `{"identifiers":null}`} {
		t.Run(body, func(t *testing.T) {
			peer, options := newPeer(t)
			f := bindFixture(t, options, 2)
			peer.mu.Lock()
			peer.hook = func(writer http.ResponseWriter, request *http.Request) bool {
				if request.Method == "GET" && strings.HasSuffix(request.URL.Path, "/tables") {
					_, _ = writer.Write([]byte(body))
					return true
				}
				return false
			}
			peer.mu.Unlock()
			receipt, err := f.client.ListTables(deadline(t), correlation("missing-identifiers"))
			result := settle(t, receipt, err)
			if result.Err() == nil || result.Outcome.Value.Complete() {
				t.Fatalf("malformed response became complete empty namespace: error=%v complete=%v names=%d", result.Err(), result.Outcome.Value.Complete(), len(result.Outcome.Value.NamesCopy()))
			}
		})
	}
}

func TestRegressionWrongNamespaceReplyIsNotAcknowledged(t *testing.T) {
	peer, options := newPeer(t)
	f := bindFixture(t, options, 2)
	peer.mu.Lock()
	peer.hook = func(writer http.ResponseWriter, request *http.Request) bool {
		if request.Method == "POST" && strings.HasSuffix(request.URL.Path, "/namespaces") {
			_, _ = writer.Write([]byte(`{"namespace":["not_selected"],"properties":{}}`))
			return true
		}
		return false
	}
	peer.mu.Unlock()
	receipt, err := f.client.CreateNamespace(deadline(t), correlation("wrong-namespace"))
	result := settle(t, receipt, err)
	if result.Err() == nil || result.Outcome.Value.Complete() || result.Outcome.Value.Effect() == Acknowledged {
		t.Fatalf("wrong namespace acknowledged: error=%v complete=%v effect=%v", result.Err(), result.Outcome.Value.Complete(), result.Outcome.Value.Effect())
	}
}

func TestRegressionMetadataCasingCannotSkipValidation(t *testing.T) {
	peer, options := newPeer(t)
	f := bindFixture(t, options, 8)
	setupTable(t, f)
	peer.mu.Lock()
	valid := bytes.Clone(peer.tables["data"])
	var metadata map[string]any
	_ = json.Unmarshal(valid, &metadata)
	metadata["location"] = "s3://fixture/not-authorized/data"
	metadata["properties"].(map[string]any)["commit.retry.num-retries"] = "7"
	unsafe, _ := json.Marshal(metadata)
	location := peer.locations["data"]
	peer.objects[strings.TrimPrefix(location, "s3://fixture/")] = unsafe
	peer.hook = func(writer http.ResponseWriter, request *http.Request) bool {
		if request.Method == "GET" && strings.HasSuffix(request.URL.Path, "/tables/data") {
			_, _ = fmt.Fprintf(writer, `{"metadata":%s,"Metadata":%s,"metadata-location":%q}`, valid, unsafe, location)
			return true
		}
		return false
	}
	peer.mu.Unlock()
	receipt, err := f.client.InspectTable(deadline(t), correlation("case-metadata"), "data")
	result := settle(t, receipt, err)
	if result.Err() == nil {
		loaded, parseErr := table.ParseMetadataBytes(result.Outcome.Value.MetadataCopy())
		if parseErr != nil {
			t.Fatal(parseErr)
		}
		t.Fatalf("unvalidated SDK metadata accepted: location=%q retries=%q complete=%v", loaded.Location(), loaded.Properties()["commit.retry.num-retries"], result.Outcome.Value.Complete())
	}
}

func TestRegressionMalformed404RetainsNoSuchTable(t *testing.T) {
	peer, options := newPeer(t)
	f := bindFixture(t, options, 2)
	peer.mu.Lock()
	peer.hook = func(writer http.ResponseWriter, request *http.Request) bool {
		if request.Method == "GET" && strings.HasSuffix(request.URL.Path, "/tables/data") {
			writer.WriteHeader(404)
			_, _ = writer.Write([]byte("<html>not found</html>"))
			return true
		}
		return false
	}
	peer.mu.Unlock()
	receipt, err := f.client.InspectTable(deadline(t), correlation("malformed-missing"), "data")
	result := settle(t, receipt, err)
	if !errors.Is(result.Err(), catalog.ErrNoSuchTable) {
		t.Fatalf("HTTP 404 identity lost: %s", causes(result.Err()))
	}
}

func FuzzMetadataBoundary(f *testing.F) {
	options := OptionsV1{Namespace: "fixture", Location: "s3://fixture/owned/"}
	settings := defaults(options)
	metadata, err := table.NewMetadata(testSchema(), native.UnpartitionedSpec, table.UnsortedSortOrder, options.Location+"data", tableProperties(settings))
	if err != nil {
		f.Fatal(err)
	}
	raw, err := json.Marshal(metadata)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(raw)
	f.Add([]byte(`{"schemas":[null]}`))
	f.Add([]byte(`{"metadata":null}`))
	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) > 64<<10 {
			t.Skip()
		}
		value, err := parseMetadata(settings, raw)
		if err == nil && (value == nil || value.Version() != 2 || validateMetadata(settings, value) != nil) {
			t.Fatal("invalid metadata accepted")
		}
	})
}
