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
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	sdk "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/apache/iceberg-go/udf"
	"github.com/apache/iceberg-go/view"
	"github.com/google/uuid"
)

func installManagedViewPeer(peer *testPeer) {
	views := map[string]view.Metadata{}
	locations := map[string]string{}
	peer.hook = func(w http.ResponseWriter, r *http.Request) bool {
		if r.URL.Path == "/catalog/v1/config" {
			writeJSON(w, map[string]any{"defaults": map[string]string{"prefix": "warehouse"}, "endpoints": []string{
				"GET /v1/{prefix}/namespaces/{namespace}/views",
				"GET /v1/{prefix}/namespaces/{namespace}/views/{view}",
				"POST /v1/{prefix}/namespaces/{namespace}/views",
				"POST /v1/{prefix}/namespaces/{namespace}/views/{view}",
				"DELETE /v1/{prefix}/namespaces/{namespace}/views/{view}",
				"POST /v1/{prefix}/views/rename",
				"POST /v1/{prefix}/namespaces/{namespace}/register-view",
			}})
			return true
		}
		path := strings.TrimPrefix(r.URL.Path, "/catalog/v1/warehouse/")
		if !strings.Contains(path, "views") && !strings.Contains(path, "register-view") {
			return false
		}
		peer.mu.Lock()
		defer peer.mu.Unlock()
		persist := func(name string, meta view.Metadata) {
			views[name] = meta
			location := peer.options.Location + "views/" + uuid.NewString() + ".metadata.json"
			locations[name] = location
			raw, _ := json.Marshal(meta)
			peer.objects[strings.TrimPrefix(location, "s3://fixture/")] = raw
		}
		reply := func(name string) {
			if views[name] == nil {
				catalogError(w, 404)
				return
			}
			writeJSON(w, map[string]any{"metadata": views[name], "metadata-location": locations[name]})
		}
		if path == "views/rename" {
			var input struct{ Source, Destination struct{ Name string } }
			if json.NewDecoder(r.Body).Decode(&input) != nil || views[input.Source.Name] == nil {
				catalogError(w, 404)
				return true
			}
			views[input.Destination.Name], locations[input.Destination.Name] = views[input.Source.Name], locations[input.Source.Name]
			delete(views, input.Source.Name)
			delete(locations, input.Source.Name)
			w.WriteHeader(204)
			return true
		}
		if path == "namespaces/isolated/register-view" {
			var input struct {
				Name     string
				Location string `json:"metadata-location"`
			}
			if json.NewDecoder(r.Body).Decode(&input) != nil {
				catalogError(w, 400)
				return true
			}
			meta, err := view.ParseMetadataBytes(peer.objects[strings.TrimPrefix(input.Location, "s3://fixture/")])
			if err != nil {
				catalogError(w, 404)
				return true
			}
			views[input.Name], locations[input.Name] = meta, input.Location
			reply(input.Name)
			return true
		}
		if path == "namespaces/isolated/views" {
			if r.Method == http.MethodGet {
				names := make([]string, 0, len(views))
				for name := range views {
					names = append(names, name)
				}
				slices.Sort(names)
				start, _ := strconv.Atoi(r.URL.Query().Get("pageToken"))
				size := len(names)
				if text := r.URL.Query().Get("pageSize"); text != "" {
					size, _ = strconv.Atoi(text)
				}
				end := min(len(names), start+size)
				ids := make([]map[string]any, 0, end-start)
				for _, name := range names[start:end] {
					ids = append(ids, map[string]any{"namespace": []string{"isolated"}, "name": name})
				}
				response := map[string]any{"identifiers": ids}
				if end < len(names) {
					response["next-page-token"] = strconv.Itoa(end)
				}
				writeJSON(w, response)
				return true
			}
			var input struct {
				Name, Location string
				Schema         *sdk.Schema
				Version        *view.Version `json:"view-version"`
				Properties     sdk.Properties
			}
			if json.NewDecoder(r.Body).Decode(&input) != nil {
				catalogError(w, 400)
				return true
			}
			builder, _ := view.NewMetadataBuilder()
			meta, err := builder.SetLoc(input.Location).SetCurrentVersion(input.Version, input.Schema).SetProperties(input.Properties).Build()
			if err != nil {
				catalogError(w, 400)
				return true
			}
			persist(input.Name, meta.Metadata)
			reply(input.Name)
			return true
		}
		name := strings.TrimPrefix(path, "namespaces/isolated/views/")
		switch r.Method {
		case http.MethodGet:
			reply(name)
		case http.MethodDelete:
			delete(views, name)
			w.WriteHeader(204)
		case http.MethodPost:
			var input struct {
				Requirements view.Requirements
				Updates      view.Updates
			}
			if json.NewDecoder(r.Body).Decode(&input) != nil || views[name] == nil {
				catalogError(w, 400)
				return true
			}
			for _, requirement := range input.Requirements {
				if err := requirement.Validate(views[name]); err != nil {
					catalogError(w, 409)
					return true
				}
			}
			// Execute the native update protocol independently of the managed validator.
			builder, _ := view.MetadataBuilderFromBase(views[name])
			for _, update := range input.Updates {
				if err := update.Apply(builder); err != nil {
					catalogError(w, 400)
					return true
				}
			}
			meta, err := builder.Build()
			if err != nil {
				catalogError(w, 400)
				return true
			}
			peer.commits++
			persist(name, meta.Metadata)
			reply(name)
		default:
			catalogError(w, 400)
		}
		return true
	}
}

func TestManagedViewCatalogAndDetachedHistory(t *testing.T) {
	peer, options := newPeer(t)
	installManagedViewPeer(peer)
	f := bindManagedFixture(t, managedSettingsFor(options), 1)
	var detached *view.View
	f.run(t, "views", func(s *ManagedSession) error {
		id := []string{"isolated", "report"}
		version, err := view.NewVersionFromSQL(1, 0, "SELECT id, value FROM data", []string{"isolated"})
		if err != nil {
			return err
		}
		created, err := s.CreateView(id, version, testSchema(), catalog.WithViewLocation(options.Location+"views/report"), catalog.WithViewProperties(sdk.Properties{"owner": "fixture"}))
		if err != nil {
			return err
		}
		detached = created
		originalUUID := created.Metadata().ViewUUID()
		version.Representations[0].Sql = "caller changed input"
		reloaded, err := s.LoadView(id)
		if err != nil {
			return err
		}
		if !created.Equals(*reloaded) || reloaded.CurrentVersion().Representations[0].Sql != "SELECT id, value FROM data" {
			return fmt.Errorf("view input or detached state aliases caller")
		}
		next, err := view.NewVersionFromSQL(2, 0, "SELECT id FROM data", []string{"isolated"})
		if err != nil {
			return err
		}
		builder, _ := view.MetadataBuilderFromBase(reloaded.Metadata())
		built, err := builder.SetCurrentVersion(next, testSchema()).SetProperties(sdk.Properties{"review": "accepted"}).Build()
		if err != nil {
			return err
		}
		updated, err := s.UpdateView(id, []view.Requirement{view.AssertViewUUID(originalUUID)}, built.Changes)
		if err != nil {
			return err
		}
		if updated.Metadata().ViewUUID() != originalUUID || updated.CurrentVersion().VersionID != 2 || len(updated.Versions()) != 2 || created.CurrentVersion().VersionID != 1 {
			return fmt.Errorf("view history or detached generation changed")
		}
		registered, err := s.RegisterView([]string{"isolated", "registered"}, updated.MetadataLocation())
		if err != nil {
			return err
		}
		if !registered.Metadata().Equals(updated.Metadata()) {
			return fmt.Errorf("registered view metadata changed")
		}
		count := 0
		if err = s.VisitViews([]string{"isolated"}, 1, func(id []string) (bool, error) { count++; return true, nil }); err != nil || count != 2 {
			return fmt.Errorf("paged view list count=%d: %w", count, err)
		}
		peer.mu.Lock()
		before := peer.requests
		peer.mu.Unlock()
		if err = s.VisitViews([]string{"isolated"}, 1, func(id []string) (bool, error) { return false, nil }); err != nil {
			return err
		}
		peer.mu.Lock()
		added := peer.requests - before
		peer.mu.Unlock()
		if added != 1 {
			return fmt.Errorf("early break continued paging: %d", added)
		}
		if _, err = s.RenameView(id, []string{"isolated", "renamed"}); err != nil {
			return err
		}
		for _, name := range []string{"registered", "renamed"} {
			exists, err := s.CheckViewExists([]string{"isolated", name})
			if err != nil || !exists {
				return fmt.Errorf("view missing: %w", err)
			}
			if err = s.DropView([]string{"isolated", name}); err != nil {
				return err
			}
			exists, err = s.CheckViewExists([]string{"isolated", name})
			if err != nil || exists {
				return fmt.Errorf("dropped view still exists: %w", err)
			}
		}
		return nil
	})
	if detached.CurrentVersion().Representations[0].Sql != "SELECT id, value FROM data" {
		t.Fatal("pure detached view metadata expired or changed")
	}
}

func TestManagedFunctionOptionalMetadataAndNegotiation(t *testing.T) {
	for _, advertised := range []bool{false, true} {
		t.Run(strconv.FormatBool(advertised), func(t *testing.T) {
			peer, options := newPeer(t)
			var loads, pages atomic.Int32
			metadata := map[string]any{
				"function-uuid": uuid.NewString(), "format-version": 1,
				"definition-log": []any{map[string]any{"timestamp-ms": 1000, "definition-versions": []any{map[string]any{"definition-id": "long", "version-id": 1}}}},
				"definitions": []any{map[string]any{
					"definition-id": "long",
					"parameters":    []any{map[string]any{"name": "value", "type": "long"}},
					"return-type":   "long", "function-type": "udf",
					"current-version-id": 1,
					"versions": []any{map[string]any{
						"version-id": 1, "timestamp-ms": 1000,
						"representations": []any{map[string]any{"type": "sql", "dialect": "trino", "sql": "value"}},
					}},
				}},
			}
			raw, err := json.Marshal(metadata)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = udf.ParseMetadataBytes(raw); err != nil {
				t.Fatal("invalid native fixture", err)
			}
			peer.hook = func(w http.ResponseWriter, r *http.Request) bool {
				if r.URL.Path == "/catalog/v1/config" && advertised {
					writeJSON(w, map[string]any{"defaults": map[string]string{"prefix": "warehouse"}, "endpoints": []string{
						"GET /v1/{prefix}/namespaces/{namespace}/functions", "GET /v1/{prefix}/namespaces/{namespace}/functions/{function}",
					}})
					return true
				}
				if strings.HasSuffix(r.URL.Path, "/functions") {
					pages.Add(1)
					if r.URL.Query().Get("pageToken") == "" {
						writeJSON(w, map[string]any{"identifiers": [][]string{}, "next-page-token": "more"})
					} else {
						writeJSON(w, map[string]any{"identifiers": [][]string{{"isolated", "identity"}}})
					}
					return true
				}
				if strings.Contains(r.URL.Path, "/functions/") {
					loads.Add(1)
					if strings.HasSuffix(r.URL.Path, "/absent") {
						catalogError(w, 404)
					} else {
						writeJSON(w, map[string]any{"metadata": metadata})
					}
					return true
				}
				return false
			}
			f := bindManagedFixture(t, managedSettingsFor(options), 1)
			result, err := f.outcome(t, "functions", func(s *ManagedSession) error {
				ids, err := s.ListFunctions([]string{"isolated"})
				if err != nil {
					return err
				}
				if !advertised {
					if len(ids) != 0 {
						return fmt.Errorf("unsupported listing invented functions")
					}
					_, err = s.LoadFunction([]string{"isolated", "identity"})
					return err
				}
				if !reflect.DeepEqual(ids, [][]string{{"isolated", "identity"}}) {
					return fmt.Errorf("empty continuation lost functions")
				}
				fn, err := s.LoadFunction(ids[0])
				if err != nil {
					return err
				}
				if fn.MetadataLocation() != "" || fn.Metadata().Definitions()[0].CurrentVersion().Representations[0].RepresentationType() != "sql" {
					return fmt.Errorf("native pure function metadata changed")
				}
				exists, err := s.CheckFunctionExists([]string{"isolated", "absent"})
				if err != nil || exists {
					return fmt.Errorf("missing function confused with unsupported: %w", err)
				}
				return nil
			})
			if advertised {
				if err != nil || !result.Released() || pages.Load() != 2 || loads.Load() != 2 {
					t.Fatalf("advertised function profile: %s pages=%d loads=%d", causes(err), pages.Load(), loads.Load())
				}
			} else if !errors.Is(err, rest.ErrEndpointNotSupported) || pages.Load() != 0 || loads.Load() != 0 {
				t.Fatal("unsupported function endpoint was called or misclassified")
			}
		})
	}
}

func managedViewMetadata(t *testing.T, location string) view.Metadata {
	t.Helper()
	version, err := view.NewVersionFromSQL(1, testSchema().ID, "SELECT id, value FROM data", []string{"isolated"})
	if err != nil {
		t.Fatal(err)
	}
	builder, err := view.NewMetadataBuilder()
	if err != nil {
		t.Fatal(err)
	}
	meta, err := builder.SetLoc(location).SetCurrentVersion(version, testSchema()).Build()
	if err != nil {
		t.Fatal(err)
	}
	return meta.Metadata
}

func (f managedIntegrationFixture) outcome(t *testing.T, name string, work func(*ManagedSession) error) (ManagedResult, error) {
	t.Helper()
	receipt, err := f.client.Use(deadline(t), correlation(name), work)
	if err != nil {
		t.Fatal(err)
	}
	delivery, err := f.inbox.Next(deadline(t))
	if err != nil {
		t.Fatal(err)
	}
	defer delivery.Release()
	result, err := receipt.WaitReleased(deadline(t))
	if err != nil {
		t.Fatal(err)
	}
	return result.Outcome.Value, result.Err()
}

func TestManagedViewAuthorityBeforeMutation(t *testing.T) {
	peer, options := newPeer(t)
	base := managedViewMetadata(t, options.Location+"view")
	var submissions atomic.Int32
	peer.hook = func(w http.ResponseWriter, r *http.Request) bool {
		if !strings.HasSuffix(r.URL.Path, "/views/report") {
			return false
		}
		meta := base
		if r.Method == http.MethodPost {
			submissions.Add(1)
			var payload struct{ Updates view.Updates }
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				catalogError(w, 400)
				return true
			}
			builder, _ := view.MetadataBuilderFromBase(base)
			for _, update := range payload.Updates {
				if err := update.Apply(builder); err != nil {
					catalogError(w, 400)
					return true
				}
			}
			next, err := builder.Build()
			if err != nil {
				catalogError(w, 400)
				return true
			}
			meta = next.Metadata
		}
		writeJSON(w, map[string]any{"metadata-location": options.Location + "view/metadata.json", "metadata": meta})
		return true
	}
	settings := managedSettingsFor(options)
	settings.Properties = map[string]string{"view-endpoints-supported": "true"}
	f := bindManagedFixture(t, settings, 1)
	result, err := f.outcome(t, "outside-view", func(s *ManagedSession) error {
		_, err := s.UpdateView([]string{"isolated", "report"}, nil, []view.Update{view.NewSetLocationUpdate("s3://fixture/outside/")})
		return err
	})
	if !errors.Is(err, ErrAuthority) || submissions.Load() != 0 {
		t.Errorf("outside location was not rejected before mutation: error=%v submissions=%d", err, submissions.Load())
	}
	for _, effect := range result.Effects() {
		if effect.Submitted {
			t.Error("invalid view change was submitted")
		}
	}
}

func TestManagedViewWrongCommitIdentityRemainsUnknown(t *testing.T) {
	peer, options := newPeer(t)
	base := managedViewMetadata(t, options.Location+"view")
	wrong := managedViewMetadata(t, options.Location+"view")
	var submissions atomic.Int32
	peer.hook = func(w http.ResponseWriter, r *http.Request) bool {
		if !strings.HasSuffix(r.URL.Path, "/views/report") {
			return false
		}
		meta := base
		if r.Method == http.MethodPost {
			submissions.Add(1)
			meta = wrong
		}
		writeJSON(w, map[string]any{"metadata-location": options.Location + "view/metadata.json", "metadata": meta})
		return true
	}
	settings := managedSettingsFor(options)
	settings.Properties = map[string]string{"view-endpoints-supported": "true"}
	f := bindManagedFixture(t, settings, 1)
	result, err := f.outcome(t, "wrong-view", func(s *ManagedSession) error {
		_, err := s.UpdateView([]string{"isolated", "report"}, nil, []view.Update{view.NewSetPropertiesUpdate(map[string]string{"review": "requested"})})
		return err
	})
	if !errors.Is(err, rest.ErrCommitStateUnknown) || submissions.Load() != 1 {
		t.Errorf("wrong view acknowledgement: error=%v submissions=%d", err, submissions.Load())
	}
	if effects := result.Effects(); len(effects) != 1 || effects[0].Effect != Unknown || effects[0].Complete {
		t.Error("wrong UUID became confirmed view commit")
	}
}
