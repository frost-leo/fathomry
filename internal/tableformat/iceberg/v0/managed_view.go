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
	"net/http"
	"strings"

	native "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/apache/iceberg-go/udf"
	"github.com/apache/iceberg-go/view"
)

func (f *ManagedSession) validateView(meta view.Metadata) error {
	if meta == nil {
		return failure(ErrProtocol, "view-metadata")
	}
	if err := f.location(strings.TrimSuffix(meta.Location(), "/") + "/"); err != nil {
		return err
	}
	if len(meta.Schemas()) > f.source.prepared.settings.MaxListEntries || len(meta.Versions()) > f.source.prepared.settings.MaxListEntries {
		return failure(ErrLimit, "view-history")
	}
	_, err := f.boundedProperties(meta.Properties())
	return err
}

func updatedView(base view.Metadata, updates view.Updates) (view.Metadata, error) {
	builder, err := view.MetadataBuilderFromBase(base)
	if err != nil {
		return nil, err
	}
	for _, update := range updates {
		if update == nil || typedNil(update) {
			return nil, failure(ErrInput, "view-update")
		}
		if err = update.Apply(builder); err != nil {
			return nil, err
		}
	}
	result, err := builder.Build()
	if err != nil {
		return nil, err
	}
	return result.Metadata, nil
}

func (f *ManagedSession) validateViewReply(request *http.Request, sent []byte, object map[string]any) error {
	raw, err := json.Marshal(object)
	if err != nil {
		return err
	}
	actual, err := view.ParseMetadataBytes(raw)
	if err != nil {
		return err
	}
	if err = f.validateView(actual); err != nil {
		return err
	}
	if request.Method == http.MethodPost && strings.Contains(request.URL.Path, "/views/") {
		f.mu.Lock()
		base := f.viewBases[request.URL.Path]
		f.mu.Unlock()
		if base != nil {
			var payload struct{ Updates view.Updates }
			if err = json.Unmarshal(sent, &payload); err != nil {
				return err
			}
			wanted, err := updatedView(base, payload.Updates)
			if err != nil {
				return err
			}
			if !viewCommitMatches(base, wanted, actual) {
				return failure(ErrProtocol, "view-commit-intent", rest.ErrCommitStateUnknown)
			}
		}
	}
	if request.Method == http.MethodPost && strings.HasSuffix(request.URL.Path, "/views") {
		var payload struct {
			Location   string
			Schema     *native.Schema
			Properties native.Properties
			Version    *view.Version `json:"view-version"`
		}
		if err = json.Unmarshal(sent, &payload); err != nil {
			return err
		}
		builder, err := view.NewMetadataBuilder()
		if err != nil {
			return err
		}
		location := payload.Location
		if location == "" {
			location = actual.Location()
		}
		wanted, err := builder.SetLoc(location).SetCurrentVersion(payload.Version, payload.Schema).SetProperties(payload.Properties).Build()
		if err != nil {
			return err
		}
		if actual.Location() != wanted.Location() || !actual.CurrentSchema().Equals(wanted.CurrentSchema()) || !actual.CurrentVersion().Equals(wanted.CurrentVersion()) {
			return failure(ErrProtocol, "view-create-intent", rest.ErrCommitStateUnknown)
		}
		for key, value := range payload.Properties {
			if actual.Properties()[key] != value {
				return failure(ErrProtocol, "view-create-properties", rest.ErrCommitStateUnknown)
			}
		}
	}
	f.mu.Lock()
	f.viewBases[request.URL.Path] = actual
	f.mu.Unlock()
	return nil
}

func viewCommitMatches(base, wanted, actual view.Metadata) bool {
	if actual.ViewUUID() != wanted.ViewUUID() {
		return false
	}
	objects := make([]map[string]any, 3)
	for index, meta := range []view.Metadata{base, wanted, actual} {
		raw, err := json.Marshal(meta)
		if err != nil {
			return false
		}
		objects[index], err = decodeObjectBounded(raw, len(raw))
		if err != nil {
			return false
		}
	}
	for key, want := range objects[1] {
		if key == "version-log" {
			continue
		}
		if !metadataDeltaMatches(key, objects[0][key], want, objects[2][key]) {
			return false
		}
	}
	return true
}

func (f *ManagedSession) validateFunctionReply(object map[string]any, location string) error {
	if location != "" {
		if err := f.location(location); err != nil {
			return err
		}
	}
	raw, err := json.Marshal(object)
	if err != nil {
		return err
	}
	meta, err := udf.ParseMetadataBytes(raw)
	if err != nil {
		return err
	}
	if meta.Location() != "" {
		if err := f.location(strings.TrimSuffix(meta.Location(), "/") + "/"); err != nil {
			return err
		}
	}
	if len(meta.Definitions()) > f.source.prepared.settings.MaxListEntries {
		return failure(ErrLimit, "function-definitions")
	}
	_, err = f.boundedProperties(meta.Properties())
	return err
}
