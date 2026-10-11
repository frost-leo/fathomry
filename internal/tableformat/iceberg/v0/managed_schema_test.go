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
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	sdk "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
)

func TestManagedDefaultEvolutionAndPromotion(t *testing.T) {
	_, options := newPeer(t)
	f := bindManagedFixture(t, managedSettingsFor(options), 1)
	schema := sdk.NewSchema(0, sdk.NestedField{ID: 1, Name: "id", Type: sdk.PrimitiveTypes.Int32, Required: true})
	allocator := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer allocator.AssertSize(t, 0)
	f.run(t, "default-evolution", func(s *ManagedSession) error {
		tbl, err := s.CreateTable([]string{"isolated", "data"}, schema, catalog.WithLocation(options.Location+"data"), catalog.WithProperties(sdk.Properties{"format-version": "3"}))
		if err != nil {
			return err
		}
		appendID := func(id int32) error {
			inputSchema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int32}}, nil)
			builder := array.NewInt32Builder(allocator)
			builder.Append(id)
			values := builder.NewArray()
			builder.Release()
			record := array.NewRecordBatch(inputSchema, []arrow.Array{values}, 1)
			values.Release()
			defer record.Release()
			reader, err := array.NewRecordReader(inputSchema, []arrow.RecordBatch{record})
			if err != nil {
				return err
			}
			tx, err := tbl.NewTransaction("")
			if err == nil {
				err = tx.Append(reader, nil)
			}
			reader.Release()
			if err != nil {
				return err
			}
			tbl, err = tx.Commit()
			return err
		}
		if err = appendID(1); err != nil {
			return err
		}
		old := tbl.table.CurrentSnapshot().SnapshotID
		tx, err := tbl.NewTransaction("")
		if err != nil {
			return err
		}
		if err = tx.EditSchema(true, false, nil, []SchemaEdit{
			{Action: "add", Path: []string{"defaulted"}, Type: sdk.PrimitiveTypes.Int64, Required: true, Default: sdk.NewLiteral(int64(17))},
			{Action: "update", Path: []string{"id"}, Column: table.ColumnUpdate{FieldType: sdk.Optional[sdk.Type]{Valid: true, Val: sdk.PrimitiveTypes.Int64}}},
		}); err != nil {
			return err
		}
		if err = tx.EditSchema(true, false, nil, []SchemaEdit{{Action: "update", Path: []string{"defaulted"}, Column: table.ColumnUpdate{WriteDefault: sdk.Optional[sdk.Literal]{Valid: true, Val: sdk.NewLiteral(int64(29))}}}}); err != nil {
			return err
		}
		tbl, err = tx.Commit()
		if err != nil {
			return err
		}
		if err = appendID(2); err != nil {
			return err
		}
		meta, err := tbl.Metadata()
		if err != nil {
			return err
		}
		id, _ := meta.CurrentSchema().FindFieldByName("id")
		if id.ID != 1 || !id.Type.Equals(sdk.PrimitiveTypes.Int64) {
			return fmt.Errorf("promotion changed field identity")
		}
		scan, err := tbl.Scan(ManagedScanOptions{BatchSize: 1})
		if err != nil {
			return err
		}
		seen := map[int64]int64{}
		summary, err := scan.Records(func(record arrow.RecordBatch) (bool, error) {
			ids, values := record.Column(0).(*array.Int64), record.Column(1).(*array.Int64)
			for row := range ids.Len() {
				if values.IsNull(row) {
					return false, fmt.Errorf("required default became NULL")
				}
				seen[ids.Value(row)] = values.Value(row)
			}
			return true, nil
		})
		if err != nil {
			return err
		}
		if !summary.Complete || summary.Rows != 2 || len(seen) != 2 || seen[1] != 17 || seen[2] != 29 {
			return fmt.Errorf("initial/write defaults lost across old files")
		}
		historical, err := tbl.Scan(ManagedScanOptions{SnapshotID: old})
		if err != nil {
			return err
		}
		projection, err := historical.Projection()
		if err != nil {
			return err
		}
		if projection.NumFields() != 1 || !projection.Fields()[0].Type.Equals(sdk.PrimitiveTypes.Int32) {
			return fmt.Errorf("historical schema retargeted by promotion")
		}
		return nil
	})
}

func TestManagedStructuredDefaultKeysRemainData(t *testing.T) {
	_, options := newPeer(t)
	f := bindManagedFixture(t, managedSettingsFor(options), 1)
	schema := sdk.NewSchema(0, sdk.NestedField{ID: 1, Name: "payload", Type: &sdk.MapType{KeyID: 2, KeyType: sdk.PrimitiveTypes.String, ValueID: 3, ValueType: sdk.PrimitiveTypes.String}, InitialDefault: map[string]any{"MixedCase": "initial", "mixedcase": "distinct"}, WriteDefault: map[string]any{"MixedCase": "written"}})
	if _, err := table.NewMetadata(schema, sdk.UnpartitionedSpec, table.UnsortedSortOrder, options.Location+"data", sdk.Properties{"format-version": "3"}); err != nil {
		t.Fatal("native structured-default positive control", err)
	}
	f.run(t, "default-data-keys", func(s *ManagedSession) error {
		tbl, err := s.CreateTable([]string{"isolated", "data"}, schema, catalog.WithLocation(options.Location+"data"), catalog.WithProperties(sdk.Properties{"format-version": "3"}))
		if err != nil {
			return err
		}
		metadata, err := tbl.Metadata()
		if err != nil {
			return err
		}
		defaults := metadata.CurrentSchema().Fields()[0].InitialDefault.(map[string]any)
		if defaults["MixedCase"] != "initial" || defaults["mixedcase"] != "distinct" {
			return fmt.Errorf("default object changed")
		}
		return nil
	})
}

func TestManagedConfiguredMetadataCollectionLimit(t *testing.T) {
	peer, options := newPeer(t)
	settings := managedSettingsFor(options)
	settings.MaxListEntries = 4096
	f := bindManagedFixture(t, settings, 1)
	properties := sdk.Properties{}
	for index := range settings.MaxListEntries {
		properties[fmt.Sprintf("property-%04d", index)] = "exact"
	}
	f.run(t, "configured-metadata-limit", func(s *ManagedSession) error {
		tbl, err := s.CreateTable([]string{"isolated", "data"}, testSchema(), catalog.WithLocation(options.Location+"data"), catalog.WithProperties(properties))
		if err != nil {
			return err
		}
		metadata, err := tbl.Metadata()
		if err != nil {
			return err
		}
		if len(metadata.Properties()) != len(properties) {
			return fmt.Errorf("configured property collection truncated")
		}
		return nil
	})
	properties["one-too-many"] = "reject"
	peer.mu.Lock()
	before := peer.requests
	peer.mu.Unlock()
	receipt, err := f.client.Use(deadline(t), correlation("metadata-over-limit"), func(s *ManagedSession) error {
		_, err := s.CreateTable([]string{"isolated", "rejected"}, testSchema(), catalog.WithLocation(options.Location+"rejected"), catalog.WithProperties(properties))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := receipt.WaitReleased(deadline(t))
	if err != nil || !errors.Is(result.Err(), ErrLimit) {
		t.Fatal("configured limit not refused", err, result.Err())
	}
	peer.mu.Lock()
	after := peer.requests
	peer.mu.Unlock()
	if after != before {
		t.Fatal("metadata limit checked after Catalog I/O")
	}
	delivery, err := f.inbox.Next(deadline(t))
	if err != nil {
		t.Fatal(err)
	}
	_ = delivery.Release()
}

func TestManagedSchemaEditsFreezeBinaryDefaults(t *testing.T) {
	peer, options := newPeer(t)
	peer.hook = func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method != http.MethodPost || !strings.HasSuffix(r.URL.Path, "/tables/data") {
			return false
		}
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			return false
		}
		r.Body = io.NopCloser(bytes.NewReader(raw))
		var payload struct{ Requirements table.Requirements }
		if json.Unmarshal(raw, &payload) != nil {
			return false
		}
		peer.mu.Lock()
		base, parseErr := table.ParseMetadataBytes(peer.tables["data"])
		peer.mu.Unlock()
		if parseErr == nil {
			for _, requirement := range payload.Requirements {
				if err := requirement.Validate(base); err != nil {
					t.Logf("rejected requirement %s: %v", requirement.GetType(), err)
				}
			}
		}
		return false
	}
	f := bindManagedFixture(t, managedSettingsFor(options), 1)
	f.run(t, "schema-defaults", func(s *ManagedSession) error {
		tbl, err := s.CreateTable([]string{"isolated", "data"}, testSchema(), catalog.WithLocation(options.Location+"data"), catalog.WithProperties(sdk.Properties{"format-version": "3"}))
		if err != nil {
			return err
		}
		tx, err := tbl.NewTransaction("")
		if err != nil {
			return err
		}
		addDefault := []byte{1, 2, 3}
		if err = tx.EditSchema(true, false, nil, []SchemaEdit{{Action: "add", Path: []string{"payload"}, Type: sdk.PrimitiveTypes.Binary, Default: sdk.NewLiteral(addDefault)}}); err != nil {
			return err
		}
		addDefault[0] = 99
		staged, err := tx.StagedMetadata()
		if err != nil {
			return err
		}
		field, ok := staged.CurrentSchema().FindFieldByName("payload")
		if !ok || field.InitialDefault != "010203" || field.WriteDefault != "010203" {
			return fmt.Errorf("added binary default aliases caller")
		}
		writeDefault := []byte{4, 5, 6}
		if err = tx.EditSchema(true, false, nil, []SchemaEdit{{Action: "update", Path: []string{"payload"}, Column: table.ColumnUpdate{WriteDefault: sdk.Optional[sdk.Literal]{Valid: true, Val: sdk.NewLiteral(writeDefault)}}}}); err != nil {
			return err
		}
		writeDefault[0] = 88
		tbl, err = tx.Commit()
		if err != nil {
			return err
		}
		meta, err := tbl.Metadata()
		if err != nil {
			return err
		}
		field, ok = meta.CurrentSchema().FindFieldByName("payload")
		if !ok || field.InitialDefault != "010203" || field.WriteDefault != "040506" {
			return fmt.Errorf("updated binary default aliases caller or changed initial default")
		}
		return nil
	})
}
