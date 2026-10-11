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

package table

import (
	"bytes"
	"testing"

	"github.com/apache/iceberg-go"
)

func TestRepeatedSchemaEditsKeepBaseRequirements(t *testing.T) {
	schema := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true})
	base, err := NewMetadata(schema, iceberg.UnpartitionedSpec, UnsortedSortOrder, "s3://fixture/data", iceberg.Properties{"format-version": "3"})
	if err != nil {
		t.Fatal(err)
	}
	tbl := New(Identifier{"ns", "data"}, base, "s3://fixture/data/metadata.json", nil, nil)
	tx := tbl.NewTransaction()
	if err = tx.UpdateSchema(true, false).AddColumn([]string{"payload"}, iceberg.PrimitiveTypes.Binary, "", false, iceberg.BinaryLiteral([]byte{1, 2})).Commit(); err != nil {
		t.Fatal(err)
	}
	if err = tx.UpdateSchema(true, false).UpdateColumn([]string{"payload"}, ColumnUpdate{WriteDefault: iceberg.Optional[iceberg.Literal]{Valid: true, Val: iceberg.BinaryLiteral([]byte{3, 4})}}).Commit(); err != nil {
		t.Fatal(err)
	}
	commit, err := tx.TableCommit()
	if err != nil {
		t.Fatal(err)
	}
	for _, requirement := range commit.Requirements {
		if err = requirement.Validate(base); err != nil {
			t.Fatalf("staged assertion cannot hold on the actual base: %v", err)
		}
	}
	staged, err := tx.StagedTable()
	if err != nil {
		t.Fatal(err)
	}
	field, ok := staged.Schema().FindFieldByName("payload")
	if !ok || !bytes.Equal(field.InitialDefault.([]byte), []byte{1, 2}) || !bytes.Equal(field.WriteDefault.([]byte), []byte{3, 4}) {
		t.Fatal("repeated evolution lost defaults")
	}
}

func TestSchemaEvolutionComparesBinaryDefaultsByValue(t *testing.T) {
	for _, changed := range []bool{false, true} {
		field := iceberg.NestedField{ID: 1, Name: "payload", Type: iceberg.PrimitiveTypes.Binary, InitialDefault: []byte{1, 2}, WriteDefault: []byte{1, 2}}
		update := field
		update.WriteDefault = []byte{1, 2}
		if changed {
			update.WriteDefault = []byte{3, 4}
		}
		visitor := applyChanges{updates: map[int]iceberg.NestedField{1: update}}
		result := visitor.Struct(iceberg.StructType{FieldList: []iceberg.NestedField{field}}, []iceberg.Type{field.Type}).(*iceberg.StructType).Fields()[0]
		if !bytes.Equal(result.InitialDefault.([]byte), []byte{1, 2}) || !bytes.Equal(result.WriteDefault.([]byte), update.WriteDefault.([]byte)) {
			t.Fatal("schema evolution changed initial default or lost new write default")
		}
	}
}
