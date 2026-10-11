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
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	sdk "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
)

func TestManagedCreateAcceptsAssignedIDsButRejectsChangedTypes(t *testing.T) {
	for _, changed := range []bool{false, true} {
		t.Run(fmt.Sprint(changed), func(t *testing.T) {
			peer, options := newPeer(t)
			schema := sdk.NewSchema(0,
				sdk.NestedField{ID: 1, Name: "id", Type: sdk.PrimitiveTypes.Int64},
				sdk.NestedField{ID: 2, Name: "payload", Type: &sdk.StructType{FieldList: []sdk.NestedField{{ID: 3, Name: "score", Type: sdk.PrimitiveTypes.Int64}}}},
				sdk.NestedField{ID: 4, Name: "amount", Type: sdk.PrimitiveTypes.Int64})
			peer.hook = func(w http.ResponseWriter, r *http.Request) bool {
				if r.Method != http.MethodPost || !strings.HasSuffix(r.URL.Path, "/tables") {
					return false
				}
				metadata, err := table.NewMetadata(schema, sdk.UnpartitionedSpec, table.UnsortedSortOrder, options.Location+"data", nil)
				if err != nil {
					t.Error(err)
					w.WriteHeader(500)
					return true
				}
				amount := sdk.Type(sdk.PrimitiveTypes.Int64)
				if changed {
					amount = sdk.PrimitiveTypes.String
				}
				assigned := sdk.NewSchema(0,
					sdk.NestedField{ID: 1, Name: "id", Type: sdk.PrimitiveTypes.Int64},
					sdk.NestedField{ID: 2, Name: "payload", Type: &sdk.StructType{FieldList: []sdk.NestedField{{ID: 4, Name: "score", Type: sdk.PrimitiveTypes.Int64}}}},
					sdk.NestedField{ID: 3, Name: "amount", Type: amount})
				raw, _ := json.Marshal(metadata)
				var fields map[string]json.RawMessage
				if err = json.Unmarshal(raw, &fields); err != nil {
					t.Error(err)
					w.WriteHeader(500)
					return true
				}
				fields["schemas"], _ = json.Marshal([]*sdk.Schema{assigned})
				raw, _ = json.Marshal(fields)
				peer.mu.Lock()
				peer.persist("data", raw)
				reply := peer.tableResponse("data", raw)
				peer.mu.Unlock()
				writeJSON(w, reply)
				return true
			}
			f := bindManagedFixture(t, managedSettingsFor(options), 1)
			receipt, err := f.client.Use(deadline(t), correlation("assigned-ids"), func(s *ManagedSession) error {
				tbl, err := s.CreateTable([]string{"isolated", "data"}, schema, catalog.WithLocation(options.Location+"data"))
				if err != nil {
					return err
				}
				metadata, err := tbl.Metadata()
				if err != nil {
					return err
				}
				field, _ := metadata.CurrentSchema().FindFieldByName("amount")
				if field.ID != 3 {
					return fmt.Errorf("Catalog-assigned field ID was rewritten")
				}
				tx, err := tbl.NewTransaction("")
				if err != nil {
					return err
				}
				if err = tx.SetProperties(sdk.Properties{"empty": ""}); err != nil {
					return err
				}
				_, err = tx.Commit()
				return err
			})
			if err != nil {
				t.Fatal(err)
			}
			result, err := receipt.WaitReleased(deadline(t))
			if err != nil {
				t.Fatal(err)
			}
			if changed {
				if !errors.Is(result.Err(), ErrProtocol) || result.Outcome.Value.Effects()[0].Effect != Unknown {
					t.Fatal("changed create schema accepted")
				}
			} else if result.Err() != nil {
				t.Fatal(causes(result.Err()))
			}
			delivery, err := f.inbox.Next(deadline(t))
			if err != nil {
				t.Fatal(err)
			}
			_ = delivery.Release()
		})
	}
}

func TestManagedNestedDecimalTimeRoundTrip(t *testing.T) {
	_, options := newPeer(t)
	f := bindManagedFixture(t, managedSettingsFor(options), 1)
	schema := sdk.NewSchema(0,
		sdk.NestedField{ID: 1, Name: "id", Type: sdk.PrimitiveTypes.Int64, Required: true},
		sdk.NestedField{ID: 2, Name: "payload", Type: &sdk.StructType{FieldList: []sdk.NestedField{{ID: 3, Name: "score", Type: sdk.PrimitiveTypes.Int64}, {ID: 4, Name: "tag", Type: sdk.PrimitiveTypes.String}}}},
		sdk.NestedField{ID: 5, Name: "amount", Type: sdk.DecimalTypeOf(12, 2)},
		sdk.NestedField{ID: 6, Name: "at", Type: sdk.TimestampType{}})
	allocator := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer allocator.AssertSize(t, 0)
	f.run(t, "typed", func(s *ManagedSession) error {
		tbl, err := s.CreateTable([]string{"isolated", "typed"}, schema, catalog.WithLocation(options.Location+"typed"))
		if err != nil {
			return err
		}
		meta, err := tbl.Metadata()
		if err != nil {
			return err
		}
		arrowSchema, err := table.SchemaToArrowSchema(meta.CurrentSchema(), nil, true, false)
		if err != nil {
			return err
		}
		record, _, err := array.RecordFromJSON(allocator, arrowSchema, strings.NewReader(`[{"id":1,"payload":{"score":17,"tag":"precise"},"amount":"1234567.89","at":1234567890123456},{"id":2,"payload":null,"amount":null,"at":null},{"id":3,"payload":{"score":null,"tag":null},"amount":"-0.01","at":-1234567}]`))
		if err != nil {
			return err
		}
		defer record.Release()
		input, err := array.NewRecordReader(arrowSchema, []arrow.RecordBatch{record})
		if err != nil {
			return err
		}
		defer input.Release()
		tx, err := tbl.NewTransaction("")
		if err != nil {
			return err
		}
		if err = tx.Append(input, nil); err != nil {
			return err
		}
		tbl, err = tx.Commit()
		if err != nil {
			return err
		}
		metaAfter, err := tbl.Metadata()
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(meta.CurrentSchema().Fields(), metaAfter.CurrentSchema().Fields()) {
			t.Error("schema field identities changed")
		}
		scan, err := tbl.Scan(ManagedScanOptions{BatchSize: 1})
		if err != nil {
			return err
		}
		actual, summary, err := scan.ToArrowTable()
		if actual != nil {
			defer actual.Close()
		}
		if err != nil {
			return err
		}
		expected := array.NewTableFromRecords(arrowSchema, []arrow.RecordBatch{record})
		defer expected.Release()
		return actual.Visit(func(actual arrow.Table) error {
			if !summary.Complete || summary.Rows != 3 || actual.NumCols() != expected.NumCols() {
				t.Error("typed scan shape")
			}
			for i := 0; int64(i) < actual.NumCols(); i++ {
				if !array.ChunkedEqual(actual.Column(i).Data(), expected.Column(i).Data()) {
					t.Errorf("typed column %d differs", i)
				}
			}
			return nil
		})
	})
}
func TestManagedCommitAcceptsReorderedIDCollections(t *testing.T) {
	_, options := newPeer(t)
	f := bindManagedFixture(t, managedSettingsFor(options), 1)
	input := makeBatch(t, 0, 3, "order")
	f.run(t, "order", func(s *ManagedSession) error {
		tbl, err := s.CreateTable([]string{"isolated", "data"}, testSchema(), catalog.WithLocation(options.Location+"data"))
		if err != nil {
			return err
		}
		base := tbl.table.Metadata()
		for range 2 {
			tx, err := tbl.NewTransaction("")
			if err != nil {
				return err
			}
			reader, _ := array.NewRecordReader(input.Schema(), []arrow.RecordBatch{input})
			err = tx.Append(reader, nil)
			reader.Release()
			if err != nil {
				return err
			}
			tbl, err = tx.Commit()
			if err != nil {
				return err
			}
		}
		raw, err := json.Marshal(tbl.table.Metadata())
		if err != nil {
			return err
		}
		var object map[string]json.RawMessage
		if err = json.Unmarshal(raw, &object); err != nil {
			return err
		}
		var snapshots []json.RawMessage
		if err = json.Unmarshal(object["snapshots"], &snapshots); err != nil {
			return err
		}
		if len(snapshots) != 2 {
			t.Error("control has no two-snapshot permutation")
		}
		snapshots[0], snapshots[1] = snapshots[1], snapshots[0]
		object["snapshots"], _ = json.Marshal(snapshots)
		raw, _ = json.Marshal(object)
		reordered, err := table.ParseMetadataBytes(raw)
		if err != nil {
			return err
		}
		if !managedCommitMatches(base, tbl.table.Metadata(), reordered) {
			t.Error("valid reordered acknowledgement rejected")
		}
		object["table-uuid"] = json.RawMessage(`"00000000-0000-0000-0000-000000000001"`)
		raw, _ = json.Marshal(object)
		wrong, err := table.ParseMetadataBytes(raw)
		if err != nil {
			return err
		}
		if managedCommitMatches(base, tbl.table.Metadata(), wrong) {
			t.Error("wrong UUID accepted")
		}
		return nil
	})
}
func TestManagedInvalidFormatRejectedBeforeCreateIO(t *testing.T) {
	peer, options := newPeer(t)
	f := bindManagedFixture(t, managedSettingsFor(options), 1)
	peer.mu.Lock()
	before := peer.requests
	peer.mu.Unlock()
	receipt, err := f.client.Use(context.Background(), correlation("invalid-format"), func(s *ManagedSession) error {
		_, err := s.CreateTable([]string{"isolated", "data"}, testSchema(), catalog.WithLocation(options.Location+"data"), catalog.WithProperties(sdk.Properties{"format-version": "99"}))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := receipt.WaitReleased(deadline(t))
	if err != nil || result.Err() == nil {
		t.Fatal("invalid format accepted")
	}
	peer.mu.Lock()
	after := peer.requests
	peer.mu.Unlock()
	if after != before {
		t.Fatal("invalid format reached service")
	}
	record, err := f.inbox.Next(deadline(t))
	if err != nil {
		t.Fatal(err)
	}
	_ = record.Release()
}
