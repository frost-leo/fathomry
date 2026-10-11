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
	"reflect"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	sdk "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	iceberg "github.com/frost-leo/fathomry/adapters/tableformat/iceberg/v1"
	"github.com/frost-leo/fathomry/adapters/tableformat/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
)

func serviceConcurrentMutations(t *testing.T, ctx context.Context, owner *iceberg.Owner, inbox *adapters.Inbox[iceberg.Result], namespace, root string, initial arrow.RecordBatch, tableNames *[]string, proof map[string]any) {
	t.Helper()
	schema := sdk.NewSchema(0, sdk.NestedField{ID: 1, Name: "id", Type: sdk.PrimitiveTypes.Int64, Required: true}, sdk.NestedField{ID: 2, Name: "value", Type: sdk.PrimitiveTypes.String})
	for _, operation := range []string{"append", "overwrite", "delete"} {
		name := "concurrent_" + operation
		*tableNames = append(*tableNames, name)
		receipt, err := owner.Client().Use(ctx, func(s *iceberg.Session) error {
			tbl, err := s.CreateTable([]string{namespace, name}, schema, catalog.WithLocation(root+name), catalog.WithProperties(sdk.Properties{"format-version": "2", "commit.retry.num-retries": "0"}))
			if err != nil {
				return err
			}
			tx, err := tbl.NewTransaction("")
			if err != nil {
				return err
			}
			reader, err := array.NewRecordReader(initial.Schema(), []arrow.RecordBatch{initial})
			if err != nil {
				return err
			}
			err = tx.Append(reader, nil)
			reader.Release()
			if err != nil {
				return err
			}
			_, err = tx.Commit()
			return err
		})
		result := settlePublic(t, receipt, err)
		ackPublic(t, inbox)
		if result.Err() != nil {
			t.Fatal("concurrent fixture setup", result.Err())
		}
		inputs := make([]arrow.RecordBatch, 2)
		for writer := range inputs {
			builder := array.NewRecordBuilder(memory.DefaultAllocator, initial.Schema())
			for index := range 9 {
				builder.Field(0).(*array.Int64Builder).Append(int64(10000 + writer*100 + index))
				builder.Field(1).(*array.StringBuilder).Append(fmt.Sprintf("writer-%d-%d", writer, index))
			}
			inputs[writer] = builder.NewRecordBatch()
			builder.Release()
			defer inputs[writer].Release()
		}
		staged := make(chan error, 2)
		submit := make(chan struct{})
		results := make([]adapters.Snapshot[iceberg.Result], 2)
		var workers sync.WaitGroup
		for writer := range inputs {
			workers.Go(func() {
				signaled := false
				defer func() {
					if !signaled {
						staged <- fmt.Errorf("writer failed before staging")
					}
				}()
				receipt, admission := owner.Client().Use(ctx, func(s *iceberg.Session) error {
					tbl, err := s.LoadTable([]string{namespace, name})
					if err != nil {
						return err
					}
					tx, err := tbl.NewTransaction("")
					if err != nil {
						return err
					}
					reader, err := array.NewRecordReader(inputs[writer].Schema(), []arrow.RecordBatch{inputs[writer]})
					if err != nil {
						return err
					}
					switch operation {
					case "append":
						err = tx.Append(reader, nil)
					case "overwrite":
						err = tx.Overwrite(reader, nil, table.WithOverwriteFilter(sdk.LessThan(sdk.Reference("id"), int64(20))))
					case "delete":
						err = tx.Delete(sdk.LessThan(sdk.Reference("id"), int64(20+writer)), nil)
					}
					reader.Release()
					signaled = true
					staged <- err
					if err != nil {
						return err
					}
					select {
					case <-submit:
					case <-ctx.Done():
						return ctx.Err()
					}
					_, err = tx.Commit()
					return err
				})
				if admission != nil || receipt == nil {
					t.Error("concurrent family admission", admission)
					return
				}
				var err error
				results[writer], err = receipt.WaitReleased(ctx)
				if err != nil {
					t.Error("concurrent family wait", err)
				}
			})
		}
		for range inputs {
			select {
			case err := <-staged:
				if err != nil {
					t.Error("concurrent staging", err)
				}
			case <-ctx.Done():
				t.Error("concurrent stage timeout")
			}
		}
		close(submit)
		workers.Wait()
		winner := -1
		for writer, result := range results {
			ackPublic(t, inbox)
			if result.Err() == nil {
				if winner != -1 {
					t.Fatal("stale-base writers both acknowledged without retry")
				}
				winner = writer
			} else if !errors.Is(result.Err(), table.ErrCommitFailed) {
				logServiceCauseTypes(t, result.Err(), 0)
				t.Fatal("stale-base mutation did not return native conflict")
			}
			value, ok := result.ValueCopy()
			if !ok || !value.Released() {
				t.Fatal("concurrent independent evidence incomplete")
			}
			commitFound := false
			for _, effect := range value.Effects() {
				if effect.Operation != "commit" {
					continue
				}
				commitFound = true
				if !effect.Phases.Submitted || result.Err() == nil && (effect.Effect != tableformat.Acknowledged || effect.CommittedSnapshot == 0) || result.Err() != nil && effect.Effect != tableformat.Rejected {
					t.Fatal("concurrent commit facts disagree with outcome")
				}
			}
			if !commitFound {
				t.Fatal("concurrent commit evidence absent")
			}
		}
		if winner < 0 {
			t.Fatal("both concurrent mutations failed")
		}
		expected := map[int64]*string{}
		for id := range int64(4097) {
			if operation == "overwrite" && id < 20 || operation == "delete" && id < int64(20+winner) {
				continue
			}
			if id%7 == 0 {
				expected[id] = nil
			} else {
				value := fmt.Sprintf("row-%d", id)
				expected[id] = &value
			}
		}
		if operation != "delete" {
			for index := range 9 {
				value := fmt.Sprintf("writer-%d-%d", winner, index)
				expected[int64(10000+winner*100+index)] = &value
			}
		}
		receipt, err = owner.Client().Use(ctx, func(s *iceberg.Session) error {
			tbl, err := s.LoadTable([]string{namespace, name})
			if err != nil {
				return err
			}
			scan, err := tbl.Scan(iceberg.ScanOptions{BatchSize: 113})
			if err != nil {
				return err
			}
			actual := map[int64]*string{}
			summary, err := scan.Records(func(record arrow.RecordBatch) (bool, error) {
				ids, values := record.Column(0).(*array.Int64), record.Column(1).(*array.String)
				for row := range ids.Len() {
					id := ids.Value(row)
					if _, found := actual[id]; found {
						return false, fmt.Errorf("concurrent duplicate row")
					}
					if values.IsNull(row) {
						actual[id] = nil
					} else {
						value := values.Value(row)
						actual[id] = &value
					}
				}
				return true, nil
			})
			if err != nil {
				return err
			}
			if !summary.Complete || !reflect.DeepEqual(actual, expected) {
				return fmt.Errorf("concurrent accepted row multiset differs")
			}
			return nil
		})
		result = settlePublic(t, receipt, err)
		ackPublic(t, inbox)
		if result.Err() != nil {
			t.Fatal("concurrent exact readback", result.Err())
		}
		proof[name] = "one-ack-one-conflict-exact-rows"
		t.Logf("profile=%s status=pass", name)
	}
}
