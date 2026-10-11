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
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"strconv"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	sdk "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
)

type managedIntegrationFixture struct {
	client   *ManagedClient
	assembly *resource.Assembly
	inbox    *invocation.Inbox[ManagedResult]
}

func managedSettingsFor(options OptionsV1) ManagedSettings {
	return ManagedSettings{Name: options.Name, CatalogURI: options.CatalogURI, CatalogPrefix: options.CatalogPrefix, Warehouse: options.Warehouse, Namespace: []string{options.Namespace}, Location: options.Location,
		Plaintext: true, StorageEndpoint: options.StorageEndpoint, StorageRegion: options.StorageRegion, StorageAccessKey: options.StorageAccessKey, StorageSecretKey: options.StorageSecretKey,
		Permissions: Grants{Read: true, Write: true, Metadata: true, Namespace: true, Import: true, Rewrite: true, Expire: true, Orphan: true, Purge: true, DeleteFiles: true, MultiTable: true}, Timeout: options.Timeout, Workers: 1}
}
func bindManagedFixture(t *testing.T, settings ManagedSettings, capacity int) managedIntegrationFixture {
	return bindManagedNativeFixture(t, settings, ManagedNativeOptions{}, capacity)
}
func bindManagedNativeFixture(t *testing.T, settings ManagedSettings, native ManagedNativeOptions, capacity int) managedIntegrationFixture {
	t.Helper()
	prepared, err := PrepareManaged(settings, native)
	if err != nil {
		t.Fatal(err)
	}
	selected, err := prepared.Select()
	if err != nil {
		t.Fatal(err)
	}
	selected = resource.WithLimits(selected, prepared.Limits())
	assembly, err := resource.Assemble(deadline(t), deadline(t), "managed-test", selected)
	if err != nil {
		t.Fatalf("assemble: %s", causes(err))
	}
	inbox, err := invocation.NewInbox[ManagedResult](capacity, int64(capacity)*prepared.Budget().EvidenceBytes)
	if err != nil {
		t.Fatal(err)
	}
	client, err := BindManaged(assembly, selected, inbox)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := assembly.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	return managedIntegrationFixture{client, assembly, inbox}
}
func (f managedIntegrationFixture) run(t *testing.T, name string, work func(*ManagedSession) error) ManagedResult {
	t.Helper()
	receipt, err := f.client.Use(deadline(t), correlation(name), work)
	if err != nil {
		t.Fatal(err)
	}
	record, err := f.inbox.Next(deadline(t))
	if err != nil {
		t.Fatal(err)
	}
	defer record.Release()
	result, err := receipt.WaitReleased(deadline(t))
	if err != nil {
		t.Fatal(err)
	}
	if result.Err() != nil {
		t.Fatalf("managed operation %s: %s", name, causes(result.Err()))
	}
	return result.Outcome.Value
}
func managedExactRows(t *testing.T, scan *ManagedScan, expected map[int64]cell) {
	t.Helper()
	actual := map[int64]cell{}
	summary, err := scan.Records(func(record arrow.RecordBatch) (bool, error) {
		ids := record.Column(0).(*array.Int64)
		values := record.Column(1).(*array.String)
		for i := 0; i < ids.Len(); i++ {
			id := ids.Value(i)
			if _, exists := actual[id]; exists {
				return false, fmt.Errorf("duplicate row %d", id)
			}
			actual[id] = cell{value: values.Value(i), valid: values.IsValid(i)}
		}
		return true, nil
	})
	if err != nil {
		t.Fatalf("records: %s", causes(err))
	}
	if !summary.Complete || summary.Rows != int64(len(expected)) || !reflect.DeepEqual(actual, expected) {
		t.Fatalf("exact rows mismatch: rows %d want %d complete %t", summary.Rows, len(expected), summary.Complete)
	}
}
func TestManagedNativeWriteScanNullDeleteAndRevocation(t *testing.T) {
	for _, version := range []int{1, 2, 3} {
		t.Run(strconv.Itoa(version), func(t *testing.T) {
			peer, options := newPeer(t)
			f := bindManagedFixture(t, managedSettingsFor(options), 2)
			input := makeBatch(t, 0, 4097, "managed")
			var escaped *ManagedTable
			result := f.run(t, "native", func(session *ManagedSession) error {
				if err := session.CreateNamespace([]string{"isolated"}, nil); err != nil {
					return err
				}
				tbl, err := session.CreateTable([]string{"isolated", "data"}, testSchema(), catalog.WithLocation(options.Location+"data"), catalog.WithProperties(sdk.Properties{"format-version": strconv.Itoa(version), "write.parquet.row-group-limit": "1024"}))
				if err != nil {
					return err
				}
				tx, err := tbl.NewTransaction("")
				if err != nil {
					return err
				}
				reader, err := array.NewRecordReader(input.Schema(), []arrow.RecordBatch{input})
				if err != nil {
					return err
				}
				err = tx.Append(reader, nil)
				reader.Release()
				if err != nil {
					return err
				}
				tbl, err = tx.Commit()
				if err != nil {
					return err
				}
				scan, err := tbl.Scan(ManagedScanOptions{BatchSize: 128})
				if err != nil {
					return err
				}
				managedExactRows(t, scan, expectedRows(0, 4097, "managed"))
				tx, err = tbl.NewTransaction("")
				if err != nil {
					return err
				}
				if err = tx.Delete(sdk.EqualTo(sdk.Reference("value"), "managed-1"), nil); err != nil {
					return err
				}
				tbl, err = tx.Commit()
				if err != nil {
					return err
				}
				scan, err = tbl.Scan(ManagedScanOptions{BatchSize: 127})
				if err != nil {
					return err
				}
				expected := expectedRows(0, 4097, "managed")
				delete(expected, 1)
				managedExactRows(t, scan, expected)
				escaped = tbl
				return nil
			})
			if !result.Released() || len(result.Files()) == 0 {
				t.Fatal("missing released file evidence")
			}
			if _, err := escaped.Metadata(); !errors.Is(err, ErrAuthority) {
				t.Fatalf("escaped table stayed usable: %v", err)
			}
			peer.mu.Lock()
			commits := peer.commits
			peer.mu.Unlock()
			if commits != 2 {
				t.Fatalf("commits=%d", commits)
			}
			confirmed := 0
			for _, effect := range result.Effects() {
				if effect.CommittedSnapshot != 0 {
					if effect.Effect != Acknowledged {
						t.Fatal("commit ID without acknowledgement")
					}
					confirmed++
				}
			}
			if confirmed != 2 {
				t.Fatalf("commit identities=%d", confirmed)
			}
		})
	}
}
func TestManagedUnknownCommitRetainsFilesWithoutResend(t *testing.T) {
	peer, options := newPeer(t)
	f := bindManagedFixture(t, managedSettingsFor(options), 2)
	f.run(t, "setup", func(s *ManagedSession) error {
		_, err := s.CreateTable([]string{"isolated", "data"}, testSchema(), catalog.WithLocation(options.Location+"data"))
		return err
	})
	peer.mu.Lock()
	peer.afterCommit = func(w http.ResponseWriter, r *http.Request, body []byte) bool {
		w.WriteHeader(503)
		_, _ = w.Write([]byte("<bad>"))
		return true
	}
	peer.mu.Unlock()
	input := makeBatch(t, 0, 9, "unknown")
	var tx *ManagedTransaction
	receipt, err := f.client.Use(deadline(t), correlation("unknown"), func(s *ManagedSession) error {
		tbl, err := s.LoadTable([]string{"isolated", "data"})
		if err != nil {
			return err
		}
		tx, err = tbl.NewTransaction("")
		if err != nil {
			return err
		}
		reader, _ := array.NewRecordReader(input.Schema(), []arrow.RecordBatch{input})
		defer reader.Release()
		if err = tx.Append(reader, nil); err != nil {
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
	if result.Err() == nil {
		t.Fatal("lost reply certified")
	}
	found := false
	for _, effect := range result.Outcome.Value.Effects() {
		if effect.Operation == "commit" {
			found = true
			if effect.Effect != Unknown || effect.CommittedSnapshot != 0 {
				t.Fatal("unknown certified")
			}
		}
	}
	if !found || len(result.Outcome.Value.Files()) == 0 {
		t.Fatal("effect evidence erased")
	}
	if _, err = tx.Commit(); err == nil {
		t.Fatal("consumed transaction resubmitted")
	}
	peer.mu.Lock()
	commits := peer.commits
	peer.mu.Unlock()
	if commits != 1 {
		t.Fatal("resubmitted")
	}
	record, err := f.inbox.Next(deadline(t))
	if err != nil {
		t.Fatal(err)
	}
	if err = record.Release(); err != nil {
		t.Fatal(err)
	}
}
func TestManagedIndependentEvidenceSaturation(t *testing.T) {
	_, options := newPeer(t)
	settings := managedSettingsFor(options)
	settings.MaxActive = 1
	f := bindManagedFixture(t, settings, 1)
	receipt, err := f.client.Use(deadline(t), correlation("ignored"), func(*ManagedSession) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if _, err = receipt.WaitReleased(deadline(t)); err != nil {
		t.Fatal(err)
	}
	called := false
	if _, err = f.client.Use(deadline(t), correlation("blocked"), func(*ManagedSession) error { called = true; return nil }); !errors.Is(err, invocation.ErrEvidence) || called {
		t.Fatal("evidence bypass", err)
	}
	record, err := f.inbox.Next(deadline(t))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = record.Receipt().WaitReleased(deadline(t)); err != nil {
		t.Fatal(err)
	}
	if err = record.Release(); err != nil {
		t.Fatal(err)
	}
	f.run(t, "unblocked", func(*ManagedSession) error { return nil })
}
