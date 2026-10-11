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
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	sdk "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/apache/iceberg-go/metrics"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/table/compaction"
	"github.com/apache/iceberg-go/view"
	iceberg "github.com/frost-leo/fathomry/adapters/tableformat/iceberg/v1"
	"github.com/frost-leo/fathomry/adapters/tableformat/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/internal/fault"
	"go.yaml.in/yaml/v3"
)

type liveConfig struct {
	Fathomry struct {
		Storage struct {
			Catalog struct {
				Implementation string `yaml:"implementation"`
				Authentication string `yaml:"authentication"`
				URI            string `yaml:"catalog_uri"`
				Warehouse      string `yaml:"warehouse"`
				WarehouseID    string `yaml:"warehouse_id"`
				Prefix         string `yaml:"warehouse_prefix"`
			} `yaml:"catalog"`
			Objects struct {
				Endpoint string `yaml:"endpoint"`
				Bucket   string `yaml:"bucket"`
				Region   string `yaml:"region"`
			} `yaml:"object_store"`
		} `yaml:"storage"`
	} `yaml:"fathomry"`
	Minio struct {
		Fathomry struct {
			AccessKey string `yaml:"access_key"`
			SecretKey string `yaml:"secret_key"`
			Bucket    string `yaml:"bucket"`
		} `yaml:"fathomry"`
	} `yaml:"minio"`
}

type serviceRoundTripper func(*http.Request) (*http.Response, error)

func (transport serviceRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	return transport(request)
}

func TestIsolatedRESTS3PublicService(t *testing.T) {
	path := os.Getenv("FATHOMRY_ICEBERG_ADAPTER_SERVICE_CONFIG")
	if path == "" || os.Getenv("FATHOMRY_ICEBERG_ADAPTER_SERVICE_WRITES") != "1" {
		t.Skip("requires explicit isolated REST/S3 service authority")
	}
	manifest := os.Getenv("FATHOMRY_ICEBERG_ADAPTER_SERVICE_MANIFEST")
	if manifest == "" {
		t.Fatal("private fixture manifest required")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal("service configuration unavailable")
	}
	var cfg liveConfig
	if err = yaml.Unmarshal(raw, &cfg); err != nil {
		t.Fatal("invalid service configuration")
	}
	clear(raw)
	storage := cfg.Fathomry.Storage
	if storage.Catalog.Implementation != "lakekeeper" || storage.Catalog.Authentication != "none_trusted_network" || storage.Objects.Bucket != cfg.Minio.Fathomry.Bucket {
		t.Fatal("service profile differs from authorized fixture")
	}
	var random [8]byte
	if _, err = rand.Read(random[:]); err != nil {
		t.Fatal(err)
	}
	namespace := "fathomry_gh137_" + hex.EncodeToString(random[:])
	root := strings.TrimRight(storage.Catalog.Prefix, "/") + "/" + namespace + "/"
	settings := iceberg.Settings{Name: "service", CatalogURI: storage.Catalog.URI, CatalogPrefix: storage.Catalog.WarehouseID, Warehouse: storage.Catalog.Warehouse, Namespace: []string{namespace}, Location: root,
		Plaintext: true, StorageEndpoint: storage.Objects.Endpoint, StorageRegion: storage.Objects.Region, StorageAccessKey: cfg.Minio.Fathomry.AccessKey, StorageSecretKey: cfg.Minio.Fathomry.SecretKey,
		Permissions: iceberg.Grants{Read: true, Write: true, Metadata: true, Namespace: true, Import: true, Rewrite: true, Expire: true, Orphan: true, Purge: true, DeleteFiles: true, MultiTable: true}, Timeout: 2 * time.Minute, MaxOperations: 256, MaxFileOps: 16384, MaxHandles: 256, Workers: 2}
	proof := map[string]any{"namespace": namespace, "root": root, "table": "data", "cleanup_complete": false}
	manifestData, _ := json.MarshalIndent(proof, "", "  ")
	if err = os.WriteFile(manifest, manifestData, 0600); err != nil {
		t.Fatal("cannot preserve private fixture manifest")
	}
	policy, err := iceberg.Recommend(settings)
	if err != nil {
		t.Fatal(err)
	}
	lifetime, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	runtime, err := adapters.New(lifetime, policy.Runtime)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close(context.Background())
	inbox, err := adapters.NewInbox[iceberg.Result](policy.Evidence)
	if err != nil {
		t.Fatal(err)
	}
	var loseCommitResponse atomic.Bool
	var lostResponses atomic.Int32
	var lostCommitAttempts atomic.Int32
	responseLost := errors.New("fixture response deliberately lost after server acknowledgement")
	reports := &metrics.InMemoryReporter{}
	owner, err := iceberg.Open(lifetime, settings, iceberg.Dependencies{Runtime: runtime, Evidence: inbox, Native: iceberg.NativeOptions{Reporter: reports, Transport: func(base *http.Transport) (http.RoundTripper, func() error, error) {
		return serviceRoundTripper(func(request *http.Request) (*http.Response, error) {
			if request.Method == http.MethodPost && strings.HasSuffix(request.URL.Path, "/tables/lost_response") {
				lostCommitAttempts.Add(1)
			}
			response, err := base.RoundTrip(request)
			if err == nil && response.StatusCode >= 200 && response.StatusCode < 300 && request.Method == http.MethodPost && strings.HasSuffix(request.URL.Path, "/tables/lost_response") && loseCommitResponse.CompareAndSwap(true, false) {
				_, readErr := io.Copy(io.Discard, response.Body)
				closeErr := response.Body.Close()
				lostResponses.Add(1)
				return nil, errors.Join(responseLost, readErr, closeErr)
			}
			return response, err
		}), func() error { return nil }, nil
	}}})
	if owner != nil {
		defer owner.Close(context.Background())
	}
	if err != nil {
		t.Fatal("service source open failed", err)
	}
	namespaceCreated, viewAttempted := false, false
	tableNames := []string{"data"}
	defer func() {
		if !namespaceCreated {
			return
		}
		receipt, err := owner.Client().Use(lifetime, func(s *iceberg.Session) error {
			for _, name := range tableNames {
				exists, err := s.CheckTableExists([]string{namespace, name})
				if err != nil {
					return err
				}
				if !exists {
					continue
				}
				if err = s.DropTable([]string{namespace, name}); err != nil {
					return err
				}
				exists, err = s.CheckTableExists([]string{namespace, name})
				if err != nil {
					return err
				}
				if exists {
					return fmt.Errorf("table still exists")
				}
			}
			if viewAttempted {
				for _, name := range []string{"report", "renamed_report"} {
					exists, err := s.CheckViewExists([]string{namespace, name})
					if err != nil {
						return err
					}
					if exists {
						if err = s.DropView([]string{namespace, name}); err != nil {
							return err
						}
					}
				}
			}
			var paths []string
			if err := s.WalkFiles(root, func(path string, entry fs.DirEntry, err error) error {
				if err != nil {
					return err
				}
				if !entry.IsDir() {
					paths = append(paths, path)
				}
				return nil
			}); err != nil {
				return err
			}
			if _, err := s.DeleteFiles(paths); err != nil {
				return err
			}
			remaining := 0
			if err := s.WalkFiles(root, func(_ string, entry fs.DirEntry, err error) error {
				if err != nil {
					return err
				}
				if !entry.IsDir() {
					remaining++
				}
				return nil
			}); err != nil {
				return err
			}
			if remaining != 0 {
				return fmt.Errorf("test-owned current objects remain")
			}
			return s.DropNamespace([]string{namespace})
		})
		if err != nil {
			t.Error("fixture cleanup admission failed", err)
			return
		}
		snapshot, err := receipt.WaitReleased(lifetime)
		if err != nil || snapshot.Err() != nil {
			t.Error("fixture cleanup incomplete", err, snapshot.Err())
			return
		}
		proof["cleanup_complete"] = true
		data, _ := json.MarshalIndent(proof, "", "  ")
		if err = os.WriteFile(manifest, data, 0600); err != nil {
			t.Error("cannot update fixture cleanup proof")
		}
		ackPublic(t, inbox)
	}()
	allocator := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer allocator.AssertSize(t, 0)
	schema := sdk.NewSchema(0, sdk.NestedField{ID: 1, Name: "id", Type: sdk.PrimitiveTypes.Int64, Required: true}, sdk.NestedField{ID: 2, Name: "value", Type: sdk.PrimitiveTypes.String})
	arrowSchema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}, {Name: "value", Type: arrow.BinaryTypes.String, Nullable: true}}, nil)
	builder := array.NewRecordBuilder(allocator, arrowSchema)
	expected := map[int64]*string{}
	for row := 0; row < 4097; row++ {
		builder.Field(0).(*array.Int64Builder).Append(int64(row))
		if row%7 == 0 {
			builder.Field(1).(*array.StringBuilder).AppendNull()
			expected[int64(row)] = nil
		} else {
			value := fmt.Sprintf("row-%d", row)
			builder.Field(1).(*array.StringBuilder).Append(value)
			expected[int64(row)] = &value
		}
	}
	record := builder.NewRecordBatch()
	builder.Release()
	defer record.Release()
	receipt, err := owner.Client().Use(lifetime, func(s *iceberg.Session) error {
		if err := s.CreateNamespace([]string{namespace}, sdk.Properties{"fixture": "gh137"}); err != nil {
			return err
		}
		namespaceCreated = true
		if exists, err := s.CheckNamespaceExists([]string{namespace}); err != nil || !exists {
			if err != nil {
				return err
			}
			return fmt.Errorf("namespace absent")
		}
		tbl, err := s.CreateTable([]string{namespace, "data"}, schema, catalog.WithLocation(root+"data"), catalog.WithProperties(sdk.Properties{"format-version": "2", "write.parquet.row-group-limit": "1024"}))
		if err != nil {
			return err
		}
		tx, err := tbl.NewTransaction("")
		if err != nil {
			return err
		}
		reader, err := array.NewRecordReader(arrowSchema, []arrow.RecordBatch{record})
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
		initial, err := tbl.Metadata()
		if err != nil {
			return err
		}
		savedID := initial.CurrentSnapshot().SnapshotID
		scan, err := tbl.Scan(iceberg.ScanOptions{BatchSize: 128})
		if err != nil {
			return err
		}
		actual := map[int64]*string{}
		summary, err := scan.Records(func(batch arrow.RecordBatch) (bool, error) {
			ids := batch.Column(0).(*array.Int64)
			values := batch.Column(1).(*array.String)
			for i := 0; i < ids.Len(); i++ {
				id := ids.Value(i)
				if _, ok := actual[id]; ok {
					return false, fmt.Errorf("duplicate row")
				}
				if values.IsNull(i) {
					actual[id] = nil
				} else {
					v := values.Value(i)
					actual[id] = &v
				}
			}
			return true, nil
		})
		if err != nil {
			return err
		}
		if !summary.Complete || !reflect.DeepEqual(actual, expected) {
			return fmt.Errorf("exact 4097-row multiset differs")
		}
		tx, err = tbl.NewTransaction("")
		if err != nil {
			return err
		}
		if err = tx.Delete(sdk.EqualTo(sdk.Reference("value"), "row-1"), nil); err != nil {
			return err
		}
		tbl, err = tx.Commit()
		if err != nil {
			return err
		}
		scan, err = tbl.Scan(iceberg.ScanOptions{BatchSize: 127})
		if err != nil {
			return err
		}
		actual = map[int64]*string{}
		delete(expected, 1)
		summary, err = scan.Records(func(batch arrow.RecordBatch) (bool, error) {
			ids := batch.Column(0).(*array.Int64)
			values := batch.Column(1).(*array.String)
			for i := 0; i < ids.Len(); i++ {
				id := ids.Value(i)
				if _, ok := actual[id]; ok {
					return false, fmt.Errorf("duplicate row")
				}
				if values.IsNull(i) {
					actual[id] = nil
				} else {
					v := values.Value(i)
					actual[id] = &v
				}
			}
			return true, nil
		})
		if err != nil {
			return err
		}
		if !summary.Complete || !reflect.DeepEqual(actual, expected) {
			return fmt.Errorf("nullable delete multiset differs")
		}
		tbl, err = s.UpdateTable([]string{namespace, "data"}, nil, []table.Update{table.NewSetSnapshotRefUpdate("saved", savedID, table.TagRef, 0, 0, 0)})
		if err != nil {
			return err
		}
		tx, err = tbl.NewTransaction("")
		if err != nil {
			return err
		}
		if err = tx.EditSchema(true, false, nil, []iceberg.SchemaEdit{{Action: "add", Path: []string{"extra"}, Type: sdk.PrimitiveTypes.Int64}}); err != nil {
			return err
		}
		if err = tx.EditPartitions(true, []iceberg.PartitionEdit{{Action: "add", Source: "id", Name: "id_bucket", Transform: sdk.BucketTransform{NumBuckets: 4}}}); err != nil {
			return err
		}
		order, err := table.NewSortOrder(1, []table.SortField{{SourceIDs: []int{1}, Transform: sdk.IdentityTransform{}, Direction: table.SortDirection("asc"), NullOrder: table.NullsFirst}})
		if err != nil {
			return err
		}
		if err = tx.ReplaceSortOrder(order); err != nil {
			return err
		}
		tbl, err = tx.Commit()
		if err != nil {
			return err
		}
		meta, err := tbl.Metadata()
		if err != nil {
			return err
		}
		spec := meta.PartitionSpec()
		if len(meta.CurrentSchema().Fields()) != 3 || meta.CurrentSchema().Field(2).ID != 3 || spec.NumFields() != 1 || meta.SortOrder().IsUnsorted() {
			return fmt.Errorf("evolution identity mismatch")
		}
		tx, err = tbl.NewTransaction("")
		if err != nil {
			return err
		}
		addedBuilder := array.NewRecordBuilder(allocator, arrowSchema)
		for row := 10000; row < 10008; row++ {
			addedBuilder.Field(0).(*array.Int64Builder).Append(int64(row))
			value := fmt.Sprintf("delta-%d", row)
			addedBuilder.Field(1).(*array.StringBuilder).Append(value)
			expected[int64(row)] = &value
		}
		added := addedBuilder.NewRecordBatch()
		addedBuilder.Release()
		addedReader, _ := array.NewRecordReader(arrowSchema, []arrow.RecordBatch{added})
		added.Release()
		err = tx.Append(addedReader, nil)
		addedReader.Release()
		if err != nil {
			return err
		}
		tbl, err = tx.Commit()
		if err != nil {
			return err
		}
		compactionConfig := compaction.DefaultConfig()
		compactionConfig.TargetFileSizeBytes = 1 << 20
		compactionConfig.MinFileSizeBytes = 3 << 18
		compactionConfig.MaxFileSizeBytes = 2 << 20
		compactionConfig.MinInputFiles = 1
		plan, err := tbl.PlanCompaction(compactionConfig)
		if err != nil {
			return err
		}
		groups := make([]table.CompactionTaskGroup, len(plan.Groups))
		for i, group := range plan.Groups {
			groups[i] = table.CompactionTaskGroup{PartitionKey: group.PartitionKey, Tasks: group.Tasks, TotalSizeBytes: group.TotalSizeBytes}
		}
		if len(groups) == 0 {
			return fmt.Errorf("compaction positive control had no groups")
		}
		rewritten, err := tbl.RewriteDataFiles(groups, table.RewriteDataFilesOptions{GroupOptions: []table.CompactionGroupOption{table.WithCompactionArrowBatchSize(128), table.WithCompactionTargetFileSize(1 << 20)}})
		if err != nil {
			return err
		}
		if rewritten == nil || rewritten.Table == nil {
			return fmt.Errorf("rewrite omitted committed handle")
		}
		tbl = rewritten.Table
		scan, err = tbl.Scan(iceberg.ScanOptions{BatchSize: 127})
		if err != nil {
			return err
		}
		actual = map[int64]*string{}
		summary, err = scan.Records(func(batch arrow.RecordBatch) (bool, error) {
			ids := batch.Column(0).(*array.Int64)
			values := batch.Column(1).(*array.String)
			if batch.NumCols() != 3 || batch.Column(2).NullN() != int(batch.NumRows()) {
				return false, fmt.Errorf("evolved nullable column changed")
			}
			for i := 0; i < ids.Len(); i++ {
				id := ids.Value(i)
				if _, ok := actual[id]; ok {
					return false, fmt.Errorf("duplicate after rewrite")
				}
				if values.IsNull(i) {
					actual[id] = nil
				} else {
					value := values.Value(i)
					actual[id] = &value
				}
			}
			return true, nil
		})
		if err != nil {
			return err
		}
		if !summary.Complete || !reflect.DeepEqual(actual, expected) {
			return fmt.Errorf("evolution/rewrite changed exact rows")
		}
		tagScan, err := tbl.Scan(iceberg.ScanOptions{Ref: "saved", BatchSize: 128})
		if err != nil {
			return err
		}
		tagCount := int64(0)
		tagSummary, err := tagScan.Records(func(batch arrow.RecordBatch) (bool, error) {
			if batch.NumCols() != 2 {
				return false, fmt.Errorf("tag schema not retained")
			}
			tagCount += batch.NumRows()
			return true, nil
		})
		if err != nil {
			return err
		}
		if !tagSummary.Complete || tagCount != 4097 {
			return fmt.Errorf("tag snapshot changed")
		}
		tx, err = tbl.NewTransaction("")
		if err != nil {
			return err
		}
		manifestRewrite, err := tx.RewriteManifests()
		if err != nil {
			return err
		}
		if !manifestRewrite.IsNoOp() {
			tbl, err = tx.Commit()
			if err != nil {
				return err
			}
		}
		tx, err = tbl.NewTransaction("")
		if err != nil {
			return err
		}
		if err = tx.ExpireSnapshots(1, 10*time.Millisecond, false); err != nil {
			return err
		}
		tbl, err = tx.Commit()
		if err != nil {
			return err
		}
		meta, err = tbl.Metadata()
		if err != nil {
			return err
		}
		if meta.SnapshotByID(savedID) == nil {
			return fmt.Errorf("expiry removed protected tag")
		}
		orphanPlan, err := tbl.PlanOrphanFiles(iceberg.OrphanOptions{OlderThan: 72 * time.Hour})
		if err != nil {
			return err
		}
		if len(orphanPlan.Files()) != 0 {
			return fmt.Errorf("fresh fixture unexpectedly eligible for old-age deletion")
		}
		proof["evolution_rewrite_exact_rows_pass"] = true
		proof["tag_schema_and_expiry_protection_pass"] = true
		proof["orphan_plan_pass"] = true
		remotePlan, full, err := s.RemotePlanningCapabilities()
		if err != nil {
			return err
		}
		proof["remote_plan_advertised"] = remotePlan
		proof["remote_full_advertised"] = full
		return nil
	})
	value := settlePublic(t, receipt, err)
	if value.Err() != nil {
		observed, _ := value.ValueCopy()
		for _, effect := range observed.Effects() {
			t.Logf("stage=%s effect=%d completed=%t error=%v", effect.Operation, effect.Effect, effect.Complete, effect.Err)
		}
		logServiceCauseTypes(t, value.Primary(), 0)
		logServiceCauseTypes(t, value.Cleanup(), 0)
		t.Fatal("public service operation failed", value.Err())
	}
	result, _ := value.ValueCopy()
	if len(result.Files()) == 0 || !result.Released() {
		t.Fatal("service effect/release evidence absent")
	}
	proof["exact_rows_pass"] = true
	proof["nullable_delete_pass"] = true
	data, _ := json.MarshalIndent(proof, "", "  ")
	if err = os.WriteFile(manifest, data, 0600); err != nil {
		t.Fatal("cannot record service proof")
	}
	ackPublic(t, inbox)
	serviceCase := func(name string, work func(*iceberg.Session) error) bool {
		t.Helper()
		receipt, err := owner.Client().Use(lifetime, work)
		outcome := settlePublic(t, receipt, err)
		ackPublic(t, inbox)
		if name == "read-only-udf" && errors.Is(outcome.Err(), catalog.ErrNoSuchFunction) {
			proof[name] = "live-missing-fixture/refusal"
			t.Logf("profile=%s status=live-missing-fixture/refusal", name)
			return false
		}
		if errors.Is(outcome.Err(), rest.ErrEndpointNotSupported) || errors.Is(outcome.Err(), sdk.ErrNotImplemented) {
			proof[name] = "live-unavailable/refusal"
			t.Logf("profile=%s status=live-unavailable/refusal", name)
			return false
		}
		if outcome.Err() != nil {
			logServiceCauseTypes(t, outcome.Primary(), 0)
			logServiceCauseTypes(t, outcome.Cleanup(), 0)
			t.Fatalf("profile=%s failed: %v", name, outcome.Err())
		}
		proof[name] = "pass"
		t.Logf("profile=%s status=pass", name)
		return true
	}
	serviceCase("views", func(s *iceberg.Session) error {
		version, err := view.NewVersionFromSQL(1, schema.ID, "SELECT id, value FROM data", []string{namespace})
		if err != nil {
			return err
		}
		viewAttempted = true
		created, err := s.CreateView([]string{namespace, "report"}, version, schema, catalog.WithViewLocation(root+"report"))
		if err != nil {
			if errors.Is(err, rest.ErrEndpointNotSupported) || errors.Is(err, sdk.ErrNotImplemented) {
				viewAttempted = false
			}
			return err
		}
		updated, err := s.UpdateView(created.Identifier(), []view.Requirement{view.AssertViewUUID(created.Metadata().ViewUUID())}, []view.Update{view.NewSetPropertiesUpdate(sdk.Properties{"fixture": "reviewed"})})
		if err != nil {
			return err
		}
		if updated.Properties()["fixture"] != "reviewed" || updated.Metadata().ViewUUID() != created.Metadata().ViewUUID() {
			return fmt.Errorf("view commit identity changed")
		}
		renamed, err := s.RenameView(updated.Identifier(), []string{namespace, "renamed_report"})
		if err != nil {
			return err
		}
		loaded, err := s.LoadView(renamed.Identifier())
		if err != nil {
			return err
		}
		if !loaded.Metadata().Equals(updated.Metadata()) {
			return fmt.Errorf("view rename changed metadata")
		}
		views, err := s.ListViews([]string{namespace})
		if err != nil || len(views) != 1 {
			return fmt.Errorf("view listing mismatch: %w", err)
		}
		return nil
	})
	serviceCase("read-only-udf", func(s *iceberg.Session) error {
		_, err := s.LoadFunction([]string{namespace, "absent_fixture_function"})
		if errors.Is(err, catalog.ErrNoSuchFunction) {
			proof["udf_missing_function_refused"] = true
		}
		return err
	})
	serviceCase("remote-planning", func(s *iceberg.Session) error {
		_, err := s.PlanTableScan([]string{namespace, "data"}, rest.PlanTableScanRequest{})
		return err
	})
	tableNames = append(tableNames, "staged")
	serviceCase("staged-create", func(s *iceberg.Session) error {
		tbl, err := s.CreateTable([]string{namespace, "staged"}, schema, catalog.WithLocation(root+"staged"), catalog.WithStagedUpdates(table.NewSetPropertiesUpdate(sdk.Properties{"stage": "accepted"})))
		if err != nil {
			return err
		}
		meta, err := tbl.Metadata()
		if err == nil && meta.Properties()["stage"] != "accepted" {
			return fmt.Errorf("staged metadata intent missing")
		}
		return err
	})
	tableNames = append(tableNames, "multi_a", "multi_b")
	proof["tables"] = tableNames
	serviceCase("multi-table-commit", func(s *iceberg.Session) error {
		transactions := make([]*iceberg.Transaction, 0, 2)
		for _, name := range []string{"multi_a", "multi_b"} {
			tbl, err := s.CreateTable([]string{namespace, name}, schema, catalog.WithLocation(root+name), catalog.WithProperties(sdk.Properties{"format-version": "3"}))
			if err != nil {
				return err
			}
			tx, err := tbl.NewTransaction("")
			if err != nil {
				return err
			}
			reader, err := array.NewRecordReader(arrowSchema, []arrow.RecordBatch{record})
			if err != nil {
				return err
			}
			err = tx.Append(reader, nil)
			reader.Release()
			if err != nil {
				return err
			}
			transactions = append(transactions, tx)
		}
		if err := s.CommitTransaction(transactions...); err != nil {
			return err
		}
		for _, name := range []string{"multi_a", "multi_b"} {
			tbl, err := s.LoadTable([]string{namespace, name})
			if err != nil {
				return err
			}
			meta, err := tbl.Metadata()
			if err != nil || meta.Version() != 3 {
				return fmt.Errorf("explicit format-3 selection lost: %w", err)
			}
			scan, err := tbl.Scan(iceberg.ScanOptions{BatchSize: 127})
			if err != nil {
				return err
			}
			seen := map[int64]bool{}
			summary, err := scan.Records(func(batch arrow.RecordBatch) (bool, error) {
				ids, values := batch.Column(0).(*array.Int64), batch.Column(1).(*array.String)
				for row := range int(batch.NumRows()) {
					id := ids.Value(row)
					if id < 0 || id >= 4097 || seen[id] || values.IsNull(row) != (id%7 == 0) || !values.IsNull(row) && values.Value(row) != fmt.Sprintf("row-%d", id) {
						return false, fmt.Errorf("multi-table exact row mismatch")
					}
					seen[id] = true
				}
				return true, nil
			})
			if err != nil || !summary.Complete || len(seen) != 4097 {
				return fmt.Errorf("multi-table read incomplete: %w", err)
			}
		}
		proof["explicit_v3_rows_pass"] = true
		return nil
	})
	serviceCase("v3-equality-delete", func(s *iceberg.Session) error {
		tbl, err := s.LoadTable([]string{namespace, "multi_a"})
		if err != nil {
			return err
		}
		tx, err := tbl.NewTransaction("")
		if err != nil {
			return err
		}
		keySchema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
		err = tx.CommitEqualityDeletes([]int{1}, func(yield func(arrow.RecordBatch, error) bool) {
			builder := array.NewRecordBuilder(allocator, keySchema)
			builder.Field(0).(*array.Int64Builder).AppendValues([]int64{2, 7}, nil)
			keys := builder.NewRecordBatch()
			builder.Release()
			defer keys.Release()
			yield(keys, nil)
		}, nil)
		if err != nil {
			return err
		}
		tbl, err = tx.Commit()
		if err != nil {
			return err
		}
		scan, err := tbl.Scan(iceberg.ScanOptions{BatchSize: 31})
		if err != nil {
			return err
		}
		seen := map[int64]bool{}
		summary, err := scan.Records(func(batch arrow.RecordBatch) (bool, error) {
			ids, values := batch.Column(0).(*array.Int64), batch.Column(1).(*array.String)
			for row := range ids.Len() {
				id := ids.Value(row)
				if id < 0 || id >= 4097 || id == 2 || id == 7 || seen[id] || values.IsNull(row) != (id%7 == 0) || !values.IsNull(row) && values.Value(row) != fmt.Sprintf("row-%d", id) {
					return false, fmt.Errorf("equality delete changed exact row multiset")
				}
				seen[id] = true
			}
			return true, nil
		})
		if err != nil || !summary.Complete || len(seen) != 4095 {
			return fmt.Errorf("equality delete scan incomplete: %w", err)
		}
		return nil
	})
	serviceCase("client-purge-files", func(s *iceberg.Session) error {
		tbl, err := s.LoadTable([]string{namespace, "multi_b"})
		if err != nil {
			return err
		}
		if err = s.DropTable(tbl.Identifier()); err != nil {
			return err
		}
		if err = tbl.PurgeFiles(true); err != nil {
			return err
		}
		remaining := 0
		if err = s.WalkFiles(root+"multi_b", func(_ string, entry fs.DirEntry, err error) error {
			if err == nil && !entry.IsDir() {
				remaining++
			}
			return err
		}); err != nil {
			return err
		}
		if remaining != 0 {
			return fmt.Errorf("purged current objects remain")
		}
		return nil
	})
	tableNames = append(tableNames, "lost_response")
	proof["tables"] = tableNames
	receipt, err = owner.Client().Use(lifetime, func(s *iceberg.Session) error {
		tbl, err := s.CreateTable([]string{namespace, "lost_response"}, schema, catalog.WithLocation(root+"lost_response"))
		if err != nil {
			return err
		}
		tx, err := tbl.NewTransaction("")
		if err != nil {
			return err
		}
		reader, err := array.NewRecordReader(arrowSchema, []arrow.RecordBatch{record})
		if err != nil {
			return err
		}
		err = tx.Append(reader, nil)
		reader.Release()
		if err != nil {
			return err
		}
		loseCommitResponse.Store(true)
		_, err = tx.Commit()
		return err
	})
	lost := settlePublic(t, receipt, err)
	ackPublic(t, inbox)
	if !errors.Is(lost.Err(), responseLost) || lostResponses.Load() != 1 || lostCommitAttempts.Load() != 1 {
		t.Fatal("live applied response-loss control did not execute")
	}
	lostResult, _ := lost.ValueCopy()
	intended := int64(0)
	for _, effect := range lostResult.Effects() {
		if effect.Operation == "commit" {
			if effect.Effect != tableformat.Unknown || effect.CommittedSnapshot != 0 || !effect.Phases.Submitted {
				t.Fatal("lost reply became a confirmed or unsubmitted commit")
			}
			intended = effect.IntendedSnapshot
		}
	}
	if intended == 0 {
		t.Fatal("lost commit intention missing")
	}
	serviceCase("applied-response-loss", func(s *iceberg.Session) error {
		tbl, err := s.LoadTable([]string{namespace, "lost_response"})
		if err != nil {
			return err
		}
		meta, err := tbl.Metadata()
		if err != nil {
			return err
		}
		if meta.CurrentSnapshot() == nil || meta.CurrentSnapshot().SnapshotID != intended {
			return fmt.Errorf("server did not apply the lost-ack commit")
		}
		scan, err := tbl.Scan(iceberg.ScanOptions{BatchSize: 127})
		if err != nil {
			return err
		}
		seen := map[int64]bool{}
		summary, err := scan.Records(func(batch arrow.RecordBatch) (bool, error) {
			ids, values := batch.Column(0).(*array.Int64), batch.Column(1).(*array.String)
			for row := range ids.Len() {
				id := ids.Value(row)
				if id < 0 || id >= 4097 || seen[id] || values.IsNull(row) != (id%7 == 0) || !values.IsNull(row) && values.Value(row) != fmt.Sprintf("row-%d", id) {
					return false, fmt.Errorf("lost commit readback changed exact rows")
				}
				seen[id] = true
			}
			return true, nil
		})
		if err != nil || !summary.Complete || len(seen) != 4097 {
			return fmt.Errorf("lost commit readback incomplete: %w", err)
		}
		return nil
	})
	serviceConcurrentMutations(t, lifetime, owner, inbox, namespace, root, record, &tableNames, proof)
	serviceCase("incremental-append-changelog-streams", func(s *iceberg.Session) error {
		tbl, err := s.LoadTable([]string{namespace, "concurrent_append"})
		if err != nil {
			return err
		}
		meta, err := tbl.Metadata()
		if err != nil {
			return err
		}
		snapshot := meta.CurrentSnapshot()
		if snapshot == nil || snapshot.ParentSnapshotID == nil {
			return fmt.Errorf("incremental live history missing")
		}
		span := iceberg.IncrementalRange{From: *snapshot.ParentSnapshotID, To: snapshot.SnapshotID}
		seen := map[int64]bool{}
		writer := int64(-1)
		visit := func(batch arrow.RecordBatch) (bool, error) {
			ids, values := batch.Column(0).(*array.Int64), batch.Column(1).(*array.String)
			for row := range ids.Len() {
				id := ids.Value(row)
				selected, offset := (id-10000)/100, (id-10000)%100
				if selected < 0 || selected > 1 || offset < 0 || offset >= 9 || seen[id] || values.IsNull(row) || values.Value(row) != fmt.Sprintf("writer-%d-%d", selected, offset) {
					return false, fmt.Errorf("incremental live multiset mismatch")
				}
				if writer != -1 && writer != selected {
					return false, fmt.Errorf("incremental read combined rejected writer")
				}
				writer, seen[id] = selected, true
			}
			return true, nil
		}
		summary, err := tbl.IncrementalAppendRecords(iceberg.ScanOptions{BatchSize: 2}, span, visit)
		if err != nil {
			return err
		}
		if !summary.Complete || len(seen) != 9 {
			return fmt.Errorf("incremental append incomplete")
		}
		clear(seen)
		summary, err = tbl.IncrementalChangelogRecords(iceberg.ScanOptions{BatchSize: 2}, span, func(change iceberg.Change, batch arrow.RecordBatch) (bool, error) {
			if change.Operation != table.ChangelogOpInsert || change.Ordinal != 0 || change.SnapshotID != snapshot.SnapshotID {
				return false, fmt.Errorf("incremental change identity differs")
			}
			return visit(batch)
		})
		if err != nil {
			return err
		}
		if !summary.Complete || len(seen) != 9 {
			return fmt.Errorf("incremental changelog incomplete")
		}
		return nil
	})
	tableNames = append(tableNames, "typed")
	serviceCase("typed-4097-nested-decimal-time-uuid", func(s *iceberg.Session) error {
		return serviceTypedRows(s, namespace, root, allocator)
	})
	tableNames = append(tableNames, "catalog_original", "catalog_renamed", "catalog_registered")
	serviceCase("catalog-rename-register-purge", func(s *iceberg.Session) error {
		if _, err := s.UpdateNamespaceProperties([]string{namespace}, nil, sdk.Properties{"lifecycle": "exact"}); err != nil {
			return err
		}
		properties, err := s.LoadNamespaceProperties([]string{namespace})
		if err != nil {
			return err
		}
		if properties["lifecycle"] != "exact" {
			return fmt.Errorf("namespace property lost")
		}
		tbl, err := s.CreateTable([]string{namespace, "catalog_original"}, schema)
		if err != nil {
			return err
		}
		meta, err := tbl.Metadata()
		if err != nil {
			return err
		}
		if !strings.HasPrefix(meta.Location(), root) {
			return fmt.Errorf("default create location escaped source root")
		}
		identity := meta.TableUUID()
		tbl, err = s.RenameTable(tbl.Identifier(), []string{namespace, "catalog_renamed"})
		if err != nil {
			return err
		}
		location, err := tbl.MetadataLocation()
		if err != nil {
			return err
		}
		if err = s.DropTable(tbl.Identifier()); err != nil {
			return err
		}
		tbl, err = s.RegisterTable([]string{namespace, "catalog_registered"}, location)
		if err != nil {
			return err
		}
		meta, err = tbl.Metadata()
		if err != nil {
			return err
		}
		if meta.TableUUID() != identity {
			return fmt.Errorf("register changed native UUID")
		}
		found := false
		if err = s.VisitTables([]string{namespace}, 1, func(id table.Identifier) (bool, error) {
			found = found || id[len(id)-1] == "catalog_registered"
			return true, nil
		}); err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("paginated table listing omitted registration")
		}
		if err = s.PurgeTable(tbl.Identifier(), true); err != nil {
			return err
		}
		exists, err := s.CheckTableExists(tbl.Identifier())
		if err != nil {
			return err
		}
		if exists {
			return fmt.Errorf("server purge did not drop Catalog entry")
		}
		return nil
	})
	scans, commits := 0, 0
	for _, report := range reports.Reports() {
		switch report.(type) {
		case metrics.ScanReport:
			scans++
		case metrics.CommitReport:
			commits++
		}
	}
	if scans == 0 || commits == 0 {
		t.Fatal("live native scan/commit reports absent")
	}
	proof["native-reporting"] = "scan-and-commit-reports-received"
}

func logServiceCauseTypes(t *testing.T, err error, depth int) {
	if err == nil || depth > 12 {
		return
	}
	t.Logf("cause depth=%d type=%T", depth, err)
	if boundary, ok := err.(*fault.Error); ok {
		t.Logf("safe native boundary: %v operation=%s", err, boundary.Diagnostic().Context.Operation)
	}
	switch wrapped := err.(type) {
	case interface{ Unwrap() []error }:
		for _, cause := range wrapped.Unwrap() {
			logServiceCauseTypes(t, cause, depth+1)
		}
	case interface{ Unwrap() error }:
		logServiceCauseTypes(t, wrapped.Unwrap(), depth+1)
	}
}
