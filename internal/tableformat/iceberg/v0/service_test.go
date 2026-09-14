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
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"context"
	"github.com/apache/arrow-go/v18/arrow"
	nativecatalog "github.com/apache/iceberg-go/catalog"
	nativeio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
	"go.yaml.in/yaml/v3"
)

type serviceConfig struct {
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

func TestIsolatedLakekeeperMinIOService(t *testing.T) {
	path := os.Getenv("FATHOMRY_ICEBERG_SERVICE_CONFIG")
	if path == "" || os.Getenv("FATHOMRY_ICEBERG_SERVICE_WRITES") != "1" {
		t.Skip("requires explicit service configuration and isolated write/cleanup authorization")
	}
	manifestPath := os.Getenv("FATHOMRY_ICEBERG_SERVICE_MANIFEST")
	if manifestPath == "" {
		t.Fatal("service writes require a new private fixture-manifest path")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal("cannot read service configuration")
	}
	var cfg serviceConfig
	if err = yaml.Unmarshal(raw, &cfg); err != nil {
		t.Fatal("invalid service configuration")
	}
	clear(raw)
	storage := cfg.Fathomry.Storage
	if storage.Catalog.Implementation != "lakekeeper" || storage.Catalog.Authentication != "none_trusted_network" ||
		storage.Objects.Bucket != cfg.Minio.Fathomry.Bucket || cfg.Minio.Fathomry.AccessKey == "" {
		t.Fatal("unexpected authorized service profile")
	}
	var random [8]byte
	if _, err = rand.Read(random[:]); err != nil {
		t.Fatal("cannot allocate isolated identity")
	}
	namespace := "fathomry_gh43_" + hex.EncodeToString(random[:])
	location := strings.TrimRight(storage.Catalog.Prefix, "/") + "/" + namespace + "/"
	parsed, err := url.Parse(location)
	if err != nil || parsed.Scheme != "s3" || parsed.Host != storage.Objects.Bucket {
		t.Fatal("warehouse and object-store scopes differ")
	}
	options := OptionsV1{Name: "service", CatalogURI: storage.Catalog.URI, CatalogPrefix: storage.Catalog.WarehouseID,
		Warehouse: storage.Catalog.Warehouse, Namespace: namespace, Location: location, Plaintext: true,
		StorageEndpoint: storage.Objects.Endpoint, StorageRegion: storage.Objects.Region,
		StorageAccessKey: cfg.Minio.Fathomry.AccessKey, StorageSecretKey: cfg.Minio.Fathomry.SecretKey, Writes: true, Timeout: 20 * time.Second}
	selected, err := Select(options)
	if err != nil {
		t.Fatal("service configuration rejected", err)
	}
	selected = resource.WithLimits(selected, LimitsV1(options))
	assembly, err := resource.Assemble(deadline(t), deadline(t), "service", selected)
	if err != nil {
		if assembly != nil {
			_ = assembly.Close(deadline(t))
		}
		t.Fatal("service readiness failed", err)
	}
	inbox, _ := invocation.NewInbox[Result](32, 32*defaults(options).evidenceBytes())
	client, err := Bind(assembly, selected, inbox, nil)
	if err != nil {
		t.Fatal("service binding failed", err)
	}
	f := fixture{client, assembly, selected, inbox}
	t.Cleanup(func() {
		drain(t, f)
		if err := assembly.Close(deadline(t)); err != nil {
			t.Error("source cleanup incomplete", err)
		}
	})
	manifest := map[string]any{"namespace": namespace, "location": location, "table": "data", "started_at": time.Now().UTC().Format(time.RFC3339Nano), "cleanup_confirmed": false}
	manifestFile, err := os.OpenFile(manifestPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal("cannot reserve private fixture manifest; no service writes started")
	}
	writeErr := json.NewEncoder(manifestFile).Encode(manifest)
	if err = errors.Join(writeErr, manifestFile.Close()); err != nil {
		t.Fatal("cannot persist private fixture manifest; no service writes started")
	}
	call := func(receipt *invocation.Receipt[Result], err error) Result {
		t.Helper()
		if err != nil {
			t.Fatal("service operation rejected", err)
		}
		result, err := receipt.WaitReleased(deadline(t))
		if err != nil {
			t.Fatal("service waiting incomplete", err)
		}
		if result.Err() != nil {
			for _, label := range serviceFailureLabels(result.Err()) {
				if strings.HasSuffix(label, ":metadata-file-mismatch") {
					diagnoseMetadataDifference(t, f)
				}
			}
			t.Fatal("service operation failed", serviceFailureLabels(result.Err()))
		}
		return result.Outcome.Value
	}
	receipt, err := client.CreateNamespace(deadline(t), correlation("namespace"))
	created := call(receipt, err)
	if created.Effect() != Acknowledged {
		t.Fatal("namespace creation unconfirmed; no existing namespace adopted")
	}
	t.Cleanup(func() {
		if cleanupService(t, f, options) {
			manifest["cleanup_confirmed"] = true
			encoded, err := json.Marshal(manifest)
			if err != nil || os.WriteFile(manifestPath, encoded, 0o600) != nil {
				t.Error("cannot update fixture cleanup evidence")
			}
		}
	})
	receipt, err = client.CreateTable(deadline(t), correlation("create"), "data", testSchema())
	call(receipt, err)
	started := time.Now()
	receipt, err = client.Write(deadline(t), correlation("append"), "data",
		BatchWrite{Mode: Append, Batches: []arrow.RecordBatch{makeBatch(t, 0, 4097, "service")}})
	written := call(receipt, err)
	writeElapsed := time.Since(started)
	if !written.Staged() || written.Effect() != Acknowledged || !written.Reloaded() || written.CommittedSnapshotID() == 0 {
		t.Fatal("missing write/commit/reload evidence")
	}
	started = time.Now()
	receipt, err = client.Read(deadline(t), correlation("read"), "data", BatchRead{})
	observed := call(receipt, err)
	readElapsed := time.Since(started)
	if !observed.Complete() || observed.SnapshotID() != written.CommittedSnapshotID() {
		t.Fatal("committed snapshot not observed")
	}
	expected := expectedRows(0, 4097, "service")
	inspectRows(t, observed, expected)
	receipt, err = client.Write(deadline(t), correlation("overwrite"), "data", BatchWrite{Mode: Overwrite,
		Predicates: []Predicate{{Column: "id", Operator: "lt", Value: "8"}}, Batches: []arrow.RecordBatch{makeBatch(t, 0, 8, "replacement")}})
	call(receipt, err)
	for id, value := range expectedRows(0, 8, "replacement") {
		expected[id] = value
	}
	receipt, err = client.Write(deadline(t), correlation("delete"), "data", BatchWrite{Mode: Delete, Predicates: []Predicate{{Column: "id", Operator: "ge", Value: "4090"}}})
	call(receipt, err)
	for id := int64(4090); id < 4097; id++ {
		delete(expected, id)
	}
	receipt, err = client.Compact(deadline(t), correlation("compact"), "data")
	call(receipt, err)
	receipt, err = client.Read(deadline(t), correlation("after-mutations"), "data", BatchRead{})
	inspectRows(t, call(receipt, err), expected)
	receipt, err = client.Read(deadline(t), correlation("old-snapshot"), "data", BatchRead{SnapshotID: written.CommittedSnapshotID()})
	inspectRows(t, call(receipt, err), expectedRows(0, 4097, "service"))
	first, firstErr := client.Write(deadline(t), correlation("concurrent-first"), "data", BatchWrite{Mode: Append, Batches: []arrow.RecordBatch{makeBatch(t, 5000, 11, "first")}})
	second, secondErr := client.Write(deadline(t), correlation("concurrent-second"), "data", BatchWrite{Mode: Append, Batches: []arrow.RecordBatch{makeBatch(t, 6000, 11, "second")}})
	if firstErr != nil || secondErr != nil {
		t.Fatal("concurrent service calls rejected before execution")
	}
	accepted := 0
	for index, receipt := range []*invocation.Receipt[Result]{first, second} {
		result, err := receipt.WaitReleased(deadline(t))
		if err != nil {
			t.Fatal("concurrent service wait incomplete")
		}
		if result.Err() == nil {
			accepted++
			start, label := 5000, "first"
			if index == 1 {
				start, label = 6000, "second"
			}
			for id, value := range expectedRows(start, 11, label) {
				expected[id] = value
			}
		} else if !errors.Is(result.Err(), table.ErrCommitFailed) || result.Outcome.Value.Effect() != Rejected {
			t.Fatal("concurrent service result was neither acknowledged nor a confirmed conflict", serviceFailureLabels(result.Err()))
		}
	}
	if accepted == 0 {
		t.Fatal("no concurrent writer made progress")
	}
	receipt, err = client.Read(deadline(t), correlation("after-concurrency"), "data", BatchRead{})
	inspectRows(t, call(receipt, err), expected)
	receipt, err = client.EvolveSchema(deadline(t), correlation("schema"), "data", []SchemaChange{{Operation: "add", Name: "extra", Type: "long"}})
	call(receipt, err)
	receipt, err = client.Read(deadline(t), correlation("evolved"), "data", BatchRead{})
	inspectRows(t, call(receipt, err), expected)
	receipt, err = client.EvolvePartitions(deadline(t), correlation("partition"), "data", []PartitionChange{{Operation: "bucket", Column: "id", Name: "id_bucket", Buckets: 4}})
	call(receipt, err)
	receipt, err = client.Write(deadline(t), correlation("partitioned-append"), "data", BatchWrite{Mode: Append, Batches: []arrow.RecordBatch{makeBatch(t, 7000, 16, "partitioned")}})
	call(receipt, err)
	for id, value := range expectedRows(7000, 16, "partitioned") {
		expected[id] = value
	}
	receipt, err = client.Read(deadline(t), correlation("after-partition"), "data", BatchRead{})
	inspectRows(t, call(receipt, err), expected)
	receipt, err = client.RollbackSnapshot(deadline(t), correlation("rollback"), "data", written.CommittedSnapshotID())
	call(receipt, err)
	receipt, err = client.Read(deadline(t), correlation("after-rollback"), "data", BatchRead{})
	inspectRows(t, call(receipt, err), expectedRows(0, 4097, "service"))
	if bytes.Equal(written.MetadataCopy(), observed.IPCCopy()) {
		t.Fatal("metadata confused with rows")
	}
	t.Logf("real isolated batch/commit/reload/read and mutation checks passed: initial_rows=4097 append_and_reload=%s read=%s; single-run timings, not a capacity benchmark", writeElapsed, readElapsed)
}

func serviceFailureLabels(err error) []string {
	labels := []string{}
	var visit func(error)
	visit = func(err error) {
		if err == nil || len(labels) >= 16 {
			return
		}
		if cause, ok := err.(*fault.Error); ok {
			diagnostic := cause.Diagnostic()
			labels = append(labels, string(diagnostic.Kind)+":"+diagnostic.Context.Operation)
		}
		switch cause := err.(type) {
		case interface{ Unwrap() []error }:
			for _, child := range cause.Unwrap() {
				visit(child)
			}
		case interface{ Unwrap() error }:
			visit(cause.Unwrap())
		}
	}
	visit(err)
	return labels
}

// This failure probe reports only standard metadata field names, never values.
func diagnoseMetadataDifference(t *testing.T, f fixture) {
	t.Helper()
	state := newExchange(f.client.owner, nil)
	ctx := withExchange(deadline(t), state)
	defer state.finish()
	reported, err := f.client.owner.catalog.LoadTable(ctx, f.client.identifier("data"))
	if err != nil {
		t.Log("metadata comparison probe could not reload")
		return
	}
	persisted, err := table.NewFromLocation(ctx, f.client.identifier("data"), reported.MetadataLocation(),
		func(ctx context.Context) (nativeio.IO, error) { return &fileIO{ctx: ctx, state: state}, nil }, f.client.owner.catalog)
	if err != nil {
		t.Log("metadata comparison probe could not decode the object")
		return
	}
	left, _ := json.Marshal(reported.Metadata())
	right, _ := json.Marshal(persisted.Metadata())
	var a, b map[string]json.RawMessage
	_ = json.Unmarshal(left, &a)
	_ = json.Unmarshal(right, &b)
	for _, key := range []string{"format-version", "table-uuid", "location", "last-sequence-number", "last-updated-ms", "last-column-id", "schemas", "current-schema-id", "partition-specs", "default-spec-id", "last-partition-id", "properties", "current-snapshot-id", "snapshots", "snapshot-log", "metadata-log", "sort-orders", "default-sort-order-id", "refs", "statistics", "partition-statistics"} {
		if !bytes.Equal(a[key], b[key]) {
			t.Log("metadata difference field:", key)
		}
	}
	same := len(reported.Metadata().Snapshots()) == len(persisted.Metadata().Snapshots())
	for _, snapshot := range reported.Metadata().Snapshots() {
		other := persisted.SnapshotByID(snapshot.SnapshotID)
		same = same && other != nil && snapshot.Equals(*other)
	}
	t.Logf("metadata snapshot members equal by ID: %t", same)
}

func cleanupService(t *testing.T, f fixture, options OptionsV1) bool {
	t.Helper()
	drain(t, f)
	for _, source := range f.assembly.Snapshot().Sources {
		if source.Usage.Active != 0 {
			t.Error("active operation remains; isolated resources retained")
			return false
		}
	}
	receipt, err := f.client.DropTable(deadline(t), correlation("cleanup-table"), "data")
	if err != nil {
		t.Error("table cleanup rejected", err)
		return false
	}
	result, err := receipt.WaitReleased(deadline(t))
	if err != nil || result.Err() != nil && !errors.Is(result.Err(), nativecatalog.ErrNoSuchTable) {
		t.Error("table cleanup unconfirmed")
		return false
	}
	receipt, err = f.client.InspectTable(deadline(t), correlation("cleanup-observe-table"), "data")
	if err != nil {
		t.Error("table absence check rejected", err)
		return false
	}
	result, err = receipt.WaitReleased(deadline(t))
	if err != nil || !errors.Is(result.Err(), nativecatalog.ErrNoSuchTable) {
		t.Error("table absence not observed")
		return false
	}
	transport := &http.Transport{Proxy: nil, DisableKeepAlives: true, ResponseHeaderTimeout: 20 * time.Second}
	defer transport.CloseIdleConnections()
	store := newStorageClient(defaults(options), transport)
	bucket, prefix := defaults(options).storageAddress()
	cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	listed, err := store.ListObjectsV2(cleanupCtx, &s3.ListObjectsV2Input{Bucket: aws.String(bucket), Prefix: aws.String(prefix), MaxKeys: aws.Int32(128)})
	if err != nil || aws.ToBool(listed.IsTruncated) {
		t.Error("isolated cleanup listing incomplete")
		return false
	}
	var addresses []string
	for _, object := range listed.Contents {
		key := aws.ToString(object.Key)
		if !strings.HasPrefix(key, prefix) {
			t.Error("cleanup scope escaped")
			return false
		}
		addresses = append(addresses, key)
	}
	for _, key := range addresses {
		if _, err = store.DeleteObject(cleanupCtx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)}); err != nil {
			t.Error("object cleanup failed")
			return false
		}
	}
	listed, err = store.ListObjectsV2(cleanupCtx, &s3.ListObjectsV2Input{Bucket: aws.String(bucket), Prefix: aws.String(prefix), MaxKeys: aws.Int32(1)})
	if err != nil || aws.ToBool(listed.IsTruncated) || len(listed.Contents) != 0 {
		t.Error("object absence not observed")
		return false
	}
	receipt, err = f.client.DropNamespace(deadline(t), correlation("cleanup-namespace"))
	if err != nil {
		t.Error("namespace cleanup rejected", err)
		return false
	}
	result, err = receipt.WaitReleased(deadline(t))
	if err != nil || result.Err() != nil {
		t.Error("namespace cleanup unconfirmed")
		return false
	}
	state := newExchange(f.client.owner, nil)
	exists, err := f.client.owner.catalog.CheckNamespaceExists(withExchange(deadline(t), state), table.Identifier{options.Namespace})
	_, _ = state.finish()
	if err != nil || exists {
		t.Error("namespace absence not observed")
		return false
	}
	t.Logf("cleanup: table, %d removed objects' current prefix, and namespace absence observed", len(addresses))
	return true
}
