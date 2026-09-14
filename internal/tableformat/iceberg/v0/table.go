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
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"slices"
	"strconv"
	"strings"

	native "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	nativeio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/klauspost/compress/zstd"
)

func validateSchema(schema *native.Schema) error {
	if schema == nil || len(schema.Fields()) == 0 || len(schema.Fields()) > 128 {
		return failure(ErrInput, "schema")
	}
	ids, names := map[int]bool{}, map[string]bool{}
	for _, field := range schema.Fields() {
		if field.ID <= 0 || ids[field.ID] || !nameOK(field.Name) || names[field.Name] || field.Type == nil {
			return failure(ErrInput, "schema-field")
		}
		ids[field.ID], names[field.Name] = true, true
		switch field.Type {
		case native.PrimitiveTypes.Bool, native.PrimitiveTypes.Int32, native.PrimitiveTypes.Int64,
			native.PrimitiveTypes.Float32, native.PrimitiveTypes.Float64, native.PrimitiveTypes.String, native.PrimitiveTypes.Binary:
		default:
			return failure(ErrUnsupported, "schema-type")
		}
	}
	return nil
}
func tableProperties(s settings) native.Properties {
	return native.Properties{
		"commit.retry.num-retries":                   "0",
		"write.metadata.delete-after-commit.enabled": "false",
		"write.delete.mode":                          "copy-on-write",
		"write.format.default":                       "parquet",
		"write.target-file-size-bytes":               strconv.Itoa(s.MaxObjectBytes / 2),
		"write.parquet.row-group-size-bytes":         strconv.Itoa(s.MaxBatchBytes),
		"write.parquet.row-group-limit":              strconv.Itoa(min(s.MaxRows, 8192)),
		"write.parquet.page-size-bytes":              strconv.Itoa(64 << 10),
		"write.parquet.page-row-limit":               strconv.Itoa(min(s.MaxRows, 1024)),
		"write.parquet.dict-size-bytes":              strconv.Itoa(64 << 10),
		"read.parquet.batch-size":                    strconv.Itoa(min(s.MaxRows, 1024)),
	}
}
func validateMetadata(s settings, metadata table.Metadata) error {
	if err := validateSchema(metadata.CurrentSchema()); err != nil {
		return err
	}
	if metadata.Version() != 2 {
		return failure(ErrUnsupported, "table-format")
	}
	if !strings.HasPrefix(metadata.Location()+"/", s.Location) || len(metadata.Location()) > 1024 {
		return failure(ErrAuthority, "table-location")
	}
	if len(metadata.Schemas()) > 128 || len(metadata.PartitionSpecs()) > 128 || len(metadata.Snapshots()) > s.MaxFileOps {
		return failure(ErrLimit, "metadata-count")
	}
	maxColumn := 0
	for _, schema := range metadata.Schemas() {
		if err := validateSchema(schema); err != nil {
			return err
		}
		for _, field := range schema.Fields() {
			maxColumn = max(maxColumn, field.ID)
		}
	}
	if metadata.LastColumnID() < maxColumn {
		return failure(ErrProtocol, "column-history")
	}
	maxPartition := 999
	definitions := map[int]string{}
	for _, spec := range metadata.PartitionSpecs() {
		if spec.NumFields() > 16 {
			return failure(ErrLimit, "partition-count")
		}
		for _, field := range spec.Fields() {
			definition := strconv.Itoa(field.SourceID()) + ":" + field.Transform.String()
			if previous, exists := definitions[field.FieldID]; exists && previous != definition {
				return failure(ErrProtocol, "partition-id-reused")
			}
			definitions[field.FieldID] = definition
			maxPartition = max(maxPartition, field.FieldID)
		}
	}
	if last := metadata.LastPartitionSpecID(); last == nil && maxPartition > 999 || last != nil && *last < maxPartition {
		return failure(ErrProtocol, "partition-history")
	}
	props := metadata.Properties()
	for key, want := range tableProperties(s) {
		value, exists := props[key]
		if !exists {
			return failure(ErrUnsupported, "unqualified-table-properties")
		}
		if value != want {
			return failure(ErrUnsupported, "table-properties")
		}
	}
	for key := range props {
		if strings.HasPrefix(key, "write.") {
			if _, allowed := tableProperties(s)[key]; !allowed {
				return failure(ErrUnsupported, "write-property")
			}
		}
		if strings.HasPrefix(key, "commit.retry.") && key != "commit.retry.num-retries" {
			return failure(ErrUnsupported, "commit-property")
		}
	}
	return nil
}
func (client *Client) identifier(name string) table.Identifier {
	return table.Identifier{client.owner.settings.Namespace, name}
}
func (client *Client) load(ctx context.Context, state *exchange, name string) (*table.Table, error) {
	loaded, err := client.owner.catalog.LoadTable(ctx, client.identifier(name))
	if err != nil {
		return nil, err
	}
	files := &fileIO{ctx: ctx, state: state}
	file, err := files.Open(loaded.MetadataLocation())
	if err != nil {
		return nil, err
	}
	var reader io.Reader = file
	var closeDecoder func() error
	location := loaded.MetadataLocation()
	if strings.HasSuffix(location, ".gz.metadata.json") || strings.HasSuffix(location, "metadata.json.gz") {
		decoder, err := gzip.NewReader(file)
		if err != nil {
			state.note(file.Close(), true)
			return nil, err
		}
		reader, closeDecoder = decoder, decoder.Close
	} else if strings.HasSuffix(location, ".zstd.metadata.json") || strings.HasSuffix(location, "metadata.json.zstd") {
		decoder, err := zstd.NewReader(file, zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxMemory(uint64(client.owner.settings.MaxMetadataBytes)))
		if err != nil {
			state.note(file.Close(), true)
			return nil, err
		}
		reader, closeDecoder = decoder, func() error { decoder.Close(); return nil }
	}
	raw, readErr := io.ReadAll(io.LimitReader(reader, int64(client.owner.settings.MaxMetadataBytes)+1))
	if closeDecoder != nil {
		state.note(closeDecoder(), true)
	}
	state.note(file.Close(), true)
	if readErr != nil {
		return nil, readErr
	}
	if len(raw) > client.owner.settings.MaxMetadataBytes {
		return nil, failure(ErrLimit, "metadata-file")
	}
	persisted, err := parseMetadata(client.owner.settings, raw)
	if err != nil {
		return nil, failure(ErrProtocol, "metadata-file-json", err)
	}
	// Native Metadata.Equals omits some persisted fields (for example statistics)
	// and Schema.Equals omits schema IDs. Compare normalized complete SDK metadata.
	if err = validateMetadata(client.owner.settings, loaded.Metadata()); err != nil {
		return nil, err
	}
	equal, err := equalMetadata(persisted, loaded.Metadata())
	if err != nil {
		return nil, err
	}
	if !equal {
		return nil, failure(ErrProtocol, "metadata-file-mismatch", err)
	}
	// The REST SDK only constructs a lazy native FileIO factory. Do not invoke
	// it or pass it to the transaction: it can use vended/ambient credentials.
	return table.New(client.identifier(name), loaded.Metadata(), loaded.MetadataLocation(),
		func(ctx context.Context) (nativeio.IO, error) { return &fileIO{ctx: ctx, state: state}, nil }, client.owner.catalog), nil
}
func capture(data *resultData, tbl *table.Table) error {
	raw, err := json.Marshal(tbl.Metadata())
	if err != nil {
		return err
	}
	data.metadata = raw
	data.reloaded = true
	if snapshot := tbl.CurrentSnapshot(); snapshot != nil {
		data.snapshotID = snapshot.SnapshotID
	}
	return nil
}
func (client *Client) commit(ctx context.Context, state *exchange, data *resultData, name string, base *table.Table, transaction *table.Transaction) error {
	data.staged = true
	staged, err := transaction.StagedTable()
	if err != nil {
		return err
	}
	if err = validateMetadata(client.owner.settings, staged.Metadata()); err != nil {
		return err
	}
	committed, err := transaction.Commit(ctx)
	if err != nil {
		return err
	}
	if !matchesCommit(base.Metadata(), staged.Metadata(), committed.Metadata()) {
		state.mu.Lock()
		if state.effect != NotSubmitted {
			state.effect = Unknown
		}
		state.mu.Unlock()
		return failure(ErrProtocol, "commit-intent-mismatch")
	}
	if snapshot := committed.CurrentSnapshot(); snapshot != nil {
		data.committedSnapshotID = snapshot.SnapshotID
	}
	reloaded, err := client.load(ctx, state, name)
	if err != nil {
		return err
	}
	if reloaded.Metadata().TableUUID() != committed.Metadata().TableUUID() {
		return failure(ErrProtocol, "reload-uuid")
	}
	if snapshot := committed.CurrentSnapshot(); snapshot != nil {
		if reloaded.Metadata().SnapshotByID(snapshot.SnapshotID) == nil {
			return failure(ErrProtocol, "reload-snapshot")
		}
	}
	if err := capture(data, reloaded); err != nil {
		return err
	}
	data.complete = true
	return nil
}

func matchesCommit(base, wanted, actual table.Metadata) bool {
	if actual == nil || actual.TableUUID() != wanted.TableUUID() {
		return false
	}
	wantedSnapshot, baseSnapshot := wanted.CurrentSnapshot(), base.CurrentSnapshot()
	if wantedSnapshot != nil && (baseSnapshot == nil || wantedSnapshot.SnapshotID != baseSnapshot.SnapshotID) {
		if actual.CurrentSnapshot() == nil || actual.CurrentSnapshot().SnapshotID != wantedSnapshot.SnapshotID {
			return false
		}
	}
	if wanted.CurrentSchema().ID != base.CurrentSchema().ID {
		left, _ := json.Marshal(wanted.CurrentSchema())
		right, _ := json.Marshal(actual.CurrentSchema())
		if !bytes.Equal(left, right) {
			return false
		}
	}
	if wanted.DefaultPartitionSpec() != base.DefaultPartitionSpec() {
		left, _ := json.Marshal(wanted.PartitionSpec())
		right, _ := json.Marshal(actual.PartitionSpec())
		if !bytes.Equal(left, right) {
			return false
		}
	}
	for key, value := range wanted.Properties() {
		if old, exists := base.Properties()[key]; !exists || old != value {
			if got, exists := actual.Properties()[key]; !exists || got != value {
				return false
			}
		}
	}
	for key := range base.Properties() {
		if _, kept := wanted.Properties()[key]; !kept {
			if _, exists := actual.Properties()[key]; exists {
				return false
			}
		}
	}
	return true
}

// CreateNamespace creates only the explicitly selected namespace, without
// fallback or create-if-missing retries. A lost reply remains an unknown effect.
func (client *Client) CreateNamespace(ctx context.Context, id fault.Correlation) (*invocation.Receipt[Result], error) {
	if err := client.valid(ctx, "namespace", true); err != nil {
		return nil, err
	}
	return client.start(ctx, id, "create-namespace", nil, func(ctx context.Context, _ *exchange, data *resultData) error {
		err := client.owner.catalog.CreateNamespace(ctx, table.Identifier{client.owner.settings.Namespace}, nil)
		data.complete = err == nil
		return err
	})
}

// DropNamespace requests removal of the selected namespace. It never drops tables
// or data files in order to make a nonempty namespace removable.
func (client *Client) DropNamespace(ctx context.Context, id fault.Correlation) (*invocation.Receipt[Result], error) {
	if err := client.valid(ctx, "namespace", true); err != nil {
		return nil, err
	}
	return client.start(ctx, id, "drop-namespace", nil, func(ctx context.Context, _ *exchange, data *resultData) error {
		err := client.owner.catalog.DropNamespace(ctx, table.Identifier{client.owner.settings.Namespace})
		data.complete = err == nil
		return err
	})
}

// ListTables returns bounded names in the selected namespace. Complete=false
// distinguishes a truncated list from a complete empty namespace.
func (client *Client) ListTables(ctx context.Context, id fault.Correlation) (*invocation.Receipt[Result], error) {
	if err := client.valid(ctx, "namespace", false); err != nil {
		return nil, err
	}
	return client.start(ctx, id, "list-tables", nil, func(ctx context.Context, _ *exchange, data *resultData) error {
		data.names = []string{}
		for identifier, err := range client.owner.catalog.ListTables(ctx, table.Identifier{client.owner.settings.Namespace}) {
			if err != nil {
				return err
			}
			if len(identifier) != 2 || identifier[0] != client.owner.settings.Namespace || !nameOK(identifier[1]) {
				return failure(ErrProtocol, "table-identifier")
			}
			if len(data.names) == client.owner.settings.MaxFileOps {
				return nil
			}
			data.names = append(data.names, identifier[1])
		}
		data.complete = true
		return nil
	})
}

// CreateTable freezes a flat scalar native schema before returning. Format 2,
// unpartitioned layout and the documented bounded writer properties are explicit.
// It neither imports external files nor replaces a table that already exists.
func (client *Client) CreateTable(ctx context.Context, id fault.Correlation, name string, schema *native.Schema) (*invocation.Receipt[Result], error) {
	if err := client.valid(ctx, name, true); err != nil {
		return nil, err
	}
	if err := validateSchema(schema); err != nil {
		return nil, err
	}
	var copied native.Schema
	return client.start(ctx, id, "create-table", func() error {
		raw, err := json.Marshal(schema)
		if err != nil {
			return err
		}
		if len(raw) > client.owner.settings.MaxMetadataBytes {
			return failure(ErrLimit, "schema-bytes")
		}
		return json.Unmarshal(raw, &copied)
	}, func(ctx context.Context, state *exchange, data *resultData) error {
		props := tableProperties(client.owner.settings)
		props["format-version"] = "2"
		_, err := client.owner.catalog.CreateTable(ctx, client.identifier(name), &copied,
			catalog.WithLocation(client.owner.settings.Location+name), catalog.WithProperties(props))
		if err != nil {
			return err
		}
		loaded, err := client.load(ctx, state, name)
		if err != nil {
			return err
		}
		data.complete = true
		return capture(data, loaded)
	})
}

// InspectTable reloads metadata without exposing a native Table or its FileIO.
func (client *Client) InspectTable(ctx context.Context, id fault.Correlation, name string) (*invocation.Receipt[Result], error) {
	if err := client.valid(ctx, name, false); err != nil {
		return nil, err
	}
	return client.start(ctx, id, "inspect-table", nil, func(ctx context.Context, state *exchange, data *resultData) error {
		loaded, err := client.load(ctx, state, name)
		if err != nil {
			return err
		}
		data.complete = true
		return capture(data, loaded)
	})
}

// DropTable removes the Catalog entry only. It intentionally does not purge any
// file. Retention and deletion of physical files require a separate authorization.
func (client *Client) DropTable(ctx context.Context, id fault.Correlation, name string) (*invocation.Receipt[Result], error) {
	if err := client.valid(ctx, name, true); err != nil {
		return nil, err
	}
	return client.start(ctx, id, "drop-table", nil, func(ctx context.Context, _ *exchange, data *resultData) error {
		err := client.owner.catalog.DropTable(ctx, client.identifier(name))
		data.complete = err == nil
		return err
	})
}

// SchemaChange selects optional flat-column addition, rename, or removal.
// Type is one of boolean, int, long, float, double, string, binary for additions.
// Removing or renaming a partition source must first satisfy native requirements.
type SchemaChange struct {
	private
	Operation string
	Name      string
	NewName   string
	Type      string
}

func primitive(name string) native.Type {
	switch name {
	case "boolean":
		return native.PrimitiveTypes.Bool
	case "int":
		return native.PrimitiveTypes.Int32
	case "long":
		return native.PrimitiveTypes.Int64
	case "float":
		return native.PrimitiveTypes.Float32
	case "double":
		return native.PrimitiveTypes.Float64
	case "string":
		return native.PrimitiveTypes.String
	case "binary":
		return native.PrimitiveTypes.Binary
	default:
		return nil
	}
}

// EvolveSchema copies changes and validates the staged profile before submitting
// any metadata commit. It does not rewrite existing data files.
func (client *Client) EvolveSchema(ctx context.Context, id fault.Correlation, name string, changes []SchemaChange) (*invocation.Receipt[Result], error) {
	if err := client.valid(ctx, name, true); err != nil {
		return nil, err
	}
	if len(changes) == 0 || len(changes) > 32 {
		return nil, failure(ErrInput, "schema-changes")
	}
	for _, change := range changes {
		if !nameOK(change.Name) {
			return nil, failure(ErrInput, "schema-change")
		}
		switch change.Operation {
		case "add":
			if primitive(change.Type) == nil {
				return nil, failure(ErrUnsupported, "schema-type")
			}
		case "rename":
			if !nameOK(change.NewName) {
				return nil, failure(ErrInput, "column-name")
			}
		case "drop":
		default:
			return nil, failure(ErrUnsupported, "schema-change")
		}
	}
	return client.start(ctx, id, "evolve-schema", func() error { changes = slices.Clone(changes); return nil }, func(ctx context.Context, state *exchange, data *resultData) error {
		tbl, err := client.load(ctx, state, name)
		if err != nil {
			return err
		}
		tx := tbl.NewTransaction()
		update := tx.UpdateSchema(true, false)
		for _, change := range changes {
			switch change.Operation {
			case "add":
				update.AddColumn([]string{change.Name}, primitive(change.Type), "", false, nil)
			case "rename":
				update.RenameColumn([]string{change.Name}, change.NewName)
			case "drop":
				update.DeleteColumn([]string{change.Name})
			}
		}
		if err = update.Commit(); err != nil {
			return err
		}
		return client.commit(ctx, state, data, name, tbl, tx)
	})
}

// PartitionChange adds identity or bounded bucket transforms, or removes a field.
// History is checked before using the SDK's allocation counter.
type PartitionChange struct {
	private
	Operation string
	Column    string
	Name      string
	Buckets   int
}

// EvolvePartitions changes the default partition spec for subsequent writes;
// existing files retain their historical specs and are not rewritten.
func (client *Client) EvolvePartitions(ctx context.Context, id fault.Correlation, name string, changes []PartitionChange) (*invocation.Receipt[Result], error) {
	if err := client.valid(ctx, name, true); err != nil {
		return nil, err
	}
	if len(changes) == 0 || len(changes) > 16 {
		return nil, failure(ErrInput, "partition-changes")
	}
	for _, change := range changes {
		if !nameOK(change.Name) {
			return nil, failure(ErrInput, "partition-name")
		}
		switch change.Operation {
		case "identity":
			if !nameOK(change.Column) {
				return nil, failure(ErrInput, "partition-column")
			}
		case "bucket":
			if !nameOK(change.Column) || change.Buckets < 1 || change.Buckets > 16 {
				return nil, failure(ErrInput, "partition-buckets")
			}
		case "drop":
		default:
			return nil, failure(ErrUnsupported, "partition-transform")
		}
	}
	return client.start(ctx, id, "evolve-partitions", func() error { changes = slices.Clone(changes); return nil }, func(ctx context.Context, state *exchange, data *resultData) error {
		tbl, err := client.load(ctx, state, name)
		if err != nil {
			return err
		}
		tx := tbl.NewTransaction()
		update := tx.UpdateSpec(true)
		for _, change := range changes {
			switch change.Operation {
			case "identity":
				update.AddField(change.Column, native.IdentityTransform{}, change.Name)
			case "bucket":
				update.AddField(change.Column, native.BucketTransform{NumBuckets: change.Buckets}, change.Name)
			case "drop":
				update.RemoveField(change.Name)
			}
		}
		if err = update.Commit(); err != nil {
			return err
		}
		return client.commit(ctx, state, data, name, tbl, tx)
	})
}

// RollbackSnapshot moves the main snapshot reference; it does not undo external
// effects or delete data. The target snapshot must still exist.
func (client *Client) RollbackSnapshot(ctx context.Context, id fault.Correlation, name string, snapshotID int64) (*invocation.Receipt[Result], error) {
	if err := client.valid(ctx, name, true); err != nil {
		return nil, err
	}
	if snapshotID <= 0 {
		return nil, failure(ErrInput, "snapshot")
	}
	return client.start(ctx, id, "rollback-snapshot", nil, func(ctx context.Context, state *exchange, data *resultData) error {
		tbl, err := client.load(ctx, state, name)
		if err != nil {
			return err
		}
		tx := tbl.NewTransaction()
		if err = tx.RollbackToSnapshot(snapshotID); err != nil {
			return err
		}
		return client.commit(ctx, state, data, name, tbl, tx)
	})
}
