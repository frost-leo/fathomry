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
	"iter"
	"maps"
	"slices"
	"strconv"
	"strings"

	native "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/udf"
	"github.com/apache/iceberg-go/view"
	"github.com/google/uuid"
)

func (f *ManagedSession) createLocation(location string) (string, error) {
	if location == "" {
		id, err := uuid.NewRandom()
		if err != nil {
			return "", err
		}
		location = f.source.prepared.settings.Location + id.String()
	}
	if err := f.location(strings.TrimSuffix(location, "/") + "/"); err != nil {
		return "", err
	}
	return strings.TrimSuffix(location, "/"), nil
}

func (f *ManagedSession) LoadTable(id table.Identifier) (*ManagedTable, error) {
	return managedCall(f, "load-table", grantRead, func(ctx context.Context) (*ManagedTable, error) {
		if err := f.identifier(id); err != nil {
			return nil, err
		}
		tbl, err := f.source.catalog.LoadTable(ctx, slices.Clone(id))
		if err != nil {
			return nil, err
		}
		return f.wrapTable(ctx, tbl)
	})
}
func (f *ManagedSession) CreateTable(id table.Identifier, schema *native.Schema, opts ...catalog.CreateTableOpt) (*ManagedTable, error) {
	return managedCall(f, "create-table", grantMetadata, func(ctx context.Context) (*ManagedTable, error) {
		if err := f.identifier(id); err != nil {
			return nil, err
		}
		if schema == nil {
			return nil, failure(ErrInput, "schema")
		}
		cfg := catalog.NewCreateTableCfg()
		for _, opt := range opts {
			if opt == nil {
				return nil, failure(ErrInput, "option")
			}
			opt(&cfg)
		}
		if cfg.Location != "" {
			if err := f.location(strings.TrimSuffix(cfg.Location, "/") + "/"); err != nil {
				return nil, err
			}
		}
		var frozen struct {
			Location      string
			Properties    native.Properties
			PartitionSpec *native.PartitionSpec
			SortOrder     table.SortOrder
			StagedUpdates table.Updates
		}
		if err := f.cloneData(cfg, &frozen); err != nil {
			return nil, err
		}
		var copied native.Schema
		if err := f.cloneData(schema, &copied); err != nil {
			return nil, err
		}
		if err := f.validateUpdates(frozen.StagedUpdates); err != nil {
			return nil, err
		}
		spec := frozen.PartitionSpec
		if spec == nil {
			spec = native.UnpartitionedSpec
		}
		location, err := f.createLocation(frozen.Location)
		if err != nil {
			return nil, err
		}
		frozen.Location = location
		prospective, err := table.NewMetadata(&copied, spec, frozen.SortOrder, location, frozen.Properties)
		if err != nil {
			return nil, err
		}
		if err = f.validateTable(prospective); err != nil {
			return nil, err
		}
		expected := prospective
		if len(frozen.StagedUpdates) != 0 {
			builder, err := table.MetadataBuilderFromBase(prospective, "")
			if err != nil {
				return nil, err
			}
			for _, update := range frozen.StagedUpdates {
				if err = update.Apply(builder); err != nil {
					return nil, err
				}
			}
			staged, err := builder.Build()
			if err != nil {
				return nil, err
			}
			if err = f.validateTable(staged); err != nil {
				return nil, err
			}
			expected = staged
		}
		if frozen.Properties == nil {
			frozen.Properties = native.Properties{}
		}
		frozen.Properties[table.PropertyFormatVersion] = strconv.Itoa(prospective.Version())
		actual := []catalog.CreateTableOpt{catalog.WithLocation(frozen.Location), catalog.WithProperties(frozen.Properties), catalog.WithSortOrder(frozen.SortOrder)}
		if frozen.PartitionSpec != nil {
			actual = append(actual, catalog.WithPartitionSpec(frozen.PartitionSpec))
		}
		if len(frozen.StagedUpdates) > 0 {
			actual = append(actual, catalog.WithStagedUpdates(frozen.StagedUpdates...))
		}
		f.creationIntent = expected
		defer func() { f.creationIntent = nil }()
		tbl, err := f.source.catalog.CreateTable(ctx, slices.Clone(id), &copied, actual...)
		if err != nil {
			return nil, err
		}
		matches, err := managedCreationMatches(expected, tbl.Metadata())
		if err != nil || !matches {
			f.mu.Lock()
			f.data.effects[f.current].Effect = Unknown
			f.mu.Unlock()
			return nil, failure(ErrProtocol, "create-intent", err)
		}
		return f.wrapTable(ctx, tbl)
	})
}

// A REST catalog may assign fresh IDs using a different traversal from Go.
// Compare schema/spec/sort bindings under the same native normalization without
// rewriting any returned IDs or weakening subsequent commit identity checks.
func managedCreationMatches(wanted, actual table.Metadata) (bool, error) {
	if wanted.Version() != actual.Version() || strings.TrimSuffix(wanted.Location(), "/") != strings.TrimSuffix(actual.Location(), "/") {
		return false, nil
	}
	normalize := func(metadata table.Metadata) (table.Metadata, error) {
		spec := metadata.PartitionSpec()
		properties := maps.Clone(metadata.Properties())
		if properties == nil {
			properties = native.Properties{}
		}
		properties["format-version"] = strconv.Itoa(metadata.Version())
		return table.NewMetadata(metadata.CurrentSchema(), &spec, metadata.SortOrder(), metadata.Location(), properties)
	}
	left, err := normalize(wanted)
	if err != nil {
		return false, err
	}
	right, err := normalize(actual)
	if err != nil {
		return false, err
	}
	if !left.CurrentSchema().Equals(right.CurrentSchema()) || !left.PartitionSpec().Equals(right.PartitionSpec()) || !left.SortOrder().Equals(right.SortOrder()) {
		return false, nil
	}
	for key, value := range wanted.Properties() {
		if observed, found := actual.Properties()[key]; !found || observed != value {
			return false, nil
		}
	}
	return true, nil
}

func (f *ManagedSession) RegisterTable(id table.Identifier, location string) (*ManagedTable, error) {
	return managedCall(f, "register-table", grantImport, func(ctx context.Context) (*ManagedTable, error) {
		if err := f.identifier(id); err != nil {
			return nil, err
		}
		if err := f.location(location); err != nil {
			return nil, err
		}
		tbl, err := f.source.catalog.RegisterTable(ctx, slices.Clone(id), location)
		if err != nil {
			return nil, err
		}
		return f.wrapTable(ctx, tbl)
	})
}
func (f *ManagedSession) RenameTable(from, to table.Identifier) (*ManagedTable, error) {
	return managedCall(f, "rename-table", grantMetadata, func(ctx context.Context) (*ManagedTable, error) {
		if err := f.identifier(from); err != nil {
			return nil, err
		}
		if err := f.identifier(to); err != nil {
			return nil, err
		}
		tbl, err := f.source.catalog.RenameTable(ctx, slices.Clone(from), slices.Clone(to))
		if err != nil {
			return nil, err
		}
		return f.wrapTable(ctx, tbl)
	})
}
func (f *ManagedSession) UpdateTable(id table.Identifier, requirements []table.Requirement, updates []table.Update) (*ManagedTable, error) {
	return managedCall(f, "update-table", grantMetadata, func(ctx context.Context) (*ManagedTable, error) {
		commit, _, err := f.prepareCommit(ctx, table.TableCommit{Identifier: id, Requirements: requirements, Updates: updates})
		if err != nil {
			return nil, err
		}
		tbl, err := f.source.catalog.UpdateTable(ctx, commit.Identifier, commit.Requirements, commit.Updates)
		if err != nil {
			return nil, err
		}
		return f.wrapTable(ctx, tbl)
	})
}
func (f *ManagedSession) CommitTable(id table.Identifier, requirements []table.Requirement, updates []table.Update) (table.Metadata, string, error) {
	type output struct {
		metadata table.Metadata
		location string
	}
	out, err := managedCall(f, "commit-table", grantMetadata, func(ctx context.Context) (output, error) {
		commit, _, err := f.prepareCommit(ctx, table.TableCommit{Identifier: id, Requirements: requirements, Updates: updates})
		if err != nil {
			return output{}, err
		}
		meta, loc, err := f.source.catalog.CommitTable(ctx, commit.Identifier, commit.Requirements, commit.Updates)
		if err != nil {
			return output{}, err
		}
		copied, err := f.copyMetadata(meta)
		return output{copied, loc}, err
	})
	return out.metadata, out.location, err
}

func (f *ManagedSession) prepareCommit(ctx context.Context, commit table.TableCommit) (table.TableCommit, ManagedTableEffect, error) {
	if err := f.identifier(commit.Identifier); err != nil {
		return table.TableCommit{}, ManagedTableEffect{}, err
	}
	if len(commit.Updates) > f.source.prepared.settings.MaxListEntries || len(commit.Requirements) > f.source.prepared.settings.MaxListEntries {
		return table.TableCommit{}, ManagedTableEffect{}, failure(ErrLimit, "commit")
	}
	frozen, err := cloneCommit(commit, f.source.prepared.settings.MaxMetadataBytes)
	if err != nil {
		return table.TableCommit{}, ManagedTableEffect{}, err
	}
	if err = f.validateUpdates(frozen.Updates); err != nil {
		return table.TableCommit{}, ManagedTableEffect{}, err
	}
	loaded, err := f.source.catalog.LoadTable(ctx, frozen.Identifier)
	if err != nil {
		return table.TableCommit{}, ManagedTableEffect{}, err
	}
	if _, err = f.wrapTable(ctx, loaded); err != nil {
		return table.TableCommit{}, ManagedTableEffect{}, err
	}
	for _, req := range frozen.Requirements {
		if err = req.Validate(loaded.Metadata()); err != nil {
			return table.TableCommit{}, ManagedTableEffect{}, err
		}
	}
	builder, err := table.MetadataBuilderFromBase(loaded.Metadata(), loaded.MetadataLocation())
	if err != nil {
		return table.TableCommit{}, ManagedTableEffect{}, err
	}
	for _, update := range frozen.Updates {
		if err = update.Apply(builder); err != nil {
			return table.TableCommit{}, ManagedTableEffect{}, err
		}
	}
	prospective, err := builder.Build()
	if err != nil {
		return table.TableCommit{}, ManagedTableEffect{}, err
	}
	if err = f.validateTable(prospective); err != nil {
		return table.TableCommit{}, ManagedTableEffect{}, err
	}
	frozen.Requirements = append(frozen.Requirements, table.AssertTableUUID(loaded.Metadata().TableUUID()))
	fact := ManagedTableEffect{Identifier: slices.Clone(frozen.Identifier), TableUUID: loaded.Metadata().TableUUID().String(), Ref: table.MainBranch, References: referenceChanges(loaded.Metadata(), prospective)}
	if snapshot := loaded.Metadata().CurrentSnapshot(); snapshot != nil {
		fact.BaseSnapshot = snapshot.SnapshotID
	}
	if snapshot := prospective.CurrentSnapshot(); snapshot != nil {
		fact.IntendedSnapshot = snapshot.SnapshotID
	}
	return frozen, fact, nil
}
func (f *ManagedSession) validateUpdates(updates []table.Update) error {
	for _, update := range updates {
		if update == nil {
			return failure(ErrInput, "update")
		}
		switch update.Action() {
		case table.UpdateAddSnapshot, table.UpdateSetStatistics, table.UpdateSetPartitionStatistics:
			if !f.allowed(grantImport) {
				return failure(ErrAuthority, "adopt-file")
			}
		case table.UpdateRemoveSnapshots:
			if !f.allowed(grantExpire) {
				return failure(ErrAuthority, "expire")
			}
		}
		raw, err := json.Marshal(update)
		if err != nil {
			return err
		}
		var object map[string]json.RawMessage
		if err = json.Unmarshal(raw, &object); err != nil {
			return err
		}
		if data := object["location"]; len(data) > 0 {
			var location string
			if err = json.Unmarshal(data, &location); err != nil {
				return err
			}
			if err = f.location(strings.TrimSuffix(location, "/") + "/"); err != nil {
				return err
			}
		}
	}
	return nil
}
func (f *ManagedSession) cloneData(from, to any) error {
	raw, err := json.Marshal(from)
	if err != nil {
		return err
	}
	if int64(len(raw)) > f.source.prepared.settings.MaxMetadataBytes {
		return failure(ErrLimit, "input-bytes")
	}
	return json.Unmarshal(raw, to)
}
func (f *ManagedSession) CreateNamespace(id table.Identifier, props native.Properties) error {
	return managedDo(f, "create-namespace", grantNamespace, func(ctx context.Context) error {
		if err := f.namespace(id); err != nil {
			return err
		}
		frozen, err := f.boundedProperties(props)
		if err != nil {
			return err
		}
		if location, present := frozen["location"]; present {
			if err = f.location(strings.TrimSuffix(location, "/") + "/"); err != nil {
				return err
			}
		}
		return f.source.catalog.CreateNamespace(ctx, slices.Clone(id), frozen)
	})
}
func (f *ManagedSession) LoadNamespaceProperties(id table.Identifier) (native.Properties, error) {
	return managedCall(f, "namespace-properties", grantRead, func(ctx context.Context) (native.Properties, error) {
		if err := f.namespace(id); err != nil {
			return nil, err
		}
		props, err := f.source.catalog.LoadNamespaceProperties(ctx, slices.Clone(id))
		if err != nil {
			return nil, err
		}
		return f.boundedProperties(props)
	})
}
func (f *ManagedSession) UpdateNamespaceProperties(id table.Identifier, removals []string, updates native.Properties) (catalog.PropertiesUpdateSummary, error) {
	return managedCall(f, "update-namespace", grantNamespace, func(ctx context.Context) (catalog.PropertiesUpdateSummary, error) {
		if err := f.namespace(id); err != nil {
			return catalog.PropertiesUpdateSummary{}, err
		}
		props, err := f.boundedProperties(updates)
		if err != nil {
			return catalog.PropertiesUpdateSummary{}, err
		}
		if location, present := props["location"]; present {
			if err = f.location(strings.TrimSuffix(location, "/") + "/"); err != nil {
				return catalog.PropertiesUpdateSummary{}, err
			}
		}
		if len(removals) > f.source.prepared.settings.MaxListEntries {
			return catalog.PropertiesUpdateSummary{}, failure(ErrLimit, "removals")
		}
		return f.source.catalog.UpdateNamespaceProperties(ctx, slices.Clone(id), slices.Clone(removals), props)
	})
}
func (f *ManagedSession) ListNamespaces(parent table.Identifier) ([]table.Identifier, error) {
	return managedCall(f, "list-namespaces", grantRead, func(ctx context.Context) ([]table.Identifier, error) {
		if err := f.namespace(parent); err != nil {
			return nil, err
		}
		values, err := f.source.catalog.ListNamespaces(ctx, slices.Clone(parent))
		if err != nil {
			return nil, err
		}
		if len(values) > f.source.prepared.settings.MaxListEntries {
			return nil, failure(ErrLimit, "namespaces")
		}
		for _, id := range values {
			if err = f.namespace(id); err != nil {
				return nil, err
			}
		}
		return values, nil
	})
}
func (f *ManagedSession) list(ctx context.Context, sequence iter.Seq2[table.Identifier, error], visit func(table.Identifier) (bool, error)) ([]table.Identifier, error) {
	var result []table.Identifier
	count := 0
	for id, err := range sequence {
		if err != nil {
			return result, err
		}
		if err = f.identifier(id); err != nil {
			return result, err
		}
		if count >= f.source.prepared.settings.MaxListEntries {
			return result, failure(ErrLimit, "catalog-list")
		}
		count++
		if visit != nil {
			next, err := visit(slices.Clone(id))
			if err != nil {
				return result, err
			}
			if !next {
				return result, nil
			}
		} else {
			result = append(result, slices.Clone(id))
		}
	}
	return result, errors.Join(ctx.Err())
}
func (f *ManagedSession) DropTable(id table.Identifier) error {
	return managedDo(f, "droptable", grantMetadata, func(ctx context.Context) error {
		if err := f.identifier(id); err != nil {
			return err
		}
		return f.source.catalog.DropTable(ctx, slices.Clone(id))
	})
}
func (f *ManagedSession) PurgeTable(id table.Identifier, quiescent bool) error {
	return managedDo(f, "purgetable", grantPurge, func(ctx context.Context) error {
		if !quiescent {
			return failure(ErrAuthority, "purge-quiescence")
		}
		if err := f.identifier(id); err != nil {
			return err
		}
		tbl, err := f.source.catalog.LoadTable(ctx, slices.Clone(id))
		if err != nil {
			return err
		}
		if err = f.inspectPurgeTargets(ctx, tbl); err != nil {
			return err
		}
		return f.source.catalog.PurgeTable(ctx, slices.Clone(id))
	})
}
func (f *ManagedSession) DropNamespace(id table.Identifier) error {
	return managedDo(f, "dropnamespace", grantNamespace, func(ctx context.Context) error {
		if err := f.namespace(id); err != nil {
			return err
		}
		return f.source.catalog.DropNamespace(ctx, slices.Clone(id))
	})
}
func (f *ManagedSession) DropView(id table.Identifier) error {
	return managedDo(f, "dropview", grantMetadata, func(ctx context.Context) error {
		if err := f.identifier(id); err != nil {
			return err
		}
		return f.source.catalog.DropView(ctx, slices.Clone(id))
	})
}
func (f *ManagedSession) CheckNamespaceExists(id table.Identifier) (bool, error) {
	return managedCall(f, "checknamespaceexists", grantRead, func(ctx context.Context) (bool, error) {
		if err := f.namespace(id); err != nil {
			return false, err
		}
		return f.source.catalog.CheckNamespaceExists(ctx, slices.Clone(id))
	})
}
func (f *ManagedSession) CheckTableExists(id table.Identifier) (bool, error) {
	return managedCall(f, "checktableexists", grantRead, func(ctx context.Context) (bool, error) {
		if err := f.identifier(id); err != nil {
			return false, err
		}
		return f.source.catalog.CheckTableExists(ctx, slices.Clone(id))
	})
}
func (f *ManagedSession) CheckViewExists(id table.Identifier) (bool, error) {
	return managedCall(f, "checkviewexists", grantRead, func(ctx context.Context) (bool, error) {
		if err := f.identifier(id); err != nil {
			return false, err
		}
		return f.source.catalog.CheckViewExists(ctx, slices.Clone(id))
	})
}
func (f *ManagedSession) CheckFunctionExists(id table.Identifier) (bool, error) {
	return managedCall(f, "checkfunctionexists", grantRead, func(ctx context.Context) (bool, error) {
		if err := f.identifier(id); err != nil {
			return false, err
		}
		return f.source.catalog.CheckFunctionExists(ctx, slices.Clone(id))
	})
}
func (f *ManagedSession) ListTables(namespace table.Identifier) ([]table.Identifier, error) {
	return managedCall(f, "list-tables", grantRead, func(ctx context.Context) ([]table.Identifier, error) {
		if err := f.namespace(namespace); err != nil {
			return nil, err
		}
		return f.list(ctx, f.source.catalog.ListTables(ctx, slices.Clone(namespace)), nil)
	})
}
func (f *ManagedSession) VisitTables(namespace table.Identifier, pageSize int, visit func(table.Identifier) (bool, error)) error {
	return managedDo(f, "visit-tables", grantRead, func(ctx context.Context) error {
		if err := f.namespace(namespace); err != nil {
			return err
		}
		if visit == nil || pageSize < 1 || pageSize > f.source.prepared.settings.MaxListEntries {
			return failure(ErrInput, "catalog-visitor")
		}
		ctx = f.source.catalog.SetPageSize(ctx, pageSize)
		_, err := f.list(ctx, f.source.catalog.ListTables(ctx, slices.Clone(namespace)), visit)
		return err
	})
}
func (f *ManagedSession) ListViews(namespace table.Identifier) ([]table.Identifier, error) {
	return managedCall(f, "list-views", grantRead, func(ctx context.Context) ([]table.Identifier, error) {
		if err := f.namespace(namespace); err != nil {
			return nil, err
		}
		return f.list(ctx, f.source.catalog.ListViews(ctx, slices.Clone(namespace)), nil)
	})
}
func (f *ManagedSession) VisitViews(namespace table.Identifier, pageSize int, visit func(table.Identifier) (bool, error)) error {
	return managedDo(f, "visit-views", grantRead, func(ctx context.Context) error {
		if err := f.namespace(namespace); err != nil {
			return err
		}
		if visit == nil || pageSize < 1 || pageSize > f.source.prepared.settings.MaxListEntries {
			return failure(ErrInput, "catalog-visitor")
		}
		ctx = f.source.catalog.SetPageSize(ctx, pageSize)
		_, err := f.list(ctx, f.source.catalog.ListViews(ctx, slices.Clone(namespace)), visit)
		return err
	})
}
func (f *ManagedSession) ListFunctions(namespace table.Identifier) ([]table.Identifier, error) {
	return managedCall(f, "list-functions", grantRead, func(ctx context.Context) ([]table.Identifier, error) {
		if err := f.namespace(namespace); err != nil {
			return nil, err
		}
		return f.list(ctx, f.source.catalog.ListFunctions(ctx, slices.Clone(namespace)), nil)
	})
}
func (f *ManagedSession) VisitFunctions(namespace table.Identifier, pageSize int, visit func(table.Identifier) (bool, error)) error {
	return managedDo(f, "visit-functions", grantRead, func(ctx context.Context) error {
		if err := f.namespace(namespace); err != nil {
			return err
		}
		if visit == nil || pageSize < 1 || pageSize > f.source.prepared.settings.MaxListEntries {
			return failure(ErrInput, "catalog-visitor")
		}
		ctx = f.source.catalog.SetPageSize(ctx, pageSize)
		_, err := f.list(ctx, f.source.catalog.ListFunctions(ctx, slices.Clone(namespace)), visit)
		return err
	})
}

func (f *ManagedSession) copyView(v *view.View) (*view.View, error) {
	if v == nil {
		return nil, failure(ErrProtocol, "view")
	}
	if err := f.location(v.MetadataLocation()); err != nil {
		return nil, err
	}
	if err := f.validateView(v.Metadata()); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(v.Metadata())
	if err != nil {
		return nil, err
	}
	if int64(len(raw)) > f.source.prepared.settings.MaxMetadataBytes {
		return nil, failure(ErrLimit, "view-metadata")
	}
	meta, err := view.ParseMetadataBytes(raw)
	if err != nil {
		return nil, err
	}
	return view.New(v.Identifier(), meta, v.MetadataLocation()), nil
}
func (f *ManagedSession) LoadView(id table.Identifier) (*view.View, error) {
	return managedCall(f, "load-view", grantRead, func(ctx context.Context) (*view.View, error) {
		if err := f.identifier(id); err != nil {
			return nil, err
		}
		v, err := f.source.catalog.LoadView(ctx, slices.Clone(id))
		if err != nil {
			return nil, err
		}
		return f.copyView(v)
	})
}
func (f *ManagedSession) CreateView(id table.Identifier, version *view.Version, schema *native.Schema, opts ...catalog.CreateViewOpt) (*view.View, error) {
	return managedCall(f, "create-view", grantMetadata, func(ctx context.Context) (*view.View, error) {
		if err := f.identifier(id); err != nil {
			return nil, err
		}
		if version == nil || schema == nil {
			return nil, failure(ErrInput, "view")
		}
		var frozenVersion view.Version
		var frozenSchema native.Schema
		if err := f.cloneData(version, &frozenVersion); err != nil {
			return nil, err
		}
		if err := f.cloneData(schema, &frozenSchema); err != nil {
			return nil, err
		}
		cfg := catalog.NewCreateViewCfg()
		for _, opt := range opts {
			if opt == nil {
				return nil, failure(ErrInput, "option")
			}
			opt(&cfg)
		}
		if cfg.Location != "" {
			if err := f.location(strings.TrimSuffix(cfg.Location, "/") + "/"); err != nil {
				return nil, err
			}
		}
		props, err := f.boundedProperties(cfg.Properties)
		if err != nil {
			return nil, err
		}
		fresh, err := native.AssignFreshSchemaIDs(&frozenSchema, nil)
		if err != nil {
			return nil, err
		}
		frozenVersion.VersionID = 1
		if frozenVersion.DefaultCatalog == "" {
			frozenVersion.DefaultCatalog = f.source.prepared.settings.Name
		}
		location, err := f.createLocation(cfg.Location)
		if err != nil {
			return nil, err
		}
		cfg.Location = location
		builder, err := view.NewMetadataBuilder()
		if err != nil {
			return nil, err
		}
		prospective, err := builder.SetLoc(location).SetCurrentVersion(&frozenVersion, fresh).SetProperties(props).Build()
		if err != nil {
			return nil, err
		}
		if err = f.validateView(prospective.Metadata); err != nil {
			return nil, err
		}
		v, err := f.source.catalog.CreateView(ctx, slices.Clone(id), &frozenVersion, &frozenSchema, catalog.WithViewLocation(cfg.Location), catalog.WithViewProperties(props))
		if err != nil {
			return nil, err
		}
		return f.copyView(v)
	})
}
func (f *ManagedSession) RegisterView(id table.Identifier, location string) (*view.View, error) {
	return managedCall(f, "register-view", grantImport, func(ctx context.Context) (*view.View, error) {
		if err := f.identifier(id); err != nil {
			return nil, err
		}
		if err := f.location(location); err != nil {
			return nil, err
		}
		v, err := f.source.catalog.RegisterView(ctx, slices.Clone(id), location)
		if err != nil {
			return nil, err
		}
		return f.copyView(v)
	})
}
func (f *ManagedSession) RenameView(from, to table.Identifier) (*view.View, error) {
	return managedCall(f, "rename-view", grantMetadata, func(ctx context.Context) (*view.View, error) {
		if err := f.identifier(from); err != nil {
			return nil, err
		}
		if err := f.identifier(to); err != nil {
			return nil, err
		}
		v, err := f.source.catalog.RenameView(ctx, slices.Clone(from), slices.Clone(to))
		if err != nil {
			return nil, err
		}
		return f.copyView(v)
	})
}
func (f *ManagedSession) UpdateView(id table.Identifier, requirements []view.Requirement, updates []view.Update) (*view.View, error) {
	return managedCall(f, "update-view", grantMetadata, func(ctx context.Context) (*view.View, error) {
		if err := f.identifier(id); err != nil {
			return nil, err
		}
		if len(requirements) > f.source.prepared.settings.MaxListEntries || len(updates) > f.source.prepared.settings.MaxListEntries {
			return nil, failure(ErrLimit, "view-updates")
		}
		var reqs view.Requirements
		var changes view.Updates
		if err := f.cloneData(requirements, &reqs); err != nil {
			return nil, err
		}
		if err := f.cloneData(updates, &changes); err != nil {
			return nil, err
		}
		base, err := f.source.catalog.LoadView(ctx, slices.Clone(id))
		if err != nil {
			return nil, err
		}
		if _, err = f.copyView(base); err != nil {
			return nil, err
		}
		for _, requirement := range reqs {
			if err = requirement.Validate(base.Metadata()); err != nil {
				return nil, err
			}
		}
		prospective, err := updatedView(base.Metadata(), changes)
		if err != nil {
			return nil, err
		}
		if err = f.validateView(prospective); err != nil {
			return nil, err
		}
		reqs = append(reqs, view.AssertViewUUID(base.Metadata().ViewUUID()))
		v, err := f.source.catalog.UpdateView(ctx, slices.Clone(id), reqs, changes)
		if err != nil {
			return nil, err
		}
		return f.copyView(v)
	})
}
func (f *ManagedSession) LoadFunction(id table.Identifier) (*udf.UDF, error) {
	return managedCall(f, "load-function", grantRead, func(ctx context.Context) (*udf.UDF, error) {
		if err := f.identifier(id); err != nil {
			return nil, err
		}
		v, err := f.source.catalog.LoadFunction(ctx, slices.Clone(id))
		if err != nil {
			return nil, err
		}
		if v.MetadataLocation() != "" {
			if err = f.location(v.MetadataLocation()); err != nil {
				return nil, err
			}
		}
		raw, err := json.Marshal(v.Metadata())
		if err != nil {
			return nil, err
		}
		if int64(len(raw)) > f.source.prepared.settings.MaxMetadataBytes {
			return nil, failure(ErrLimit, "function-metadata")
		}
		meta, err := udf.ParseMetadataBytes(raw)
		if err != nil {
			return nil, err
		}
		return udf.New(v.Identifier(), meta, v.MetadataLocation()), nil
	})
}
func (f *ManagedSession) RemotePlanningCapabilities() (bool, bool, error) {
	type output struct{ plan, full bool }
	out, err := managedCall(f, "planning-capabilities", grantRead, func(context.Context) (output, error) {
		return output{f.source.catalog.SupportsPlanTableScan(), f.source.catalog.SupportsFullRemoteScanPlanning()}, nil
	})
	return out.plan, out.full, err
}
func (f *ManagedSession) PlanTableScan(id table.Identifier, request rest.PlanTableScanRequest) (rest.PlanTableScanResponse, error) {
	return managedCall(f, "plantablescan", grantRead, func(ctx context.Context) (rest.PlanTableScanResponse, error) {
		if err := f.identifier(id); err != nil {
			return rest.PlanTableScanResponse{}, err
		}
		var frozen rest.PlanTableScanRequest
		if err := f.cloneData(request, &frozen); err != nil {
			return rest.PlanTableScanResponse{}, err
		}
		frozen.IdempotencyKey = copyManagedScalar(request.IdempotencyKey)
		frozen.AccessDelegation = copyManagedScalar(request.AccessDelegation)
		return f.source.catalog.PlanTableScan(ctx, slices.Clone(id), frozen)
	})
}
func (f *ManagedSession) FetchPlanningResult(id table.Identifier, planID string, options rest.FetchPlanningResultOptions) (rest.FetchPlanningResultResponse, error) {
	return managedCall(f, "fetchplanningresult", grantRead, func(ctx context.Context) (rest.FetchPlanningResultResponse, error) {
		plan, err := f.remotePlan(id, planID, false)
		if err != nil {
			return rest.FetchPlanningResultResponse{}, err
		}
		options.AccessDelegation = copyManagedScalar(options.AccessDelegation)
		return f.source.catalog.FetchPlanningResult(context.WithValue(ctx, managedRemotePlanKey{}, plan), slices.Clone(id), planID, options)
	})
}
func (f *ManagedSession) WaitForPlan(id table.Identifier, planID string, options rest.WaitForPlanOptions) (rest.CompletedPlanningResult, error) {
	return managedCall(f, "waitforplan", grantRead, func(ctx context.Context) (rest.CompletedPlanningResult, error) {
		plan, err := f.remotePlan(id, planID, false)
		if err != nil {
			return rest.CompletedPlanningResult{}, err
		}
		options.AccessDelegation = copyManagedScalar(options.AccessDelegation)
		return f.source.catalog.WaitForPlan(context.WithValue(ctx, managedRemotePlanKey{}, plan), slices.Clone(id), planID, options)
	})
}
func (f *ManagedSession) FetchScanTasks(id table.Identifier, request rest.FetchScanTasksRequest) (rest.FetchScanTasksResponse, error) {
	return managedCall(f, "fetchscantasks", grantRead, func(ctx context.Context) (rest.FetchScanTasksResponse, error) {
		plan, err := f.taskPlan(id, request.PlanTask)
		if err != nil {
			return rest.FetchScanTasksResponse{}, err
		}
		request.IdempotencyKey = copyManagedScalar(request.IdempotencyKey)
		return f.source.catalog.FetchScanTasks(context.WithValue(ctx, managedRemotePlanKey{}, plan), slices.Clone(id), request)
	})
}
func (f *ManagedSession) CancelPlanning(id table.Identifier, planID string) error {
	if f == nil || f.source == nil {
		return failure(ErrInput, "session")
	}
	if !f.gate.TryLock() {
		return failure(ErrAuthority, "session-busy")
	}
	defer f.gate.Unlock()
	plan, err := f.remotePlan(id, planID, true)
	if err != nil {
		return err
	}
	err = plan.close()
	f.noteCleanup(err)
	return failureIf(err)
}
