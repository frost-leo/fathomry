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
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/url"
	"strings"
	"sync/atomic"

	"github.com/apache/iceberg-go/metrics"
	"github.com/apache/iceberg-go/table"
	"github.com/klauspost/compress/zstd"
)

// ManagedTable retains exactly the loaded metadata and originating callback
// family. Refresh returns a new handle; it cannot retarget an existing scan.
type ManagedTable struct {
	private
	scope  *ManagedSession
	table  *table.Table
	purged bool
}

func (f *ManagedSession) wrapTable(ctx context.Context, tbl *table.Table) (*ManagedTable, error) {
	if tbl == nil {
		return nil, failure(ErrProtocol, "nil-table")
	}
	if err := f.identifier(tbl.Identifier()); err != nil {
		return nil, err
	}
	if err := f.validateTable(tbl.Metadata()); err != nil {
		return nil, err
	}
	if err := f.location(tbl.MetadataLocation()); err != nil {
		return nil, err
	}
	if err := f.handle(); err != nil {
		return nil, err
	}
	if existing, ok := tbl.MetricsReporter().(*managedReporter); !ok || existing.source != f.source {
		reporter := tbl.MetricsReporter()
		if f.source.prepared.native.Reporter != nil {
			reporter = metrics.CombineWithLogger(f.source.logger, reporter, f.source.prepared.native.Reporter)
		}
		table.WithMetricsReporter(&managedReporter{source: f.source, reporter: reporter})(tbl)
	}
	f.mu.Lock()
	verified := f.verifiedTables[tbl]
	delete(f.verifiedTables, tbl)
	f.mu.Unlock()
	if !verified {
		if err := f.verifyTable(ctx, tbl); err != nil {
			return nil, err
		}
	}
	return &ManagedTable{scope: f, table: tbl}, nil
}
func (f *ManagedSession) verifyTable(ctx context.Context, tbl *table.Table) error {
	if tbl == nil {
		return failure(ErrProtocol, "nil-table")
	}
	if err := f.identifier(tbl.Identifier()); err != nil {
		return err
	}
	if err := f.validateTable(tbl.Metadata()); err != nil {
		return err
	}
	if err := f.location(tbl.MetadataLocation()); err != nil {
		return err
	}
	files, err := tbl.FS(ctx)
	if err != nil {
		return err
	}
	file, err := files.Open(tbl.MetadataLocation())
	if err != nil {
		return err
	}
	defer func() { f.noteCleanup(file.Close()) }()
	var reader io.Reader = file
	loc := tbl.MetadataLocation()
	if strings.HasSuffix(loc, ".gz.metadata.json") || strings.HasSuffix(loc, "metadata.json.gz") {
		decoder, err := gzip.NewReader(file)
		if err != nil {
			return err
		}
		defer func() { f.noteCleanup(decoder.Close()) }()
		reader = decoder
	} else if strings.HasSuffix(loc, ".zstd.metadata.json") || strings.HasSuffix(loc, "metadata.json.zstd") {
		decoder, err := zstd.NewReader(file, zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxMemory(uint64(f.source.prepared.settings.MaxMetadataBytes)))
		if err != nil {
			return err
		}
		defer decoder.Close()
		reader = decoder
	}
	raw, err := io.ReadAll(io.LimitReader(reader, f.source.prepared.settings.MaxMetadataBytes+1))
	if err != nil {
		return err
	}
	if int64(len(raw)) > f.source.prepared.settings.MaxMetadataBytes {
		return failure(ErrLimit, "metadata-object")
	}
	persisted, err := table.ParseMetadataBytes(raw)
	if err != nil {
		return err
	}
	comparison := tbl.Metadata()
	f.mu.Lock()
	subset := f.snapshotSubsets[tbl.MetadataLocation()]
	f.mu.Unlock()
	if subset {
		for _, snapshot := range comparison.Snapshots() {
			expected := persisted.SnapshotByID(snapshot.SnapshotID)
			if expected == nil || !snapshot.Equals(*expected) {
				return failure(ErrProtocol, "snapshot-subset")
			}
		}
		for _, ref := range comparison.Refs() {
			if comparison.SnapshotByID(ref.SnapshotID) == nil {
				return failure(ErrProtocol, "snapshot-ref-subset")
			}
		}
		view, err := json.Marshal(comparison)
		if err != nil {
			return err
		}
		var fields map[string]json.RawMessage
		if err = json.Unmarshal(view, &fields); err != nil {
			return err
		}
		fields["snapshots"], err = json.Marshal(persisted.Snapshots())
		if err != nil {
			return err
		}
		view, err = json.Marshal(fields)
		if err != nil {
			return err
		}
		comparison, err = table.ParseMetadataBytes(view)
		if err != nil {
			return err
		}
	}
	equal, err := equalMetadataWithin(persisted, comparison, f.source.prepared.settings.MaxListEntries)
	if err != nil {
		return err
	}
	if !equal {
		return failure(ErrProtocol, "metadata-object-mismatch")
	}
	base, _ := url.Parse(f.source.prepared.settings.CatalogURI)
	path := base.Path + "/v1"
	if prefix := f.source.prepared.settings.CatalogPrefix; prefix != "" {
		path += "/" + prefix
	}
	id := tbl.Identifier()
	path += "/namespaces/" + strings.Join(id[:len(id)-1], "\x1f") + "/tables/" + id[len(id)-1]
	f.mu.Lock()
	f.bases[path] = tbl.Metadata()
	f.mu.Unlock()
	return nil
}
func (s *managedSource) validateLoadedTable(ctx context.Context, tbl *table.Table) error {
	family, _ := ctx.Value(managedFamilyKey{}).(*ManagedSession)
	if family == nil || family.source != s {
		return failure(ErrAuthority, "table-validation-context")
	}
	if err := family.verifyTable(ctx, tbl); err != nil {
		return err
	}
	family.mu.Lock()
	family.verifiedTables[tbl] = true
	family.mu.Unlock()
	return nil
}

func (t *ManagedTable) valid() error {
	if t == nil || t.scope == nil || t.table == nil {
		return failure(ErrInput, "table")
	}
	return nil
}

// Metadata returns independent native metadata containers, with no FileIO.
func (t *ManagedTable) Metadata() (table.Metadata, error) {
	if err := t.valid(); err != nil {
		return nil, err
	}
	return managedCall(t.scope, "metadata", grantRead, func(context.Context) (table.Metadata, error) { return t.scope.copyMetadata(t.table.Metadata()) })
}

// MetadataLocation is the exact loaded metadata object, not a new head lookup.
func (t *ManagedTable) MetadataLocation() (string, error) {
	if err := t.valid(); err != nil {
		return "", err
	}
	return managedCall(t.scope, "metadata-location", grantRead, func(context.Context) (string, error) {
		return t.table.MetadataLocation(), nil
	})
}

func (t *ManagedTable) Identifier() []string {
	if t == nil || t.table == nil {
		return nil
	}
	return t.table.Identifier()
}
func (t *ManagedTable) Refresh() (*ManagedTable, error) {
	if err := t.valid(); err != nil {
		return nil, err
	}
	return managedCall(t.scope, "refresh", grantRead, func(ctx context.Context) (*ManagedTable, error) {
		fresh, err := t.scope.source.catalog.LoadTable(ctx, t.table.Identifier())
		if err != nil {
			return nil, err
		}
		if fresh.Metadata().TableUUID() != t.table.Metadata().TableUUID() {
			return nil, failure(ErrAuthority, "table-origin")
		}
		wrapped, err := t.scope.wrapTable(ctx, fresh)
		if err != nil {
			return nil, err
		}
		t.scope.mu.Lock()
		effect := &t.scope.data.effects[t.scope.current]
		effect.Reloaded = true
		effect.TableUUID = fresh.Metadata().TableUUID().String()
		effect.MetadataLocation = fresh.MetadataLocation()
		if snapshot := fresh.CurrentSnapshot(); snapshot != nil {
			effect.ReloadedSnapshot = snapshot.SnapshotID
		}
		t.scope.mu.Unlock()
		return wrapped, nil
	})
}
func (t *ManagedTable) NewTransaction(branch string) (*ManagedTransaction, error) {
	if err := t.valid(); err != nil {
		return nil, err
	}
	return managedCall(t.scope, "transaction", grantRead, func(context.Context) (*ManagedTransaction, error) {
		if branch == "" {
			branch = table.MainBranch
		}
		if err := t.scope.handle(); err != nil {
			return nil, err
		}
		tx, err := t.table.NewTransactionOnBranchWithError(branch)
		if err != nil {
			return nil, err
		}
		return &ManagedTransaction{scope: t.scope, table: t.table, transaction: tx, branch: branch}, nil
	})
}

// ManagedTransaction is serial mutable staging, consumed at commit submission.
// A failed/unknown commit never authorizes resending this handle.
type ManagedTransaction struct {
	branch string
	private
	scope           *ManagedSession
	table           *table.Table
	transaction     *table.Transaction
	consumed, write atomic.Bool
}

func (t *ManagedTransaction) valid() error {
	if t == nil || t.scope == nil || t.transaction == nil || t.consumed.Load() {
		return failure(ErrAuthority, "transaction-state")
	}
	return nil
}
func (t *ManagedTransaction) StagedMetadata() (table.Metadata, error) {
	if err := t.valid(); err != nil {
		return nil, err
	}
	return managedCall(t.scope, "staged-metadata", grantRead, func(context.Context) (table.Metadata, error) {
		staged, err := t.transaction.StagedTable()
		if err != nil {
			return nil, err
		}
		return t.scope.copyMetadata(staged.Metadata())
	})
}
func (t *ManagedTransaction) TableCommit() (table.TableCommit, error) {
	if err := t.valid(); err != nil {
		return table.TableCommit{}, err
	}
	return managedCall(t.scope, "table-commit", grantRead, func(context.Context) (table.TableCommit, error) {
		commit, err := t.transaction.TableCommit()
		if err != nil {
			return table.TableCommit{}, err
		}
		return cloneCommit(commit, t.scope.source.prepared.settings.MaxMetadataBytes)
	})
}
func cloneCommit(value table.TableCommit, maximum int64) (table.TableCommit, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return table.TableCommit{}, err
	}
	if int64(len(raw)) > maximum {
		return table.TableCommit{}, failure(ErrLimit, "commit")
	}
	var copy struct {
		Identifier   []string
		Requirements table.Requirements
		Updates      table.Updates
	}
	if err = json.Unmarshal(raw, &copy); err != nil {
		return table.TableCommit{}, err
	}
	return table.TableCommit{Identifier: copy.Identifier, Requirements: copy.Requirements, Updates: copy.Updates}, nil
}
func (t *ManagedTransaction) Commit() (*ManagedTable, error) {
	if err := t.valid(); err != nil {
		return nil, err
	}
	grant := grantMetadata
	if t.write.Load() {
		grant = grantWrite
	}
	return managedCall(t.scope, "commit", grant, func(ctx context.Context) (*ManagedTable, error) {
		if err := t.valid(); err != nil {
			return nil, err
		}
		staged, err := t.transaction.StagedTable()
		if err != nil {
			return nil, err
		}
		if err = t.scope.validateTable(staged.Metadata()); err != nil {
			return nil, err
		}
		t.consumed.Store(true)
		t.scope.mu.Lock()
		fact := &t.scope.data.effects[t.scope.current]
		fact.Staged = true
		fact.TableUUID = t.table.Metadata().TableUUID().String()
		fact.Ref = t.branch
		if snap := transactionSnapshot(t.table, t.branch); snap != nil {
			fact.BaseSnapshot = snap.SnapshotID
		}
		if snap := transactionSnapshot(staged.Table, t.branch); snap != nil {
			fact.IntendedSnapshot = snap.SnapshotID
		}
		t.scope.mu.Unlock()
		committed, err := t.transaction.Commit(ctx)
		if err != nil {
			return nil, err
		}
		if committed == nil || committed.Metadata().TableUUID() != t.table.Metadata().TableUUID() {
			return nil, failure(ErrProtocol, "commit-identity")
		}
		t.scope.mu.Lock()
		fact = &t.scope.data.effects[t.scope.current]
		if fact.Submitted && fact.Effect == Acknowledged {
			fact.MetadataLocation = committed.MetadataLocation()
			if snap := transactionSnapshot(committed, t.branch); snap != nil && fact.IntendedSnapshot != fact.BaseSnapshot {
				fact.CommittedSnapshot = snap.SnapshotID
			}
		}
		t.scope.mu.Unlock()
		result, err := t.scope.wrapTable(ctx, committed)
		return result, errors.Join(err, ctx.Err())
	})
}

func transactionSnapshot(tbl *table.Table, branch string) *table.Snapshot {
	if branch == "" || branch == table.MainBranch {
		return tbl.CurrentSnapshot()
	}
	return tbl.SnapshotByName(branch)
}
