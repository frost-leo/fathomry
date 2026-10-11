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
	"slices"
	"sync"
	"time"

	native "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/table/compaction"
)

// ExpiryOptions distinguishes omitted native retention defaults from explicit
// zero age. File deletion is opt-in and requires independent delete permission.
type ExpiryOptions struct {
	RetainLast  *int
	OlderThan   *time.Duration
	DeleteFiles bool
}

func (t *ManagedTransaction) ExpireSnapshots(retainLast int, olderThan time.Duration, deleteFiles bool) error {
	return t.ExpireSnapshotsWithOptions(ExpiryOptions{RetainLast: &retainLast, OlderThan: &olderThan, DeleteFiles: deleteFiles})
}

func (t *ManagedTransaction) ExpireSnapshotsWithOptions(options ExpiryOptions) error {
	if err := t.valid(); err != nil {
		return err
	}
	return managedDo(t.scope, "expire-snapshots", grantExpire, func(context.Context) error {
		if err := t.valid(); err != nil {
			return err
		}
		if options.DeleteFiles && !t.scope.allowed(grantDeleteFiles) {
			return failure(ErrAuthority, "expiry-deletion")
		}
		opts := []table.ExpireSnapshotsOpt{table.WithPostCommit(options.DeleteFiles)}
		if options.RetainLast != nil {
			opts = append(opts, table.WithRetainLast(*options.RetainLast))
		}
		if options.OlderThan != nil {
			opts = append(opts, table.WithOlderThan(*options.OlderThan))
		}
		return t.transaction.ExpireSnapshots(opts...)
	})
}

// PurgeFiles physically purges a dropped table using its retained metadata.
// The caller must quiesce all writers/readers and catalog registrations. It is
// native best-effort deletion, not a claim that object versions are absent.
func (t *ManagedTable) PurgeFiles(quiescent bool) error {
	if err := t.valid(); err != nil {
		return err
	}
	return managedDo(t.scope, "purge-files", grantPurge, func(ctx context.Context) error {
		if !quiescent || !t.scope.allowed(grantDeleteFiles) || t.purged {
			return failure(ErrAuthority, "purge-files-quiescence-or-grant")
		}
		exists, err := t.scope.source.catalog.CheckTableExists(ctx, t.table.Identifier())
		if err != nil {
			return err
		}
		if exists {
			return failure(ErrAuthority, "purge-files-table-not-dropped")
		}
		if err = t.scope.inspectPurgeTargets(ctx, t.table); err != nil {
			return err
		}
		t.purged = true
		return t.table.PurgeFiles(ctx)
	})
}
func (t *ManagedTransaction) RewriteManifests(opts ...table.RewriteManifestsOpt) (*table.RewriteManifestsResult, error) {
	if err := t.valid(); err != nil {
		return nil, err
	}
	return managedCall(t.scope, "rewrite-manifests", grantRewrite, func(ctx context.Context) (*table.RewriteManifestsResult, error) {
		if err := t.valid(); err != nil {
			return nil, err
		}
		for _, opt := range opts {
			if opt == nil {
				return nil, failure(ErrInput, "option")
			}
		}
		t.write.Store(true)
		return t.transaction.RewriteManifests(ctx, slices.Clone(opts)...)
	})
}
func (t *ManagedTable) PlanCompaction(config compaction.Config) (compaction.Plan, error) {
	if err := t.valid(); err != nil {
		return compaction.Plan{}, err
	}
	return managedCall(t.scope, "plan-compaction", grantRead, func(ctx context.Context) (compaction.Plan, error) {
		if err := config.Validate(); err != nil {
			return compaction.Plan{}, err
		}
		scan := t.table.Scan(table.WithMaxConcurrency(t.scope.source.prepared.settings.Workers))
		defer func() { t.scope.noteCleanup(scan.Close()) }()
		tasks, err := scan.PlanFiles(ctx)
		if err != nil {
			return compaction.Plan{}, err
		}
		if err = t.scope.validateScanTasks(tasks); err != nil {
			return compaction.Plan{}, err
		}
		return config.PlanCompaction(tasks)
	})
}
func (t *ManagedTable) ExecuteCompactionGroup(group table.CompactionTaskGroup, opts ...table.CompactionGroupOption) (table.CompactionGroupResult, error) {
	if err := t.valid(); err != nil {
		return table.CompactionGroupResult{}, err
	}
	return managedCall(t.scope, "execute-compaction-group", grantRewrite, func(ctx context.Context) (table.CompactionGroupResult, error) {
		if !t.scope.allowed(grantImport) {
			return table.CompactionGroupResult{}, failure(ErrAuthority, "compaction-task-adoption")
		}
		if err := t.scope.validateScanTasks(group.Tasks); err != nil {
			return table.CompactionGroupResult{}, err
		}
		for _, opt := range opts {
			if opt == nil {
				return table.CompactionGroupResult{}, failure(ErrInput, "option")
			}
		}
		options := append(slices.Clone(opts), table.WithCompactionScanConcurrency(t.scope.source.prepared.settings.Workers))
		return table.ExecuteCompactionGroup(ctx, t.table, group, options...)
	})
}

// ManagedRewriteResult removes the native owning Table field while preserving
// native partial-progress counters, group outputs and failed commit causes.
type ManagedRewriteResult struct {
	Table                                                                              *ManagedTable
	RewrittenGroups, AddedDataFiles, RemovedDataFiles                                  int
	RemovedPositionDeleteFiles, RemovedEqualityDeleteFiles, RemovedDeletionVectorFiles int
	BytesBefore, BytesAfter                                                            int64
	CompletedGroups                                                                    []table.CompactionGroupResult
	FailedGroups                                                                       []table.CompactionGroupFailure
}

func (f *ManagedSession) rewriteResult(ctx context.Context, r *table.RewriteResult) (*ManagedRewriteResult, error) {
	if r == nil {
		return nil, nil
	}
	out := &ManagedRewriteResult{RewrittenGroups: r.RewrittenGroups, AddedDataFiles: r.AddedDataFiles, RemovedDataFiles: r.RemovedDataFiles,
		RemovedPositionDeleteFiles: r.RemovedPositionDeleteFiles, RemovedEqualityDeleteFiles: r.RemovedEqualityDeleteFiles, RemovedDeletionVectorFiles: r.RemovedDeletionVectorFiles,
		BytesBefore: r.BytesBefore, BytesAfter: r.BytesAfter, CompletedGroups: slices.Clone(r.CompletedGroups), FailedGroups: slices.Clone(r.FailedGroups)}
	var err error
	if r.Table != nil {
		out.Table, err = f.wrapTable(ctx, r.Table)
	}
	return out, err
}
func (f *ManagedSession) rewriteOptions(groups []table.CompactionTaskGroup, o table.RewriteDataFilesOptions) (table.RewriteDataFilesOptions, error) {
	if len(groups) != 0 && !f.allowed(grantImport) {
		return o, failure(ErrAuthority, "compaction-task-adoption")
	}
	if len(groups) > f.source.prepared.settings.MaxListEntries || o.MaxCommits < 0 || o.MaxCommits > f.source.prepared.settings.MaxOperations {
		return o, failure(ErrLimit, "rewrite-groups")
	}
	for _, group := range groups {
		if err := f.validateScanTasks(group.Tasks); err != nil {
			return o, err
		}
	}
	if err := f.checkDataFiles(o.ExtraDeleteFilesToRemove); err != nil {
		return o, err
	}
	var err error
	o.SnapshotProps, err = f.boundedProperties(o.SnapshotProps)
	if err != nil {
		return o, err
	}
	o.ExtraDeleteFilesToRemove = slices.Clone(o.ExtraDeleteFilesToRemove)
	o.GroupOptions = append(slices.Clone(o.GroupOptions), table.WithCompactionScanConcurrency(f.source.prepared.settings.Workers))
	if o.MaxCommits == 0 {
		o.MaxCommits = min(10, f.source.prepared.settings.MaxOperations)
	}
	if o.MaxFailedCommits <= 0 {
		o.MaxFailedCommits = o.MaxCommits
	}
	return o, nil
}
func (t *ManagedTable) RewriteDataFiles(groups []table.CompactionTaskGroup, options table.RewriteDataFilesOptions) (*ManagedRewriteResult, error) {
	if err := t.valid(); err != nil {
		return nil, err
	}
	return managedCall(t.scope, "rewrite-data-files", grantRewrite, func(ctx context.Context) (*ManagedRewriteResult, error) {
		options, err := t.scope.rewriteOptions(groups, options)
		if err != nil {
			return nil, err
		}
		result, err := t.table.RewriteDataFiles(ctx, groups, options)
		out, copyErr := t.scope.rewriteResult(ctx, result)
		return out, errors.Join(err, copyErr)
	})
}
func (t *ManagedTransaction) RewriteDataFiles(groups []table.CompactionTaskGroup, options table.RewriteDataFilesOptions) (*ManagedRewriteResult, error) {
	if err := t.valid(); err != nil {
		return nil, err
	}
	return managedCall(t.scope, "stage-rewrite", grantRewrite, func(ctx context.Context) (*ManagedRewriteResult, error) {
		if err := t.valid(); err != nil {
			return nil, err
		}
		options, err := t.scope.rewriteOptions(groups, options)
		if err != nil {
			return nil, err
		}
		t.write.Store(true)
		if options.PartialProgress {
			t.consumed.Store(true)
		}
		result, err := t.transaction.RewriteDataFiles(ctx, groups, options)
		out, copyErr := t.scope.rewriteResult(ctx, result)
		return out, errors.Join(err, copyErr)
	})
}

type RewriteChanges struct {
	Remove, AddData, RemoveDeletes []native.DataFile
	AddDeletes                     []table.DeleteFileAddition
	DataSequenceNumber             *int64
	SnapshotProperties             native.Properties
}

func (t *ManagedTransaction) RewriteFiles(changes RewriteChanges) error {
	if err := t.valid(); err != nil {
		return err
	}
	return managedDo(t.scope, "rewrite-files", grantRewrite, func(ctx context.Context) error {
		if err := t.valid(); err != nil {
			return err
		}
		for _, files := range [][]native.DataFile{changes.Remove, changes.AddData, changes.RemoveDeletes} {
			if err := t.scope.checkDataFiles(files); err != nil {
				return err
			}
		}
		if err := t.scope.checkAdoption(changes.AddData); err != nil {
			return err
		}
		props, err := t.scope.boundedProperties(changes.SnapshotProperties)
		if err != nil {
			return err
		}
		rewrite := t.transaction.NewRewrite(props)
		if changes.DataSequenceNumber != nil {
			rewrite.DataSequenceNumber(*changes.DataSequenceNumber)
		}
		for _, file := range changes.Remove {
			rewrite.DeleteFile(file)
		}
		for _, file := range changes.RemoveDeletes {
			rewrite.DeleteFile(file)
		}
		for _, file := range changes.AddData {
			rewrite.AddDataFile(file)
		}
		for _, addition := range changes.AddDeletes {
			if err := t.scope.checkAdoption([]native.DataFile{addition.File}); err != nil {
				return err
			}
			if addition.DataSequenceNumber >= 0 {
				rewrite.AddDeleteFileWithDataSequenceNumber(addition.File, addition.DataSequenceNumber)
			} else {
				rewrite.AddDeleteFile(addition.File)
			}
		}
		t.write.Store(true)
		return rewrite.Commit(ctx)
	})
}

type OrphanOptions struct {
	Location                       string
	OlderThan                      time.Duration
	PrefixMismatch                 table.PrefixMismatchMode
	EqualSchemes, EqualAuthorities map[string]string
}
type ManagedOrphanPlan struct {
	private
	scope *ManagedSession
	table *table.Table
	plan  table.OrphanCleanupPlan
	used  bool
}

func (p *ManagedOrphanPlan) Files() []string {
	if p == nil {
		return nil
	}
	return p.plan.Files()
}
func (p *ManagedOrphanPlan) OrphanFiles() []table.OrphanFile {
	if p == nil {
		return nil
	}
	return p.plan.OrphanFiles()
}
func (p *ManagedOrphanPlan) Cutoff() time.Time {
	if p == nil {
		return time.Time{}
	}
	return p.plan.Cutoff()
}
func (t *ManagedTable) PlanOrphanFiles(options OrphanOptions) (*ManagedOrphanPlan, error) {
	if err := t.valid(); err != nil {
		return nil, err
	}
	return managedCall(t.scope, "plan-orphans", grantOrphan, func(ctx context.Context) (*ManagedOrphanPlan, error) {
		if options.OlderThan <= 0 {
			return nil, failure(ErrInput, "orphan-age")
		}
		if options.Location != "" {
			if err := t.scope.location(options.Location); err != nil {
				return nil, err
			}
		}
		var err error
		options.EqualSchemes, err = t.scope.boundedProperties(options.EqualSchemes)
		if err != nil {
			return nil, err
		}
		options.EqualAuthorities, err = t.scope.boundedProperties(options.EqualAuthorities)
		if err != nil {
			return nil, err
		}
		if err = t.scope.handle(); err != nil {
			return nil, err
		}
		opts := []table.OrphanCleanupOption{table.WithFilesOlderThan(options.OlderThan), table.WithCleanupMaxConcurrency(t.scope.source.prepared.settings.Workers), table.WithPrefixMismatchMode(options.PrefixMismatch),
			table.WithEqualSchemes(options.EqualSchemes), table.WithEqualAuthorities(options.EqualAuthorities)}
		if options.Location != "" {
			opts = append(opts, table.WithLocation(options.Location))
		}
		plan, err := t.table.PlanOrphanFiles(ctx, opts...)
		if err != nil {
			return nil, err
		}
		if len(plan.Files()) > t.scope.source.prepared.settings.MaxListEntries {
			return nil, failure(ErrLimit, "orphan-plan")
		}
		for _, file := range plan.Files() {
			if err = t.scope.location(file); err != nil {
				return nil, err
			}
		}
		return &ManagedOrphanPlan{scope: t.scope, table: t.table, plan: plan}, nil
	})
}
func (t *ManagedTable) ExecuteOrphanCleanup(plan *ManagedOrphanPlan, quiescent bool) (table.OrphanCleanupResult, error) {
	return t.ExecuteOrphanCleanupWithOptions(plan, OrphanExecutionOptions{Quiescent: quiescent})
}

// OrphanExecutionOptions preserves the native dry-run and custom deletion
// callback contract. A callback acknowledgement does not prove object absence.
type OrphanExecutionOptions struct {
	Quiescent bool
	DryRun    bool
	Delete    func(string) error
}

func (t *ManagedTable) ExecuteOrphanCleanupWithOptions(plan *ManagedOrphanPlan, options OrphanExecutionOptions) (table.OrphanCleanupResult, error) {
	if err := t.valid(); err != nil {
		return table.OrphanCleanupResult{}, err
	}
	operation := "delete-orphans"
	if options.DryRun {
		operation = "orphan-dry-run"
	}
	return managedCall(t.scope, operation, grantOrphan, func(ctx context.Context) (table.OrphanCleanupResult, error) {
		if plan == nil || plan.scope != t.scope || plan.table != t.table || plan.used || !options.DryRun && (!options.Quiescent || !t.scope.allowed(grantDeleteFiles)) {
			return table.OrphanCleanupResult{}, failure(ErrAuthority, "orphan-origin-or-quiescence")
		}
		fresh, err := t.scope.source.catalog.LoadTable(ctx, t.table.Identifier())
		if err != nil {
			return table.OrphanCleanupResult{}, err
		}
		equal, err := equalMetadata(fresh.Metadata(), t.table.Metadata())
		if err != nil {
			return table.OrphanCleanupResult{}, err
		}
		if !equal {
			return table.OrphanCleanupResult{}, failure(ErrAuthority, "orphan-base-changed")
		}
		opts := []table.OrphanCleanupOption{table.WithCleanupMaxConcurrency(t.scope.source.prepared.settings.Workers), table.WithDryRun(options.DryRun)}
		var callbackMu sync.Mutex
		var callbackErrors []error
		if options.Delete != nil {
			opts = append(opts, table.WithDeleteFunc(func(uri string) (err error) {
				if err = ctx.Err(); err != nil {
					return errors.Join(err, context.Cause(ctx))
				}
				if err = t.scope.location(uri); err != nil {
					return err
				}
				t.scope.mu.Lock()
				if t.scope.fileOps >= t.scope.source.prepared.settings.MaxFileOps {
					t.scope.mu.Unlock()
					return failure(ErrLimit, "custom-delete")
				}
				t.scope.fileOps++
				t.scope.data.files = append(t.scope.data.files, ManagedFileEffect{Operation: "custom-delete", URI: uri, Effect: Unknown})
				t.scope.mu.Unlock()
				returned := false
				defer func() {
					if !returned {
						cause, _ := recover().(error)
						err = failure(ErrOperation, "delete-callback-exit", cause)
					}
					effect := Unknown
					if err == nil {
						effect = Acknowledged
					}
					t.scope.mu.Lock()
					t.scope.data.files = append(t.scope.data.files, ManagedFileEffect{Operation: "custom-delete", URI: uri, Effect: effect, Err: failureIf(err)})
					t.scope.mu.Unlock()
					if err != nil {
						callbackMu.Lock()
						callbackErrors = append(callbackErrors, err)
						callbackMu.Unlock()
					}
				}()
				err = options.Delete(uri)
				returned = true
				return err
			}))
		}
		if !options.DryRun {
			plan.used = true
		}
		result, err := t.table.ExecuteOrphanCleanup(ctx, plan.plan, opts...)
		callbackMu.Lock()
		defer callbackMu.Unlock()
		return result, errors.Join(err, errors.Join(callbackErrors...))
	})
}

// inspectPurgeTargets does not grant an atomic orphan oracle. The caller's
// quiescence declaration covers the interval between this read and server purge.
func (f *ManagedSession) inspectPurgeTargets(ctx context.Context, tbl *table.Table) error {
	if err := f.validateTable(tbl.Metadata()); err != nil {
		return err
	}
	files, err := tbl.FS(ctx)
	if err != nil {
		return err
	}
	count := 0
	for manifest, err := range tbl.AllManifests(ctx) {
		if err != nil {
			return err
		}
		if manifest == nil {
			return failure(ErrProtocol, "manifest")
		}
		count++
		if count > f.source.prepared.settings.MaxListEntries {
			return failure(ErrLimit, "purge-targets")
		}
		if err = f.location(manifest.FilePath()); err != nil {
			return err
		}
		entries, err := manifest.FetchEntries(files, false)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			count++
			if count > f.source.prepared.settings.MaxListEntries {
				return failure(ErrLimit, "purge-targets")
			}
			file := entry.DataFile()
			if file == nil {
				return failure(ErrProtocol, "manifest-entry")
			}
			if err = f.location(file.FilePath()); err != nil {
				return err
			}
		}
	}
	return ctx.Err()
}
