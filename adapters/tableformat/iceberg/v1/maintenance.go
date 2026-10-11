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
	"slices"
	"time"

	sdk "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/table/compaction"
	native "github.com/frost-leo/fathomry/internal/tableformat/iceberg/v0"
)

// ExpiryOptions preserves native property defaults for omitted retention fields.
// DeleteFiles is explicit and requires its independent source grant.
type ExpiryOptions struct {
	RetainLast  *int
	OlderThan   *time.Duration
	DeleteFiles bool
}

func (t *Transaction) ExpireSnapshotsWithOptions(options ExpiryOptions) error {
	if err := t.valid(); err != nil {
		return err
	}
	return translate(t.native.ExpireSnapshotsWithOptions(native.ExpiryOptions(options)), "expire-snapshots")
}

// PurgeFiles requires a retained Table after DropTable, explicit quiescence and
// both purge/delete-files grants. Success does not prove all versions disappeared.
func (t *Table) PurgeFiles(quiescent bool) error {
	if err := t.valid(); err != nil {
		return err
	}
	return translate(t.native.PurgeFiles(quiescent), "purge-files")
}

type RewriteChanges struct {
	Remove, AddData, RemoveDeletes []sdk.DataFile
	AddDeletes                     []table.DeleteFileAddition
	DataSequenceNumber             *int64
	SnapshotProperties             sdk.Properties
}
type OrphanOptions struct {
	Location                       string
	OlderThan                      time.Duration
	PrefixMismatch                 table.PrefixMismatchMode
	EqualSchemes, EqualAuthorities map[string]string
}

type RewriteResult struct {
	private
	Table                                                                              *Table
	RewrittenGroups, AddedDataFiles, RemovedDataFiles                                  int
	RemovedPositionDeleteFiles, RemovedEqualityDeleteFiles, RemovedDeletionVectorFiles int
	BytesBefore, BytesAfter                                                            int64
	CompletedGroups                                                                    []table.CompactionGroupResult
	FailedGroups                                                                       []table.CompactionGroupFailure
}

func wrapRewrite(v *native.ManagedRewriteResult) *RewriteResult {
	if v == nil {
		return nil
	}
	failures := slices.Clone(v.FailedGroups)
	for index := range failures {
		failures[index].Err = translate(failures[index].Err, "rewrite-group")
	}
	return &RewriteResult{Table: wrapTable(v.Table), RewrittenGroups: v.RewrittenGroups, AddedDataFiles: v.AddedDataFiles, RemovedDataFiles: v.RemovedDataFiles, RemovedPositionDeleteFiles: v.RemovedPositionDeleteFiles, RemovedEqualityDeleteFiles: v.RemovedEqualityDeleteFiles, RemovedDeletionVectorFiles: v.RemovedDeletionVectorFiles, BytesBefore: v.BytesBefore, BytesAfter: v.BytesAfter, CompletedGroups: slices.Clone(v.CompletedGroups), FailedGroups: failures}
}
func (t *Transaction) ExpireSnapshots(retainLast int, olderThan time.Duration, deleteFiles bool) error {
	if err := t.valid(); err != nil {
		return err
	}
	return translate(t.native.ExpireSnapshots(retainLast, olderThan, deleteFiles), "expire-snapshots")
}
func (t *Transaction) RewriteManifests(opts ...table.RewriteManifestsOpt) (*table.RewriteManifestsResult, error) {
	if err := t.valid(); err != nil {
		return nil, err
	}
	v, err := t.native.RewriteManifests(opts...)
	return v, translate(err, "rewrite-manifests")
}
func (t *Table) PlanCompaction(config compaction.Config) (compaction.Plan, error) {
	if err := t.valid(); err != nil {
		return compaction.Plan{}, err
	}
	v, err := t.native.PlanCompaction(config)
	return v, translate(err, "plan-compaction")
}
func (t *Table) ExecuteCompactionGroup(group table.CompactionTaskGroup, opts ...table.CompactionGroupOption) (table.CompactionGroupResult, error) {
	if err := t.valid(); err != nil {
		return table.CompactionGroupResult{}, err
	}
	v, err := t.native.ExecuteCompactionGroup(group, opts...)
	return v, translate(err, "execute-compaction-group")
}
func (t *Table) RewriteDataFiles(groups []table.CompactionTaskGroup, options table.RewriteDataFilesOptions) (*RewriteResult, error) {
	if err := t.valid(); err != nil {
		return nil, err
	}
	v, err := t.native.RewriteDataFiles(groups, options)
	return wrapRewrite(v), translate(err, "rewrite-data-files")
}
func (t *Transaction) RewriteDataFiles(groups []table.CompactionTaskGroup, options table.RewriteDataFilesOptions) (*RewriteResult, error) {
	if err := t.valid(); err != nil {
		return nil, err
	}
	v, err := t.native.RewriteDataFiles(groups, options)
	return wrapRewrite(v), translate(err, "rewrite-data-files")
}

func (t *Transaction) RewriteFiles(changes RewriteChanges) error {
	if err := t.valid(); err != nil {
		return err
	}
	return translate(t.native.RewriteFiles(native.RewriteChanges(changes)), "rewrite-files")
}

type OrphanPlan struct {
	private
	native *native.ManagedOrphanPlan
}

func (p *OrphanPlan) Files() []string {
	if p == nil || p.native == nil {
		return nil
	}
	return p.native.Files()
}
func (p *OrphanPlan) OrphanFiles() []table.OrphanFile {
	if p == nil || p.native == nil {
		return nil
	}
	return p.native.OrphanFiles()
}
func (p *OrphanPlan) Cutoff() time.Time {
	if p == nil || p.native == nil {
		return time.Time{}
	}
	return p.native.Cutoff()
}
func (t *Table) PlanOrphanFiles(options OrphanOptions) (*OrphanPlan, error) {
	if err := t.valid(); err != nil {
		return nil, err
	}
	v, err := t.native.PlanOrphanFiles(native.OrphanOptions(options))
	if v == nil {
		return nil, translate(err, "plan-orphans")
	}
	return &OrphanPlan{native: v}, translate(err, "plan-orphans")
}
func (t *Table) ExecuteOrphanCleanup(plan *OrphanPlan, quiescent bool) (table.OrphanCleanupResult, error) {
	return t.ExecuteOrphanCleanupWithOptions(plan, OrphanExecutionOptions{Quiescent: quiescent})
}

// OrphanExecutionOptions supplies native dry-run or explicit cooperative deletion.
// Delete runs only after target/capacity checks; it must finish before returning.
type OrphanExecutionOptions struct {
	private
	Quiescent bool
	DryRun    bool
	Delete    func(string) error
}

func (t *Table) ExecuteOrphanCleanupWithOptions(plan *OrphanPlan, options OrphanExecutionOptions) (table.OrphanCleanupResult, error) {
	if err := t.valid(); err != nil {
		return table.OrphanCleanupResult{}, err
	}
	if plan == nil {
		return table.OrphanCleanupResult{}, fail(ErrInput, "orphan-plan")
	}
	v, err := t.native.ExecuteOrphanCleanupWithOptions(plan.native, native.OrphanExecutionOptions{Quiescent: options.Quiescent, DryRun: options.DryRun, Delete: options.Delete})
	return v, translate(err, "delete-orphans")
}
