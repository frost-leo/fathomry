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
	"maps"
	"reflect"
	"slices"
	"strings"

	"github.com/apache/iceberg-go/table"
)

// ManagedTableEffect records multi-table intent separately from the aggregate
// server acknowledgement. A later reload cannot supply this call's commit ID.
type ManagedTableEffect struct {
	References                     []ManagedReferenceChange
	Ref                            string
	Identifier                     []string
	TableUUID                      string
	BaseSnapshot, IntendedSnapshot int64
	Effect                         Effect
}

// ManagedReferenceChange preserves changed reference intent, including retention
// settings and removal. Nil is absence, not snapshot zero or unknown commit state.
type ManagedReferenceChange struct {
	Name          string
	Before, After *table.SnapshotRef
}

func copyReference(ref *table.SnapshotRef) *table.SnapshotRef {
	if ref == nil {
		return nil
	}
	copy := *ref
	copy.MinSnapshotsToKeep = copyManagedScalar(ref.MinSnapshotsToKeep)
	copy.MaxSnapshotAgeMs = copyManagedScalar(ref.MaxSnapshotAgeMs)
	copy.MaxRefAgeMs = copyManagedScalar(ref.MaxRefAgeMs)
	return &copy
}

func referenceChanges(base, intended table.Metadata) []ManagedReferenceChange {
	before, after := maps.Collect(base.Refs()), maps.Collect(intended.Refs())
	var names []string
	for name := range before {
		names = append(names, name)
	}
	for name := range after {
		if _, exists := before[name]; !exists {
			names = append(names, name)
		}
	}
	slices.Sort(names)
	var changes []ManagedReferenceChange
	for _, name := range names {
		previous, had := before[name]
		next, has := after[name]
		if had == has && reflect.DeepEqual(previous, next) {
			continue
		}
		change := ManagedReferenceChange{Name: name}
		if had {
			change.Before = copyReference(&previous)
		}
		if has {
			change.After = copyReference(&next)
		}
		changes = append(changes, change)
	}
	return changes
}

func (f *ManagedSession) CommitTransaction(transactions ...*ManagedTransaction) error {
	return managedDo(f, "commit-transaction", grantMultiTable, func(ctx context.Context) error {
		if len(transactions) == 0 || len(transactions) > f.source.prepared.settings.MaxHandles {
			return failure(ErrLimit, "table-commits")
		}
		commits := make([]table.TableCommit, 0, len(transactions))
		effects := make([]ManagedTableEffect, 0, len(transactions))
		seen := map[*ManagedTransaction]bool{}
		identifiers := map[string]bool{}
		for _, transaction := range transactions {
			if transaction == nil || transaction.scope != f || seen[transaction] {
				return failure(ErrAuthority, "transaction-origin")
			}
			seen[transaction] = true
			if err := transaction.valid(); err != nil {
				return err
			}
			key := strings.Join(transaction.table.Identifier(), "\x00")
			if identifiers[key] {
				return failure(ErrInput, "duplicate-table-commit")
			}
			identifiers[key] = true
			commit, err := transaction.transaction.TableCommit()
			if err != nil {
				return err
			}
			frozen, err := cloneCommit(commit, f.source.prepared.settings.MaxMetadataBytes)
			if err != nil {
				return err
			}
			staged, err := transaction.transaction.StagedTable()
			if err != nil {
				return err
			}
			if err = f.validateTable(staged.Metadata()); err != nil {
				return err
			}
			fact := ManagedTableEffect{Identifier: slices.Clone(commit.Identifier), TableUUID: transaction.table.Metadata().TableUUID().String(), Ref: transaction.branch, References: referenceChanges(transaction.table.Metadata(), staged.Metadata())}
			if snapshot := transactionSnapshot(transaction.table, transaction.branch); snapshot != nil {
				fact.BaseSnapshot = snapshot.SnapshotID
			}
			if snapshot := transactionSnapshot(staged.Table, transaction.branch); snapshot != nil {
				fact.IntendedSnapshot = snapshot.SnapshotID
			}
			commits = append(commits, frozen)
			effects = append(effects, fact)
		}
		f.mu.Lock()
		f.data.effects[f.current].Tables = effects
		f.data.effects[f.current].Staged = true
		f.mu.Unlock()
		for _, transaction := range transactions {
			transaction.consumed.Store(true)
		}
		err := f.source.catalog.CommitTransaction(ctx, commits)
		f.mu.Lock()
		effect := f.data.effects[f.current].Effect
		for i := range f.data.effects[f.current].Tables {
			f.data.effects[f.current].Tables[i].Effect = effect
		}
		f.mu.Unlock()
		if err == nil {
			for _, transaction := range transactions {
				transaction.transaction.MarkCommitted()
			}
		}
		return err
	})
}
func (f *ManagedSession) CommitTableChanges(commits []table.TableCommit) error {
	return managedDo(f, "commit-table-changes", grantMultiTable, func(ctx context.Context) error {
		if len(commits) == 0 || len(commits) > f.source.prepared.settings.MaxHandles {
			return failure(ErrLimit, "table-commits")
		}
		prepared := make([]table.TableCommit, len(commits))
		facts := make([]ManagedTableEffect, len(commits))
		identifiers := map[string]bool{}
		for i, commit := range commits {
			key := strings.Join(commit.Identifier, "\x00")
			if identifiers[key] {
				return failure(ErrInput, "duplicate-table-commit")
			}
			identifiers[key] = true
			value, fact, err := f.prepareCommit(ctx, commit)
			if err != nil {
				return err
			}
			prepared[i] = value
			facts[i] = fact
		}
		f.mu.Lock()
		f.data.effects[f.current].Tables = facts
		f.data.effects[f.current].Staged = true
		f.mu.Unlock()
		err := f.source.catalog.CommitTransaction(ctx, prepared)
		f.mu.Lock()
		for i := range f.data.effects[f.current].Tables {
			f.data.effects[f.current].Tables[i].Effect = f.data.effects[f.current].Effect
		}
		f.mu.Unlock()
		return err
	})
}
func (f *ManagedSession) CommitTransactionAndReload(transactions ...*ManagedTransaction) ([]*ManagedTable, error) {
	if err := f.CommitTransaction(transactions...); err != nil {
		return nil, err
	}
	var result []*ManagedTable
	var failures []error
	for _, transaction := range transactions {
		tbl, err := managedCall(f, "reload-transaction-table", grantRead, func(ctx context.Context) (*ManagedTable, error) {
			fresh, err := f.source.catalog.LoadTable(ctx, transaction.table.Identifier())
			if err != nil {
				return nil, err
			}
			if fresh.Metadata().TableUUID() != transaction.table.Metadata().TableUUID() {
				return nil, failure(ErrAuthority, "reload-table-origin")
			}
			f.mu.Lock()
			effect := &f.data.effects[f.current]
			effect.Reloaded = true
			effect.Ref = transaction.branch
			effect.TableUUID = fresh.Metadata().TableUUID().String()
			effect.MetadataLocation = fresh.MetadataLocation()
			if snapshot := transactionSnapshot(fresh, transaction.branch); snapshot != nil {
				effect.ReloadedSnapshot = snapshot.SnapshotID
			}
			f.mu.Unlock()
			return f.wrapTable(ctx, fresh)
		})
		if err != nil {
			failures = append(failures, err)
		}
		result = append(result, tbl)
	}
	return result, errors.Join(failures...)
}
