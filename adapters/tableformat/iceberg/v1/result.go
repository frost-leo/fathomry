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

	"github.com/apache/iceberg-go/table"
	"github.com/frost-leo/fathomry/adapters/tableformat/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	native "github.com/frost-leo/fathomry/internal/tableformat/iceberg/v0"
)

type EffectResult struct {
	private
	Submissions                                                         []CommitSubmission
	Ref                                                                 string
	Tables                                                              []TableEffect
	Operation                                                           string
	Effect                                                              tableformat.Effect
	Phases                                                              tableformat.Phases
	Complete                                                            bool
	TableUUID, MetadataLocation                                         string
	BaseSnapshot, IntendedSnapshot, CommittedSnapshot, ReloadedSnapshot int64
	Err                                                                 error
}

// CommitSubmission is one actual single-table Catalog request, not a later
// reload. Native retries and partial-progress batches retain separate entries.
// A status code alone is not an acknowledgement: the reply must match intent.
type CommitSubmission struct {
	private
	Ref, TableUUID, MetadataLocation                  string
	BaseSnapshot, IntendedSnapshot, CommittedSnapshot int64
	HTTPStatus                                        int
	Effect                                            tableformat.Effect
}
type TableEffect struct {
	private
	References                     []ReferenceChange
	Ref                            string
	Identifier                     []string
	TableUUID                      string
	BaseSnapshot, IntendedSnapshot int64
	Effect                         tableformat.Effect
}

// ReferenceChange is detached native reference intent. Nil means absence;
// acknowledgement comes from the enclosing TableEffect, never from reloading.
type ReferenceChange struct {
	private
	Name          string
	Before, After *table.SnapshotRef
}

func copyReference(ref *table.SnapshotRef) *table.SnapshotRef {
	if ref == nil {
		return nil
	}
	copy := *ref
	if ref.MinSnapshotsToKeep != nil {
		value := *ref.MinSnapshotsToKeep
		copy.MinSnapshotsToKeep = &value
	}
	if ref.MaxSnapshotAgeMs != nil {
		value := *ref.MaxSnapshotAgeMs
		copy.MaxSnapshotAgeMs = &value
	}
	if ref.MaxRefAgeMs != nil {
		value := *ref.MaxRefAgeMs
		copy.MaxRefAgeMs = &value
	}
	return &copy
}

type FileEffect struct {
	private
	Operation, URI, UploadID string
	Bytes                    int64
	Effect                   tableformat.Effect
	Err                      error
}
type Result struct {
	private
	attribution          tableformat.Attribution
	effects              []EffectResult
	files                []FileEffect
	operations, requests int
	rows, bytes          int64
	released             bool
}

func (r Result) Attribution() tableformat.Attribution { return r.attribution }
func (r Result) Effects() []EffectResult {
	copy := slices.Clone(r.effects)
	for i := range copy {
		copy[i].Submissions = slices.Clone(copy[i].Submissions)
		copy[i].Tables = slices.Clone(copy[i].Tables)
		for j := range copy[i].Tables {
			copy[i].Tables[j].Identifier = slices.Clone(copy[i].Tables[j].Identifier)
			copy[i].Tables[j].References = slices.Clone(copy[i].Tables[j].References)
			for k := range copy[i].Tables[j].References {
				change := &copy[i].Tables[j].References[k]
				change.Before, change.After = copyReference(change.Before), copyReference(change.After)
			}
		}
	}
	return copy
}
func (r Result) Files() []FileEffect { return slices.Clone(r.files) }
func (r Result) Operations() int     { return r.operations }
func (r Result) Requests() int       { return r.requests }
func (r Result) Rows() int64         { return r.rows }
func (r Result) Bytes() int64        { return r.bytes }
func (r Result) Released() bool      { return r.released }
func projectEffect(effect native.Effect) tableformat.Effect {
	switch effect {
	case native.Unknown:
		return tableformat.Unknown
	case native.Acknowledged:
		return tableformat.Acknowledged
	case native.Rejected:
		return tableformat.Rejected
	}
	return tableformat.NotSubmitted
}
func project(value native.ManagedResult, info adapters.Info, sourceID string) Result {
	r := Result{attribution: tableformat.Attribution{Runtime: info.Runtime, Operation: info.Operation, ID: info.ID, Sequence: info.Sequence, Parent: info.Parent, Depth: info.Depth, Source: info.Source, SourceID: sourceID, UseID: info.Sequence},
		operations: value.Operations(), requests: value.Requests(), rows: value.Rows(), bytes: value.Bytes(), released: value.Released()}
	for _, effect := range value.Effects() {
		var submissions []CommitSubmission
		for _, item := range effect.Submissions {
			submissions = append(submissions, CommitSubmission{Ref: item.Ref, TableUUID: item.TableUUID, MetadataLocation: item.MetadataLocation, BaseSnapshot: item.BaseSnapshot, IntendedSnapshot: item.IntendedSnapshot, CommittedSnapshot: item.CommittedSnapshot, HTTPStatus: item.HTTPStatus, Effect: projectEffect(item.Effect)})
		}
		var tables []TableEffect
		for _, item := range effect.Tables {
			var references []ReferenceChange
			for _, change := range item.References {
				references = append(references, ReferenceChange{Name: change.Name, Before: copyReference(change.Before), After: copyReference(change.After)})
			}
			tables = append(tables, TableEffect{Identifier: slices.Clone(item.Identifier), TableUUID: item.TableUUID, BaseSnapshot: item.BaseSnapshot, IntendedSnapshot: item.IntendedSnapshot, Effect: projectEffect(item.Effect), Ref: item.Ref, References: references})
		}
		r.effects = append(r.effects, EffectResult{Operation: effect.Operation, Effect: projectEffect(effect.Effect), Complete: effect.Complete,
			Submissions: submissions,
			Ref:         effect.Ref,
			Tables:      tables,
			Phases:      tableformat.Phases{Staged: effect.Staged, Uploaded: effect.Uploaded, Submitted: effect.Submitted, Reloaded: effect.Reloaded, Released: value.Released()},
			TableUUID:   effect.TableUUID, MetadataLocation: effect.MetadataLocation, BaseSnapshot: effect.BaseSnapshot, IntendedSnapshot: effect.IntendedSnapshot, CommittedSnapshot: effect.CommittedSnapshot, ReloadedSnapshot: effect.ReloadedSnapshot, Err: translate(effect.Err, "effect")})
	}
	for _, file := range value.Files() {
		r.files = append(r.files, FileEffect{Operation: file.Operation, URI: file.URI, UploadID: file.UploadID, Bytes: file.Bytes, Effect: projectEffect(file.Effect), Err: translate(file.Err, "file")})
	}
	return r
}
