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

package configuration

import (
	"context"
	"errors"
	"io"

	"github.com/frost-leo/fathomry/adapters/v1"
)

// RecordInfo is detached technical metadata. Sequence is local to one result's
// operation runtime, not a durable cursor. Explicit identifiers may be sensitive.
type RecordInfo struct {
	Provider           string
	Runtime            string
	Operation          string
	ID                 string
	Sequence           uint64
	Parent             uint64
	Depth              int
	ResourceName       string
	ResourceGeneration uint64
	Resolved           bool
	Released           bool
}

// SourceEvidence projects the read-only source facts, never document contents.
type SourceEvidence struct {
	Documents   int
	FailedIndex int
	Missing     bool
}

type recordFacts struct {
	configuration        Evidence
	configurationPresent bool
	source               SourceEvidence
	sourcePresent        bool
}

// Record is immutable released in-process evidence, not a durable audit record.
// Default formatting/serialization is guarded; deliberate causes remain sensitive.
type Record struct {
	private
	info    RecordInfo
	facts   recordFacts
	primary error
	cleanup error
}

func (record Record) Info() RecordInfo { return record.info }
func (record Record) Configuration() (Evidence, bool) {
	return record.facts.configuration, record.facts.configurationPresent
}
func (record Record) Source() (SourceEvidence, bool) {
	return record.facts.source, record.facts.sourcePresent
}
func (record Record) Primary() error { return record.primary }
func (record Record) Cleanup() error { return record.cleanup }
func (record Record) Err() error     { return errors.Join(record.primary, record.cleanup) }

// Result contains accepted configuration separately from released technical facts.
// State is nil on initial rejection. Successful publication remains available if
// only cleanup fails. The caller owns Records; there are no live source handles.
type Result[T any] struct {
	private
	State   *State[T]
	Records []Record
}

func takeRecords[T any](ctx context.Context, inbox *adapters.Inbox[T], provider string, project func(T) recordFacts) ([]Record, error) {
	if err := inbox.Seal(); err != nil {
		return nil, err
	}
	var records []Record
	for {
		delivery, err := inbox.NextReleased(ctx)
		if errors.Is(err, io.EOF) {
			return records, nil
		}
		if err != nil {
			return records, err
		}
		receipt, err := delivery.Receipt()
		if err != nil {
			_ = delivery.Retry()
			return records, err
		}
		snapshot, ready := receipt.Snapshot()
		info := snapshot.Info()
		if !ready || !info.Released {
			_ = delivery.Retry()
			return records, fail(ErrObservation, "records")
		}
		facts := recordFacts{}
		if value, present := snapshot.ValueCopy(); present {
			facts = project(value)
		}
		record := Record{info: RecordInfo{Provider: provider, Runtime: info.Runtime, Operation: info.Operation, ID: info.ID, Sequence: info.Sequence, Parent: info.Parent, Depth: info.Depth, ResourceName: info.Source.Name, ResourceGeneration: info.Source.Generation, Resolved: info.Resolved, Released: info.Released}, facts: facts, primary: snapshot.Primary(), cleanup: snapshot.Cleanup()}
		records = append(records, record)
		if err := delivery.Ack(); err != nil {
			return records, err
		}
	}
}
