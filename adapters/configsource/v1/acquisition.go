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

package configsource

import (
	"context"
	"slices"
	"unicode/utf8"
)

const (
	MaxDocuments  = 16
	MaxBatchBytes = 4 << 20
)

// Source is a selected original-document profile, not a universal provider API.
// Capture returns a whole batch or none; failedIndex=-1 means unknown/success.
// Concurrent captures are independent, not common-time transactions. Observe's
// context owns its lifetime, and the returned owner must remain reachable through
// cancellation and timed-out Close. Implementations must expose no usable prefix.
type Source interface {
	Capture(context.Context) (Batch, int, error)
	Observe(context.Context) (Observer, error)
}

// Observer owns a source observation path. Next cancellation affects only that
// wait/acquisition. Close stops and joins its work; a timeout retains ownership.
type Observer interface {
	Next(context.Context) (Observation, error)
	Close(context.Context) error
}

// Raw is deliberate sensitive data. Missing is positive absence, never a failed
// read. Content must be empty when Missing; present-empty is a distinct valid case.
type Raw struct {
	Content []byte
	Missing bool
}

// Batch retains immutable complete data. A zero Batch is invalid, not empty data.
// DocumentsCopy makes fresh bytes and containers on every call.
type Batch struct{ state *batchState }
type batchState struct{ documents []Raw }

// NewBatch checks count, UTF-8 and original-byte bounds before retaining copies.
// Input is borrowed only for the call; up to 16 documents, 1 MiB each/4 MiB total.
func NewBatch(documents []Raw) (Batch, error) {
	if len(documents) == 0 || len(documents) > MaxDocuments {
		return Batch{}, fail(ErrInput, "batch")
	}
	total := 0
	for _, document := range documents {
		if len(document.Content) > MaxDocumentBytes {
			return Batch{}, fail(ErrLimit, "batch")
		}
		if document.Missing && len(document.Content) != 0 || !utf8.Valid(document.Content) {
			return Batch{}, fail(ErrInput, "batch")
		}
		total += len(document.Content)
		if total > MaxBatchBytes {
			return Batch{}, fail(ErrLimit, "batch")
		}
	}
	result := make([]Raw, len(documents))
	for index, document := range documents {
		result[index] = Raw{Content: slices.Clone(document.Content), Missing: document.Missing}
	}
	return Batch{state: &batchState{documents: result}}, nil
}
func (value Batch) Valid() bool { return value.state != nil }
func (value Batch) Len() int {
	if value.state == nil {
		return 0
	}
	return len(value.state.documents)
}
func (value Batch) DocumentsCopy() ([]Raw, error) {
	if value.state == nil {
		return nil, fail(ErrInput, "batch")
	}
	result := make([]Raw, len(value.state.documents))
	for index, document := range value.state.documents {
		result[index] = Raw{Content: slices.Clone(document.Content), Missing: document.Missing}
	}
	return result, nil
}

// Observation describes source acquisition, not accepted settings. Err means no
// usable Batch; FailedIndex is -1 when unknown/success. Gap requires considering
// the entire selected set; the complete Batch already contains that reacquisition.
// A zero observation is invalid. Consumers must check Batch validity and count.
type Observation struct {
	Batch       Batch
	FailedIndex int
	Err         error
	Gap         bool
}
