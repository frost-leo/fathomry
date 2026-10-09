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

package zerolog

import (
	"context"
	"encoding/json"
	"github.com/frost-leo/fathomry/adapters/configsource/v1"
	native "github.com/frost-leo/fathomry/internal/logging/zerolog/v1"
	source "github.com/frost-leo/fathomry/internal/resource"
)

// Prepared freezes one final inert native selection and its complete envelope.
type Prepared struct {
	private
	native   native.Prepared
	metadata native.Metadata
}

// Prepare obtains defaults from Native, then preserves every explicit pointer
// value in a final frozen selection. No dependency, file or native event is acquired.
func Prepare(value Settings) (Prepared, error) {
	if value.Version != 1 {
		return Prepared{}, fail(ErrInput, "prepare")
	}
	bounded, err := configsource.Prepare(context.Background(), configsource.Schema[Settings]{Version: 1, Defaults: value}, nil)
	if err != nil {
		return Prepared{}, translate(err, "prepare")
	}
	frozen, err := bounded.ValueCopy()
	if err != nil {
		return Prepared{}, translate(err, "prepare")
	}
	initial, err := native.PrepareV1(nativeOptions(frozen))
	if err != nil {
		return Prepared{}, translate(err, "prepare")
	}
	effective := initial.Options()
	if frozen.MinLevel != nil {
		effective.MinLevel = native.Level(*frozen.MinLevel)
	}
	if frozen.MaxRecordBytes != nil {
		effective.MaxRecordBytes = *frozen.MaxRecordBytes
	}
	if frozen.Timeout != nil {
		effective.Timeout = *frozen.Timeout
	}
	if frozen.QueuedCalls != nil {
		effective.QueuedCalls = *frozen.QueuedCalls
	}
	if frozen.Caller != nil {
		effective.Caller = *frozen.Caller
	}
	sinks := make([]map[string]any, len(effective.Sinks))
	for index, sink := range effective.Sinks {
		requested := frozen.Sinks[index]
		if requested.MinLevel != nil {
			sink.MinLevel = native.Level(*requested.MinLevel)
		}
		var file any
		if sink.File != nil {
			selected := *sink.File
			if requested.File != nil {
				if requested.File.MaxBytes != nil {
					selected.MaxBytes = *requested.File.MaxBytes
				}
				if requested.File.Backups != nil {
					selected.Backups = *requested.File.Backups
				}
				if requested.File.Compress != nil {
					selected.Compress = *requested.File.Compress
				}
			}
			file = map[string]any{"directory": selected.Directory, "max_bytes": selected.MaxBytes, "backups": selected.Backups, "compress": selected.Compress}
		}
		sinks[index] = map[string]any{"name": sink.Name, "kind": sink.Kind, "min_level": sink.MinLevel, "file": file}
	}
	encoded, err := json.Marshal(map[string]any{"min_level": effective.MinLevel, "max_record_bytes": effective.MaxRecordBytes, "timeout_ns": effective.Timeout, "queued_calls": effective.QueuedCalls, "caller": effective.Caller, "sinks": sinks})
	if err != nil {
		return Prepared{}, translate(err, "prepare")
	}
	final, err := native.PrepareV1(nativeOptions(frozen), source.Layer{Kind: source.Local, Content: encoded})
	if err != nil {
		return Prepared{}, translate(err, "prepare")
	}
	prepared := Prepared{native: final, metadata: final.Metadata()}
	if _, err := prepared.Policy(); err != nil {
		return Prepared{}, err
	}
	return prepared, nil
}

// PhysicalEquivalent admits only source/sink threshold changes, never dependency
// substitution, encoding, caller, limits, directories or file-maintenance changes.
func (prepared Prepared) PhysicalEquivalent(other Prepared) bool {
	return prepared.native.PhysicalEquivalent(other.native)
}
