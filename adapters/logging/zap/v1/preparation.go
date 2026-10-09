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

package zap

import (
	"context"
	"encoding/json"
	"github.com/frost-leo/fathomry/adapters/configsource/v1"
	native "github.com/frost-leo/fathomry/internal/logging/zap/v1"
	source "github.com/frost-leo/fathomry/internal/resource"
)

// Prepared freezes one exact native selection and its preconstruction envelope.
type Prepared struct {
	private
	native     native.Prepared
	metadata   native.Metadata
	structured bool
}

// Prepare is inert: no sink, file, goroutine or network acquisition.
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
	initial, err := native.PrepareV1(nativeOptions(frozen), frozen.Structured)
	if err != nil {
		return Prepared{}, translate(err, "prepare")
	}
	effective := initial.Options()
	// Native owns defaults; explicit pointer values overwrite even zero/false.
	if frozen.ExtensionLevel != nil {
		effective.ExtensionLevel = *frozen.ExtensionLevel
	}
	if frozen.QueuedCalls != nil {
		effective.QueuedCalls = *frozen.QueuedCalls
	}
	if frozen.Timeout != nil {
		effective.Timeout = *frozen.Timeout
	}
	if frozen.MaxEntryBytes != nil {
		effective.MaxEntryBytes = *frozen.MaxEntryBytes
	}
	if frozen.Caller != nil {
		effective.Caller = *frozen.Caller
	}
	outputs := make([]map[string]any, len(effective.Outputs))
	for index, output := range effective.Outputs {
		requested := frozen.Outputs[index]
		if requested.Level != nil {
			output.Level = *requested.Level
		}
		if requested.MaxFileBytes != nil {
			output.MaxFileBytes = *requested.MaxFileBytes
		}
		if requested.MaxBackups != nil {
			output.MaxBackups = *requested.MaxBackups
		}
		if requested.Compress != nil {
			output.Compress = *requested.Compress
		}
		outputs[index] = map[string]any{"name": output.Name, "kind": output.Kind, "level": output.Level, "directory": output.Directory, "max_file_bytes": output.MaxFileBytes, "max_backups": output.MaxBackups, "compress": output.Compress}
	}
	encoded, err := json.Marshal(map[string]any{"outputs": outputs, "extension_level": effective.ExtensionLevel, "queued_calls": effective.QueuedCalls, "timeout_ns": effective.Timeout, "max_entry_bytes": effective.MaxEntryBytes, "caller": effective.Caller})
	if err != nil {
		return Prepared{}, translate(err, "prepare")
	}
	final, err := native.PrepareV1(nativeOptions(frozen), frozen.Structured, source.Layer{Kind: source.Local, Content: encoded})
	if err != nil {
		return Prepared{}, translate(err, "prepare")
	}
	prepared := Prepared{native: final, metadata: final.Metadata(), structured: frozen.Structured}
	if _, err := prepared.Policy(); err != nil {
		return Prepared{}, err
	}
	return prepared, nil
}

// PhysicalEquivalent permits thresholds only; dependencies remain the original
// physical sink's. Names, ordering, queues, formatting and file policy cannot change.
func (prepared Prepared) PhysicalEquivalent(other Prepared) bool {
	return prepared.native.PhysicalEquivalent(other.native)
}
