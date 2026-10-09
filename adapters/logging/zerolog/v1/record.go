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
	logging "github.com/frost-leo/fathomry/adapters/logging/v1"
	native "github.com/frost-leo/fathomry/internal/logging/zerolog/v1"
	"log/slog"
	"slices"
	"time"
)

// Record is an immutable borrowed snapshot, not an owning SDK Event. Explicit
// accessors reveal caller-authorized content; formatting/JSON remain restricted.
// A sink retaining it owns its independent bounded storage/lifetime.
type Record struct {
	private
	data *recordData
}
type recordData struct {
	native native.Record
	fields []logging.Field
}

// Caller is detached original caller metadata; PC is the adjusted frame PC.
type Caller struct {
	Defined  bool
	PC       uintptr
	File     string
	Line     int
	Function string
}

func (record Record) Time() time.Time {
	if record.data == nil {
		return time.Time{}
	}
	return record.data.native.Time()
}
func (record Record) PC() uintptr {
	if record.data == nil {
		return 0
	}
	return record.data.native.PC()
}
func (record Record) Caller() Caller {
	if record.data == nil {
		return Caller{}
	}
	value := record.data.native.Caller()
	return Caller{Defined: value.Defined, PC: value.PC, File: value.File, Line: value.Line, Function: value.Function}
}
func (record Record) Level() Level {
	if record.data == nil {
		return ""
	}
	return Level(record.data.native.Level())
}
func (record Record) Message() string {
	if record.data == nil {
		return ""
	}
	return record.data.native.Message()
}
func (record Record) Source() SourceInfo {
	if record.data == nil {
		return SourceInfo{}
	}
	return info(record.data.native.Source())
}
func (record Record) Correlation() Correlation {
	if record.data == nil {
		return Correlation{}
	}
	value := record.data.native.Correlation()
	return Correlation{Call: value.Call, Parent: value.Parent, Owner: value.Owner}
}

// FieldsCopy copies the top-level field slice; closed Values expose only copied
// collection/byte accessors, never mutable SDK or caller-owned storage.
func (record Record) FieldsCopy() []logging.Field {
	if record.data == nil {
		return nil
	}
	return slices.Clone(record.data.fields)
}

// AttributesCopy uses explicit closed public values, preserving null/empty/width
// distinctions without arbitrary slog Any/formatter authority.
func (record Record) AttributesCopy() []slog.Attr {
	if record.data == nil {
		return nil
	}
	result := make([]slog.Attr, len(record.data.fields))
	for index, field := range record.data.fields {
		result[index] = Attribute(field.Key, field.Value)
	}
	return result
}

// JSONCopy returns a detached native JSON line; OTel composition never parses it.
func (record Record) JSONCopy() []byte {
	if record.data == nil {
		return nil
	}
	return record.data.native.JSONCopy()
}

func wrapRecord(value native.Record, limit int) (Record, error) {
	values, err := native.AttributesValues(value.AttributesCopy(), limit)
	if err != nil {
		return Record{}, translate(err, "record")
	}
	fields := make([]logging.Field, len(values))
	for index, field := range values {
		fields[index] = logging.Field{Key: field.Key, Value: publicValue(field.Value)}
	}
	return Record{data: &recordData{native: value, fields: fields}}, nil
}

// RecordState is explicit managed-target classification, not a retry decision.
type RecordState string

const (
	RecordAccepted RecordState = "accepted"
	RecordRejected RecordState = "rejected"
	RecordStopped  RecordState = "stopped"
)

// RecordOutcome requires nil Err only for Accepted. Rejected preserves a failed
// event while allowing later independent events; Stopped latches destination
// failure. Unknown/malformed acknowledgements are conservatively stopped.
type RecordOutcome struct {
	private
	State RecordState
	Err   error
}
