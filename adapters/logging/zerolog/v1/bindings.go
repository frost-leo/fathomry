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
	native "github.com/frost-leo/fathomry/internal/logging/zerolog/v1"
	"io"
	"reflect"
)

func nilBinding(value any) bool {
	if value == nil {
		return true
	}
	candidate := reflect.ValueOf(value)
	switch candidate.Kind() {
	case reflect.Pointer, reflect.Interface, reflect.Map, reflect.Slice, reflect.Func, reflect.Chan:
		return candidate.IsNil()
	}
	return false
}
func (prepared Prepared) bindings(deps Dependencies) (native.BindingsV1, []RuntimeCheck, error) {
	if len(deps.Writers)+len(deps.Records)+len(deps.ManagedRecords) > native.MaxSinks {
		return native.BindingsV1{}, nil, fail(ErrLimit, "bindings")
	}
	result := native.BindingsV1{}
	var checks []RuntimeCheck
	if deps.Writers != nil {
		result.Writers = make(map[string]io.Writer, len(deps.Writers))
	}
	for name, writer := range deps.Writers {
		if nilBinding(writer) {
			return native.BindingsV1{}, nil, fail(ErrInput, "writer")
		}
		result.Writers[name] = writer
		if check, ok := writer.(RuntimeCheck); ok {
			checks = append(checks, check)
		}
	}
	if deps.Records != nil {
		result.Records = make(map[string]native.RecordWriter, len(deps.Records))
	}
	for name, writer := range deps.Records {
		if nilBinding(writer) {
			return native.BindingsV1{}, nil, fail(ErrInput, "record-writer")
		}
		result.Records[name] = recordBridge{writer: writer, limit: prepared.metadata.MaxRecordBytes}
		if check, ok := writer.(RuntimeCheck); ok {
			checks = append(checks, check)
		}
	}
	if deps.ManagedRecords != nil {
		result.ManagedRecords = make(map[string]native.ManagedRecordWriter, len(deps.ManagedRecords))
	}
	for name, writer := range deps.ManagedRecords {
		if nilBinding(writer) {
			return native.BindingsV1{}, nil, fail(ErrInput, "managed-record-writer")
		}
		result.ManagedRecords[name] = managedBridge{writer: writer, limit: prepared.metadata.MaxRecordBytes}
		if check, ok := writer.(RuntimeCheck); ok {
			checks = append(checks, check)
		}
	}
	return result, checks, nil
}

type recordBridge struct {
	writer RecordWriter
	limit  int
}

func (bridge recordBridge) WriteRecord(ctx context.Context, value native.Record) error {
	record, err := wrapRecord(value, bridge.limit)
	if err != nil {
		return err
	}
	return bridge.writer.WriteRecord(ctx, record)
}

type managedBridge struct {
	writer ManagedRecordWriter
	limit  int
}

func (bridge managedBridge) WriteManagedRecord(ctx context.Context, value native.Record) native.RecordOutcome {
	record, err := wrapRecord(value, bridge.limit)
	if err != nil {
		return native.RecordOutcome{State: native.RecordRejected, Err: err}
	}
	outcome := bridge.writer.WriteManagedRecord(ctx, record)
	return native.RecordOutcome{State: native.RecordState(outcome.State), Err: outcome.Err}
}
