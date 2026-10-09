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
	"io"
	"time"

	"github.com/frost-leo/fathomry/adapters/v1"
	native "github.com/frost-leo/fathomry/internal/logging/zerolog/v1"
)

// Level represents severity only; Fatal and Panic never perform terminal actions.
type Level string

const (
	Trace Level = "trace"
	Debug Level = "debug"
	Info  Level = "info"
	Warn  Level = "warn"
	Error Level = "error"
	Fatal Level = "fatal"
	Panic Level = "panic"
)
const ProviderID = "logging.zerolog.v1"

// Settings is strict-loadable format1. Nil pointers select native defaults;
// explicit zero/false remain present. Durations are integer nanoseconds.
type Settings struct {
	Name           string         `json:"name" mapstructure:"name"`
	Version        uint32         `json:"format" mapstructure:"format"`
	MinLevel       *Level         `json:"min_level" mapstructure:"min_level"`
	MaxRecordBytes *int           `json:"max_record_bytes" mapstructure:"max_record_bytes"`
	Timeout        *time.Duration `json:"timeout_ns" mapstructure:"timeout_ns"`
	QueuedCalls    *int           `json:"queued_calls" mapstructure:"queued_calls"`
	Caller         *bool          `json:"caller" mapstructure:"caller"`
	Sinks          []Sink         `json:"sinks" mapstructure:"sinks"`
}

// Sink explicitly declares writer, record, managed-record or owned file output.
// Managed-record alone opts into qualified independent-event recovery.
type Sink struct {
	Name     string `json:"name" mapstructure:"name"`
	Kind     string `json:"kind" mapstructure:"kind"`
	MinLevel *Level `json:"min_level" mapstructure:"min_level"`
	File     *File  `json:"file" mapstructure:"file"`
}

// File selects an existing private dedicated directory and synchronous native
// size/count/gzip policy. It is not a durable-write or crash-repair promise.
type File struct {
	Directory string `json:"directory" mapstructure:"directory"`
	MaxBytes  *int64 `json:"max_bytes" mapstructure:"max_bytes"`
	Backups   *int   `json:"backups" mapstructure:"backups"`
	Compress  *bool  `json:"compress" mapstructure:"compress"`
}

// RecordWriter is the legacy conservative borrowed record boundary. Any error
// stops this destination. Its resources, retained records and lifetime remain
// caller-owned; neither Sync nor Close is called.
type RecordWriter interface {
	WriteRecord(context.Context, Record) error
}

// ManagedRecordWriter explicitly reports independent-event or stopped-target
// outcomes. It is trusted bounded synchronous assembly authority, never a worker
// or retry policy. A malformed outcome is conservatively stopped.
type ManagedRecordWriter interface {
	WriteManagedRecord(context.Context, Record) RecordOutcome
}

// RuntimeCheck is an inert, bounded composition check against the actual calling
// Runtime. It must reject unsafe nested-root use; it must not acquire resources.
type RuntimeCheck interface{ CheckRuntime(*adapters.Runtime) error }

// Dependencies contains borrowed explicit authority, separate from Settings.
// Maps are borrowed until Open returns, then selected bindings are captured.
// Output authority/destinations must remain unchanged and alive while borrowed.
type Dependencies struct {
	Runtime        *adapters.Runtime
	Evidence       *adapters.Inbox[Result]
	Observer       *adapters.Observer
	Writers        map[string]io.Writer
	Records        map[string]RecordWriter
	ManagedRecords map[string]ManagedRecordWriter
}

func Validate(value Settings) error { _, err := Prepare(value); return err }
func selectedValue[T any](value *T) (result T) {
	if value != nil {
		return *value
	}
	return
}
func nativeOptions(value Settings) native.OptionsV1 {
	result := native.OptionsV1{Name: value.Name, MinLevel: native.Level(selectedValue(value.MinLevel)), MaxRecordBytes: selectedValue(value.MaxRecordBytes), Timeout: selectedValue(value.Timeout), QueuedCalls: selectedValue(value.QueuedCalls), Caller: selectedValue(value.Caller)}
	for _, sink := range value.Sinks {
		entry := native.SinkV1{Name: sink.Name, Kind: sink.Kind, MinLevel: native.Level(selectedValue(sink.MinLevel))}
		if sink.File != nil {
			entry.File = &native.FileOptionsV1{Directory: sink.File.Directory, MaxBytes: selectedValue(sink.File.MaxBytes), Backups: selectedValue(sink.File.Backups), Compress: selectedValue(sink.File.Compress)}
		}
		result.Sinks = append(result.Sinks, entry)
	}
	return result
}
