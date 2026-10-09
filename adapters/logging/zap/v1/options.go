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
	logging "github.com/frost-leo/fathomry/adapters/logging/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	native "github.com/frost-leo/fathomry/internal/logging/zap/v1"
	"go.uber.org/zap/zapcore"
	"time"
)

const ProviderID = "logging.zap.v1"

// Settings is strict-loadable format 1. Nil selects native defaults; explicit
// zero/false remain present. Durations are nanoseconds. No output is implicit.
type Settings struct {
	Name           string         `json:"name" mapstructure:"name"`
	Version        uint32         `json:"format" mapstructure:"format"`
	Outputs        []Output       `json:"outputs" mapstructure:"outputs"`
	Structured     bool           `json:"structured" mapstructure:"structured"`
	ExtensionLevel *string        `json:"extension_level" mapstructure:"extension_level"`
	QueuedCalls    *int           `json:"queued_calls" mapstructure:"queued_calls"`
	Timeout        *time.Duration `json:"timeout_ns" mapstructure:"timeout_ns"`
	MaxEntryBytes  *int           `json:"max_entry_bytes" mapstructure:"max_entry_bytes"`
	Caller         *bool          `json:"caller" mapstructure:"caller"`
}

// Output configures a borrowed stdout/stderr or an owned Linux file directory.
// File bounds/permissions and synchronous rotation are the native profile.
type Output struct {
	Name         string  `json:"name" mapstructure:"name"`
	Kind         string  `json:"kind" mapstructure:"kind"`
	Level        *string `json:"level" mapstructure:"level"`
	Directory    string  `json:"directory" mapstructure:"directory"`
	MaxFileBytes *int64  `json:"max_file_bytes" mapstructure:"max_file_bytes"`
	MaxBackups   *int    `json:"max_backups" mapstructure:"max_backups"`
	Compress     *bool   `json:"compress" mapstructure:"compress"`
}

// StructuredSink is explicit trusted assembly authority. Write receives detached
// closed data, never JSON or an owning SDK object. Calls are synchronous and
// must terminate cooperatively; no fallback/retry/worker is installed. A sink
// must not recursively log, including from asynchronous evidence receivers.
// Sync has only the sink's explicitly documented meaning; Close is never called.
// Its destination/authority must remain unchanged while borrowed. A caller-owned
// custom sink is responsible for honoring that immutable dependency contract.
type StructuredSink interface {
	Write(context.Context, Record) error
	Sync(context.Context) error
}

// RuntimeCheck is implemented by compositions that borrow another public
// capability. It must reject unsafe nested roots on the same Runtime. The check
// is inert; an arbitrary custom sink remains trusted composition authority.
type RuntimeCheck interface{ CheckRuntime(*adapters.Runtime) error }

// Record is a detached structured sink observation. PC is the original ingress
// return PC; Caller follows native Zap frame semantics. Fields include reserved
// correlation attributes. Data remains caller-authorized, potentially sensitive.
type Record struct {
	private
	Time          time.Time
	PC            uintptr
	Level         zapcore.Level
	Message, Name string
	Caller        Caller
	Fields        []logging.Field
}
type Caller struct {
	Defined  bool
	PC       uintptr
	File     string
	Line     int
	Function string
}

// Dependencies borrows mechanisms and the optional structured sink. The latter
// must remain alive AND admissible until all logging ownership is joined.
type Dependencies struct {
	Runtime    *adapters.Runtime
	Evidence   *adapters.Inbox[Result]
	Observer   *adapters.Observer
	Structured StructuredSink
}

func Validate(value Settings) error { _, err := Prepare(value); return err }
func selectedValue[T any](value *T) (result T) {
	if value != nil {
		return *value
	}
	return
}
func nativeOptions(value Settings) native.OptionsV1 {
	result := native.OptionsV1{Name: value.Name, ExtensionLevel: selectedValue(value.ExtensionLevel), QueuedCalls: selectedValue(value.QueuedCalls), Timeout: selectedValue(value.Timeout), MaxEntryBytes: selectedValue(value.MaxEntryBytes), Caller: selectedValue(value.Caller)}
	for _, output := range value.Outputs {
		result.Outputs = append(result.Outputs, native.OutputV1{Name: output.Name, Kind: output.Kind, Level: selectedValue(output.Level), Directory: output.Directory, MaxFileBytes: selectedValue(output.MaxFileBytes), MaxBackups: selectedValue(output.MaxBackups), Compress: selectedValue(output.Compress)})
	}
	return result
}
