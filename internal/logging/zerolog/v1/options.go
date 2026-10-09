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
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/frost-leo/fathomry/internal/compatibility"
	"github.com/frost-leo/fathomry/internal/resource"
)

const (
	MaxSinks          = 8
	MaxAttributes     = 64
	MaxDepth          = 8
	maxBackups        = 128
	maxBootstrapBytes = 1 << 20
)

// Level is severity only. Even Fatal and Panic never terminate, panic or close.
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

func (level Level) rank() int {
	switch level {
	case Trace:
		return 0
	case Debug:
		return 1
	case Info:
		return 2
	case Warn:
		return 3
	case Error:
		return 4
	case Fatal:
		return 5
	case Panic:
		return 6
	default:
		return -1
	}
}

// RecordWriter is an explicit, trusted structured-output boundary, not an SDK
// hook or an Observer. WriteRecord runs synchronously and must respect ctx, avoid
// reentry, and return bounded errors. Nil acknowledges only its own acceptance.
// A retained Record is immutable; retaining it, queueing/export and eventual
// delivery are the sink owner's separate bounded responsibilities. The owner
// must bound its native retries; this integration does not intercept them.
type RecordWriter interface {
	WriteRecord(context.Context, Record) error
}

// SinkV1 declares writer, record, managed-record or file output. Kind may be
// omitted by legacy Select when Writer, Records or File identifies it. PrepareV1
// accepts only inert declarations; live bindings are supplied to Prepared.Select.
// Borrowed handles are never flushed or closed. No output is implicit.
type SinkV1 struct {
	private
	Name     string
	Kind     string
	MinLevel Level
	Writer   io.Writer
	Records  RecordWriter
	File     *FileOptionsV1
}

// FileOptionsV1 selects an existing private dedicated directory. No discovery,
// directory creation, shared-directory writer or automatic crash repair is used.
// MaxBytes defaults to 8MiB (1KiB–64MiB); Backups defaults to 4 (1–128).
// Compress selects synchronous gzip for new archives. Rotation occurs before a
// record would exceed MaxBytes, or explicitly through Logger.Rotate.
type FileOptionsV1 struct {
	private
	Directory string
	MaxBytes  int64
	Backups   int
	Compress  bool
}

// OptionsV1 is process-local bootstrap borrowed during Select, not a durable DTO.
// MinLevel defaults to Info; MaxRecordBytes to 16KiB (1KiB–1MiB); Timeout to 5s
// (1ms–1min). QueuedCalls defaults to zero (reject overload), at most 64.
// One admitted operation serializes all sinks of an authoritative source.
// Supplied settings strings have an aggregate 1MiB pre-serialization ceiling;
// shared preparation separately enforces its encoded-document limit.
type OptionsV1 struct {
	private
	Name           string
	MinLevel       Level
	MaxRecordBytes int
	Timeout        time.Duration
	QueuedCalls    int
	// Caller includes supplied/captured business ingress information when true.
	Caller bool
	Sinks  []SinkV1
}

type fileSettings struct {
	Directory string `json:"directory"`
	MaxBytes  int64  `json:"max_bytes"`
	Backups   int    `json:"backups"`
	Compress  bool   `json:"compress"`
}
type sinkSettings struct {
	Name     string        `json:"name"`
	Kind     string        `json:"kind"`
	MinLevel string        `json:"min_level"`
	File     *fileSettings `json:"file"`
}
type settings struct {
	MinLevel       string         `json:"min_level"`
	MaxRecordBytes int            `json:"max_record_bytes"`
	Timeout        time.Duration  `json:"timeout_ns"`
	QueuedCalls    int            `json:"queued_calls"`
	Caller         bool           `json:"caller"`
	Sinks          []sinkSettings `json:"sinks"`
}
type borrowedSink struct {
	writer  io.Writer
	records RecordWriter
	managed ManagedRecordWriter
}

// This necessary byte bound precedes hashing/serialization, not semantic overlay
// validation. Defaults exceeding the shared document bound cannot be repaired.
func withinBootstrapBudget(options OptionsV1) bool {
	remaining := maxBootstrapBytes
	charge := func(value string) bool {
		if len(value) > remaining {
			return false
		}
		remaining -= len(value)
		return true
	}
	if !charge(string(options.MinLevel)) {
		return false
	}
	for _, sink := range options.Sinks {
		if !charge(sink.Name) || !charge(sink.Kind) || !charge(string(sink.MinLevel)) {
			return false
		}
		if sink.File != nil && !charge(sink.File.Directory) {
			return false
		}
	}
	return true
}

func defaults(options OptionsV1) settings {
	value := settings{MinLevel: string(options.MinLevel), MaxRecordBytes: options.MaxRecordBytes,
		Timeout: options.Timeout, QueuedCalls: options.QueuedCalls, Caller: options.Caller}
	if value.MinLevel == "" {
		value.MinLevel = string(Info)
	}
	if value.MaxRecordBytes == 0 {
		value.MaxRecordBytes = 16 << 10
	}
	if value.Timeout == 0 {
		value.Timeout = 5 * time.Second
	}
	for _, sink := range options.Sinks {
		item := sinkSettings{Name: sink.Name, Kind: sink.Kind, MinLevel: string(sink.MinLevel)}
		if item.MinLevel == "" {
			item.MinLevel = string(Trace)
		}
		if item.Kind == "" {
			switch {
			case sink.Writer != nil:
				item.Kind = "writer"
			case sink.Records != nil:
				item.Kind = "record"
			case sink.File != nil:
				item.Kind = "file"
			}
		}
		if sink.File != nil {
			item.File = &fileSettings{Directory: sink.File.Directory, MaxBytes: sink.File.MaxBytes,
				Backups: sink.File.Backups, Compress: sink.File.Compress}
			if item.File.MaxBytes == 0 {
				item.File.MaxBytes = 8 << 20
			}
			if item.File.Backups == 0 {
				item.File.Backups = 4
			}
		}
		value.Sinks = append(value.Sinks, item)
	}
	return value
}

func label(value string) bool {
	if len(value) == 0 || len(value) > 64 {
		return false
	}
	for _, char := range value {
		if !(char >= 'a' && char <= 'z' || char >= '0' && char <= '9' || char == '.' || char == '_' || char == '-') {
			return false
		}
	}
	return true
}

func nilHandle(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Pointer, reflect.Interface, reflect.Func, reflect.Map, reflect.Slice, reflect.Chan:
		return reflected.IsNil()
	}
	return false
}

func validate(value settings) error {
	if Level(value.MinLevel).rank() < 0 || value.MaxRecordBytes < 1024 || value.MaxRecordBytes > 1<<20 ||
		value.Timeout < time.Millisecond || value.Timeout > time.Minute || value.QueuedCalls < 0 || value.QueuedCalls > 64 ||
		len(value.Sinks) == 0 || len(value.Sinks) > MaxSinks {
		return failure(ErrInput, "options")
	}
	names, directories := map[string]bool{}, map[string]bool{}
	for _, sink := range value.Sinks {
		if !label(sink.Name) || names[sink.Name] || Level(sink.MinLevel).rank() < 0 {
			return failure(ErrInput, "sink")
		}
		names[sink.Name] = true
		switch sink.Kind {
		case "writer", "record", "managed-record":
			if sink.File != nil {
				return failure(ErrInput, "sink")
			}
		case "file":
			file := sink.File
			if file == nil || len(file.Directory) > 4096 || !utf8.ValidString(file.Directory) || strings.ContainsRune(file.Directory, 0) ||
				!filepath.IsAbs(file.Directory) || filepath.Clean(file.Directory) != file.Directory ||
				directories[file.Directory] || file.MaxBytes < int64(value.MaxRecordBytes) || file.MaxBytes > 64<<20 ||
				file.Backups < 1 || file.Backups > maxBackups {
				return failure(ErrInput, "file")
			}
			directories[file.Directory] = true
		default:
			return failure(ErrUnsupported, "sink")
		}
	}
	return nil
}

func (value settings) reservation() int64 {
	// Includes bounded native JSON expansion/copies and one sequential gzip
	// workspace. This is declared accounting, not a bound on the SDK global pool.
	return int64(value.MaxRecordBytes)*24 + 2<<20
}
func (value settings) evidenceReservation() int64 { return 64 << 10 }
func (value settings) limits() resource.Limits {
	return resource.Limits{Active: 1, Bytes: value.reservation(), Queued: value.QueuedCalls,
		QueuedBytes: int64(value.QueuedCalls) * value.reservation(), MaxLeases: 1}
}

// LimitsV1 derives bootstrap limits. Layered construction must use the exact
// final Metadata from PrepareV1 instead of reconstructing effective defaults.
func LimitsV1(options OptionsV1) resource.Limits { return defaults(options).limits() }

// EvidenceBytesV1 is the declared reservation per result, excluding arbitrary
// native error graphs. Injected sink owners must bound their original causes.
func EvidenceBytesV1() int64 { return 64 << 10 }

// Profile returns this logical policy's payload/path-free effective settings.
// Physical attribution remains unchanged in evidence; PolicyDescription reports
// the separate logical revision. This is not a durability/compatibility certificate.
func (logger *Logger) Profile() compatibility.Profile {
	if logger == nil || logger.owner == nil || logger.policy == nil {
		return compatibility.Profile{}
	}
	value := logger.policy.value
	options := []compatibility.Option{
		{Name: "min-level", Value: value.MinLevel}, {Name: "max-record-bytes", Value: strconv.Itoa(value.MaxRecordBytes)},
		{Name: "timeout-ns", Value: strconv.FormatInt(int64(value.Timeout), 10)}, {Name: "queued-calls", Value: strconv.Itoa(value.QueuedCalls)},
		{Name: "caller", Value: strconv.FormatBool(value.Caller)},
		{Name: "delivery", Value: "synchronous-provider-no-retry"}, {Name: "file-policy", Value: "exclusive-sequence-fail-stop"},
		{Name: "sdk-attempts", Value: "unobserved"},
	}
	for index, sink := range value.Sinks {
		prefix := "sink-" + strconv.Itoa(index) + "-"
		options = append(options, compatibility.Option{Name: prefix + "kind", Value: sink.Kind}, compatibility.Option{Name: prefix + "min-level", Value: sink.MinLevel})
		if sink.File != nil {
			options = append(options, compatibility.Option{Name: prefix + "max-bytes", Value: strconv.FormatInt(sink.File.MaxBytes, 10)},
				compatibility.Option{Name: prefix + "backups", Value: strconv.Itoa(sink.File.Backups)}, compatibility.Option{Name: prefix + "gzip", Value: strconv.FormatBool(sink.File.Compress)})
		}
	}
	return compatibility.Profile{ImplementationModule: compatibility.FrameworkModule, SDKMode: "bounded-json-synchronous",
		ServiceMode: compatibility.Fact{Kind: compatibility.NotApplicable}, ServiceVersion: compatibility.Fact{Kind: compatibility.NotApplicable},
		Protocol: compatibility.Fact{Kind: compatibility.Declared, Value: "json-lines"}, Native: compatibility.Fact{Kind: compatibility.NotApplicable}, Options: options}
}
