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
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/frost-leo/fathomry/internal/compatibility"
	"github.com/frost-leo/fathomry/internal/resource"
	"go.uber.org/zap/zapcore"
)

// OutputV1 selects one JSON destination. Name is a non-secret technical label,
// unique within this source. Kind is stdout, stderr or file. Level defaults to
// info and accepts debug/info/warn/error. Streams are borrowed, never closed.
type OutputV1 struct {
	private
	Name  string
	Kind  string
	Level string
	// Directory is an existing, dedicated absolute directory, required for file.
	// Its owner must be the effective user, with no group/other permission bits.
	// The Linux local-filesystem profile exclusively locks its inode. No other
	// writer or external rotator may mutate its contents, ancestors or aliases.
	Directory string
	// MaxFileBytes defaults to 10 MiB, range MaxEntryBytes–64 MiB.
	MaxFileBytes int64
	// MaxBackups defaults to 5, range 1–64. Retention deletes oldest archives
	// only after a successful rotation. No age/time trigger is implemented.
	MaxBackups int
	// Compress selects synchronous gzip. False retains uncompressed archives.
	Compress bool
}

// OptionsV1 is the version-1 bootstrap contract, not a durable DTO. Select
// borrows it during preparation, then freezes independently copied settings.
type OptionsV1 struct {
	private
	Name    string
	Outputs []OutputV1
	// ExtensionLevel defaults to info. It applies only to the explicit StructuredSink.
	ExtensionLevel string
	// QueuedCalls defaults to zero (reject overload), range 0–64. One admitted
	// call per source serializes all destinations, including across aliases.
	QueuedCalls int
	// Timeout bounds admission and cooperative execution separately. Default
	// 5 s, range 1 ms–1 min. It cannot interrupt a blocked file/stream syscall.
	Timeout time.Duration
	// MaxEntryBytes bounds each encoded JSON record before writer entry.
	// Default 64 KiB, range 1 KiB–1 MiB. Input bounds also apply before encoding.
	MaxEntryBytes int
	// Caller opts into source file/function information; it can disclose paths.
	Caller bool
}

type outputSettings struct {
	Name         string `json:"name"`
	Kind         string `json:"kind"`
	Level        string `json:"level"`
	Directory    string `json:"directory"`
	MaxFileBytes int64  `json:"max_file_bytes"`
	MaxBackups   int    `json:"max_backups"`
	Compress     bool   `json:"compress"`
}
type settings struct {
	Outputs        []outputSettings `json:"outputs"`
	ExtensionLevel string           `json:"extension_level"`
	QueuedCalls    int              `json:"queued_calls"`
	Timeout        time.Duration    `json:"timeout_ns"`
	MaxEntryBytes  int              `json:"max_entry_bytes"`
	Caller         bool             `json:"caller"`
}

func defaults(options OptionsV1) settings {
	value := settings{ExtensionLevel: options.ExtensionLevel, QueuedCalls: options.QueuedCalls,
		Timeout: options.Timeout, MaxEntryBytes: options.MaxEntryBytes, Caller: options.Caller}
	if value.ExtensionLevel == "" {
		value.ExtensionLevel = "info"
	}
	if value.Timeout == 0 {
		value.Timeout = 5 * time.Second
	}
	if value.MaxEntryBytes == 0 {
		value.MaxEntryBytes = 64 << 10
	}
	for _, output := range options.Outputs {
		sink := outputSettings{Name: output.Name, Kind: output.Kind, Level: output.Level, Directory: output.Directory,
			MaxFileBytes: output.MaxFileBytes, MaxBackups: output.MaxBackups, Compress: output.Compress}
		if sink.Level == "" {
			sink.Level = "info"
		}
		if sink.Kind == "file" {
			if sink.MaxFileBytes == 0 {
				sink.MaxFileBytes = 10 << 20
			}
			if sink.MaxBackups == 0 {
				sink.MaxBackups = 5
			}
		}
		value.Outputs = append(value.Outputs, sink)
	}
	return value
}

func label(value string) bool {
	if value == "" || len(value) > 64 {
		return false
	}
	for _, char := range value {
		if !(char >= 'a' && char <= 'z' || char >= '0' && char <= '9' || strings.ContainsRune("._-", char)) {
			return false
		}
	}
	return true
}
func level(value string) (zapcore.Level, bool) {
	switch value {
	case "debug":
		return zapcore.DebugLevel, true
	case "info":
		return zapcore.InfoLevel, true
	case "warn":
		return zapcore.WarnLevel, true
	case "error":
		return zapcore.ErrorLevel, true
	default:
		return 0, false
	}
}
func validate(value settings, extension bool) error {
	count := len(value.Outputs)
	if extension {
		count++
	}
	_, validLevel := level(value.ExtensionLevel)
	if count < 1 || count > MaxSinks || !validLevel || value.QueuedCalls < 0 || value.QueuedCalls > 64 ||
		value.Timeout < time.Millisecond || value.Timeout > time.Minute ||
		value.MaxEntryBytes < 1024 || value.MaxEntryBytes > 1<<20 {
		return failure(ErrInput, "options")
	}
	names := map[string]bool{"structured": true}
	directories := make(map[string]bool)
	streams := make(map[string]bool)
	for _, output := range value.Outputs {
		_, validLevel := level(output.Level)
		if !label(output.Name) || names[output.Name] || !validLevel {
			return failure(ErrInput, "output")
		}
		names[output.Name] = true
		switch output.Kind {
		case "stdout", "stderr":
			if streams[output.Kind] || output.Directory != "" || output.MaxFileBytes != 0 || output.MaxBackups != 0 || output.Compress {
				return failure(ErrInput, "stream")
			}
			streams[output.Kind] = true
		case "file":
			if runtime.GOOS != "linux" {
				return failure(ErrUnsupported, "file-platform")
			}
			if !filepath.IsAbs(output.Directory) || filepath.Clean(output.Directory) != output.Directory ||
				len(output.Directory) > 4096 || !utf8.ValidString(output.Directory) || strings.ContainsRune(output.Directory, 0) ||
				directories[output.Directory] || output.MaxFileBytes < int64(value.MaxEntryBytes) || output.MaxFileBytes > 64<<20 ||
				output.MaxBackups < 1 || output.MaxBackups > 64 {
				return failure(ErrInput, "file")
			}
			directories[output.Directory] = true
		default:
			return failure(ErrUnsupported, "output")
		}
	}
	return nil
}

func (settings) reservation() int64         { return 4 << 20 }
func (settings) evidenceReservation() int64 { return 16 << 10 }
func (value settings) limits() resource.Limits {
	return resource.Limits{Active: 1, Queued: value.QueuedCalls, Bytes: value.reservation(),
		QueuedBytes: int64(value.QueuedCalls) * value.reservation(), MaxLeases: 1}
}

// LimitsV1 returns the defaulted bootstrap policy. After overlays change
// QueuedCalls, composition must supply the corresponding resolved policy.
func LimitsV1(options OptionsV1) resource.Limits {
	return (settings{QueuedCalls: options.QueuedCalls}).limits()
}

// Profile returns an isolated non-sensitive projection of actual frozen settings.
// Extension behavior/build/lifecycle require separate owner-supplied evidence.
func (logger *Logger) Profile() compatibility.Profile {
	if logger == nil || logger.owner == nil {
		return compatibility.Profile{}
	}
	value := logger.owner.settings
	options := []compatibility.Option{
		{Name: "encoding", Value: "json"}, {Name: "sampling", Value: "off"}, {Name: "buffering", Value: "off"},
		{Name: "queued-calls", Value: strconv.Itoa(value.QueuedCalls)}, {Name: "timeout-ns", Value: strconv.FormatInt(int64(value.Timeout), 10)},
		{Name: "max-entry-bytes", Value: strconv.Itoa(value.MaxEntryBytes)}, {Name: "caller", Value: strconv.FormatBool(value.Caller)},
		{Name: "structured-sink", Value: strconv.FormatBool(logger.owner.extension != nil)}, {Name: "extension-level", Value: value.ExtensionLevel},
	}
	for index, output := range value.Outputs {
		prefix := "sink-" + strconv.Itoa(index) + "-"
		options = append(options, compatibility.Option{Name: prefix + "kind", Value: output.Kind}, compatibility.Option{Name: prefix + "level", Value: output.Level})
		if output.Kind == "file" {
			options = append(options, compatibility.Option{Name: prefix + "bytes", Value: strconv.FormatInt(output.MaxFileBytes, 10)},
				compatibility.Option{Name: prefix + "backups", Value: strconv.Itoa(output.MaxBackups)},
				compatibility.Option{Name: prefix + "gzip", Value: strconv.FormatBool(output.Compress)})
		}
	}
	profile := compatibility.Profile{ImplementationModule: compatibility.FrameworkModule, SDKMode: "zap-synchronous-json",
		ServiceMode: compatibility.Fact{Kind: compatibility.NotApplicable}, ServiceVersion: compatibility.Fact{Kind: compatibility.NotApplicable},
		Protocol: compatibility.Fact{Kind: compatibility.Declared, Value: "json-lines"}, Native: compatibility.Fact{Kind: compatibility.NotApplicable}, Options: options}
	if logger.owner.extension != nil {
		profile.ServiceMode, profile.ServiceVersion, profile.Protocol, profile.Native = compatibility.Fact{}, compatibility.Fact{}, compatibility.Fact{}, compatibility.Fact{}
	}
	return profile
}
