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

package duckdb

import (
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	bindings "github.com/duckdb/duckdb-go-bindings"
	"github.com/frost-leo/fathomry/internal/compatibility"
	"github.com/frost-leo/fathomry/internal/resource"
)

// OptionsV1 is explicitly supplied bootstrap configuration, not a DSN.
// Path empty means a new isolated in-memory database; otherwise it authorizes
// opening/creating that absolute native database file and its native WAL.
// The caller owns file removal and must coordinate all users of a persistent file.
// No extension installation, spill directory or external I/O is authorized.
type OptionsV1 struct {
	private
	Name string
	Path string
	// Connections defaults to 1 (1–8); QueuedCalls defaults to 0 (0–64).
	Connections int
	QueuedCalls int
	// Threads defaults to 1 (1–32). MemoryBytes defaults to 256 MiB
	// (16 MiB–64 GiB); this is the engine setting, NOT a process memory cap.
	Threads     int
	MemoryBytes int64
	// Timeout defaults to 30s; CleanupTimeout to 5s. Range: 1ms–5min.
	// Deadlines are cooperative, including native calls without cancellation.
	Timeout        time.Duration
	CleanupTimeout time.Duration
	// MaxRows defaults to 8192 (1–65536), across the entire call.
	MaxRows int
	// MaxBatchRows defaults to 65536 (1–65536), across all input batches.
	MaxBatchRows int
	// InputBytes and ResultBytes count scalar payloads plus conservative Go
	// container overhead, not native/driver memory. Defaults: 8 MiB / 4 MiB;
	// each may be 1 KiB–64 MiB. SQL and result metadata are included.
	InputBytes  int64
	ResultBytes int64
}

type settings struct {
	Path           string        `json:"path"`
	Connections    int           `json:"connections"`
	QueuedCalls    int           `json:"queued_calls"`
	Threads        int           `json:"threads"`
	MemoryBytes    int64         `json:"memory_bytes"`
	Timeout        time.Duration `json:"timeout_ns"`
	CleanupTimeout time.Duration `json:"cleanup_timeout_ns"`
	MaxRows        int           `json:"max_rows"`
	MaxBatchRows   int           `json:"max_batch_rows"`
	InputBytes     int64         `json:"input_bytes"`
	ResultBytes    int64         `json:"result_bytes"`
}

func defaults(options OptionsV1) settings {
	config := settings{Path: options.Path, Connections: options.Connections, QueuedCalls: options.QueuedCalls,
		Threads: options.Threads, MemoryBytes: options.MemoryBytes, Timeout: options.Timeout,
		CleanupTimeout: options.CleanupTimeout, MaxRows: options.MaxRows, MaxBatchRows: options.MaxBatchRows,
		InputBytes: options.InputBytes, ResultBytes: options.ResultBytes}
	if config.Connections == 0 {
		config.Connections = 1
	}
	if config.Threads == 0 {
		config.Threads = 1
	}
	if config.MemoryBytes == 0 {
		config.MemoryBytes = 256 << 20
	}
	if config.Timeout == 0 {
		config.Timeout = 30 * time.Second
	}
	if config.CleanupTimeout == 0 {
		config.CleanupTimeout = 5 * time.Second
	}
	if config.MaxRows == 0 {
		config.MaxRows = 8192
	}
	if config.MaxBatchRows == 0 {
		config.MaxBatchRows = 65536
	}
	if config.InputBytes == 0 {
		config.InputBytes = 8 << 20
	}
	if config.ResultBytes == 0 {
		config.ResultBytes = 4 << 20
	}
	return config
}

func validText(value string, limit int, empty bool) bool {
	return (empty || value != "") && len(value) <= limit && utf8.ValidString(value) && !strings.ContainsRune(value, 0)
}

func validate(config settings) error {
	if config.Path != "" && (!filepath.IsAbs(config.Path) || filepath.Clean(config.Path) != config.Path ||
		!validText(config.Path, 4096, false) || strings.ContainsAny(config.Path, "?#%\r\n")) {
		return failure(ErrInput, "path")
	}
	if config.Connections < 1 || config.Connections > 8 || config.QueuedCalls < 0 || config.QueuedCalls > 64 ||
		config.Threads < 1 || config.Threads > 32 || config.MemoryBytes < 16<<20 || config.MemoryBytes > 64<<30 ||
		config.MaxRows < 1 || config.MaxRows > 65536 || config.MaxBatchRows < 1 || config.MaxBatchRows > 65536 ||
		config.InputBytes < 1024 || config.InputBytes > 64<<20 || config.ResultBytes < 1024 || config.ResultBytes > 64<<20 {
		return failure(ErrInput, "options")
	}
	for _, duration := range []time.Duration{config.Timeout, config.CleanupTimeout} {
		if duration < time.Millisecond || duration > 5*time.Minute {
			return failure(ErrInput, "timeout")
		}
	}
	return nil
}

func (config settings) dsn() string {
	values := url.Values{
		"threads": {strconv.Itoa(config.Threads)}, "memory_limit": {strconv.FormatInt(config.MemoryBytes, 10) + "B"},
		"autoload_known_extensions":    {"false"},
		"autoinstall_known_extensions": {"false"}, "allow_community_extensions": {"false"},
		"allow_unsigned_extensions": {"false"}, "allow_persistent_secrets": {"false"},
		"temp_directory": {""}, "max_temp_directory_size": {"0B"},
	}
	return config.Path + "?" + values.Encode()
}

func (config settings) reservation() int64 {
	return 2*config.InputBytes + 2*config.ResultBytes + 256<<10
}
func (config settings) evidenceReservation() int64 {
	return config.InputBytes + config.ResultBytes + 128<<10
}
func (config settings) limits() resource.Limits {
	return resource.Limits{Active: config.Connections, Queued: config.QueuedCalls,
		Bytes: int64(config.Connections) * config.reservation(), QueuedBytes: int64(config.QueuedCalls) * config.reservation(), MaxLeases: 1}
}

// LimitsV1 returns the admission policy for defaulted, unoverridden options.
// Overlays that change bounds require a matching composition-owned policy.
func LimitsV1(options OptionsV1) resource.Limits { return defaults(options).limits() }

// Profile returns effective non-secret settings, not a compatibility certificate.
// Driver/bindings/platform build facts belong to compatibility.Inspect. The core
// version is observed from the linked C library; no extension artifact is selected.
func (database *Database) Profile() compatibility.Profile {
	if database == nil || database.owner == nil {
		return compatibility.Profile{}
	}
	config := database.owner.config
	target := "memory"
	if config.Path != "" {
		target = "native-file"
	}
	return compatibility.Profile{ImplementationModule: compatibility.FrameworkModule, SDKMode: "native-finite-scalar-v1",
		ServiceMode:    compatibility.Fact{Kind: compatibility.Declared, Value: target},
		ServiceVersion: compatibility.Fact{Kind: compatibility.NotApplicable},
		Protocol:       compatibility.Fact{Kind: compatibility.NotApplicable},
		Native:         compatibility.Fact{Kind: compatibility.Observed, Value: bindings.LibraryVersion()},
		Options: []compatibility.Option{
			{Name: "connections", Value: strconv.Itoa(config.Connections)}, {Name: "threads", Value: strconv.Itoa(config.Threads)},
			{Name: "memory-bytes", Value: strconv.FormatInt(config.MemoryBytes, 10)}, {Name: "queued-calls", Value: strconv.Itoa(config.QueuedCalls)},
			{Name: "timeout-ns", Value: strconv.FormatInt(int64(config.Timeout), 10)}, {Name: "cleanup-timeout-ns", Value: strconv.FormatInt(int64(config.CleanupTimeout), 10)},
			{Name: "max-rows", Value: strconv.Itoa(config.MaxRows)}, {Name: "max-batch-rows", Value: strconv.Itoa(config.MaxBatchRows)},
			{Name: "input-bytes", Value: strconv.FormatInt(config.InputBytes, 10)}, {Name: "result-bytes", Value: strconv.FormatInt(config.ResultBytes, 10)},
			{Name: "external-access", Value: "disabled"}, {Name: "spill", Value: "disabled"}, {Name: "extensions", Value: "disabled"},
			{Name: "arrow-api", Value: "unsupported"}, {Name: "iceberg-extension", Value: "unselected"},
		}}
}
