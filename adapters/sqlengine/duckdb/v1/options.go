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
	"context"
	"time"

	"github.com/frost-leo/fathomry/adapters/configsource/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	native "github.com/frost-leo/fathomry/internal/sqlengine/duckdb/v2"
)

// Dependencies borrows caller-owned admission and required evidence custody.
type Dependencies struct {
	Runtime  *adapters.Runtime
	Evidence *adapters.Inbox[Result]
	Observer *adapters.Observer
}

const (
	ProviderID  = native.ProviderID
	MaxSQLBytes = native.MaxSQLBytes
	MaxColumns  = native.MaxColumns
	MaxSteps    = native.MaxSteps
)

// Settings covers the local embedded profile. Durations use integer nanoseconds.
// Zero selects native defaults except QueuedCalls=0 disables queueing and an
// empty Path selects an isolated in-memory engine. Explicit JSON configuration
// serialization is intentional disclosure. Validate performs no I/O.
type Settings struct {
	Name             string        `json:"name" mapstructure:"name"`
	Path             string        `json:"path" mapstructure:"path"`
	Connections      int           `json:"connections" mapstructure:"connections"`
	QueuedCalls      int           `json:"queued_calls" mapstructure:"queued_calls"`
	Threads          int           `json:"threads" mapstructure:"threads"`
	MemoryBytes      int64         `json:"memory_bytes" mapstructure:"memory_bytes"`
	Timeout          time.Duration `json:"timeout_ns" mapstructure:"timeout_ns"`
	CleanupTimeout   time.Duration `json:"cleanup_timeout_ns" mapstructure:"cleanup_timeout_ns"`
	MaxRows          int           `json:"max_rows" mapstructure:"max_rows"`
	MaxBatchRows     int           `json:"max_batch_rows" mapstructure:"max_batch_rows"`
	InputBytes       int64         `json:"input_bytes" mapstructure:"input_bytes"`
	ResultBytes      int64         `json:"result_bytes" mapstructure:"result_bytes"`
	ReaderChunkRows  int           `json:"reader_chunk_rows" mapstructure:"reader_chunk_rows"`
	ReaderChunkBytes int64         `json:"reader_chunk_bytes" mapstructure:"reader_chunk_bytes"`
	ReaderTotalRows  int64         `json:"reader_total_rows" mapstructure:"reader_total_rows"`
	ReaderTotalBytes int64         `json:"reader_total_bytes" mapstructure:"reader_total_bytes"`
	ReaderLifetime   time.Duration `json:"reader_lifetime_ns" mapstructure:"reader_lifetime_ns"`
}

func options(value Settings) native.OptionsV1 {
	return native.OptionsV1{
		Name: value.Name, Path: value.Path, Connections: value.Connections, QueuedCalls: value.QueuedCalls,
		Threads: value.Threads, MemoryBytes: value.MemoryBytes, Timeout: value.Timeout, CleanupTimeout: value.CleanupTimeout,
		MaxRows: value.MaxRows, MaxBatchRows: value.MaxBatchRows, InputBytes: value.InputBytes, ResultBytes: value.ResultBytes,
		ReaderChunkRows: value.ReaderChunkRows, ReaderChunkBytes: value.ReaderChunkBytes,
		ReaderTotalRows: value.ReaderTotalRows, ReaderTotalBytes: value.ReaderTotalBytes, ReaderLifetime: value.ReaderLifetime,
	}
}

// Validate resolves and validates options without opening an engine or file.
func Validate(value Settings) error {
	_, err := native.PrepareV1(options(value))
	return translate(err, "validate")
}

// Configuration supplies the existing strict configuration loader with fully
// resolved defaults and a no-redefaulting validator. Absent fields inherit;
// explicit zero bounds reject, while zero queueing and empty Path remain valid.
// Direct Settings literals passed to Open/Validate retain zero-default semantics.
func Configuration(defaults Settings) (configsource.Schema[Settings], error) {
	prepared, err := native.PrepareV1(options(defaults))
	if err != nil {
		return configsource.Schema[Settings]{}, translate(err, "validate")
	}
	return configsource.Schema[Settings]{Version: 1, Defaults: settingsFor(prepared.Options()),
		Validate: func(_ context.Context, value Settings) error {
			_, err := native.PrepareResolvedV1(options(value))
			return translate(err, "validate")
		}}, nil
}

func settingsFor(value native.OptionsV1) Settings {
	return Settings{Name: value.Name, Path: value.Path, Connections: value.Connections, QueuedCalls: value.QueuedCalls,
		Threads: value.Threads, MemoryBytes: value.MemoryBytes, Timeout: value.Timeout, CleanupTimeout: value.CleanupTimeout,
		MaxRows: value.MaxRows, MaxBatchRows: value.MaxBatchRows, InputBytes: value.InputBytes, ResultBytes: value.ResultBytes,
		ReaderChunkRows: value.ReaderChunkRows, ReaderChunkBytes: value.ReaderChunkBytes, ReaderTotalRows: value.ReaderTotalRows,
		ReaderTotalBytes: value.ReaderTotalBytes, ReaderLifetime: value.ReaderLifetime}
}
