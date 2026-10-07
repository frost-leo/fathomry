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
	"errors"
	"strings"
	"unicode/utf8"

	"github.com/frost-leo/fathomry/internal/resource"
)

// Preparation freezes validated settings and their authoritative reservations.
// It performs no native I/O and may be reused concurrently. Zero is invalid.
type Preparation struct {
	private
	prepared resource.Prepared[settings]
	config   settings
	name     string
}

// Budget contains declared per-call, per-evidence and per-source envelopes.
// ReaderWorkBytes includes input, one chunk, one maximum-size lookahead row,
// metadata, terminal state and copying overhead. Each retained Next evidence
// separately consumes ReaderEvidenceBytes; the root reserves ReaderTerminalBytes
// before setup. SourceBytes includes the configured engine memory allowance and
// control overhead, NOT a hard RSS limit. Native materialization and transient
// oversized scalar decoding can exceed these declared Go retention envelopes.
type Budget struct {
	WorkBytes           int64
	EvidenceBytes       int64
	ReaderWorkBytes     int64
	ReaderEvidenceBytes int64
	ReaderTerminalBytes int64
	SourceBytes         int64
	Active              int
	Queued              int
	MaxLeases           int
}

// PrepareV1 defaults options once, then strictly resolves every authorized layer.
// Absent fields inherit; explicit zero is validated as zero, not re-defaulted.
// Construction and recommendations consume the same frozen preparation.
func PrepareV1(options OptionsV1, layers ...resource.Layer) (Preparation, error) {
	return prepareConfiguration(options.Name, defaults(options), layers)
}

// PrepareResolvedV1 validates options exactly as supplied, without interpreting
// zero as a request for defaults. Use it for typed settings whose defaults and
// strict overlays have already been resolved by the public configuration layer.
func PrepareResolvedV1(options OptionsV1) (Preparation, error) {
	return prepareConfiguration(options.Name, configured(options), nil)
}

func prepareConfiguration(name string, base settings, layers []resource.Layer) (Preparation, error) {
	if len(name) <= 64 {
		name = strings.Clone(name)
	}
	oversized := len(base.Path) > 1<<20
	if oversized {
		// Defaults beyond the preparation document bound cannot be repaired by
		// layers. Keep resource-owned validation order without copying the payload.
		if utf8.ValidString(base.Path) {
			base.Path = ""
		} else {
			base.Path = "\xff"
		}
		layers = nil
	}
	var resolved settings
	prepared, err := resource.Prepare(resource.Schema[settings]{Format: 1, Defaults: base, Validate: func(config settings) error {
		if oversized {
			return errors.New("source: defaults exceed size limit")
		}
		if err := validate(config); err != nil {
			return err
		}
		resolved = config
		return nil
	}}, resource.Input{Identity: resource.Identity{Provider: ProviderID, Name: name}, Format: 1, Layers: layers})
	if err != nil {
		return Preparation{}, err
	}
	return Preparation{prepared: prepared, config: resolved, name: name}, nil
}

// Options returns a detached, fully resolved value for deliberate inspection.
func (prepared Preparation) Options() OptionsV1 {
	config := prepared.config
	return OptionsV1{Name: prepared.name, Path: config.Path, Connections: config.Connections,
		QueuedCalls: config.QueuedCalls, Threads: config.Threads, MemoryBytes: config.MemoryBytes,
		Timeout: config.Timeout, CleanupTimeout: config.CleanupTimeout, MaxRows: config.MaxRows,
		MaxBatchRows: config.MaxBatchRows, InputBytes: config.InputBytes, ResultBytes: config.ResultBytes,
		ReaderChunkRows: config.ReaderChunkRows, ReaderChunkBytes: config.ReaderChunkBytes,
		ReaderTotalRows: config.ReaderTotalRows, ReaderTotalBytes: config.ReaderTotalBytes, ReaderLifetime: config.ReaderLifetime}
}

// Limits returns admission limits matching the exact resolved preparation.
func (prepared Preparation) Limits() resource.Limits {
	if prepared.name == "" {
		return resource.Limits{}
	}
	return prepared.config.limits()
}

// Budget returns reservations matching the exact resolved preparation.
func (prepared Preparation) Budget() Budget {
	if prepared.name == "" {
		return Budget{}
	}
	config := prepared.config
	return Budget{WorkBytes: config.reservation(), EvidenceBytes: config.evidenceReservation(),
		ReaderWorkBytes: config.readerReservation(), ReaderEvidenceBytes: config.readerEvidenceReservation(),
		ReaderTerminalBytes: config.readerEvidenceReservation(), SourceBytes: config.MemoryBytes + 256<<10,
		Active: config.Connections, Queued: config.QueuedCalls, MaxLeases: 2}
}
