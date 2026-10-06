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

package redis

import (
	"github.com/frost-leo/fathomry/internal/resource"
	"time"
)

// Prepared is an immutable offline configuration and authoritative budget.
// Copies share frozen configuration, never mutable SDK options.
type Prepared struct {
	private
	configuration resource.Prepared[settings]
	metadata      Metadata
	password      *Password
}

// Metadata contains non-secret resolved limits, not measured heap/RSS or a
// distributed quota. WorkBytes covers one native root; EvidenceBytes covers
// each root/child. SourceBytes covers resident pools, wire buffers, topology,
// cache and bounded background work, even when no application call is active.
type Metadata struct {
	Limits                                resource.Limits
	WorkBytes, EvidenceBytes, SourceBytes int64
	MaxCommands, MaxArgs, MaxRequestBytes int
	Timeout, CloseTimeout                 time.Duration
}

func (prepared Prepared) Metadata() Metadata { return prepared.metadata }

func (value settings) metadata() Metadata {
	// Every open socket can retain a frame plus native/transport buffers. Native
	// topology and maintenance workers can decode replies outside user admission.
	resident := int64(value.MaxConnections) * (int64(3*value.MaxReplyBytes) + int64(value.MaxReplyElements)*1024 + 64<<10)
	resident += int64(value.poolCount())*(64<<10) + 1<<20
	if value.ExperimentalCache {
		resident += value.CacheBytes + int64(value.CacheEntries)*1024
	}
	if value.ExperimentalAutoPipeline {
		resident += int64(value.MaxActive) * int64(value.MaxRequestBytes+4096)
	}
	return Metadata{Limits: value.limits(), WorkBytes: value.reservation(), EvidenceBytes: value.evidenceReservation(),
		SourceBytes: resident, MaxCommands: value.MaxCommands, MaxArgs: value.MaxArgs, MaxRequestBytes: value.MaxRequestBytes,
		Timeout: value.Timeout, CloseTimeout: value.CloseTimeout}
}
