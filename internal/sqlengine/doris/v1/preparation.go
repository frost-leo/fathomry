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

package doris

import "github.com/frost-leo/fathomry/internal/resource"

// Preparation freezes one validated resolution, including strict overlays.
// Selection and Reservation describe the same settings; no service I/O occurs.
type Preparation struct {
	private
	prepared resource.Prepared[settings]
	resolved settings
}

// Reservation describes declared bytes, not RSS. Source budgets cover local
// ownership metadata; WorkBytes includes one page plus bounded lookahead.
// EvidenceBytes covers each independently retained finite/page/terminal result.
type Reservation struct {
	WorkBytes, EvidenceBytes             int64
	SourceWorkBytes, SourceEvidenceBytes int64
	Limits                               resource.Limits
}

// PrepareV1 is the authoritative resolution used by Select and public policy.
// Typed zero bounds select defaults; explicit zero overlays are validated as zero.
func PrepareV1(options OptionsV1, layers ...resource.Layer) (Preparation, error) {
	var resolved settings
	prepared, err := resource.Prepare(resource.Schema[settings]{Format: 1, Defaults: defaults(options),
		Validate: func(value settings) error {
			if err := validate(value); err != nil {
				return err
			}
			resolved = value
			return nil
		}}, resource.Input{Identity: resource.Identity{Provider: ProviderID, Name: options.Name}, Format: 1, Layers: layers})
	if err != nil {
		return Preparation{}, err
	}
	return Preparation{prepared: prepared, resolved: resolved}, nil
}

func (value Preparation) Reservation() Reservation {
	if value.prepared.Description().Format == 0 {
		return Reservation{}
	}
	return Reservation{WorkBytes: value.resolved.reservation(), EvidenceBytes: value.resolved.evidenceReservation(),
		SourceWorkBytes: 1 << 20, SourceEvidenceBytes: 64 << 10, Limits: value.resolved.limits()}
}

// Description returns detached provenance, without configuration secrets.
func (value Preparation) Description() resource.Description { return value.prepared.Description() }

// OptionsCopy deliberately exposes sensitive resolved settings in a detached copy.
func (value Preparation) OptionsCopy() OptionsV1 {
	s := value.resolved
	return OptionsV1{Name: value.prepared.Description().Identity.Name,
		SQLAddress: s.SQLAddress, SQLServerName: s.SQLServerName, HTTPOrigins: append([]string(nil), s.HTTPOrigins...),
		Database: s.Database, User: s.User, Password: s.Password, RootCAPEM: s.RootCAPEM, Plaintext: s.Plaintext,
		Active: s.Active, Queued: s.Queued, Timeout: s.Timeout, MaxBatchBytes: s.MaxBatchBytes, MaxRows: s.MaxRows,
		MaxResultBytes: s.MaxResultBytes, MaxPacketBytes: s.MaxPacketBytes, MaxResponseBytes: s.MaxResponseBytes,
		MaxHTTPResponseBytes: s.MaxHTTPResponseBytes, CursorTimeout: s.CursorTimeout,
		MaxPageRows: s.MaxPageRows, MaxPageBytes: s.MaxPageBytes, MaxCursorRows: s.MaxCursorRows}
}
