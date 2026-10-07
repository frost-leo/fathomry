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

package tlsclient

import (
	"context"
	"github.com/frost-leo/fathomry/adapters/httpclient/v1"
	native "github.com/frost-leo/fathomry/internal/httpclient/tlsclient/v1"
)

// Bandwidth is a source-generation aggregate from native TLS-over-TCP counters.
// It excludes cleartext, HTTP/3, proxy setup, outer proxy TLS/framing, and borrowed
// dialer traffic. It is not a request total or atomic read/write pair. Disabled or
// overflowed counters are unavailable; true zero is meaningful only in this scope.
type Bandwidth struct {
	private
	native     native.Bandwidth
	source     httpclient.Info
	generation uint64
}

func (value Bandwidth) Enabled() bool             { return value.native.Enabled() }
func (value Bandwidth) Scope() string             { return value.native.Scope() }
func (value Bandwidth) ReadBytes() (int64, bool)  { return value.native.ReadBytes() }
func (value Bandwidth) WriteBytes() (int64, bool) { return value.native.WriteBytes() }
func (value Bandwidth) Source() httpclient.Info   { return value.source.Clone() }
func (value Bandwidth) Generation() uint64        { return value.generation }

// Bandwidth inspects the currently selected source under a generation lease.
// Retired Handle observations remain attributable; no mutable tracker escapes.
func (client *Client) Bandwidth(ctx context.Context) (Bandwidth, error) {
	if client == nil || client.lifetime == nil || ctx == nil {
		return Bandwidth{}, fail(ErrInput, "bandwidth")
	}
	if ctx.Err() != nil {
		return Bandwidth{}, fail(ErrState, "bandwidth", ctx.Err(), context.Cause(ctx))
	}
	if client.lifetime.Err() != nil {
		return Bandwidth{}, fail(ErrState, "bandwidth", client.lifetime.Err(), context.Cause(client.lifetime))
	}
	handle := client.direct
	var generation uint64
	if client.source != nil {
		lease, err := client.source.Acquire(ctx)
		if err != nil {
			return Bandwidth{}, err
		}
		defer lease.Release()
		handle, err = lease.Value()
		if err != nil {
			return Bandwidth{}, err
		}
		generation = lease.Generation()
	}
	result, err := handle.Bandwidth()
	result.generation = generation
	return result, err
}

// Bandwidth is a read-only observation and remains valid after source shutdown.
func (handle Handle) Bandwidth() (Bandwidth, error) {
	if handle.state == nil || handle.state.inspection == nil {
		return Bandwidth{}, fail(ErrState, "bandwidth")
	}
	return Bandwidth{native: handle.state.inspection.Bandwidth(), source: info(handle.state.info)}, nil
}
