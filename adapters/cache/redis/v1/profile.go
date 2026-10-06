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

import "context"

// Stats is a detached, payload-free snapshot, not server capacity/effect evidence.
type Stats struct {
	Hits, Misses, Timeouts uint32
	Total, Idle, Stale     uint32
	Sockets, PendingDials  int
	CacheHits, CacheMisses uint64
	CacheEntries           int
	CacheBytes             int64
	AutomaticBatches       uint64
	AutomaticMaxWidth      uint32
}
type Option struct{ Name, Value string }

// Profile reports declared configuration, not observed server/module support.
// ServiceVersion remains unknown until independent qualification; SDKVersion is
// the selected dependency, not an observed Redis server version.
type Profile struct {
	private
	SDKVersion, SDKMode, ServiceMode, Protocol string
	ServiceVersion                             string
	options                                    []Option
}

func (profile Profile) Options() []Option { return append([]Option(nil), profile.options...) }
func (client *Client) inspect(ctx context.Context, run func(*sourceState)) error {
	if client == nil || ctx == nil {
		return fail(ErrInput, "inspect")
	}
	handle := client.direct
	if client.source != nil {
		lease, err := client.source.Acquire(ctx)
		if err != nil {
			return err
		}
		defer lease.Release()
		handle, err = lease.Value()
		if err != nil {
			return err
		}
	}
	if handle.state == nil {
		return fail(ErrState, "inspect")
	}
	release, err := handle.state.use()
	if err != nil {
		return err
	}
	defer release()
	run(handle.state)
	return nil
}
func (client *Client) Stats(ctx context.Context) (Stats, error) {
	var stats Stats
	err := client.inspect(ctx, func(state *sourceState) {
		value := state.diagnostics.Stats()
		stats = Stats{Hits: value.Hits, Misses: value.Misses, Timeouts: value.Timeouts, Total: value.Total, Idle: value.Idle, Stale: value.Stale,
			Sockets: value.Sockets, PendingDials: value.PendingDials, CacheHits: value.CacheHits, CacheMisses: value.CacheMisses, CacheEntries: value.CacheEntries, CacheBytes: value.CacheBytes,
			AutomaticBatches: value.AutomaticBatches, AutomaticMaxWidth: value.AutomaticMaxWidth}
	})
	return stats, err
}
func (client *Client) Profile(ctx context.Context) (Profile, error) {
	var profile Profile
	err := client.inspect(ctx, func(state *sourceState) {
		value := state.diagnostics.Profile()
		profile = Profile{SDKVersion: "v9.22.0", SDKMode: value.SDKMode, ServiceMode: value.ServiceMode.Value, Protocol: value.Protocol.Value}
		for _, option := range value.Options {
			profile.options = append(profile.options, Option{Name: option.Name, Value: option.Value})
		}
	})
	return profile, err
}
