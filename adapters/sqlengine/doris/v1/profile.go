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

import (
	"context"
	"github.com/frost-leo/fathomry/adapters/sqlengine/v1"
	"github.com/frost-leo/fathomry/internal/invocation"
	native "github.com/frost-leo/fathomry/internal/sqlengine/doris/v1"
)

// Info returns detached native preparation metadata, never readiness.
func (owner *Owner) Info() sqlengine.Info { return owner.Handle().Info() }
func (handle Handle) Info() sqlengine.Info {
	if handle.state == nil {
		return sqlengine.Info{}
	}
	report := handle.state.assembly.Snapshot()
	if len(report.Sources) != 1 {
		return sqlengine.Info{}
	}
	return info(report.Sources[0].Info)
}

// Profile borrows the current generation for one local metadata read. No SQL,
// readiness probe or operation reservation is performed; active cursors stay pinned.
func (client *Client) Profile(ctx context.Context) (sqlengine.Profile, error) {
	if client == nil || client.lifetime == nil || ctx == nil {
		return sqlengine.Profile{}, fail(ErrInput, "profile")
	}
	if ctx.Err() != nil {
		return sqlengine.Profile{}, fail(ErrState, "profile", ctx.Err(), context.Cause(ctx))
	}
	if client.lifetime.Err() != nil {
		return sqlengine.Profile{}, fail(ErrState, "profile", client.lifetime.Err(), context.Cause(client.lifetime))
	}
	handle := client.direct
	if client.source != nil {
		lease, err := client.source.Acquire(ctx)
		if err != nil {
			return sqlengine.Profile{}, err
		}
		defer lease.Release()
		handle, err = lease.Value()
		if err != nil {
			return sqlengine.Profile{}, err
		}
	}
	if handle.state == nil {
		return sqlengine.Profile{}, fail(ErrState, "profile")
	}
	release, err := handle.state.use()
	if err != nil {
		return sqlengine.Profile{}, err
	}
	defer release()
	inbox, err := invocation.NewInbox[native.Result](1, handle.state.policy.Budget.EvidenceBytes)
	if err != nil {
		return sqlengine.Profile{}, translate(err, "profile")
	}
	inspector, err := native.Bind(handle.state.assembly, handle.state.selection, inbox, nil)
	if err != nil {
		return sqlengine.Profile{}, translate(err, "profile")
	}
	value := inspector.Profile()
	result := sqlengine.Profile{ImplementationModule: value.ImplementationModule, SDKMode: value.SDKMode,
		ServiceMode:    sqlengine.Fact{Kind: string(value.ServiceMode.Kind), Value: value.ServiceMode.Value},
		ServiceVersion: sqlengine.Fact{Kind: string(value.ServiceVersion.Kind), Value: value.ServiceVersion.Value},
		Protocol:       sqlengine.Fact{Kind: string(value.Protocol.Kind), Value: value.Protocol.Value},
		Native:         sqlengine.Fact{Kind: string(value.Native.Kind), Value: value.Native.Value}}
	for _, option := range value.Options {
		result.Options = append(result.Options, sqlengine.Option{Name: option.Name, Value: option.Value})
	}
	return result, nil
}
