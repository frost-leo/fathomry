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

	sdk "github.com/duckdb/duckdb-go/v2"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
)

// Source is an opaque native database ownership token for resource composition.
type Source struct {
	private
	owner *owner
}

type owner struct {
	config    settings
	connector *sdk.Connector
}

// Select freezes configuration without opening files or constructing native state.
// Assembly opens the database synchronously. Initialization/close cannot forcibly
// time out. Each admitted call owns a fresh connection to this database; there is
// no database/sql retry loop, ambient SQL pool, or provider background worker.
func Select(options OptionsV1, layers ...resource.Layer) (resource.Selection[Source], error) {
	prepared, err := resource.Prepare(resource.Schema[settings]{Format: 1, Defaults: defaults(options), Validate: validate},
		resource.Input{Identity: resource.Identity{Provider: ProviderID, Name: options.Name}, Format: 1, Layers: layers})
	if err != nil {
		return resource.Selection[Source]{}, err
	}
	return resource.Select(prepared, func(ctx context.Context, config settings) (resource.Resource[Source], error) {
		work, cancel, err := (invocation.Budget{Limit: config.Timeout}).Context(ctx, invocation.Establish)
		if err != nil {
			return resource.Resource[Source]{}, err
		}
		defer cancel()
		connector, err := sdk.NewConnector(config.dsn(), nil)
		if err != nil {
			return resource.Resource[Source]{}, failure(ErrNative, "open", err)
		}
		owned := &owner{config: config, connector: connector}
		initErr := owned.initialize(work)
		return resource.Resource[Source]{Acquired: true, Capability: Source{owner: owned}, Release: owned.close},
			joined(ErrNative, "open", initErr, work.Err(), context.Cause(work))
	}), nil
}

func (owned *owner) initialize(ctx context.Context) (err error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	native, err := owned.connector.Connect(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := native.Close(); closeErr != nil {
			err = joined(ErrCleanup, "initialization-close", err, closeErr)
		}
	}()
	conn := native.(*sdk.Conn)
	// Setting temp_directory after disabling external access is rejected by
	// core. The SDK iterates DSN options in map order. Seal only after opening
	// with spill disabled, before any capability can be bound or caller SQL run.
	for _, query := range []string{"SET enable_external_access=false", "SET lock_configuration=true"} {
		if _, err := conn.ExecContext(ctx, query, nil); err != nil {
			return err
		}
	}
	return nil
}

func (owned *owner) close(ctx context.Context) resource.ReleaseResult {
	if err := ctx.Err(); err != nil {
		return resource.ReleaseResult{Err: failure(ErrCleanup, "database-close", err, context.Cause(ctx)), Continue: owned.close}
	}
	err := owned.connector.Close()
	return resource.ReleaseResult{Quiescent: true, Released: true, Err: joined(ErrCleanup, "database-close", err)}
}

// Database is a concurrent non-owning facade. Its copies share the original
// resource admission and independent evidence inbox. It exposes no native handles.
type Database struct {
	private
	owner    *owner
	access   *resource.Access
	inbox    *invocation.Inbox[Result]
	observer *invocation.Observer
}

// Bind requires caller-owned receipt capacity and a sufficient original resource
// reservation. Borrowed/delegated selections retain that original policy.
func Bind(assembly *resource.Assembly, selection resource.Selection[Source], inbox *invocation.Inbox[Result], observer *invocation.Observer) (*Database, error) {
	source, _, err := resource.Bind(assembly, selection)
	if err != nil {
		return nil, err
	}
	access, err := resource.AccessFor(assembly, selection)
	if err != nil {
		return nil, err
	}
	if source.owner == nil || inbox == nil {
		return nil, failure(ErrInput, "bind")
	}
	config, limits := source.owner.config, access.Limits()
	if limits.Active > config.Connections || limits.Bytes < config.reservation() ||
		limits.Queued > config.QueuedCalls || limits.Queued > 0 && limits.QueuedBytes < config.reservation() {
		return nil, failure(ErrInput, "limits")
	}
	return &Database{owner: source.owner, access: access, inbox: inbox, observer: observer}, nil
}
