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

package framework

import (
	"context"

	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/resource/v1"
)

// Options composes existing public ceilings without an all-SDK settings union.
type Options struct {
	Operations adapters.Options `json:"operations"`
	Resources  resource.Options `json:"resources"`
}

// Runtime owns exactly one public operation runtime and resource scope. Copies
// share authority. It owns no service registry, source selection or settings data.
type Runtime struct {
	private
	state *runtimeState
}
type runtimeState struct {
	operations *adapters.Runtime
	resources  *resource.Scope
	cancel     context.CancelCauseFunc
}

// New prepares local composition only. Service construction remains explicit
// resource bindings; source bootstrap and accepted settings remain separate.
func New(ctx context.Context, options Options) (*Runtime, error) {
	if ctx == nil {
		return nil, fail(ErrOptions, "new")
	}
	lifetime, cancel := context.WithCancelCause(ctx)
	operations, err := adapters.New(lifetime, options.Operations)
	if err != nil {
		cancel(nil)
		return nil, err
	}
	resources, err := resource.New(lifetime, options.Resources)
	if err != nil {
		cancel(nil)
		cleanup := operations.Close(context.Background())
		if cleanup != nil {
			return nil, fail(ErrClose, "new", err, cleanup)
		}
		return nil, err
	}
	return &Runtime{state: &runtimeState{operations: operations, resources: resources, cancel: cancel}}, nil
}

// Operations grants explicit public operation assembly, never a native client.
func (runtime *Runtime) Operations() *adapters.Runtime {
	if runtime == nil || runtime.state == nil {
		return nil
	}
	return runtime.state.operations
}

// Resources grants public typed binding/adoption; it is the existing holder.
func (runtime *Runtime) Resources() *resource.Scope {
	if runtime == nil || runtime.state == nil {
		return nil
	}
	return runtime.state.resources
}

// Close seals/cancels both owners, joins operation work before instance cleanup,
// and preserves both error graphs. Expiration retains the same Runtime. Receivers
// are separate custody owners; Finish them after producer shutdown.
func (runtime *Runtime) Close(ctx context.Context) error {
	if runtime == nil || runtime.state == nil {
		return fail(ErrHandle, "close")
	}
	if ctx == nil {
		return fail(ErrOptions, "close")
	}
	runtime.state.cancel(nil)
	operationError := runtime.state.operations.Close(ctx)
	resourceError := runtime.state.resources.Close(ctx)
	if operationError != nil || resourceError != nil {
		return fail(ErrClose, "close", operationError, resourceError)
	}
	return nil
}
