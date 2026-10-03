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

package configuration

import (
	"context"

	configsource "github.com/frost-leo/fathomry/adapters/configsource/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
)

// Load owns one complete configuration scenario over an explicitly declared
// provider. It joins acquired owners before returning and transfers released
// records even on failure. A cleanup error never erases accepted State.
func Load[T any](ctx context.Context, declaration Declaration[T], dependencies Dependencies) (result Result[T], err error) {
	if ctx == nil {
		return result, fail(ErrDeclaration, "load")
	}
	selected, err := freeze(ctx, declaration, dependencies.Provider, false)
	if err != nil {
		return result, err
	}
	scenario, err := newScenario(ctx)
	if err != nil {
		return result, err
	}
	defer func() {
		var cleanup error
		result.Records, cleanup = scenario.finish(context.WithoutCancel(ctx))
		if result.State != nil {
			result.State.state.mu.Lock()
			result.State.state.status.Released = true
			result.State.state.mu.Unlock()
		}
		if cleanup != nil {
			err = fail(ErrCleanup, "load_cleanup", err, cleanup)
		}
	}()
	endpoint, err := scenario.endpoint()
	if err != nil {
		return result, err
	}
	receipt, err := endpoint.Run(ctx, request("load"), func(call *adapters.Call[Evidence]) {
		evidence := Evidence{Observed: 1}
		if err := scenario.open(call.Context(), dependencies.Provider); err != nil {
			evidence.Rejected = 1
			_ = call.Resolve(adapters.Outcome[Evidence]{Value: evidence, Present: true, Primary: fail(ErrSource, "open", err)})
			return
		}
		selected.source = scenario.binding.source
		observed := configsource.Observation{FailedIndex: -1}
		if selected.source != nil {
			observed.Batch, observed.FailedIndex, observed.Err = selected.source.Capture(call.Context())
		}
		prepared, err := prepare(call.Context(), selected, observed)
		if err != nil {
			evidence.Rejected = 1
			_ = call.Resolve(adapters.Outcome[Evidence]{Value: evidence, Present: true, Primary: err})
			return
		}
		snapshot, err := prepared.Snapshot()
		if err != nil {
			_ = call.Resolve(adapters.Outcome[Evidence]{Value: evidence, Present: true, Primary: err})
			return
		}
		state := newState[T]()
		state.state.status.Observed = 1
		state.state.mu.Lock()
		err = state.state.publish(call.Context(), 1, prepared, snapshot)
		state.state.mu.Unlock()
		if err != nil {
			_ = call.Resolve(adapters.Outcome[Evidence]{Value: evidence, Present: true, Primary: err})
			return
		}
		state.adopt(call.Context(), dependencies.Resources)
		state.state.mu.Lock()
		state.state.status.Closed = true
		evidence = state.state.status.Evidence
		state.state.mu.Unlock()
		result.State = state
		_ = call.Resolve(adapters.Outcome[Evidence]{Value: evidence, Present: true})
	})
	if err != nil {
		return result, err
	}
	snapshot, _ := receipt.Snapshot()
	if cleanup := snapshot.Cleanup(); cleanup != nil {
		return result, fail(ErrCleanup, "load", snapshot.Primary(), cleanup)
	}
	return result, snapshot.Primary()
}
func prepare[T any](ctx context.Context, plan plan[T], observation configsource.Observation) (configsource.Prepared[T], error) {
	if observation.Err != nil {
		return configsource.Prepared[T]{}, at(ErrSource, "acquire", observation.FailedIndex, observation.Err)
	}
	var layers []configsource.Layer
	if len(plan.layers) > 0 {
		if !observation.Batch.Valid() || observation.Batch.Len() != len(plan.layers) {
			return configsource.Prepared[T]{}, fail(ErrObservation, "acquire")
		}
		documents, err := observation.Batch.DocumentsCopy()
		if err != nil {
			return configsource.Prepared[T]{}, err
		}
		for index, declared := range plan.layers {
			document := documents[index]
			if document.Missing {
				if declared.Optional {
					continue
				}
				return configsource.Prepared[T]{}, at(ErrMissing, "prepare", index)
			}
			layers = append(layers, configsource.Layer{Kind: configsource.LayerKind(declared.Kind), Encoding: configsource.Encoding(declared.Encoding), Content: document.Content})
		}
	}
	if plan.variables != nil {
		layers = append(layers, *plan.variables)
	}
	return configsource.Prepare(ctx, plan.schema, layers)
}
