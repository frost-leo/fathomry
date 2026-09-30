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

// Load acquires, strictly prepares and publishes one whole typed data domain.
// Source/bootstrap/read/schema failure returns no usable State. Optional adoption
// is reported separately by Status; success never claims all instances ready.
func Load[T any](ctx context.Context, declaration Declaration[T], dependencies Dependencies) (*State[T], error) {
	endpoint, err := bind(dependencies)
	if err != nil {
		return nil, err
	}
	var result *State[T]
	receipt, err := endpoint.Run(ctx, request("load"), func(call *adapters.Call[Evidence]) {
		evidence := Evidence{Observed: 1}
		plan, err := freeze(call.Context(), declaration, false)
		if err != nil {
			_ = call.Resolve(adapters.Outcome[Evidence]{Value: evidence, Present: true, Primary: err})
			return
		}
		observed := configsource.Observation{FailedIndex: -1}
		if plan.source != nil {
			observed.Batch, observed.FailedIndex, observed.Err = plan.source.Capture(call.Context())
		}
		prepared, err := prepare(call.Context(), plan, observed)
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
		result = state
		_ = call.Resolve(adapters.Outcome[Evidence]{Value: evidence, Present: true})
	})
	if err != nil {
		return nil, err
	}
	value, _ := receipt.Snapshot()
	if err := value.Err(); err != nil {
		return nil, err
	}
	return result, nil
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
			layers = append(layers, configsource.Layer{Kind: declared.Kind, Encoding: declared.Encoding, Content: document.Content})
		}
	}
	if plan.variables != nil {
		layers = append(layers, *plan.variables)
	}
	return configsource.Prepare(ctx, plan.schema, layers)
}
