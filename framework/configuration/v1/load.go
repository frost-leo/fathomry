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
	"sort"

	source "github.com/frost-leo/fathomry/adapters/configsource/v1"
	"github.com/frost-leo/fathomry/internal/resource"
)

// Load admits all declarations before finite I/O and starts no observers.
// Capture owners join before return even if cooperative acquisition is canceled.
func Load[T any](ctx context.Context, schema Schema[T], plan Plan) (Snapshot[T], error) {
	if nilValue(ctx) {
		return Snapshot[T]{}, fail(ErrValue)
	}
	prepared, err := admit(schema, plan, false)
	if err != nil {
		return Snapshot[T]{}, err
	}
	batches := make([]source.Batch, len(prepared.inputs))
	for index, input := range prepared.inputs {
		if ctx.Err() != nil {
			return Snapshot[T]{}, fail(ErrPreparation, ctx.Err(), context.Cause(ctx))
		}
		batch, err := input.source.Capture(ctx)
		if err != nil {
			return Snapshot[T]{}, err
		}
		batches[index] = batch
	}
	if ctx.Err() != nil {
		return Snapshot[T]{}, fail(ErrPreparation, ctx.Err(), context.Cause(ctx))
	}
	snapshot, err := prepared.prepare(batches)
	if ctx.Err() != nil {
		return Snapshot[T]{}, fail(ErrPreparation, err, ctx.Err(), context.Cause(ctx))
	}
	return snapshot, err
}
func (input *admitted[T]) prepare(batches []source.Batch) (Snapshot[T], error) {
	if len(batches) != len(input.inputs) {
		return Snapshot[T]{}, fail(ErrValue)
	}
	layers := append([]resource.Layer(nil), input.variables...)
	var supplied []SuppliedLayer
	for index, selected := range input.inputs {
		if nilValue(batches[index]) {
			return Snapshot[T]{}, fail(ErrValue)
		}
		metadata := batches[index].Documents()
		if len(metadata) != len(selected.documents) {
			return Snapshot[T]{}, fail(ErrPlan)
		}
		for _, document := range selected.documents {
			raw, presence, err := batches[index].RawCopy(document.Document)
			if err != nil {
				return Snapshot[T]{}, err
			}
			supplied = append(supplied, SuppliedLayer{Source: selected.description.Name, Document: document.Document, Layer: document.Layer, Presence: presence})
			switch presence {
			case source.Missing:
				if !document.Optional {
					return Snapshot[T]{}, fail(ErrMissing)
				}
			case source.Present:
				if err := resource.CheckDocumentFormat(raw, input.version); err != nil {
					return Snapshot[T]{}, fail(ErrFormat)
				}
				layers = append(layers, resource.Layer{Kind: resource.LayerKind(document.Layer) + resource.Defaults, Content: raw})
			default:
				return Snapshot[T]{}, fail(ErrValue)
			}
		}
	}
	sort.Slice(supplied, func(left, right int) bool { return supplied[left].Layer < supplied[right].Layer })
	var validationError error
	prepared, err := resource.PrepareData(resource.Schema[envelope[T]]{Format: input.version, Defaults: input.defaults, Validate: func(value envelope[T]) error {
		// Validate Core before arbitrary validator mutation of its isolated copy.
		if err := validateCore(value.Framework); err != nil {
			return err
		}
		if input.validate != nil {
			validationError = input.validate(Settings[T]{Framework: value.Framework, Project: value.Project})
			return validationError
		}
		return nil
	}}, layers)
	if err != nil {
		return Snapshot[T]{}, fail(ErrPreparation, validationError)
	}
	description := prepared.Description()
	result := Description{FormatVersion: input.version, Revision: description.Revision, Sources: supplied}
	names := map[resource.LayerKind]string{resource.Defaults: "defaults", resource.Base: "base", resource.Environment: "environment", resource.Local: "local", resource.Variables: "variables"}
	for _, layer := range description.Provenance {
		info := Provenance{Layer: names[layer.Kind]}
		for _, field := range layer.Fields {
			if field != "/format" {
				info.Fields = append(info.Fields, field)
			}
		}
		result.Provenance = append(result.Provenance, info)
	}
	return Snapshot[T]{state: &snapshotState[T]{prepared: prepared, description: result}}, nil
}
