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
	"slices"
	"sync"

	configsource "github.com/frost-leo/fathomry/adapters/configsource/v1"
	"github.com/frost-leo/fathomry/resource/v1"
	"github.com/frost-leo/fathomry/settings/v1"
)

// Evidence counts configuration observations/decisions, not SDK requests or
// adopted instances. Accepted snapshots and required custody remain independent.
// AdoptionFailed reports immediate Apply refusal only; inspect the adoption
// receipt for asynchronous constructor failures.
type Evidence struct {
	Observed       uint64
	Accepted       uint64
	Rejected       uint64
	Superseded     uint64
	AdoptionFailed bool
}

// Status separates latest source health, preparation, publication and requested
// resource adoption. Published is the accepted local observation sequence, not an
// ordered preparation revision. Gap means history was coalesced or dropped.
type Status struct {
	Evidence
	Published        uint64
	Gap              bool
	Closed           bool
	Released         bool
	SourceError      error
	PreparationError error
	AdoptionError    error
	Adoption         *resource.Update
}

// Accepted captures one coherent value/revision/view. It does not certify source
// health at a later time or readiness of independently adopting instances.
type Accepted[T any] struct {
	private
	prepared configsource.Prepared[T]
	view     settings.View
	sequence uint64
	_        typeIdentity[T]
}
type typeIdentity[T any] [0]func() T

// State owns one accepted-data domain, not a native source or second resource
// holder. Reader preserves the project's original root type, without an envelope.
// Capture returns value and revision together; separate status/Reader calls are
// not a transaction. Readable last-good data survives Watch shutdown.
type State[T any] struct {
	private
	state *state[T]
	_     typeIdentity[T]
}
type state[T any] struct {
	mu      sync.Mutex
	store   settings.Store[T]
	current Accepted[T]
	status  Status
}

func newState[T any]() *State[T] { return &State[T]{state: &state[T]{store: settings.NewStore[T]()}} }
func (value *State[T]) Reader() settings.Reader {
	if value == nil || value.state == nil {
		return settings.Reader{}
	}
	return value.state.store.Reader()
}
func (value *State[T]) Capture() (Accepted[T], error) {
	if value == nil || value.state == nil {
		return Accepted[T]{}, fail(ErrHandle, "capture")
	}
	value.state.mu.Lock()
	defer value.state.mu.Unlock()
	if value.state.current.sequence == 0 {
		_, err := value.state.store.Reader().Capture()
		return Accepted[T]{}, err
	}
	return value.state.current, nil
}
func (value *State[T]) Status() (Status, error) {
	if value == nil || value.state == nil {
		return Status{}, fail(ErrHandle, "status")
	}
	value.state.mu.Lock()
	defer value.state.mu.Unlock()
	return value.state.status, nil
}

// Description contains detached schema/provenance facts, not source credentials.
type Description struct {
	Version  uint32
	Revision string
	Layers   []LayerInfo
}
type LayerInfo struct {
	Kind   LayerKind
	Fields []string
}

func (value Accepted[T]) ValueCopy() (T, error) { return value.prepared.ValueCopy() }
func (value Accepted[T]) Description() Description {
	prepared := value.prepared.Description()
	result := Description{Version: prepared.Version, Revision: prepared.Revision}
	for _, layer := range prepared.Layers {
		result.Layers = append(result.Layers, LayerInfo{Kind: LayerKind(layer.Kind), Fields: slices.Clone(layer.Fields)})
	}
	return result
}
func (value Accepted[T]) View() settings.View { return value.view }
func (value Accepted[T]) Sequence() uint64    { return value.sequence }

// publish runs under the State lock. Snapshot construction/validation are outside
// it; settings.Publish performs no user callback.
func (value *state[T]) publish(ctx context.Context, sequence uint64, prepared configsource.Prepared[T], snapshot settings.Snapshot[T]) error {
	if ctx.Err() != nil || value.status.Closed {
		return fail(ErrClosed, "publish", ctx.Err(), context.Cause(ctx))
	}
	if err := value.store.Publish(snapshot); err != nil {
		return fail(ErrPublish, "publish", err)
	}
	value.current = Accepted[T]{prepared: prepared, view: snapshot.View(), sequence: sequence}
	value.status.Accepted++
	value.status.Published = sequence
	value.status.PreparationError = nil
	value.status.Adoption = nil
	value.status.AdoptionError = nil
	value.status.AdoptionFailed = false
	return nil
}
func (value *State[T]) adopt(ctx context.Context, scope *resource.Scope) {
	if scope == nil {
		return
	}
	value.state.mu.Lock()
	current := value.state.current
	value.state.mu.Unlock()
	update, err := scope.Apply(ctx, current.view)
	value.state.mu.Lock()
	if value.state.current.sequence == current.sequence {
		value.state.status.Adoption = update
		value.state.status.AdoptionError = err
		value.state.status.AdoptionFailed = err != nil
	}
	value.state.mu.Unlock()
}
