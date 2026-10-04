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

package database

import (
	"errors"
	"fmt"
	"log/slog"
	"slices"

	"github.com/frost-leo/fathomry/adapters/v1"
)

// LayerInfo copies preparation provenance without a settings payload.
type LayerInfo struct {
	Kind   uint8
	Fields []string
}

// Info identifies a native preparation, not readiness or a public generation.
// Callers own returned metadata; Clone isolates its mutable provenance slices.
type Info struct {
	private
	Scope, Provider, Name string
	FormatVersion         uint32
	Revision              string
	Provenance            []LayerInfo
}

// Clone returns an independent metadata snapshot.
func (value Info) Clone() Info {
	value.Provenance = slices.Clone(value.Provenance)
	for index := range value.Provenance {
		value.Provenance[index].Fields = slices.Clone(value.Provenance[index].Fields)
	}
	return value
}

// Attribution records public identity captured for native dispatch.
// Source is the borrowed public generation, or zero for direct owner access.
type Attribution struct {
	private
	Runtime, Operation, ID string
	Sequence, Parent       uint64
	Depth                  int
	Source                 adapters.Source
}

// Attempts counts observed native dispatches, not logical operations or retries.
// Exact=false permits an unobserved request even when Observed=0.
type Attempts struct {
	Observed uint64
	Exact    bool
}

// Fact separates declarations from observations. Kind is empty (unknown),
// declared, observed, redacted, development or not-applicable. Empty is not proof.
type Fact struct {
	private
	Kind, Value string
}

// Option is a non-secret effective setting, not service-support evidence.
type Option struct{ Name, Value string }

// Profile separates SDK execution and declared protocol/service settings.
// ServiceVersion remains unknown unless explicitly observed; concrete Results
// carry server text. Options is a detached, caller-owned snapshot.
type Profile struct {
	private
	ImplementationModule, SDKMode                 string
	ServiceMode, ServiceVersion, Protocol, Native Fact
	Options                                       []Option
}

// These observations are process-local values, not a durable serialization format.
type private struct{}

func (private) Format(state fmt.State, _ rune) { _, _ = state.Write([]byte("database[restricted]")) }

func (private) LogValue() slog.Value { return slog.StringValue("database[restricted]") }

func (*Info) LogValue() slog.Value { return private{}.LogValue() }

func (*Attribution) LogValue() slog.Value { return private{}.LogValue() }

func (*Fact) LogValue() slog.Value { return private{}.LogValue() }

func (*Profile) LogValue() slog.Value { return private{}.LogValue() }

func (private) MarshalJSON() ([]byte, error) {
	return nil, errors.New("database: runtime serialization unsupported")
}

func (*private) UnmarshalJSON([]byte) error {
	return errors.New("database: runtime reconstruction unsupported")
}
