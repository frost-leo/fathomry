/*
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

	"github.com/frost-leo/fathomry/failure"
)

// Public Go contracts evolve with the framework module's compatibility policy.
// SchemaVersion describes project data, not a separate version of each Go type.
// Runtime requests/documents refuse JSON persistence: no wire format is implied.

// Configuration failures are stable identities, not retry or business-state policy.
// Native acquisition/parser causes are withheld; caller cancellation causes and
// explicitly supplied schema-validation errors retain intentional inspection.
const (
	InvalidInput failure.Code = "fathomry.configuration.invalid_input"
	Unavailable  failure.Code = "fathomry.configuration.unavailable"
	// Missing and Denied are safe remote-acquisition causes, never native text.
	Missing           failure.Code = "fathomry.configuration.missing"
	Denied            failure.Code = "fathomry.configuration.denied"
	UnsupportedSchema failure.Code = "fathomry.configuration.unsupported_schema"
	Invalid           failure.Code = "fathomry.configuration.invalid"
	ValidationFailed  failure.Code = "fathomry.configuration.validation_failed"
	LimitExceeded     failure.Code = "fathomry.configuration.limit_exceeded"
	Cancelled         failure.Code = "fathomry.configuration.cancelled"
)

// Configuration bounds are bytes except the source/binding counts. They bound
// retained input and declared work, not a custom Provider's allocations or RSS.
const (
	MaxDocumentBytes = 1 << 20
	MaxSources       = 3
	MaxVariables     = 64
)

// Layer identifies precedence, not a Provider or source revision.
type Layer uint8

const (
	Defaults Layer = iota
	Base
	Environment
	Local
	Variables
)

// Document transfers one original YAML mapping (including JSON
// syntax) to the loader. Name is a public, non-secret 1-64 character label using
// lowercase ASCII letters, digits, dot, underscore or hyphen. One document per
// Base/Environment/Local layer and unique names are required. Absent means an
// explicitly optional source was missing; its Data must be empty. Empty present
// data is not absence and fails preparation. Data is borrowed during loading;
// the Provider must not modify it concurrently or after returning ownership.
type Document struct {
	runtimeValue
	Name   string
	Layer  Layer
	Data   []byte
	Absent bool
}

// Input is the finite acquisition handoff. Provider is a public
// label using the same grammar as document names. SchemaVersion is the Provider's
// declared project-data format, not its SDK or option version; it must match
// Schema.SchemaVersion. No common-time source snapshot is implied.
type Input struct {
	runtimeValue
	Provider      string
	SchemaVersion uint32
	Documents     []Document
}

// Provider is the consumer-owned acquisition boundary implemented
// by framework-supplied adapters. Selection is explicit, not a name registry or
// dynamic plugin loader. ReadConfiguration owns its reads and cleanup; returning
// must leave no unowned work or caller-managed native handles. It must honor the
// documented limits before allocation and return no usable prefix on failure.
// It must not log private input. Custom implementations are trusted, not sandboxed.
// Public failure occurrences may pass through; unclassified errors are withheld.
type Provider interface {
	ReadConfiguration(context.Context) (Input, error)
}

// Schema owns the project's pure data shape, format and validation.
// T follows resource preparation's plain-data rules: a nonrecursive struct with
// explicit json field names, no handles, custom serialization or embedded fields.
// SchemaVersion must be positive. Defaults and closure state must not be mutated during
// loading. Validate sees an isolated copy; its changes are discarded. Its returned
// error is deliberately retained as a public cause and must be safe for intentional
// inspection. Validation is cooperative and does not get a background goroutine.
type Schema[T any] struct {
	runtimeValue
	SchemaVersion uint32
	Defaults      T
	Validate      func(T) error
}

// Request selects an already declared Provider and environment
// bindings. It performs no ambient discovery when Variables is empty. A nil or
// typed-nil Provider is invalid, not implicit defaults-only loading.
type Request struct {
	runtimeValue
	Provider  Provider
	Variables []Variable
}

// SourceInfo exposes only public source identity, layer and presence.
type SourceInfo struct {
	Name    string
	Layer   Layer
	Present bool
}

// Contribution reports supplied schema paths, not winning values.
// Maps and lists are whole fields; dynamic keys and their values are excluded.
type Contribution struct {
	Layer  Layer
	Fields []string
}

// VariableInfo identifies a declared schema field, not the process
// variable's name or value. Present distinguishes an empty value from absence.
type VariableInfo struct {
	Field   string
	Present bool
}

// Description is detached, safe metadata, not a durable protocol.
// Revision is an opaque preparation identity, not equality, authenticity or a Run
// identifier. Contributions describe supplied fields rather than complete winning
// origins. Copying the outer struct alone shares its slices.
type Description struct {
	SchemaVersion uint32
	Revision      string
	Provider      string
	Sources       []SourceInfo
	Contributions []Contribution
	Variables     []VariableInfo
}

// VariableEncoding chooses explicit conversion. Text (zero) treats
// the value literally, including empty strings and the spelling "null". JSON
// accepts one JSON value and preserves numeric spelling for strict preparation.
type VariableEncoding uint8

const (
	VariableText VariableEncoding = iota
	VariableJSON
)

// Variable binds one exact process name to a schema field. Name
// uses ASCII letters/digits/underscore and cannot start with a digit (<=256 bytes).
// Field is a slash-separated schema path, e.g. "/database/host", with at most
// 16 components. Only declared struct fields are selectable, not array indices
// or dynamic map keys. A whole map/list can be supplied using JSON encoding.
// Text requires a string or pointer-to-string field, including when absent.
// Prefix-overlapping/duplicate field bindings reject, even when values are absent.
// Required rejects absence, not an empty text value. Names and values are private.
type Variable struct {
	runtimeValue
	Name     string
	Field    string
	Encoding VariableEncoding
	Required bool
}
