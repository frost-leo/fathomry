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

package declaration

// Supported wire identifiers are independent of business version labels.
const (
	BundleFormat = "fathomry.workflow-bundle/v1"
	SpecFormat   = "fathomry.workflow-spec/v1"
	ModeFormat   = "fathomry.mode-values/v1"
	Semantics    = "fathomry.item-dag/v1"
	SpecProfile  = "fathomry.workflow-conflict/v1"
	ModeProfile  = "fathomry.mode-conflict/v1"
)

// Admission bounds constrain data, not measured execution capacity.
const (
	MaxBundleBytes      = 16 << 20
	MaxSpecBytes        = 4 << 20
	MaxModeBytes        = 1 << 20
	MaxBundleDepth      = 64
	MaxInnerDepth       = 48
	MaxWorkflows        = 64
	MaxModesPerWorkflow = 64
	MaxModes            = 256
	MaxInputs           = 16
	MaxInputBytes       = 64 << 20
	MaxDiagnostics      = 128
)

// Decimal is a canonical nonnegative signed-int64 decimal string. Duration slots
// use milliseconds; ratio_milli uses thousandths. Validation rejects whitespace,
// signs, leading zeroes, exponents and overflow; zero is not unlimited.
type Decimal string

// Declaration is caller-owned authoring data, not an export or a runtime registry.
type Declaration struct {
	Workflows []WorkflowDeclaration
}

// WorkflowDeclaration owns modes for one exact Namespace/code/version tuple.
type WorkflowDeclaration struct {
	Namespace string
	Code      string
	Version   string
	Spec      Specification
	Modes     []ModeDeclaration
}

// ModeDeclaration identifies immutable parameter values under its enclosing
// workflow version. Only a fixed control default can supply an omitted value.
type ModeDeclaration struct {
	Code     string
	Revision string
	Values   map[string]Decimal
}

// Specification is a finite static plan. Nil required collections become empty
// only where emptiness is admitted; no entity, binding or execution rule is invented.
type Specification struct {
	Contracts  map[string]DataSchema `json:"contracts"`
	Bindings   map[string]Binding    `json:"bindings"`
	Inputs     map[string]Port       `json:"inputs"`
	Outputs    map[string]Export     `json:"outputs"`
	Nodes      map[string]Node       `json:"nodes"`
	Edges      map[string]Edge       `json:"edges"`
	Controls   map[string]Control    `json:"controls"`
	Relations  map[string]Relation   `json:"relations"`
	Limits     Limits                `json:"limits"`
	Completion Completion            `json:"completion"`
}

// DataSchema describes the closed supported JSON Schema subset. Nil pointers
// mean absence; non-nil values, including zero bounds, are validated as supplied.
// AnyOf admits exactly [non-null schema, null].
// Object authoring may omit empty Properties/Required and fixed false
// AdditionalProperties; exported documents must contain them explicitly.
type DataSchema struct {
	Type                 string                `json:"type,omitzero"`
	Properties           map[string]DataSchema `json:"properties,omitzero"`
	Required             []string              `json:"required,omitzero"`
	AdditionalProperties *bool                 `json:"additionalProperties,omitempty"`
	Items                *DataSchema           `json:"items,omitzero"`
	MinItems             *int64                `json:"minItems,omitempty"`
	MaxItems             *int64                `json:"maxItems,omitempty"`
	MinLength            *int64                `json:"minLength,omitempty"`
	MaxLength            *int64                `json:"maxLength,omitempty"`
	Minimum              *int64                `json:"minimum,omitempty"`
	Maximum              *int64                `json:"maximum,omitempty"`
	Enum                 []string              `json:"enum,omitzero"`
	AnyOf                []DataSchema          `json:"anyOf,omitzero"`
}

// Port separates required presence from nullability and value/complete_batch
// delivery. A successful binding output is always required; empty is not absent.
type Port struct {
	Contract string `json:"contract"`
	Delivery string `json:"delivery"`
	Required bool   `json:"required"`
}

// Binding declares an implementation identity, not a function or a safety proof.
type Binding struct {
	Type        string              `json:"type"`
	Revision    string              `json:"revision"`
	RetrySafety string              `json:"retry_safety"`
	Inputs      map[string]Port     `json:"inputs"`
	Outputs     map[string]Port     `json:"outputs"`
	Effects     map[string][]string `json:"effects"`
}

// Source is a closed union. Kind is root, edge, input or activity; only the
// selected alternative's Port, Edge and Activity fields may be populated.
type Source struct {
	Kind     string `json:"kind"`
	Port     string `json:"port"`
	Edge     string `json:"edge,omitzero"`
	Activity string `json:"activity,omitzero"`
}

// Input binds a node port to a root port or an incoming edge.
type Input struct {
	Contract string `json:"contract"`
	Delivery string `json:"delivery"`
	Required bool   `json:"required"`
	Source   Source `json:"source"`
}

// Export declares a root output obligation, not proof every route produces it.
type Export struct {
	Node     string `json:"node"`
	Port     string `json:"port"`
	Required bool   `json:"required"`
}

// Setting selects exactly one fixed decimal or named control. A non-nil Value
// selects the fixed alternative even when its text is empty (and invalid).
type Setting struct {
	Value   *Decimal `json:"value,omitzero"`
	Control string   `json:"control,omitzero"`
}

// Fixed declares a constant slot without validating it or supplying defaults.
func Fixed(value Decimal) Setting { return Setting{Value: &value} }

// Tuned binds a slot to a declared control; it is not a runtime lookup.
func Tuned(control string) Setting { return Setting{Control: control} }

// Policy declares complete Activity budgets and retry values. Only a nil heartbeat
// means disabled; an empty supplied setting is invalid. These fields do not start
// Activities or install SDK defaults.
type Policy struct {
	StartToCloseMS    Setting  `json:"start_to_close_ms"`
	ScheduleToCloseMS Setting  `json:"schedule_to_close_ms"`
	HeartbeatMS       *Setting `json:"heartbeat_ms,omitzero"`
	MaximumAttempts   Setting  `json:"maximum_attempts"`
	InitialIntervalMS Setting  `json:"initial_interval_ms"`
	MaximumIntervalMS Setting  `json:"maximum_interval_ms"`
	BackoffMilli      Setting  `json:"backoff_milli"`
}

// Activity is a logical call site referencing one declared binding.
type Activity struct {
	Binding string            `json:"binding"`
	Inputs  map[string]Source `json:"inputs"`
	Policy  Policy            `json:"policy"`
}

// Node groups calls into ordered, nonempty sets of concurrent phase members.
type Node struct {
	Inputs          map[string]Input       `json:"inputs"`
	Outputs         map[string]Source      `json:"outputs"`
	Activities      map[string]Activity    `json:"activities"`
	Phases          [][]string             `json:"phases"`
	Join            string                 `json:"join"`
	Routes          []string               `json:"routes"`
	Route           Route                  `json:"route"`
	Gate            *Gate                  `json:"gate,omitempty"`
	RequiredEffects map[string]EffectCheck `json:"required_effects"`
}

// Route selects a fixed value or a closed enum output of a declared call.
type Route struct {
	Kind     string `json:"kind"`
	Value    string `json:"value,omitzero"`
	Activity string `json:"activity,omitzero"`
	Port     string `json:"port,omitzero"`
}

// Gate declares chunk/run scope and strict ancestor observations, not a controller.
type Gate struct {
	Scope     string   `json:"scope"`
	Nodes     []string `json:"nodes"`
	Condition string   `json:"condition"`
}

// EffectCheck requires an exact guarantee declared by a call's binding.
type EffectCheck struct {
	Activity  string `json:"activity"`
	Effect    string `json:"effect"`
	Guarantee string `json:"guarantee"`
}

// Edge is a conditional dependency between two nodes.
type Edge struct {
	From string   `json:"from"`
	To   string   `json:"to"`
	On   []string `json:"on"`
}

// Control declares one typed parameter contract; Override is a set of run/item
// permissions only. A non-nil Default must contain a valid decimal. This package
// does not resolve runtime overrides.
type Control struct {
	Kind     string   `json:"kind"`
	Minimum  Decimal  `json:"minimum"`
	Maximum  Decimal  `json:"maximum"`
	Default  *Decimal `json:"default,omitzero"`
	Override []string `json:"override"`
}

// Relation admits only sum_le over controls of the same kind.
type Relation struct {
	Kind  string   `json:"kind"`
	Terms []string `json:"terms"`
	Limit string   `json:"limit"`
}

// Limits declares count and millisecond slots; no value changes actual capacity.
type Limits struct {
	ActiveChunks         Setting `json:"active_chunks"`
	ItemsPerChunk        Setting `json:"items_per_chunk"`
	ChunkItemConcurrency Setting `json:"chunk_item_concurrency"`
	RunTimeoutMS         Setting `json:"run_timeout_ms"`
	CleanupReserveMS     Setting `json:"cleanup_reserve_ms"`
}

// Completion declares obligations and a fixed failure allowance, not runtime facts.
type Completion struct {
	Item               string  `json:"item"`
	Run                string  `json:"run"`
	MaximumFailedItems Decimal `json:"maximum_failed_items"`
}
