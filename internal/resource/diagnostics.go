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

package resource

import "github.com/frost-leo/fathomry/internal/fault"

// ErrConfiguration identifies rejected selected configuration, not a retry policy.
const ErrConfiguration fault.Kind = "fathomry.source.invalid_configuration"

// Identity distinguishes a selected Provider implementation from its instance.
// Names are unique within a framework assembly, even across Providers. Labels use 1–64
// lowercase ASCII letters, digits, dots, underscores, or hyphens, and no secrets.
type Identity struct {
	Provider string
	Name     string
}

// LayerKind specifies fixed precedence, lowest to highest.
type LayerKind uint8

const (
	Defaults LayerKind = iota
	Base
	Environment
	Local
	Variables
)

// LayerInfo records the schema fields supplied by a declared layer, in precedence
// order. It is input provenance, not a per-map-key effective-origin reconstruction.
// Map/list fields are recorded as a whole: their arbitrary keys and values, file
// paths, environment variable names, and credentials never enter this projection.
type LayerInfo struct {
	Kind   LayerKind
	Fields []string
}

// Description contains isolated, value-only source metadata. Revision is a random
// preparation identity, not a hash of secret settings or an equality fingerprint.
// Every successful preparation gets a new revision; reuse preserves that revision.
// Copying the outer value shares slices; Clone provides independent storage.
type Description struct {
	Identity   Identity
	Format     uint32
	Revision   string
	Provenance []LayerInfo
}

// These declarations belong to the source capability, not the shared error kernel.
const (
	ErrAssembly       = fault.Kind("fathomry.source.assembly_failed")
	ErrSelection      = fault.Kind("fathomry.source.invalid_selection")
	ErrInitialization = fault.Kind("fathomry.source.initialization_failed")
	ErrCleanup        = fault.Kind("fathomry.source.cleanup_failed")
	ErrIncomplete     = fault.Kind("fathomry.source.cleanup_incomplete")
)

// Ownership describes this assembly's relationship to a resource, not permission
// to perform arbitrary operations against its external service.
type Ownership string

const (
	Owned       Ownership = "owned"
	Borrowed    Ownership = "borrowed"
	Delegated   Ownership = "delegated"
	Transferred Ownership = "transferred"
)

// Info retains the original resource identity across borrowing/ownership transfer.
// Scope is the original composition scope; a borrowing alias never relabels it.
type Info struct {
	Scope         string
	Configuration Description
}

// Status is an isolated lifecycle snapshot. Returned applies only to a borrowed
// entry. Pending means this scope still has cleanup/return responsibility.
type Status struct {
	Name          string
	Info          Info
	Ownership     Ownership
	OwnerScope    string
	Borrowers     int
	Quiescent     bool
	Released      bool
	Returned      bool
	Pending       bool
	CanContinue   bool
	CleanupErrors []error
	Usage         Usage
	Limits        Limits
}

// Report separates primary initialization failure from cleanup evidence/errors.
// Native causes remain deliberate process-local inspection data, not a wire protocol.
type Report struct {
	Ready   bool
	Primary error
	Sources []Status
}

const (
	ErrAdmission = fault.Kind("fathomry.source.admission_failed")
	ErrCapacity  = fault.Kind("fathomry.source.capacity_exhausted")
)

// Limits are immutable process-local ceilings on one authoritative resource,
// shared across all its aliases. Active counts admitted root uses, not physical
// connections or SDK attempts. Bytes are declared reservations, NOT measured heap,
// SDK buffers, native memory or wire traffic. Providers must bound those separately.
// Queued == 0 rejects overload without waiting. MaxLeases bounds live borrowing
// nodes per admitted root, including retained ancestors; nesting never waits.
// All fields must be positive except Queued and QueuedBytes, which may both be zero.
type Limits struct {
	Active      int
	Queued      int
	Bytes       int64
	QueuedBytes int64
	MaxLeases   int
}

// Usage contains current counts only, never completed call history. ActiveBytes
// and QueuedBytes are independently reserved bytes. No SDK attempts are inferred.
type Usage struct {
	Active      int
	Queued      int
	ActiveBytes int64
	QueuedBytes int64
}

// Clone returns an independent copy of provenance and field-name slices.
func (value Description) Clone() Description {
	provenance := make([]LayerInfo, len(value.Provenance))
	for index, layer := range value.Provenance {
		provenance[index] = LayerInfo{Kind: layer.Kind, Fields: append([]string(nil), layer.Fields...)}
	}
	value.Provenance = provenance
	return value
}
