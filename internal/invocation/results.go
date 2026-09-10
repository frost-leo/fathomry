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

package invocation

import (
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/resource"
)

const (
	ErrInvalid  = fault.Kind("fathomry.operation.invalid")
	ErrBudget   = fault.Kind("fathomry.operation.budget")
	ErrFailed   = fault.Kind("fathomry.operation.failed")
	ErrCleanup  = fault.Kind("fathomry.operation.cleanup_failed")
	ErrWait     = fault.Kind("fathomry.operation.wait_ended")
	ErrState    = fault.Kind("fathomry.operation.invalid_state")
	ErrAttempts = fault.Kind("fathomry.operation.attempt_limit")
)

// Shape identifies completion semantics, not an SDK or business success condition.
type Shape uint8

const (
	Finite Shape = iota + 1
	Stream
	Async
	Session
)

// Attempts separates observed SDK attempts from the logical call and execution
// attempt. Exact is a Provider instrumentation assertion, not an inferred fact.
// Inexact zero means none observed, NOT proof of no requests.
type Attempts struct {
	Observed uint64
	Exact    bool
}

// Outcome transfers an immutable, capability-owned value/evidence to both the
// internal receipt and the evidence inbox. Value may contain partial data alongside
// Primary and Cleanup failures. Present distinguishes explicit empty data from
// missing data. The capability owns effect states, units and upstream mappings;
// nil error or an aggregate acknowledgement never invents those facts.
// T and native causes are borrowed for inspection after transfer, not deep-copied.
// Providers must freeze them, bound their size, and define concurrent-read rules.
// This is a runtime contract, deliberately not a serialization format.
type Outcome[T any] struct {
	runtimeValue
	Value   T
	Present bool
	Primary error
	Cleanup error
}

// Result adds actual resource and technical call context to the immutable outcome.
// Metadata is copied; Outcome.Value and native errors retain their explicit
// borrowing contract. Released concerns local use of this operation's subtree,
// never rollback, remote completion or business-data durability.
type Result[T any] struct {
	runtimeValue
	Outcome     Outcome[T]
	Context     fault.Context
	Source      resource.Info
	Limits      resource.Limits
	Shape       Shape
	Nested      bool
	Attempts    Attempts
	Released    bool
	Final       bool
	Observation Observation
}

// Err permits normal errors.Is/As without merging the separately inspectable
// primary and cleanup fields or allowing native error formatting.
func (result Result[T]) Err() error {
	if result.Outcome.Primary == nil {
		return result.Outcome.Cleanup
	}
	if result.Outcome.Cleanup == nil {
		return result.Outcome.Primary
	}
	return ErrFailed.New(result.Context, result.Outcome.Primary, result.Outcome.Cleanup)
}

const (
	ErrEvidence    = fault.Kind("fathomry.operation.evidence_capacity")
	ErrPending     = fault.Kind("fathomry.operation.evidence_pending")
	ErrObservation = fault.Kind("fathomry.operation.observation_failed")
)

// Observation describes only local diagnostic queueing, never durable export.
type Observation uint8

const (
	ObservationPending Observation = iota
	ObservationDisabled
	ObservationQueued
	ObservationDropped
	ObservationSuppressed
)
