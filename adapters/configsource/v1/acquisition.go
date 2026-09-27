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

package configsource

import "github.com/frost-leo/fathomry/failure/v1"

// Phase identifies the public source operation, not an inferred native retry or
// effect verdict. It is independent of SDK/protocol execution phase names.
type Phase string

const (
	SelectPhase  Phase = "select"
	CapturePhase Phase = "capture"
	ObservePhase Phase = "observe"
	ClosePhase   Phase = "close"
)

// AcquisitionInfo is a detached safe projection from one occurrence. Source and
// Document are explicitly declared non-secret aliases, never native selectors.
// Empty Document means no failing slot was identified; it is not a missing
// document or an empty document name. Phase is always a known public operation.
// An identified slot describes the attempted acquisition, not the scope of a
// server outage/permission refusal or proof that only that document is invalid.
type AcquisitionInfo struct {
	Source   string
	Document string
	Phase    Phase
}

// AcquisitionFailure binds these facts to the directly supplied occurrence.
// Acquisition returns false for invalid/nil values. For presentation use a direct
// type assertion together with failure.Inspect, not errors.As traversal that can
// select another occurrence's facts. Arbitrary cause graphs remain sensitive.
type AcquisitionFailure interface {
	failure.Occurrence
	Acquisition() (AcquisitionInfo, bool)
}
