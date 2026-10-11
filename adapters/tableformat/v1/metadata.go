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

package tableformat

import (
	"slices"

	"github.com/frost-leo/fathomry/adapters/v1"
)

// Effect is one represented action's technical observation, not the aggregate
// operation, a retry decision, durability or rollback evidence. Providers map
// their native effects explicitly; its numeric values are not an SDK contract.
type Effect uint8

const (
	// NotSubmitted means no submission of this action has been observed. Other
	// actions may already have uploaded files or changed Catalog state.
	NotSubmitted Effect = iota
	// Unknown preserves uncertainty, including a reply lost after a mutation.
	Unknown
	// Acknowledged means the selected protocol acknowledged this action. For a
	// file upload it does not mean any table commit was submitted or accepted.
	Acknowledged
	// Rejected means a response positively refused the represented action, not
	// proof that prior uploads or other actions had no effect or can be retried.
	Rejected
)

// Phases are independent positive observations for a represented operation,
// never a linear state machine. False means unobserved, not impossible or undone.
// Acknowledgement/rejection/uncertainty belongs to each action's Effect.
type Phases struct {
	// Staged reports prepared native intent, not a remote effect.
	Staged bool
	// Uploaded reports at least one acknowledged file upload, not that all
	// files were uploaded, included in a commit, or subsequently cleaned up.
	Uploaded bool
	// Submitted reports an observed Catalog mutation dispatch, not acceptance.
	Submitted bool
	// Reloaded reports a successful later metadata read. Its head may differ
	// from the intended or acknowledged snapshot because of another writer.
	Reloaded bool
	// Released reports joined local operation resources. It does not report
	// physical deletion, source shutdown, or receipt/evidence acknowledgement.
	Released bool
}

// LayerInfo records preparation field names, never their configuration values.
type LayerInfo struct {
	Kind   uint8
	Fields []string
}

// Info identifies actual frozen preparation, not readiness or a public source
// generation. Revision is opaque preparation identity, not a credential hash.
type Info struct {
	private
	Scope, Provider, Name, Revision string
	FormatVersion                   uint32
	Provenance                      []LayerInfo
}

// Clone detaches the provenance list and every nested field-name slice. Nil and
// non-nil empty slices remain distinct; the input must not be mutated concurrently.
func (value Info) Clone() Info {
	value.Provenance = slices.Clone(value.Provenance)
	for index := range value.Provenance {
		value.Provenance[index].Fields = slices.Clone(value.Provenance[index].Fields)
	}
	return value
}

// Attribution identifies the actual retained source/use and public operation.
// SourceID and UseID are provider-owned process-local identities, not endpoints,
// credentials, durable tokens or the source's current replacement. Zero means
// uncaptured. Source independently records the public generation actually
// borrowed; zero also covers direct access or failed acquisition, not absence.
// Sequence and Parent belong to the named public runtime, not the native SDK.
type Attribution struct {
	private
	SourceID               string
	UseID                  uint64
	Runtime, Operation, ID string
	Sequence, Parent       uint64
	Depth                  int
	Source                 adapters.Source
}

// Attempts counts observed dispatches, not logical calls, native retry limits or
// acknowledged effects. Exact=false with Observed=0 allows unobserved attempts.
type Attempts struct {
	Observed uint64
	Exact    bool
}
