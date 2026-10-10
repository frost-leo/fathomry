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

package client

import (
	"context"

	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/sdk/internal"
)

// FathomryEncodedValueDecoderV1 retains the source of a native encoded result.
// Cached results keep their acquiring hook; subsequent Get contexts do not
// replace it. Hooks must preserve native decoding and join actual work.
type FathomryEncodedValueDecoderV1 = internal.FathomryEncodedValueDecoderV1

// FathomryWithEncodedValueDecoderV1 opts this call into scoped native Update,
// Activity and Nexus result consumption. A nil hook keeps ordinary native Get.
// The hook is runtime authority, not durable data or permission to own a Client.
func FathomryWithEncodedValueDecoderV1(ctx context.Context, decode FathomryEncodedValueDecoderV1) context.Context {
	return internal.FathomryWithEncodedValueDecoderV1(ctx, decode)
}

// FathomryScopeOwnerV1 is an opaque local owner capability, not durable data.
type FathomryScopeOwnerV1 = internal.FathomryScopeOwnerV1

// NewFathomryScopeOwnerV1 creates a private owner capability. Other owners can
// add restrictions but cannot remove or replace this owner's decoder guard.
func NewFathomryScopeOwnerV1() *FathomryScopeOwnerV1 { return internal.NewFathomryScopeOwnerV1() }

// FathomryScopeErrorV1 is a local opt-in native lazy-decoder ownership extension.
// It preserves native error types, known cause graphs and original Failure data.
// Unknown custom errors remain caller-owned; custom error types with borrowed
// runtime decoders must implement FathomryScopeDecodersV1(func(func() error) error)
// error. Independently retained original aliases cannot be revoked by this copy.
func FathomryScopeErrorV1(cause error, owner *FathomryScopeOwnerV1, guard func(func() error) error) error {
	return internal.FathomryScopeErrorV1(cause, owner, guard)
}

// FathomryDecoderScopeV1 is an opaque, immutable decoder guard chain. Custom
// errors can implement FathomryMapDecoderScopeV1(func(*FathomryDecoderScopeV1)
// *FathomryDecoderScopeV1) error by copying themselves and mapping only their
// scope token. Decode must guard their entire synchronous borrowed retrieval.
// The hook must preserve native error shape, foreign restrictions and aliases,
// return the original error when mapping returns the same token, and never
// retain the mapper. This cooperative hook is preferred over the legacy guard-
// only FathomryScopeDecodersV1 hook when native finalization needs lazy details.
type FathomryDecoderScopeV1 = internal.FathomryDecoderScopeV1

// NewFathomryChildScopeOwnerV1 associates a private operation decoder owner with
// one callback lifetime group. Neither identity grants source or RPC authority.
func NewFathomryChildScopeOwnerV1(parent *FathomryScopeOwnerV1) *FathomryScopeOwnerV1 {
	return internal.NewFathomryChildScopeOwnerV1(parent)
}

// FathomryPrepareErrorFinalizationV1 marks this group's known decoder scopes in
// an isolated native-shaped copy handed to native task finalization, never back
// to application code. Unknown wrappers/joins and unscoped user errors stay intact.
// Legacy guard-only custom errors do not support this ownership transfer.
func FathomryPrepareErrorFinalizationV1(cause error, group *FathomryScopeOwnerV1) error {
	return internal.FathomryPrepareErrorFinalizationV1(cause, group)
}

// FathomryConvertErrorV1 invokes native conversion with a temporary marked-scope
// copy. It seals and joins entered decodes before returning, even on panic/Goexit.
// The Worker must already own the entire conversion pipeline. No fresh admission
// is performed and retained converter arguments refuse later scoped decoding.
func FathomryConvertErrorV1(cause error, convert func(error) *failurepb.Failure) *failurepb.Failure {
	return internal.FathomryConvertErrorV1(cause, convert)
}
