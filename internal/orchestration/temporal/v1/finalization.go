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

package temporal

import (
	failurepb "go.temporal.io/api/failure/v1"
	sdk "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	sdktemporal "go.temporal.io/sdk/temporal"
)

// Native task conversion has no caller context. Its whole synchronous window is
// already Worker-owned, independent of the earlier user-callback receipt.
type finalizationFailureConverter struct{ native converter.FailureConverter }

func finalizationConverter(native converter.FailureConverter) converter.FailureConverter {
	if native == nil {
		native = sdktemporal.NewDefaultFailureConverter(sdktemporal.DefaultFailureConverterOptions{})
	}
	return &finalizationFailureConverter{native: native}
}
func (conversion *finalizationFailureConverter) ErrorToFailure(cause error) *failurepb.Failure {
	return sdk.FathomryConvertErrorV1(cause, conversion.native.ErrorToFailure)
}
func (conversion *finalizationFailureConverter) FailureToError(value *failurepb.Failure) error {
	return conversion.native.FailureToError(value)
}
func (conversion *finalizationFailureConverter) WithSerializationContext(ctx converter.SerializationContext) converter.FailureConverter {
	return &finalizationFailureConverter{native: converter.WithFailureConverterSerializationContext(conversion.native, ctx)}
}
