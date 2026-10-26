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

package implementation

import (
	"context"

	"github.com/frost-leo/fathomry/failure/v1"
)

// InvocationDiagnostic belongs to the invocation detail contract, not completed
// static validation. Phase is admission/input/handler/output; Reason is fixed.
// Paths use input member/array order or output struct/array order, without names
// or values. Code/category inspection never establishes absence of external effects.
type InvocationDiagnostic struct {
	Phase  string
	Reason string
	Path   string
}

func invocationFailure(phase, reason, path string, category error, causes ...error) error {
	all := make([]error, 0, len(causes)+1)
	if category != nil {
		all = append(all, category)
	}
	all = append(all, causes...)
	value, err := failure.NewDetailed(definition(ErrInvocation), failure.Location{Operation: "invoke"},
		InvocationDiagnostic{phase, reason, path}, func(value InvocationDiagnostic) InvocationDiagnostic { return value }, all...)
	if err != nil {
		return err
	}
	return value
}

func invocationCanceled(ctx context.Context, phase string, observed ...error) error {
	if ctx == nil {
		return invocationFailure(phase, "context", "", ErrHandle, observed...)
	}
	if err := ctx.Err(); err != nil {
		causes := append([]error{err, context.Cause(ctx)}, observed...)
		return invocationFailure(phase, "canceled", "", ErrCanceled, causes...)
	}
	return nil
}

func preferInvocationCancellation(ctx context.Context, phase string, observed error) error {
	if canceled := invocationCanceled(ctx, phase, observed); canceled != nil {
		return canceled
	}
	return observed
}
