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

package http2

import (
	"context"
	"errors"
)

// FathomryCompatibilityRevision identifies the local lifecycle correction.
const FathomryCompatibilityRevision = "v1"

type fathomryWorkKey struct{}

// FathomryWithWork attaches technical ownership to request-writer goroutines.
// The callback is invoked before work is launched and released after its actual
// return, including native trace callbacks, trailers and failure cleanup.
func FathomryWithWork(ctx context.Context, enter func(context.Context) (func(), error)) context.Context {
	return context.WithValue(ctx, fathomryWorkKey{}, enter)
}
func fathomryRetain(ctx context.Context) (func(), error) {
	enter, _ := ctx.Value(fathomryWorkKey{}).(func(context.Context) (func(), error))
	if enter == nil {
		return func() {}, nil
	}
	done, err := enter(ctx)
	if err == nil && done == nil {
		return nil, errors.New("http2: missing work release")
	}
	return done, err
}
