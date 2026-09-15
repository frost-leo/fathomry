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

package nethttp

import (
	"context"
	"errors"
	"net/url"
	"testing"

	"github.com/frost-leo/fathomry/internal/conformance"
	"github.com/frost-leo/fathomry/internal/invocation"
)

func TestNativeAndCleanupCausesRemainSeparate(t *testing.T) {
	native := &url.Error{Op: "Get", URL: "https://invalid.example/?secret=private-canary", Err: context.DeadlineExceeded}
	cleanup := errors.New("private-cleanup-canary")
	primary := failure(ErrTransport, "request", native)
	result := invocation.Result[Result]{Outcome: invocation.Outcome[Result]{Primary: primary, Cleanup: failure(ErrCleanup, "close", cleanup)}}
	if !errors.Is(result.Err(), ErrTransport) || !errors.Is(result.Err(), context.DeadlineExceeded) || !errors.Is(result.Err(), cleanup) {
		t.Fatal("intentional cause inspection lost")
	}
	conformance.Cause[*url.Error](t, result.Err(), func(value *url.Error) bool { return value == native })
	conformance.Private(t, result.Err(), "private-canary", "private-cleanup-canary")
}
