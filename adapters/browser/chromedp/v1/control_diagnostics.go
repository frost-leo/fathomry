/*
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

package chromedp

import (
	"context"

	"github.com/chromedp/cdproto/cdp"
	"github.com/frost-leo/fathomry/adapters/v1"
	native "github.com/frost-leo/fathomry/internal/browser/chromedp/v0"
)

// RunDiagnosticCommand accepts exact finite native Profiler commands,
// Performance.getMetrics and Debugger pause/configuration commands. Enable the
// corresponding ControlOptions first. Debugger.pause only requests a pause;
// authority comes from a subsequent observed paused event. Configuration and
// pause requests use ordinary quotas but do not wait behind a paused evaluation.
// Parameters are copied after admission. CPU profiles, coverage data and script
// locations are deliberate sensitive outputs. Coverage collection can reset
// native counters and disable optimized execution, so it is not passive telemetry.
// Profiler/Performance events use the same bounded Control.Next ledger.
//
// Control.Close disables its native domains with the existing cleanup reservation,
// stopping ignored CPU/console profiles and precise coverage. It does not report
// discarded final profiles as collected results. Other CDP sessions keep their
// own profiler ownership. The limits do not bound browser-native profiling memory.
func RunDiagnosticCommand[P, R any](ctx context.Context, control *Control, command cdp.Command[P, R], params P) (result R, receipt *adapters.Receipt[Result], err error) {
	if control == nil || control.state == nil {
		return result, nil, fail(ErrInput, "diagnostic-control")
	}
	return runPageOperation(ctx, control.state.page, "diagnostic_command", func(work context.Context) (R, error) {
		return native.RunDiagnosticCommand(work, control.state.native, command, params)
	})
}
