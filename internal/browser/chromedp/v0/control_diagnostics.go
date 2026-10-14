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
	"reflect"

	"github.com/chromedp/cdproto/cdp"
	jsonv2 "github.com/chromedp/cdproto/cdp/jsonv2"
	sdk "github.com/chromedp/chromedp"
)

// RunDiagnosticCommand delegates finite diagnostic commands within
// explicitly enabled Control domains. Cleanup disables the native domain without
// ordinary command/evidence admission; it does not fabricate an ignored profile.
func RunDiagnosticCommand[P, R any](ctx context.Context, control *Control, command cdp.Command[P, R], params P) (result R, err error) {
	if control == nil || control.state == nil {
		return result, failure(ErrInput, "diagnostic-control")
	}
	state := control.state
	domain, known := diagnosticCommands[command.Method]
	if !known {
		return result, failure(ErrUnsupported, "diagnostic-command")
	}
	if domain == "Debugger" {
		return runDebuggerCommand(ctx, state, command, params, nil, nil)
	}
	check := func() error {
		if state.closing || state.closed || domain == "Profiler" && !state.profilerEnabled || domain == "Performance" && !state.performanceEnabled {
			return failure(ErrState, "diagnostic-domain")
		}
		return nil
	}
	work, finish, err := state.enterCommand(ctx, check)
	if err != nil {
		return result, err
	}
	defer finish()
	return RunAction(work, state.session, Action[R]{local: func(work context.Context, session *Session) (result R, err error) {
		schema := protocolCommands[command.Method]
		if !nativeCommandType(reflect.TypeFor[P](), schema.params) || !nativeCommandType(reflect.TypeFor[R](), schema.result) {
			return result, failure(ErrInput, "diagnostic-command-types")
		}
		if err := session.reserveCommand(); err != nil {
			return result, err
		}
		buffer := boundedBuffer{limit: session.owner.settings.MaxCommandBytes}
		if err := marshalBounded(&buffer, params); err != nil {
			return result, err
		}
		var copied P
		if err := jsonv2.Unmarshal(buffer.data, &copied, sdk.DefaultUnmarshalOptions); err != nil {
			return result, err
		}
		verify := func() error {
			state.mu.Lock()
			defer state.mu.Unlock()
			return check()
		}
		return cdp.Call(session.commandWork(work, false, verify), sdk.FromContext(session.native).Target, command, copied)
	}})
}

var diagnosticCommands = map[string]string{
	"Profiler.getBestEffortCoverage":  "Profiler",
	"Profiler.setSamplingInterval":    "Profiler",
	"Profiler.start":                  "Profiler",
	"Profiler.startPreciseCoverage":   "Profiler",
	"Profiler.stop":                   "Profiler",
	"Profiler.stopPreciseCoverage":    "Profiler",
	"Profiler.takePreciseCoverage":    "Profiler",
	"Performance.getMetrics":          "Performance",
	"Debugger.pause":                  "Debugger",
	"Debugger.setAsyncCallStackDepth": "Debugger",
	"Debugger.setBlackboxPatterns":    "Debugger",
	"Debugger.setBreakpointsActive":   "Debugger",
	"Debugger.setPauseOnExceptions":   "Debugger",
	"Debugger.setSkipAllPauses":       "Debugger",
}
