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
	native "github.com/frost-leo/fathomry/internal/browser/chromedp/v0"
	"time"
)

type EvaluateOption struct{ native native.EvaluateOption }
type PollOption struct{ native native.PollOption }

func nativeEval(options []EvaluateOption) []native.EvaluateOption {
	if len(options) > 64 {
		return make([]native.EvaluateOption, 65)
	}
	values := make([]native.EvaluateOption, len(options))
	for i, v := range options {
		values[i] = v.native
	}
	return values
}
func nativePoll(options []PollOption) []native.PollOption {
	if len(options) > 64 {
		// Preserve rejection before allocation without copying the caller's slice.
		return make([]native.PollOption, 65)
	}
	values := make([]native.PollOption, len(options))
	for i, v := range options {
		values[i] = v.native
	}
	return values
}
func EvalAwaitPromise() EvaluateOption       { return EvaluateOption{native.EvalAwaitPromise()} }
func EvalAsValue() EvaluateOption            { return EvaluateOption{native.EvalAsValue()} }
func EvalWithCommandLineAPI() EvaluateOption { return EvaluateOption{native.EvalWithCommandLineAPI()} }
func EvalIgnoreExceptions() EvaluateOption   { return EvaluateOption{native.EvalIgnoreExceptions()} }
func WithPollingInterval(interval time.Duration) PollOption {
	return PollOption{native.WithPollingInterval(interval)}
}
func WithPollingTimeout(timeout time.Duration) PollOption {
	return PollOption{native.WithPollingTimeout(timeout)}
}
func WithPollingMutation() PollOption { return PollOption{native.WithPollingMutation()} }

// WithPollingArgs borrows the native argument container through execution.
// Keep it immutable until the action returns; codecs run only after admission.
func WithPollingArgs(args ...any) PollOption { return PollOption{native.WithPollingArgs(args...)} }

// WithPollingInFrame uses the native enclosing frame of a managed node. Foreign,
// stale and zero nodes are refused. Known document retirement cancels active
// polling, but cancellation alone does not settle remote-result custody.
// Nil keeps the native default-world choice, including last-option precedence.
func WithPollingInFrame(node *Node) PollOption {
	if node == nil {
		return PollOption{native.WithPollingInFrame(nil)}
	}
	if node.native == nil {
		return PollOption{}
	}
	return PollOption{native.WithPollingInFrame(node.native)}
}
func PollFunction[T any](function string, options ...PollOption) Action[T] {
	return Action[T]{native: native.PollFunction[T](function, nativePoll(options)...), name: "poll_function"}
}
