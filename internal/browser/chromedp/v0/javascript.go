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
	"errors"
	"github.com/chromedp/cdproto/runtime"
	sdk "github.com/chromedp/chromedp"
	"slices"
	"time"
)

type EvaluateOption struct{ native sdk.EvaluateOption }

func EvalAwaitPromise() EvaluateOption       { return EvaluateOption{sdk.EvalAwaitPromise} }
func EvalAsValue() EvaluateOption            { return EvaluateOption{sdk.EvalAsValue} }
func EvalWithCommandLineAPI() EvaluateOption { return EvaluateOption{sdk.EvalWithCommandLineAPI} }
func EvalIgnoreExceptions() EvaluateOption   { return EvaluateOption{sdk.EvalIgnoreExceptions} }

type PollOption struct {
	native   sdk.PollOption
	frame    *Node
	frameSet bool
}

func WithPollingInterval(interval time.Duration) PollOption {
	return PollOption{native: sdk.WithPollingInterval(interval)}
}
func WithPollingTimeout(timeout time.Duration) PollOption {
	return PollOption{native: sdk.WithPollingTimeout(timeout)}
}
func WithPollingMutation() PollOption { return PollOption{native: sdk.WithPollingMutation()} }

// WithPollingArgs keeps native borrowing semantics. Arguments remain immutable
// until the action returns; the native producer bounds/copies them after admission.
func WithPollingArgs(args ...any) PollOption { return PollOption{native: sdk.WithPollingArgs(args...)} }

// WithPollingInFrame delegates the native enclosing-frame option for a managed
// node. Nil resets a preceding frame option to the native default world.
func WithPollingInFrame(node *Node) PollOption {
	if node == nil {
		return PollOption{native: sdk.WithPollingInFrame(nil), frameSet: true}
	}
	if node.native == nil {
		return PollOption{}
	}
	return PollOption{native: sdk.WithPollingInFrame(node.native), frame: node, frameSet: true}
}

func framePolling[T any](options []PollOption, build func([]sdk.PollOption) sdk.Action[T]) sdk.Action[T] {
	return func(ctx context.Context, target *sdk.Target) (value T, err error) {
		values := pollOptions(options)
		var node *Node
		for _, option := range options {
			if option.frameSet {
				node = option.frame
			}
		}
		if node == nil {
			return build(values)(ctx, target)
		}
		if node.session == nil || node.session.native == nil || sdk.FromContext(node.session.native).Target != target {
			return value, failure(ErrInput, "poll-frame-origin")
		}
		if err := node.valid(node.session); err != nil {
			return value, err
		}
		origin := node.session.objectOrigin
		work, cancel := context.WithCancelCause(ctx)
		watch := &framePoll{cancel: cancel}
		origin.nodesMu.Lock()
		if err := node.valid(node.session); err != nil || node.frame == nil {
			origin.nodesMu.Unlock()
			cancel(nil)
			return value, failure(ErrState, "poll-frame", err)
		}
		if node.frame.polls == nil {
			node.frame.polls = make(map[*framePoll]struct{})
		}
		node.frame.polls[watch] = struct{}{}
		origin.nodesMu.Unlock()
		done := make(chan struct{})
		go func() {
			defer close(done)
			select {
			case <-node.invalidated:
				cancel(failure(ErrState, "poll-node-retired"))
			case <-work.Done():
			}
		}()
		defer func() {
			cancel(nil)
			<-done
			origin.nodesMu.Lock()
			delete(node.frame.polls, watch)
			origin.nodesMu.Unlock()
		}()
		value, err = build(values)(work, target)
		return value, errors.Join(err, work.Err(), context.Cause(work))
	}
}
func pollOptions(options []PollOption) []sdk.PollOption {
	if len(options) > 64 {
		panic(failure(ErrInput, "poll-options"))
	}
	values := make([]sdk.PollOption, len(options))
	for i, v := range options {
		if v.native == nil {
			panic(failure(ErrInput, "poll-option"))
		}
		values[i] = v.native
	}
	return values
}
func PollFunction[T any](function string, options ...PollOption) Action[T] {
	if len(options) > 64 {
		return Action[T]{}
	}
	options = slices.Clone(options)
	if remoteObjectType[T]() {
		return Action[T]{bytes: int64(len(function)), object: func(string, []sdk.QueryOption) sdk.Action[*runtime.RemoteObject] {
			return framePolling(options, func(values []sdk.PollOption) sdk.Action[*runtime.RemoteObject] {
				return sdk.PollFunction[*runtime.RemoteObject](function, values...)
			})
		}}
	}
	return Action[T]{bytes: int64(len(function)), managed: func(string, []sdk.QueryOption) sdk.Action[T] {
		return framePolling(options, func(values []sdk.PollOption) sdk.Action[T] { return sdk.PollFunction[T](function, values...) })
	}}
}
