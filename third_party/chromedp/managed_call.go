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
	"github.com/chromedp/cdproto"
	jsonv2 "github.com/chromedp/cdproto/cdp/jsonv2"
	"time"
)

// ErrCommandLimit means that a command exceeded its selected logical byte limit.
var ErrCommandLimit = errors.New("command byte limit exceeded")

// CallLimits applies to calls made with one context, including native helpers.
// Before runs before encoding. Result runs before decoding. Both must be bounded.
// A limit of zero preserves the native limit. These are not wire or heap limits.
type CallLimits struct {
	MaxInputBytes  int64
	MaxOutputBytes int64
	Before         func(string) error
	Result         func(string, int64) error
}
type callLimitsKey struct{}
type queuedKey struct{}
type replyKey struct{}

// WithTargetCleanupTimeout bounds detach and close commands after cancellation.
// A nonpositive value preserves the upstream five-second default.
func WithTargetCleanupTimeout(limit time.Duration) ContextOption {
	return func(c *Context) { c.cleanupTimeout = limit }
}

// WithCallLimits binds limits to a context without adding browser authority.
func WithCallLimits(ctx context.Context, limits CallLimits) context.Context {
	return context.WithValue(ctx, callLimitsKey{}, limits)
}

func withQueued(ctx context.Context, queued func(string)) context.Context {
	previous, _ := ctx.Value(queuedKey{}).(func(string))
	return context.WithValue(ctx, queuedKey{}, func(method string) {
		queued(method)
		if previous != nil {
			previous(method)
		}
	})
}

func commandQueued(ctx context.Context, method string) {
	if queued, _ := ctx.Value(queuedKey{}).(func(string)); queued != nil {
		queued(method)
	}
}

func withReply(ctx context.Context, reply func(string)) context.Context {
	previous, _ := ctx.Value(replyKey{}).(func(string))
	return context.WithValue(ctx, replyKey{}, func(method string) {
		reply(method)
		if previous != nil {
			previous(method)
		}
	})
}
func admitCall(ctx context.Context, method string) (context.Context, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	limits, _ := ctx.Value(callLimitsKey{}).(CallLimits)
	if limits.Before != nil {
		if err := limits.Before(method); err != nil {
			return nil, err
		}
		limits.Before = nil
		ctx = WithCallLimits(ctx, limits)
	}
	return ctx, nil
}

func encodeCall(ctx context.Context, method string, params any) ([]byte, error) {
	ctx, err := admitCall(ctx, method)
	if err != nil {
		return nil, err
	}
	limits, _ := ctx.Value(callLimitsKey{}).(CallLimits)
	if params == nil {
		return nil, nil
	}
	params, err = preserveCallArguments(method, params, limits.MaxInputBytes)
	if err != nil {
		return nil, err
	}
	if limits.MaxInputBytes <= 0 {
		return jsonv2.Marshal(params, DefaultMarshalOptions)
	}
	out := bindingBuffer{limit: limits.MaxInputBytes}
	err = marshalBinding(&out, params)
	if errors.Is(err, ErrBindingLimit) {
		return nil, ErrCommandLimit
	}
	return out.data, err
}
func decodeCall(ctx context.Context, method string, msg *cdproto.Message, result any) error {
	limits, _ := ctx.Value(callLimitsKey{}).(CallLimits)
	if msg != nil {
		if reply, _ := ctx.Value(replyKey{}).(func(string)); reply != nil {
			reply(method)
		}
		if limits.MaxOutputBytes > 0 && int64(len(msg.Result)) > limits.MaxOutputBytes {
			return ErrCommandLimit
		}
		if limits.Result != nil {
			if err := limits.Result(method, int64(len(msg.Result))); err != nil {
				return err
			}
		}
	}
	return decodeReply(msg, result)
}
