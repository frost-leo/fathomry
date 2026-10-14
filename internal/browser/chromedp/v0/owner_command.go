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
	"reflect"

	"github.com/chromedp/cdproto/cdp"
	jsonv2 "github.com/chromedp/cdproto/cdp/jsonv2"
	sdk "github.com/chromedp/chromedp"
)

// RunOwnerCommand provides finite browser-owner observations. Borrowing a browser
// connection never grants this authority, even if a caller owns that connection.
func RunOwnerCommand[P, R any](ctx context.Context, source Source, command cdp.Command[P, R], params P) (result R, err error) {
	return runOwnerCommand(ctx, source, command, params, ownerObservationCommands[command.Method], nil)
}

func runOwnerCommand[P, R any](ctx context.Context, source Source, command cdp.Command[P, R], params P, allowed bool, before func(context.Context, *owner) error) (result R, err error) {
	defer func() {
		if err != nil {
			err = failure(ErrNative, "owner-command", err)
		}
	}()
	own := source.owner
	if ctx == nil || own == nil {
		return result, failure(ErrInput, "owner-command")
	}
	if own.settings.RemoteURL != "" {
		return result, failure(ErrState, "browser-owner-required")
	}
	if !allowed {
		return result, failure(ErrUnsupported, "owner-command")
	}
	schema := protocolCommands[command.Method]
	if !nativeCommandType(reflect.TypeFor[P](), schema.params) || !nativeCommandType(reflect.TypeFor[R](), schema.result) {
		return result, failure(ErrInput, "owner-command-types")
	}
	if err := own.start(ctx); err != nil {
		return result, err
	}
	if err := own.lock(ctx); err != nil {
		return result, err
	}
	defer own.unlock()
	if own.closed || own.life.Err() != nil || own.process == nil {
		return result, failure(ErrState, "browser-owner")
	}
	if own.commands >= uint64(own.settings.MaxCommands) {
		return result, failure(ErrLimit, "owner-commands")
	}
	own.commands++
	buffer := boundedBuffer{limit: own.settings.MaxCommandBytes}
	if err := marshalBounded(&buffer, params); err != nil {
		return result, err
	}
	var copied P
	if err := jsonv2.Unmarshal(buffer.data, &copied, sdk.DefaultUnmarshalOptions); err != nil {
		return result, err
	}
	work := sdk.WithCallLimits(ctx, sdk.CallLimits{
		MaxInputBytes: own.settings.MaxCommandBytes, MaxOutputBytes: own.settings.MaxResultBytes,
		Result: func(_ string, size int64) error {
			if size > own.settings.MaxResultBytes-own.resultBytes {
				return failure(ErrLimit, "owner-output")
			}
			own.resultBytes += size
			return nil
		},
	})
	if before != nil {
		if err := before(work, own); err != nil {
			return result, err
		}
	}
	result, err = cdp.Call(work, sdk.FromContext(own.root).Browser, command, copied)
	err = errors.Join(err, ctx.Err(), context.Cause(ctx))
	return result, err
}

var ownerObservationCommands = map[string]bool{
	"Browser.getVersion":            true,
	"Browser.getBrowserCommandLine": true,
	"Browser.getHistogram":          true,
	"Browser.getHistograms":         true,
	"SystemInfo.getInfo":            true,
	"SystemInfo.getProcessInfo":     true,
	"SystemInfo.getFeatureState":    true,
	"Target.getTargets":             true,
}
