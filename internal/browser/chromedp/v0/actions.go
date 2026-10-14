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
	"github.com/chromedp/cdproto/runtime"
	sdk "github.com/chromedp/chromedp"
	"slices"
	"time"
)

// QueryOption is a closed selector option.
type QueryOption struct {
	native sdk.QueryOption
	node   *Node
	nodes  int
	nodeAt func(int) *Node
}

func cloneQueryOptions(options []QueryOption) []QueryOption {
	if len(options) > 64 {
		return []QueryOption{{}}
	}
	return slices.Clone(options)
}

func queryOptions(values []QueryOption) []sdk.QueryOption {
	if len(values) > 64 {
		panic(failure(ErrInput, "query-options"))
	}
	result := make([]sdk.QueryOption, len(values))
	for i, v := range values {
		if v.native == nil {
			panic(failure(ErrInput, "query-option"))
		}
		result[i] = v.native
	}
	return result
}
func NodeVisible() QueryOption      { return QueryOption{native: sdk.NodeVisible} }
func NodeReady() QueryOption        { return QueryOption{native: sdk.NodeReady} }
func NodeNotVisible() QueryOption   { return QueryOption{native: sdk.NodeNotVisible} }
func NodeEnabled() QueryOption      { return QueryOption{native: sdk.NodeEnabled} }
func NodeSelected() QueryOption     { return QueryOption{native: sdk.NodeSelected} }
func NodeNotPresent() QueryOption   { return QueryOption{native: sdk.NodeNotPresent} }
func AtLeast(count int) QueryOption { return QueryOption{native: sdk.AtLeast(count)} }
func RetryInterval(interval time.Duration) QueryOption {
	return QueryOption{native: sdk.RetryInterval(interval)}
}

type TextSelector interface{ ~string }

func Text[S TextSelector](selector S, options ...QueryOption) Action[string] {
	options = cloneQueryOptions(options)
	return deferredQuery(int64(len(selector)), options, func(values []sdk.QueryOption) sdk.Action[string] { return sdk.Text(selector, values...) })
}
func Value[S TextSelector](selector S, options ...QueryOption) Action[string] {
	options = cloneQueryOptions(options)
	return deferredQuery(int64(len(selector)), options, func(values []sdk.QueryOption) sdk.Action[string] { return sdk.Value(selector, values...) })
}
func Click[S TextSelector](selector S, options ...QueryOption) Action[sdk.Void] {
	options = cloneQueryOptions(options)
	return deferredQuery(int64(len(selector)), options, func(values []sdk.QueryOption) sdk.Action[sdk.Void] { return sdk.Click(selector, values...) })
}
func SendKeys[S TextSelector](selector S, value string, options ...QueryOption) Action[sdk.Void] {
	options = cloneQueryOptions(options)
	return deferredQuery(int64(len(selector)+len(value)), options, func(values []sdk.QueryOption) sdk.Action[sdk.Void] { return sdk.SendKeys(selector, value, values...) })
}
func Screenshot[S TextSelector](selector S, options ...QueryOption) Action[[]byte] {
	options = cloneQueryOptions(options)
	return deferredQuery(int64(len(selector)), options, func(values []sdk.QueryOption) sdk.Action[[]byte] { return sdk.Screenshot(selector, values...) })
}
func Evaluate[T any](expression string, options ...EvaluateOption) Action[T] {
	if len(options) > 64 {
		return Action[T]{}
	}
	options = slices.Clone(options)
	if remoteObjectType[T]() {
		return Action[T]{bytes: int64(len(expression)), objectGrouped: true, object: func(group string, _ []sdk.QueryOption) sdk.Action[*runtime.RemoteObject] {
			values := make([]sdk.EvaluateOption, 0, len(options)+1)
			for _, option := range options {
				if option.native == nil {
					panic(failure(ErrInput, "evaluate-option"))
				}
				values = append(values, option.native)
			}
			values = append(values, sdk.EvalObjectGroup(group))
			return sdk.Evaluate[*runtime.RemoteObject](expression, values...)
		}}
	}
	return Action[T]{bytes: int64(len(expression)), objectGrouped: true, managed: func(group string, _ []sdk.QueryOption) sdk.Action[T] {
		values := make([]sdk.EvaluateOption, len(options)+1)
		for i, v := range options {
			if v.native == nil {
				panic(failure(ErrInput, "evaluate-option"))
			}
			values[i] = v.native
		}
		values[len(options)] = sdk.EvalObjectGroup(group)
		return sdk.Evaluate[T](expression, values...)
	}}
}
func Poll[T any](expression string, options ...PollOption) Action[T] {
	if len(options) > 64 {
		return Action[T]{}
	}
	options = slices.Clone(options)
	if remoteObjectType[T]() {
		return Action[T]{bytes: int64(len(expression)), object: func(string, []sdk.QueryOption) sdk.Action[*runtime.RemoteObject] {
			return framePolling(options, func(values []sdk.PollOption) sdk.Action[*runtime.RemoteObject] {
				return sdk.Poll[*runtime.RemoteObject](expression, values...)
			})
		}}
	}
	return Action[T]{bytes: int64(len(expression)), managed: func(string, []sdk.QueryOption) sdk.Action[T] {
		return framePolling(options, func(values []sdk.PollOption) sdk.Action[T] { return sdk.Poll[T](expression, values...) })
	}}
}
func CaptureScreenshot() Action[[]byte] { return Action[[]byte]{native: sdk.CaptureScreenshot()} }
func FullScreenshot(quality int) Action[[]byte] {
	return Action[[]byte]{native: sdk.FullScreenshot(quality)}
}
