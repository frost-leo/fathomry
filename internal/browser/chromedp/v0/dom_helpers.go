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
	"slices"
	"time"

	"github.com/chromedp/cdproto/runtime"
	sdk "github.com/chromedp/chromedp"
)

// PopulateOption configures native child-node retrieval without a Target callback.
type PopulateOption struct{ native sdk.PopulateOption }

func PopulateWait(wait time.Duration) PopulateOption {
	return PopulateOption{native: sdk.PopulateWait(wait)}
}

func Populate(depth int64, pierce bool, options ...PopulateOption) QueryOption {
	if len(options) > 64 {
		return QueryOption{}
	}
	values := make([]sdk.PopulateOption, len(options))
	for index, option := range options {
		if option.native == nil {
			return QueryOption{}
		}
		values[index] = option.native
	}
	return QueryOption{native: sdk.Populate(depth, pierce, values...)}
}

// JavascriptAttribute preserves native named-property conversion, including its
// null-to-zero rule, while retaining remote results and exceptions in one group.
func JavascriptAttribute[T any, S TextSelector](selector S, name string, options ...QueryOption) Action[T] {
	options = cloneQueryOptions(options)
	if options == nil {
		options = []QueryOption{}
	}
	if remoteObjectType[T]() {
		return Action[T]{bytes: int64(len(selector) + len(name)), query: options, objectGrouped: true, object: func(group string, values []sdk.QueryOption) sdk.Action[*runtime.RemoteObject] {
			operation := sdk.JavascriptAttribute[*runtime.RemoteObject](selector, name, values...)
			return func(ctx context.Context, target *sdk.Target) (*runtime.RemoteObject, error) {
				return operation(sdk.WithNodeObjectGroup(ctx, group), target)
			}
		}}
	}
	return Action[T]{bytes: int64(len(selector) + len(name)), query: options, objectGrouped: true, managed: func(group string, values []sdk.QueryOption) sdk.Action[T] {
		operation := sdk.JavascriptAttribute[T](selector, name, values...)
		return func(ctx context.Context, target *sdk.Target) (T, error) {
			return operation(sdk.WithNodeObjectGroup(ctx, group), target)
		}
	}}
}

func SetJavascriptAttribute[S TextSelector](selector S, name, value string, options ...QueryOption) Action[sdk.Void] {
	options = cloneQueryOptions(options)
	if options == nil {
		options = []QueryOption{}
	}
	return Action[sdk.Void]{bytes: int64(len(selector) + len(name) + len(value)), query: options, objectGrouped: true, managed: func(group string, values []sdk.QueryOption) sdk.Action[sdk.Void] {
		operation := sdk.SetJavascriptAttribute(selector, name, value, values...)
		return func(ctx context.Context, target *sdk.Target) (sdk.Void, error) {
			return operation(sdk.WithNodeObjectGroup(ctx, group), target)
		}
	}}
}

func ScreenshotScale[S TextSelector](selector S, scale float64, options ...QueryOption) Action[[]byte] {
	options = cloneQueryOptions(options)
	return deferredQuery(int64(len(selector)), options, func(values []sdk.QueryOption) sdk.Action[[]byte] {
		return sdk.ScreenshotScale(selector, scale, values...)
	})
}

func MouseClickXY(x, y float64, options ...sdk.MouseOption) Action[sdk.Void] {
	if len(options) > 64 {
		return Action[sdk.Void]{}
	}
	options = slices.Clone(options)
	return deferred(0, func() sdk.Action[sdk.Void] { return sdk.MouseClickXY(x, y, options...) })
}

func MouseClickNode(node *Node, options ...sdk.MouseOption) Action[sdk.Void] {
	if len(options) > 64 {
		return Action[sdk.Void]{}
	}
	options = slices.Clone(options)
	return nodeAction(0, 1, func(int) *Node { return node }, func(nodes []*sdk.Node) sdk.Action[sdk.Void] {
		return sdk.MouseClickNode(nodes[0], options...)
	})
}

func KeyEventNode(node *Node, keys string, options ...sdk.KeyOption) Action[sdk.Void] {
	if len(options) > 64 {
		return Action[sdk.Void]{}
	}
	options = slices.Clone(options)
	return nodeAction(int64(len(keys)), 1, func(int) *Node { return node }, func(nodes []*sdk.Node) sdk.Action[sdk.Void] {
		return sdk.KeyEventNode(nodes[0], keys, options...)
	})
}

// ScreenshotNodes preserves native multi-node extents without adding readiness.
// The accessor is called only after bounded operation admission.
func ScreenshotNodes(count int, nodeAt func(int) *Node, scale float64) Action[[]byte] {
	return nodeAction(0, count, nodeAt, func(nodes []*sdk.Node) sdk.Action[[]byte] {
		return sdk.ScreenshotNodes(nodes, scale)
	})
}

func nodeAction[T any](size int64, count int, nodeAt func(int) *Node, build func([]*sdk.Node) sdk.Action[T]) Action[T] {
	if count <= 0 || nodeAt == nil {
		return Action[T]{}
	}
	return Action[T]{bytes: size, local: func(ctx context.Context, session *Session) (value T, err error) {
		if int64(count) > session.owner.settings.MaxCommandBytes/32 {
			return value, failure(ErrLimit, "node-input")
		}
		nodes, values := make([]*Node, count), make([]*sdk.Node, count)
		for index := range nodes {
			node := nodeAt(index)
			if err := node.valid(session); err != nil {
				return value, err
			}
			nodes[index], values[index] = node, node.native
		}
		check := func() error {
			for _, node := range nodes {
				if err := node.valid(session); err != nil {
					return err
				}
			}
			return nil
		}
		return sdk.Run(session.commandWork(ctx, true, check), build(values))
	}}
}

// Dump returns the native tree format in a bounded value instead of calling a
// caller-owned writer while the SDK holds its DOM locks.
func Dump[S TextSelector](selector S, options ...QueryOption) Action[string] {
	return DumpTree(selector, "", "  ", false, -1, true, 80*time.Millisecond, options...)
}

// DumpTree retains all native DumpTo formatting, depth and population choices.
func DumpTree[S TextSelector](selector S, prefix, indent string, nodeIDs bool, depth int64, pierce bool, wait time.Duration, options ...QueryOption) Action[string] {
	options = cloneQueryOptions(options)
	return Action[string]{bytes: int64(len(selector) + len(prefix) + len(indent)), local: func(ctx context.Context, session *Session) (string, error) {
		work, values, err := session.queryWork(ctx, options)
		if err != nil {
			return "", err
		}
		record := session.accounting()
		record.mu.Lock()
		remaining := session.owner.settings.MaxResultBytes - record.delivered
		record.mu.Unlock()
		buffer := boundedBuffer{limit: remaining}
		_, err = sdk.Run(work, sdk.DumpTo(selector, &buffer, prefix, indent, nodeIDs, depth, pierce, wait, values...))
		record.mu.Lock()
		defer record.mu.Unlock()
		if int64(len(buffer.data)) > session.owner.settings.MaxResultBytes-record.delivered {
			return "", failure(ErrLimit, "dump-output", err)
		}
		record.delivered += int64(len(buffer.data))
		return string(buffer.data), err
	}}
}
