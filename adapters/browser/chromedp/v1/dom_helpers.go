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
	"time"

	sdk "github.com/chromedp/chromedp"
	native "github.com/frost-leo/fathomry/internal/browser/chromedp/v0"
)

type PopulateOption struct{ native native.PopulateOption }

func PopulateWait(wait time.Duration) PopulateOption {
	return PopulateOption{native: native.PopulateWait(wait)}
}

// Populate delegates native child retrieval, including explicit depth and piercing.
// Browser DOM-cache allocation is not a process-memory sandbox.
func Populate(depth int64, pierce bool, options ...PopulateOption) QueryOption {
	if len(options) > 64 {
		return QueryOption{}
	}
	values := make([]native.PopulateOption, len(options))
	for index, option := range options {
		values[index] = option.native
	}
	return QueryOption{native: native.Populate(depth, pierce, values...)}
}

// JavascriptAttribute retains the native property's type conversion and null
// normalization. Remote results and exceptions keep their owned group custody.
func JavascriptAttribute[T any, S TextSelector](selector S, name string, options ...QueryOption) Action[T] {
	return Action[T]{native: native.JavascriptAttribute[T](selector, name, nativeQuery(options)...), name: "javascript_attribute"}
}

func SetJavascriptAttribute[S TextSelector](selector S, name, value string, options ...QueryOption) Action[sdk.Void] {
	return Action[sdk.Void]{native: native.SetJavascriptAttribute(selector, name, value, nativeQuery(options)...), name: "set_javascript_attribute"}
}
func ScreenshotScale[S TextSelector](selector S, scale float64, options ...QueryOption) Action[[]byte] {
	return Action[[]byte]{native: native.ScreenshotScale(selector, scale, nativeQuery(options)...), name: "screenshot"}
}

// ScreenshotNodes uses native extents and scale without adding selector readiness.
// The slice is borrowed until execution returns; native node IDs are not grants.
func ScreenshotNodes(nodes []*Node, scale float64) Action[[]byte] {
	return Action[[]byte]{native: native.ScreenshotNodes(len(nodes), func(index int) *native.Node {
		return nativeNode(nodes[index])
	}, scale), name: "screenshot_nodes"}
}

func MouseClickXY(x, y float64, options ...sdk.MouseOption) Action[sdk.Void] {
	return Action[sdk.Void]{native: native.MouseClickXY(x, y, options...), name: "mouse_click"}
}

func MouseClickNode(node *Node, options ...sdk.MouseOption) Action[sdk.Void] {
	return Action[sdk.Void]{native: native.MouseClickNode(nativeNode(node), options...), name: "mouse_click_node"}
}

func KeyEventNode(node *Node, keys string, options ...sdk.KeyOption) Action[sdk.Void] {
	return Action[sdk.Void]{native: native.KeyEventNode(nativeNode(node), keys, options...), name: "key_event_node"}
}

func nativeNode(node *Node) *native.Node {
	if node == nil {
		return nil
	}
	return node.native
}

// Dump returns native tree text in a bounded, independently retained value.
func Dump[S TextSelector](selector S, options ...QueryOption) Action[string] {
	return Action[string]{native: native.Dump(selector, nativeQuery(options)...), name: "dump"}
}

// DumpTree exposes native DumpTo choices without a writer callback under DOM locks.
func DumpTree[S TextSelector](selector S, prefix, indent string, nodeIDs bool, depth int64, pierce bool, wait time.Duration, options ...QueryOption) Action[string] {
	return Action[string]{native: native.DumpTree(selector, prefix, indent, nodeIDs, depth, pierce, wait, nativeQuery(options)...), name: "dump"}
}
