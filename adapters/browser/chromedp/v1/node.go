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

	jsonv2 "github.com/chromedp/cdproto/cdp/jsonv2"
	"github.com/frost-leo/fathomry/adapters/v1"
	native "github.com/frost-leo/fathomry/internal/browser/chromedp/v0"
)

// Node is a borrowed DOM capability tied to its original page, source and
// document. It owns no independent producer and is revoked with those owners.
// DataCopy deliberately exposes frozen native protocol-node metadata.
type Node struct {
	private
	native *native.Node
}

func (node *Node) DataCopy() []byte {
	if node == nil || node.native == nil {
		return nil
	}
	return node.native.DataCopy()
}

// QueryNodes delegates native selectors and readiness, then returns opaque node
// capabilities. It never returns chromedp's mutable node cache or native owners.
func QueryNodes[S TextSelector](ctx context.Context, page *Page, selector S, options ...QueryOption) (nodes []*Node, receipt *adapters.Receipt[Result], err error) {
	_, receipt, err = runPageOperation(ctx, page, "query_nodes", func(ctx context.Context) ([]jsonv2.Value, error) {
		values, err := native.QueryNodes(ctx, page.native, selector, nativeQuery(options)...)
		if err != nil {
			return nil, err
		}
		nodes = make([]*Node, len(values))
		payload := make([]jsonv2.Value, len(values))
		for index, value := range values {
			nodes[index] = &Node{native: value}
			payload[index] = jsonv2.Value(value.DataCopy())
		}
		return payload, nil
	})
	if err != nil {
		return nil, receipt, err
	}
	return nodes, receipt, nil
}

// FromNode delegates native subtree selection and same-process iframe behavior.
// Search and JSPath have the native SDK's FromNode limitations. Nil means root;
// a foreign, stale or zero handle never falls back to root selection.
func FromNode(node *Node) QueryOption {
	if node == nil {
		return QueryOption{native: native.FromNode(nil)}
	}
	if node.native == nil {
		return QueryOption{}
	}
	return QueryOption{native: native.FromNode(node.native)}
}

// ByNodes replaces lookup with the SDK's NodeIDs selector using only managed
// node grants. The selector string becomes a label, as with native ByFunc.
// Readiness and operation helpers remain native. No raw node IDs are accepted.
// The slice is borrowed until admitted execution copies it. Empty selection has
// native empty-node semantics, so use AtLeast(0) when an empty result is expected.
func ByNodes(nodes ...*Node) QueryOption {
	return QueryOption{native: native.SelectNodes(len(nodes), func(index int) *native.Node {
		if nodes[index] == nil {
			return nil
		}
		return nodes[index].native
	})}
}
