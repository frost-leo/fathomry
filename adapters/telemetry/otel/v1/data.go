/**
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

package otel

import (
	native "github.com/frost-leo/fathomry/internal/telemetry/otel/v1"
	"log/slog"
)

const (
	MaxAttributes    = native.MaxAttributes
	MaxNodes         = native.MaxNodes
	MaxDepth         = native.MaxDepth
	MaxIdentityBytes = native.MaxIdentityBytes
)

// Value is closed typed telemetry data, not a runtime handle. Kind is null
// (including zero), bool, int64, float64, string, bytes, array or map. Only the
// selected field may contain data. Nil collections mean present empty values.
// Inputs are borrowed until return, then independently frozen. No callbacks run.
type Value struct {
	Kind    string
	Bool    bool
	Int64   int64
	Float64 float64
	String  string
	Bytes   []byte
	Array   []Value
	Map     []TypedAttribute
}

// TypedAttribute is an explicit map entry; duplicate or reserved keys are refused.
type TypedAttribute struct {
	Key   string
	Value Value
}

// ConfigAttribute supplies typed resource/scope data with a strict-loadable flat
// tree. Combined typed/legacy sets allow 16 unique keys and 16 KiB per set.
type ConfigAttribute struct {
	Key   string    `json:"key" mapstructure:"key"`
	Value ValueTree `json:"value" mapstructure:"value"`
}

// ValueTree is rooted at node zero; empty is null. Other nodes have one earlier
// parent. Cycles, sharing, unused nodes, depth over 8 and excess nodes are refused.
type ValueTree struct {
	Nodes []ValueNode `json:"nodes" mapstructure:"nodes"`
}

// ValueNode encodes a scalar or references array/map children. Base64 is canonical
// padded standard base64 for binary. Map Keys correspond to Children in order.
type ValueNode struct {
	Kind     string   `json:"kind" mapstructure:"kind"`
	Bool     bool     `json:"bool" mapstructure:"bool"`
	Int64    int64    `json:"int64" mapstructure:"int64"`
	Float64  float64  `json:"float64" mapstructure:"float64"`
	String   string   `json:"string" mapstructure:"string"`
	Base64   string   `json:"base64" mapstructure:"base64"`
	Children []int    `json:"children" mapstructure:"children"`
	Keys     []string `json:"keys" mapstructure:"keys"`
}

// Tree validates and copies a runtime Value into strict-loadable identity data.
// The final containing attribute set additionally charges keys and other values.
func Tree(value Value) (ValueTree, error) {
	nodes := MaxNodes
	converted, err := nativeValue(value, &nodes, 1)
	if err != nil {
		return ValueTree{}, err
	}
	tree, err := native.Tree(converted)
	if err != nil {
		return ValueTree{}, translate(err, "tree")
	}
	result := ValueTree{Nodes: make([]ValueNode, len(tree.Nodes))}
	for index, node := range tree.Nodes {
		result.Nodes[index] = ValueNode{Kind: node.Kind, Bool: node.Bool, Int64: node.Int64, Float64: node.Float64, String: node.String, Base64: node.Base64, Children: node.Children, Keys: node.Keys}
	}
	return result, nil
}

func nativeValue(value Value, nodes *int, depth int) (native.Value, error) {
	if depth > MaxDepth || *nodes < 1 || len(value.Array) > *nodes || len(value.Map) > *nodes {
		return native.Value{}, fail(ErrLimit, "value")
	}
	*nodes--
	result := native.Value{Kind: value.Kind, Bool: value.Bool, Int64: value.Int64, Float64: value.Float64, String: value.String, Bytes: value.Bytes}
	if value.Array != nil {
		result.Array = make([]native.Value, len(value.Array))
		for index, item := range value.Array {
			converted, err := nativeValue(item, nodes, depth+1)
			if err != nil {
				return native.Value{}, err
			}
			result.Array[index] = converted
		}
	}
	if value.Map != nil {
		result.Map = make([]native.TypedAttribute, len(value.Map))
		for index, item := range value.Map {
			converted, err := nativeValue(item.Value, nodes, depth+1)
			if err != nil {
				return native.Value{}, err
			}
			result.Map[index] = native.TypedAttribute{Key: item.Key, Value: converted}
		}
	}
	return result, nil
}

// AttributeMap preserves present empty nested maps that slog.Group would remove.
// Use slog.Any with this explicit type; arbitrary Any callbacks are not accepted.
type AttributeMap []slog.Attr

func nativeAttributes(values []slog.Attr) ([]slog.Attr, error) {
	nodes := MaxNodes
	return mapAttributes(values, &nodes, 1)
}
func mapAttributes(values []slog.Attr, nodes *int, depth int) ([]slog.Attr, error) {
	if depth > MaxDepth || len(values) > MaxAttributes || len(values) > *nodes {
		return nil, fail(ErrLimit, "attributes")
	}
	*nodes -= len(values)
	result := make([]slog.Attr, len(values))
	for index, value := range values {
		result[index] = value
		var nested []slog.Attr
		group := false
		if value.Value.Kind() == slog.KindGroup {
			nested = value.Value.Group()
			group = true
		}
		if value.Value.Kind() == slog.KindAny {
			switch input := value.Value.Any().(type) {
			case AttributeMap:
				nested = input
				group = true
			case Value:
				converted, err := nativeValue(input, nodes, depth)
				if err != nil {
					return nil, err
				}
				result[index].Value = slog.AnyValue(converted)
			}
		}
		if group {
			converted, err := mapAttributes(nested, nodes, depth+1)
			if err != nil {
				return nil, err
			}
			result[index].Value = slog.AnyValue(native.AttributeMap(converted))
		}
	}
	return result, nil
}
