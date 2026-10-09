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
	"encoding/base64"
	"log/slog"
	"math"
	"sort"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
)

const (
	MaxAttributes = 64
	MaxNodes      = 256
	MaxDepth      = 8
	// MaxIdentityBytes applies independently to resource and scope attributes.
	MaxIdentityBytes = 16 << 10
)

// Value is a closed, serializable telemetry value. Kind is null (also the zero
// value), bool, int64, float64, string, bytes, array or map. Only the field named
// by Kind may contain data. Nil collections denote present empty collections.
// Inputs are borrowed until the operation or preparation returns, then copied.
// No arbitrary marshaler, reflection callback or owning SDK value is accepted.
type Value struct {
	Kind    string           `json:"kind"`
	Bool    bool             `json:"bool,omitempty"`
	Int64   int64            `json:"int64,omitempty"`
	Float64 float64          `json:"float64,omitempty"`
	String  string           `json:"string,omitempty"`
	Bytes   []byte           `json:"bytes,omitempty"`
	Array   []Value          `json:"array,omitempty"`
	Map     []TypedAttribute `json:"map,omitempty"`
}

// TypedAttribute preserves an explicit key and a closed typed value. Duplicate
// keys are rejected, including between typed and legacy identity attributes.
type TypedAttribute struct {
	Key   string `json:"key"`
	Value Value  `json:"value"`
}

// ConfigAttribute is a strict-loadable typed resource or scope attribute. Its
// value is a flat tree because configuration schemas do not admit recursive Go
// types or custom serialization. Tree builds this representation from Value.
type ConfigAttribute struct {
	Key   string    `json:"key" mapstructure:"key"`
	Value ValueTree `json:"value" mapstructure:"value"`
}

// ValueTree encodes a closed value with node zero as root. Every other node must
// have exactly one earlier parent. Unused nodes, sharing and cycles are refused.
// An empty tree is null. Limits are shared with its containing attribute set.
type ValueTree struct {
	Nodes []ValueNode `json:"nodes" mapstructure:"nodes"`
}

// ValueNode is one scalar, binary, array or map node. Scalar field names follow
// Value.Kind; Base64 is canonical padded standard base64 for bytes. Children is
// present only for arrays/maps, and Keys only for maps in matching child order.
// Inactive nonzero fields and nonnil inactive collections are refused.
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

// Tree creates an independently owned canonical configuration tree. It applies
// the resource/scope value ceiling before copying; the final attribute set also
// charges keys and other values against the same MaxIdentityBytes/MaxNodes.
func Tree(value Value) (ValueTree, error) {
	budget := dataBudget{bytes: MaxIdentityBytes, nodes: MaxNodes}
	if _, err := freezeTypedValue(value, &budget, 1); err != nil {
		return ValueTree{}, err
	}
	result := ValueTree{}
	var appendValue func(Value) int
	appendValue = func(value Value) int {
		index := len(result.Nodes)
		node := ValueNode{Kind: strings.Clone(value.Kind), Bool: value.Bool, Int64: value.Int64,
			Float64: value.Float64, String: strings.Clone(value.String)}
		result.Nodes = append(result.Nodes, node)
		switch value.Kind {
		case "bytes":
			node.Base64 = base64.StdEncoding.EncodeToString(value.Bytes)
		case "array":
			node.Children = make([]int, len(value.Array))
			for child, item := range value.Array {
				node.Children[child] = appendValue(item)
			}
		case "map":
			node.Children = make([]int, len(value.Map))
			node.Keys = make([]string, len(value.Map))
			for child, item := range value.Map {
				node.Keys[child] = strings.Clone(item.Key)
				node.Children[child] = appendValue(item.Value)
			}
		}
		result.Nodes[index] = node
		return index
	}
	appendValue(value)
	return result, nil
}

// AttributeMap is an explicit nested attribute map. Use it with slog.Any to
// preserve empty child groups: slog's ordinary []Attr/GroupValue conversion
// removes them. Storage is borrowed until the operation returns, then copied.
// Nil and empty maps both encode as present empty maps, not absent attributes.
type AttributeMap []slog.Attr

func keyValid(key string) bool {
	return key != "" && boundedString(key, 128) && !strings.HasPrefix(key, "fathomry.")
}

type dataBudget struct{ bytes, nodes int }

func (budget *dataBudget) charge(bytes, nodes int) error {
	if bytes < 0 || bytes > budget.bytes || nodes < 0 || nodes > budget.nodes {
		return failure(ErrLimit, "data")
	}
	budget.bytes -= bytes
	budget.nodes -= nodes
	return nil
}

func freezeTypedValue(value Value, budget *dataBudget, depth int) (attribute.Value, error) {
	if depth > MaxDepth {
		return attribute.Value{}, failure(ErrLimit, "value-depth")
	}
	if value.Kind != "bool" && value.Bool || value.Kind != "int64" && value.Int64 != 0 ||
		value.Kind != "float64" && value.Float64 != 0 || value.Kind != "string" && value.String != "" ||
		value.Kind != "bytes" && value.Bytes != nil || value.Kind != "array" && value.Array != nil ||
		value.Kind != "map" && value.Map != nil {
		return attribute.Value{}, failure(ErrInput, "value-fields")
	}
	if err := budget.charge(32, 1); err != nil {
		return attribute.Value{}, err
	}
	switch value.Kind {
	case "", "null":
		return attribute.Value{}, nil
	case "bool":
		return attribute.BoolValue(value.Bool), nil
	case "int64":
		return attribute.Int64Value(value.Int64), nil
	case "float64":
		if !finite(value.Float64) {
			return attribute.Value{}, failure(ErrInput, "number")
		}
		return attribute.Float64Value(value.Float64), nil
	case "string":
		if !boundedString(value.String, len(value.String)) {
			return attribute.Value{}, failure(ErrInput, "text")
		}
		if err := budget.charge(len(value.String), 0); err != nil {
			return attribute.Value{}, err
		}
		return attribute.StringValue(strings.Clone(value.String)), nil
	case "bytes":
		if err := budget.charge(len(value.Bytes), 0); err != nil {
			return attribute.Value{}, err
		}
		return attribute.ByteSliceValue(value.Bytes), nil
	case "array":
		if len(value.Array) > budget.nodes {
			return attribute.Value{}, failure(ErrLimit, "array")
		}
		frozen := make([]attribute.Value, 0, len(value.Array))
		for _, element := range value.Array {
			item, err := freezeTypedValue(element, budget, depth+1)
			if err != nil {
				return attribute.Value{}, err
			}
			frozen = append(frozen, item)
		}
		return attribute.SliceValue(frozen...), nil
	case "map":
		frozen, err := freezeTypedAttributes(value.Map, budget, depth+1)
		if err != nil {
			return attribute.Value{}, err
		}
		return attribute.MapValue(frozen...), nil
	default:
		return attribute.Value{}, failure(ErrUnsupported, "value-kind")
	}
}

func freezeTypedAttributes(attrs []TypedAttribute, budget *dataBudget, depth int) ([]attribute.KeyValue, error) {
	if depth > MaxDepth || len(attrs) > MaxAttributes || len(attrs) > budget.nodes {
		return nil, failure(ErrLimit, "attributes")
	}
	frozen := make([]attribute.KeyValue, 0, len(attrs))
	names := make(map[string]bool, len(attrs))
	for _, attr := range attrs {
		if !keyValid(attr.Key) || names[attr.Key] {
			return nil, failure(ErrInput, "attribute-key")
		}
		names[attr.Key] = true
		if err := budget.charge(len(attr.Key)+32, 1); err != nil {
			return nil, err
		}
		value, err := freezeTypedValue(attr.Value, budget, depth)
		if err != nil {
			return nil, err
		}
		frozen = append(frozen, attribute.KeyValue{Key: attribute.Key(strings.Clone(attr.Key)), Value: value})
	}
	return frozen, nil
}

func freezeConfigValue(tree ValueTree, budget *dataBudget, depth int) (attribute.Value, error) {
	if len(tree.Nodes) == 0 {
		return freezeTypedValue(Value{}, budget, depth)
	}
	if len(tree.Nodes) > budget.nodes {
		return attribute.Value{}, failure(ErrLimit, "identity-nodes")
	}
	parents := make([]bool, len(tree.Nodes))
	for index, node := range tree.Nodes {
		if node.Kind != "array" && node.Kind != "map" && node.Children != nil ||
			node.Kind != "map" && node.Keys != nil || node.Kind == "map" && len(node.Keys) != len(node.Children) {
			return attribute.Value{}, failure(ErrInput, "identity-tree")
		}
		if len(node.Children) > len(tree.Nodes)-1 {
			return attribute.Value{}, failure(ErrLimit, "identity-nodes")
		}
		for _, child := range node.Children {
			if child <= index || child >= len(tree.Nodes) || parents[child] {
				return attribute.Value{}, failure(ErrInput, "identity-tree")
			}
			parents[child] = true
		}
	}
	for _, hasParent := range parents[1:] {
		if !hasParent {
			return attribute.Value{}, failure(ErrInput, "identity-tree")
		}
	}
	var visit func(int, int) (attribute.Value, error)
	visit = func(index, depth int) (attribute.Value, error) {
		node := tree.Nodes[index]
		value := Value{Kind: node.Kind, Bool: node.Bool, Int64: node.Int64, Float64: node.Float64, String: node.String}
		if node.Kind != "bytes" && node.Base64 != "" {
			return attribute.Value{}, failure(ErrInput, "value-fields")
		}
		if node.Kind == "bytes" {
			if len(node.Base64)%4 != 0 || len(node.Base64) > base64.StdEncoding.EncodedLen(budget.bytes) {
				return attribute.Value{}, failure(ErrLimit, "identity-bytes")
			}
			data, err := base64.StdEncoding.Strict().DecodeString(node.Base64)
			if err != nil || base64.StdEncoding.EncodeToString(data) != node.Base64 {
				return attribute.Value{}, failure(ErrInput, "identity-base64")
			}
			value.Bytes = data
		}
		frozen, err := freezeTypedValue(value, budget, depth)
		if err != nil || node.Kind != "array" && node.Kind != "map" {
			return frozen, err
		}
		if node.Kind == "array" {
			if len(node.Children) > budget.nodes {
				return attribute.Value{}, failure(ErrLimit, "identity-nodes")
			}
			items := make([]attribute.Value, 0, len(node.Children))
			for _, child := range node.Children {
				item, err := visit(child, depth+1)
				if err != nil {
					return attribute.Value{}, err
				}
				items = append(items, item)
			}
			return attribute.SliceValue(items...), nil
		}
		if len(node.Keys) > MaxAttributes || len(node.Keys) > budget.nodes {
			return attribute.Value{}, failure(ErrLimit, "identity-attributes")
		}
		items := make([]attribute.KeyValue, 0, len(node.Keys))
		names := make(map[string]bool, len(node.Keys))
		for offset, key := range node.Keys {
			if !keyValid(key) || names[key] {
				return attribute.Value{}, failure(ErrInput, "attribute-key")
			}
			names[key] = true
			if err := budget.charge(len(key)+32, 1); err != nil {
				return attribute.Value{}, err
			}
			item, err := visit(node.Children[offset], depth+1)
			if err != nil {
				return attribute.Value{}, err
			}
			items = append(items, attribute.KeyValue{Key: attribute.Key(strings.Clone(key)), Value: item})
		}
		return attribute.MapValue(items...), nil
	}
	return visit(0, depth)
}

func freezeIdentityAttributes(legacy map[string]string, typed []ConfigAttribute) ([]attribute.KeyValue, error) {
	if len(legacy)+len(typed) > 16 {
		return nil, failure(ErrLimit, "identity-attributes")
	}
	attrs := make([]ConfigAttribute, 0, len(legacy)+len(typed))
	keys := make([]string, 0, len(legacy))
	for key, text := range legacy {
		if !boundedString(text, 256) {
			return nil, failure(ErrInput, "identity-attributes")
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		attrs = append(attrs, ConfigAttribute{Key: key, Value: ValueTree{Nodes: []ValueNode{{Kind: "string", String: legacy[key]}}}})
	}
	attrs = append(attrs, typed...)
	budget := dataBudget{bytes: MaxIdentityBytes, nodes: MaxNodes}
	frozen := make([]attribute.KeyValue, 0, len(attrs))
	names := make(map[string]bool, len(attrs))
	for _, attr := range attrs {
		if !keyValid(attr.Key) || names[attr.Key] || attr.Key == "service.name" {
			return nil, failure(ErrInput, "identity-attributes")
		}
		names[attr.Key] = true
		if err := budget.charge(len(attr.Key)+32, 1); err != nil {
			return nil, err
		}
		value, err := freezeConfigValue(attr.Value, &budget, 1)
		if err != nil {
			return nil, err
		}
		frozen = append(frozen, attribute.KeyValue{Key: attribute.Key(strings.Clone(attr.Key)), Value: value})
	}
	return frozen, nil
}

// freezeAttributes accepts closed slog values, without resolving LogValuers,
// reflection, Stringers or marshalers. All returned SDK storage is independent.
func freezeAttributes(attrs []slog.Attr, budget *dataBudget, depth int) ([]attribute.KeyValue, error) {
	if depth > MaxDepth || len(attrs) > MaxAttributes || len(attrs) > budget.nodes {
		return nil, failure(ErrLimit, "attributes")
	}
	result := make([]attribute.KeyValue, 0, len(attrs))
	names := make(map[string]bool, len(attrs))
	for _, attr := range attrs {
		if !keyValid(attr.Key) || names[attr.Key] {
			return nil, failure(ErrInput, "attribute-key")
		}
		names[attr.Key] = true
		if err := budget.charge(len(attr.Key)+32, 1); err != nil {
			return nil, err
		}
		value, err := freezeValue(attr.Value, budget, depth)
		if err != nil {
			return nil, err
		}
		result = append(result, attribute.KeyValue{Key: attribute.Key(strings.Clone(attr.Key)), Value: value})
	}
	return result, nil
}
func freezeValue(value slog.Value, budget *dataBudget, depth int) (attribute.Value, error) {
	switch value.Kind() {
	case slog.KindString:
		text := value.String()
		if err := budget.charge(len(text), 0); err != nil {
			return attribute.Value{}, err
		}
		if !boundedString(text, len(text)) {
			return attribute.Value{}, failure(ErrInput, "text")
		}
		return attribute.StringValue(strings.Clone(text)), nil
	case slog.KindBool:
		return attribute.BoolValue(value.Bool()), nil
	case slog.KindInt64:
		return attribute.Int64Value(value.Int64()), nil
	case slog.KindUint64:
		if value.Uint64() > math.MaxInt64 {
			return attribute.Value{}, failure(ErrUnsupported, "unsigned-range")
		}
		return attribute.Int64Value(int64(value.Uint64())), nil
	case slog.KindFloat64:
		if !finite(value.Float64()) {
			return attribute.Value{}, failure(ErrInput, "number")
		}
		return attribute.Float64Value(value.Float64()), nil
	case slog.KindDuration:
		return attribute.Int64Value(int64(value.Duration())), nil
	case slog.KindTime:
		timestamp := value.Time().UTC()
		if timestamp.Year() < 1 || timestamp.Year() > 9999 {
			return attribute.Value{}, failure(ErrInput, "time")
		}
		if err := budget.charge(40, 0); err != nil {
			return attribute.Value{}, err
		}
		return attribute.StringValue(timestamp.Format(time.RFC3339Nano)), nil
	case slog.KindGroup:
		attrs, err := freezeAttributes(value.Group(), budget, depth+1)
		if err != nil {
			return attribute.Value{}, err
		}
		return attribute.MapValue(attrs...), nil
	case slog.KindAny:
		switch input := value.Any().(type) {
		case Value:
			return freezeTypedValue(input, budget, depth)
		case AttributeMap:
			attrs, err := freezeAttributes(input, budget, depth+1)
			if err != nil {
				return attribute.Value{}, err
			}
			return attribute.MapValue(attrs...), nil
		case nil:
			return attribute.Value{}, nil
		case []byte:
			if err := budget.charge(len(input), 0); err != nil {
				return attribute.Value{}, err
			}
			return attribute.ByteSliceValue(input), nil
		case []string:
			if len(input) > budget.nodes {
				return attribute.Value{}, failure(ErrLimit, "array")
			}
			for _, text := range input {
				if err := budget.charge(len(text)+16, 1); err != nil {
					return attribute.Value{}, err
				}
				if !boundedString(text, len(text)) {
					return attribute.Value{}, failure(ErrInput, "text")
				}
			}
			frozen := make([]string, len(input))
			for index, text := range input {
				frozen[index] = strings.Clone(text)
			}
			return attribute.StringSliceValue(frozen), nil
		case []int64:
			if len(input) > budget.nodes {
				return attribute.Value{}, failure(ErrLimit, "array")
			}
			if err := budget.charge(8*len(input), len(input)); err != nil {
				return attribute.Value{}, err
			}
			return attribute.Int64SliceValue(input), nil
		case []bool:
			if len(input) > budget.nodes {
				return attribute.Value{}, failure(ErrLimit, "array")
			}
			if err := budget.charge(len(input), len(input)); err != nil {
				return attribute.Value{}, err
			}
			return attribute.BoolSliceValue(input), nil
		case []float64:
			if len(input) > budget.nodes {
				return attribute.Value{}, failure(ErrLimit, "array")
			}
			if err := budget.charge(8*len(input), len(input)); err != nil {
				return attribute.Value{}, err
			}
			for _, number := range input {
				if !finite(number) {
					return attribute.Value{}, failure(ErrInput, "number")
				}
			}
			return attribute.Float64SliceValue(input), nil
		}
	}
	return attribute.Value{}, failure(ErrUnsupported, "attribute-value")
}
func finite(value float64) bool { return !math.IsNaN(value) && !math.IsInf(value, 0) }
func timestampValid(value time.Time) bool {
	return value.IsZero() || value.UTC().Year() >= 1970 && value.UTC().Year() <= 2261
}
