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

package viper

import (
	"encoding/json"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"

	native "github.com/frost-leo/fathomry/internal/configsource/viper/v1"
)

const (
	ProviderID        = native.ProviderID
	MaxSources        = native.MaxSources
	MaxDocumentBytes  = native.MaxDocumentBytes
	MaxTotalBytes     = native.MaxTotalBytes
	MaxBootstrapBytes = native.MaxBootstrapBytes
	MaxEntries        = native.MaxEntries
	MaxKeyBytes       = native.MaxKeyBytes
	MaxDepth          = native.MaxDepth
	MaxNodes          = native.MaxNodes
)

// Settings is loadable data, not guarded native Options. Encoding is required:
// yaml/yml/json/toml/dotenv/env. Empty lists disable those native sources. Storage
// is borrowed during Load only; retained storage is independently frozen.
type Settings struct {
	Encoding           string        `json:"encoding" mapstructure:"encoding"`
	Defaults           []Default     `json:"defaults" mapstructure:"defaults"`
	Environment        []Binding     `json:"environment" mapstructure:"environment"`
	AllowEmptyEnv      bool          `json:"allow_empty_env" mapstructure:"allow_empty_env"`
	AutomaticEnv       bool          `json:"automatic_env" mapstructure:"automatic_env"`
	EnvPrefix          string        `json:"env_prefix" mapstructure:"env_prefix"`
	EnvKeyReplacements []Replacement `json:"env_key_replacements" mapstructure:"env_key_replacements"`
}

// Scalar expresses a native default without interface fields or precision loss.
// Kind is null/string/bool, int/int8/int16/int32/int64,
// uint/uint8/uint16/uint32/uint64, or float32/float64. Text is an exact JSON-number
// spelling, true/false, arbitrary string text, or empty for null, respectively.
// Integer exponents/fractions and nonfinite floats reject. int/uint follow the
// target architecture; fixed-width kinds are portable. An absent Kind is invalid.
type Scalar struct {
	Kind string `json:"kind" mapstructure:"kind"`
	Text string `json:"text" mapstructure:"text"`
}

// Default is an ordered native default, not a strict application default layer.
type Default struct {
	Key   string `json:"key" mapstructure:"key"`
	Value Scalar `json:"value" mapstructure:"value"`
}

// Binding selects an exact environment name. Repeated keys append in order.
// Lookup remains live; AutomaticEnv takes precedence and does not enumerate keys.
type Binding struct {
	Key  string `json:"key" mapstructure:"key"`
	Name string `json:"name" mapstructure:"name"`
}

// Replacement is one ordered environment-name substitution.
type Replacement struct {
	Old string `json:"old" mapstructure:"old"`
	New string `json:"new" mapstructure:"new"`
}

// WatchSettings observes explicit absolute files, not environment variables.
// Zero Interval selects 1s (valid 10ms..5min); zero QueueCapacity selects 16 (1..64).
// Missing files are permitted. The loadable interval unit is nanoseconds.
type WatchSettings struct {
	Paths         []string      `json:"paths" mapstructure:"paths"`
	Interval      time.Duration `json:"interval_ns" mapstructure:"interval_ns"`
	QueueCapacity int           `json:"queue_capacity" mapstructure:"queue_capacity"`
}

func scalar(value Scalar) (any, error) {
	invalid := func() (any, error) { return nil, fail(ErrInput, "default") }
	if len(value.Kind) > 8 {
		return invalid()
	}
	switch value.Kind {
	case "null":
		if value.Text != "" {
			return invalid()
		}
		return nil, nil
	case "string":
		return value.Text, nil
	case "bool":
		if value.Text == "true" {
			return true, nil
		}
		if value.Text == "false" {
			return false, nil
		}
		return invalid()
	}
	types := map[string]reflect.Type{
		"int": reflect.TypeFor[int](), "int8": reflect.TypeFor[int8](), "int16": reflect.TypeFor[int16](), "int32": reflect.TypeFor[int32](), "int64": reflect.TypeFor[int64](),
		"uint": reflect.TypeFor[uint](), "uint8": reflect.TypeFor[uint8](), "uint16": reflect.TypeFor[uint16](), "uint32": reflect.TypeFor[uint32](), "uint64": reflect.TypeFor[uint64](),
		"float32": reflect.TypeFor[float32](), "float64": reflect.TypeFor[float64](),
	}
	kind, ok := types[value.Kind]
	if !ok || len(value.Text) > 64 || !json.Valid([]byte(value.Text)) {
		return invalid()
	}
	result := reflect.New(kind).Elem()
	switch kind.Kind() {
	case reflect.Float32, reflect.Float64:
		number, err := strconv.ParseFloat(value.Text, kind.Bits())
		if err != nil || math.IsNaN(number) || math.IsInf(number, 0) {
			return invalid()
		}
		result.SetFloat(number)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if strings.ContainsAny(value.Text, ".eE") {
			return invalid()
		}
		number, err := strconv.ParseInt(value.Text, 10, kind.Bits())
		if err != nil {
			return invalid()
		}
		result.SetInt(number)
	default:
		if strings.ContainsAny(value.Text, ".eE") {
			return invalid()
		}
		number, err := strconv.ParseUint(value.Text, 10, kind.Bits())
		if err != nil {
			return invalid()
		}
		result.SetUint(number)
	}
	return result.Interface(), nil
}
func options(value Settings) (native.OptionsV1, error) {
	if len(value.Defaults) > MaxEntries || len(value.Environment) > MaxEntries || len(value.EnvKeyReplacements) > MaxEntries {
		return native.OptionsV1{}, fail(ErrLimit, "settings")
	}
	result := native.OptionsV1{Encoding: value.Encoding, AllowEmptyEnv: value.AllowEmptyEnv, AutomaticEnv: value.AutomaticEnv, EnvPrefix: value.EnvPrefix}
	for _, entry := range value.Defaults {
		if len(entry.Value.Text) > MaxBootstrapBytes {
			return native.OptionsV1{}, fail(ErrLimit, "default")
		}
		decoded, err := scalar(entry.Value)
		if err != nil {
			return native.OptionsV1{}, err
		}
		result.Defaults = append(result.Defaults, native.Default{Key: entry.Key, Value: decoded})
	}
	for _, entry := range value.Environment {
		result.Environment = append(result.Environment, native.Binding{Key: entry.Key, Name: entry.Name})
	}
	for _, entry := range value.EnvKeyReplacements {
		result.EnvKeyReplacements = append(result.EnvKeyReplacements, native.Replacement{Old: entry.Old, New: entry.New})
	}
	return result, nil
}
