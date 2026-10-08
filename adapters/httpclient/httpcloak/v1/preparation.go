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

package httpcloak

import (
	"bytes"
	"encoding/json"
	"unicode/utf8"

	native "github.com/frost-leo/fathomry/internal/httpclient/httpcloak/v1"
	source "github.com/frost-leo/fathomry/internal/resource"
)

// Prepared freezes one exact data/native selection. It owns no sockets. Copies
// safely share frozen containers and the explicitly borrowed runtime dependencies.
type Prepared struct {
	private
	native   native.Prepared
	metadata native.Budget
}

// Prepare validates and freezes offline without invoking native dependencies.
// Inputs must not be mutated concurrently with this call.
func Prepare(value Settings, dependencies NativeOptions) (Prepared, error) {
	data, err := settingsData(value)
	if err != nil {
		return Prepared{}, err
	}
	prepared, err := native.PrepareV1(native.OptionsV1{Name: value.Name, Version: value.Version, Native: nativeOptions(dependencies)}, source.Layer{Kind: source.Local, Content: data})
	if err != nil {
		return Prepared{}, translate(err, "prepare")
	}
	result := Prepared{native: prepared, metadata: prepared.Metadata()}
	if _, err := result.Policy(); err != nil {
		return Prepared{}, err
	}
	return result, nil
}

func settingsData(value Settings) ([]byte, error) {
	input := native.OptionsV1{Name: value.Name, Version: value.Version, PresetName: value.PresetName,
		PresetJSON: value.PresetJSON, ProxyURL: value.ProxyURL, ResolverAddress: value.ResolverAddress}
	if value.Protocol != nil {
		input.Protocol = native.ProtocolMode(*value.Protocol)
	}
	if value.ResolverNetwork != nil {
		input.ResolverNetwork = *value.ResolverNetwork
	}
	if err := native.ValidateDataInputV1(input); err != nil {
		return nil, translate(err, "prepare")
	}
	if value.Protocol != nil && !utf8.ValidString(string(*value.Protocol)) {
		return nil, fail(ErrInput, "prepare")
	}
	if value.ResolverNetwork != nil && !utf8.ValidString(*value.ResolverNetwork) {
		return nil, fail(ErrInput, "prepare")
	}
	for _, text := range []string{value.Name, value.ProxyURL, value.PresetName, value.PresetJSON, value.ResolverAddress} {
		if !utf8.ValidString(text) {
			return nil, fail(ErrInput, "prepare")
		}
	}
	data, err := json.Marshal(value)
	if err != nil {
		return nil, translate(err, "prepare")
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return nil, translate(err, "prepare")
	}
	delete(fields, "name")
	delete(fields, "version")
	for name, input := range fields {
		if bytes.Equal(input, []byte("null")) {
			delete(fields, name)
		}
	}
	data, err = json.Marshal(fields)
	return data, translate(err, "prepare")
}
