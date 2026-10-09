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
	"bytes"
	"context"
	"encoding/json"
	"github.com/frost-leo/fathomry/adapters/configsource/v1"
	source "github.com/frost-leo/fathomry/internal/resource"
	native "github.com/frost-leo/fathomry/internal/telemetry/otel/v1"
)

// Prepared freezes one final selection. Copies share only immutable preparation;
// each Open constructs a distinct source with its own required reservations.
type Prepared struct {
	private
	native   native.Prepared
	metadata native.Metadata
}

// Prepare validates bounded plain data before encoding and performs no native,
// network, timer or goroutine acquisition. Inputs are borrowed until return.
func Prepare(value Settings) (Prepared, error) {
	bounded, err := configsource.Prepare(context.Background(), configsource.Schema[Settings]{Version: 1, Defaults: value}, nil)
	if err != nil {
		return Prepared{}, translate(err, "prepare")
	}
	frozen, err := bounded.ValueCopy()
	if err != nil {
		return Prepared{}, translate(err, "prepare")
	}
	encoded, err := json.Marshal(frozen)
	if err != nil {
		return Prepared{}, translate(err, "prepare")
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &fields); err != nil {
		return Prepared{}, translate(err, "prepare")
	}
	delete(fields, "name")
	delete(fields, "format")
	for name, data := range fields {
		if bytes.Equal(data, []byte("null")) {
			delete(fields, name)
		}
	}
	encoded, err = json.Marshal(fields)
	if err != nil {
		return Prepared{}, translate(err, "prepare")
	}
	selected, err := native.PrepareV1(nativeOptions(frozen), source.Layer{Kind: source.Local, Content: encoded})
	if err != nil {
		return Prepared{}, translate(err, "prepare")
	}
	result := Prepared{native: selected, metadata: selected.Metadata()}
	if _, err := result.Policy(); err != nil {
		return Prepared{}, err
	}
	return result, nil
}
