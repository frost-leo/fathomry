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

package redis

import (
	native "github.com/frost-leo/fathomry/internal/cache/redis/v9"
	source "github.com/frost-leo/fathomry/internal/resource"
	"strconv"
)

// Prepared freezes one resolved configuration and its authoritative reservations.
// Selection, recommendations and Open cannot disagree about overlays/defaults.
// A rotating Password remains explicitly shared; all other settings are frozen.
type Prepared struct {
	private
	native   native.Prepared
	metadata native.Metadata
}

func Prepare(value Settings) (Prepared, error) { return prepare(value, nil) }
func PrepareWithPassword(value Settings, password *Password) (Prepared, error) {
	if password == nil || password.native == nil {
		return Prepared{}, fail(ErrInput, "credentials")
	}
	return prepare(value, password.native)
}
func prepare(value Settings, password *native.Password) (Prepared, error) {
	var layers []source.Layer
	if value.MaxIdleTime != nil {
		layers = []source.Layer{{Kind: source.Local, Content: []byte("max_idle_time_ns: " + strconv.FormatInt(int64(*value.MaxIdleTime), 10) + "\n")}}
	}
	prepared, err := native.PrepareV1(options(value), password, layers...)
	if err != nil {
		return Prepared{}, translate(err, "prepare", Cache)
	}
	return Prepared{native: prepared, metadata: prepared.Metadata()}, nil
}
