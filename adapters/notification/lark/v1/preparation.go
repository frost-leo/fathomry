/*
fathomry
Copyright (C) 2026  Frost Leo
SPDX-License-Identifier: GPL-3.0-or-later

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.
*/

package lark

import (
	native "github.com/frost-leo/fathomry/internal/notification/lark/v3"
)

// Prepared freezes one exact source selection, without network or callbacks.
type Prepared struct {
	private
	native   native.Prepared
	metadata native.Budget
}

func Prepare(value Settings, hooks NativeOptions) (Prepared, error) {
	prepared, err := native.PrepareV1(options(value, hooks))
	if err != nil {
		return Prepared{}, translate(err, "prepare")
	}
	result := Prepared{native: prepared, metadata: prepared.Metadata()}
	if _, err := result.Policy(); err != nil {
		return Prepared{}, err
	}
	return result, nil
}
func (prepared Prepared) Policy() (Policy, error) { return Compose(prepared) }
