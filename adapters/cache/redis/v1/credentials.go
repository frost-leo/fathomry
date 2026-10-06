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
)

// Password is composition-only, shared local rotation authority. Replace does no
// I/O and changes only newly authenticated connections, never the frozen username.
// Existing sockets are not reauthenticated. Do not give this handle to handlers.
type Password struct {
	private
	native *native.Password
}

func NewPassword(value string) (*Password, error) {
	password, err := native.NewPassword(value)
	if err != nil {
		return nil, translate(err, "credentials", Cache)
	}
	return &Password{native: password}, nil
}
func (password *Password) Replace(value string) error {
	if password == nil || password.native == nil {
		return fail(ErrInput, "credentials")
	}
	return translate(password.native.Replace(value), "credentials", Cache)
}
