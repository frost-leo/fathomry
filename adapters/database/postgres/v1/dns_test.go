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

package postgres

import (
	"context"
	"errors"
	"testing"

	configsource "github.com/frost-leo/fathomry/adapters/configsource/v1"
)

func TestDNSSettingOptIn(t *testing.T) {
	value := peerSettings()
	value.Address = "database.test.invalid"
	if !errors.Is(Validate(value), ErrInput) {
		t.Fatal("DNS admitted by default")
	}
	value.ResolveDNS = true
	if err := Validate(value); err != nil {
		t.Fatal("explicit DNS rejected", err)
	}
	if _, err := Recommend(value); err != nil {
		t.Fatal("DNS policy unavailable", err)
	}
	prepared, err := configsource.Prepare(context.Background(), configsource.Schema[Settings]{Version: 1, Defaults: value}, []configsource.Layer{{Kind: configsource.Base, Encoding: configsource.JSON, Content: []byte(`{"resolve_dns":false}`)}})
	if err != nil {
		t.Fatal(err)
	}
	selected, err := prepared.ValueCopy()
	if err != nil || selected.ResolveDNS || !errors.Is(Validate(selected), ErrInput) {
		t.Fatal("DNS override not honored", err)
	}
}
