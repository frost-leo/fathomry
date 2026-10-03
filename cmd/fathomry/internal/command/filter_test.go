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

package command

import (
	"errors"
	"strings"
	"testing"
)

func TestOwnerFilter(t *testing.T) {
	for _, filter := range []OwnerFilter{
		{}, {Module: "example"}, {Component: "source"}, {Module: "example.one", Component: "source.two"},
		{Module: strings.Repeat("a", 128), Component: strings.Repeat("a", 64)},
	} {
		if err := filter.Validate(); err != nil {
			t.Fatal("valid namespace rejected", filter, err)
		}
	}
	for _, filter := range []OwnerFilter{
		{Module: strings.Repeat("a", 129)}, {Component: strings.Repeat("a", 65)},
		{Module: "Example"}, {Component: ".source"}, {Module: "example..one"}, {Component: "source/one"},
	} {
		if !errors.Is(filter.Validate(), ErrUsage) {
			t.Fatal("invalid namespace admitted", filter)
		}
	}
	filter := OwnerFilter{Module: "example", Component: "source"}
	if !filter.Match("example", "source") || filter.Match("example", "secondary") || filter.Match("other", "source") ||
		!errors.Is(filter.RequireMatch(false), ErrNotFound) || filter.RequireMatch(true) != nil || (OwnerFilter{}).RequireMatch(false) != nil {
		t.Fatal("owner intersection or absence contract lost")
	}
	if !ContainsQuery("mixed", "MIXED Case") || !ContainsQuery("", "anything") || ContainsQuery("[a-z]", "anything") {
		t.Fatal("literal case-insensitive search changed")
	}
}
