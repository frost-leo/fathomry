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

package create

import (
	"context"
	"github.com/frost-leo/fathomry/cmd/fathomry/internal/command"
	"github.com/frost-leo/fathomry/i18n/v1"
	"testing"
)

func TestCreateLeafIsLazyAndLocalized(t *testing.T) {
	calls := 0
	leaf := New(&command.Invocation{}, func(context.Context, string) (DatabaseChange, error) {
		calls++
		return DatabaseChange{}, nil
	})
	if leaf.Name() != "create" || len(leaf.Commands()) != 0 || calls != 0 || leaf.Flags().Lookup("admin-config") == nil {
		t.Fatal("leaf shape or lazy construction changed")
	}
	if leaf.Args(leaf, []string{"unexpected"}) == nil {
		t.Fatal("positional database name accepted")
	}
	messages, err := i18n.Prepare(Component())
	if err != nil {
		t.Fatal(err)
	}
	presenter, err := i18n.NewPresenter(messages)
	if err != nil {
		t.Fatal(err)
	}
	for _, locale := range []string{"en", "zh-CN"} {
		current, err := presenter.WithLocale(locale)
		if err != nil {
			t.Fatal(err)
		}
		text, err := current.Render(leaf.Short, nil, nil)
		if err != nil || text.Locale != locale || text.Text == "" {
			t.Fatal("missing leaf localization", err)
		}
	}
}
