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

package show

import (
	"context"
	"github.com/frost-leo/fathomry/cmd/fathomry/internal/command"
	catalog "github.com/frost-leo/fathomry/framework/workflow/catalog/v1"
	"github.com/frost-leo/fathomry/i18n/v1"
	"strings"
	"testing"
)

func TestReadLeafIsOfflineAndLocalized(t *testing.T) {
	calls := 0
	leaf := New(&command.Invocation{}, func(context.Context, catalog.ReadRequest) (catalog.View, error) { calls++; return catalog.View{}, nil })
	if leaf.Name() != "show" || len(leaf.Commands()) != 0 || calls != 0 || leaf.Args(leaf, []string{"unexpected"}) == nil {
		t.Fatal("read leaf performed work or grouped commands")
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
		selected, err := presenter.WithLocale(locale)
		if err != nil {
			t.Fatal(err)
		}
		for _, id := range []string{leaf.Short, leaf.Flags().Lookup("namespace").Usage, leaf.Flags().Lookup("locale").Usage} {
			shown, err := selected.Render(id, nil, nil)
			if err != nil || shown.Locale != locale || shown.Text == "" || strings.HasPrefix(shown.Text, "fathomry.") {
				t.Fatal("missing localization", err)
			}
		}
	}
}
