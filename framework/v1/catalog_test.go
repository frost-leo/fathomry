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

package framework_test

import (
	"errors"
	"testing"

	"github.com/frost-leo/fathomry/framework/configuration/v1"
	"github.com/frost-leo/fathomry/framework/v1"
	"github.com/frost-leo/fathomry/i18n/v1"
)

func TestPrepare(t *testing.T) {
	components := append(i18n.CoreComponents(), configuration.Component())
	catalogs, err := framework.Prepare(components...)
	if err != nil || catalogs.Errors == nil || catalogs.Messages == nil {
		t.Fatal("paired preparation failed", err)
	}
	for _, input := range [][]i18n.Component{nil, append(components, configuration.Component()), make([]i18n.Component, i18n.MaxComponents+1)} {
		pair, err := framework.Prepare(input...)
		if err == nil || pair.Errors != nil || pair.Messages != nil {
			t.Fatal("invalid pair escaped")
		}
	}
	_, expected := i18n.Prepare()
	_, actual := framework.Prepare()
	if !errors.Is(actual, i18n.ErrResource) || expected == nil {
		t.Fatal("originating error identity lost")
	}
}
