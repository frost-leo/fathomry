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

package chromedp_test

import (
	browser "github.com/frost-leo/fathomry/adapters/browser/chromedp/v1"
	"github.com/frost-leo/fathomry/failure/v1"
	"github.com/frost-leo/fathomry/i18n/v1"
	"testing"
)

func TestOfflineResourcesAndDefinitions(t *testing.T) {
	definitions := browser.Definitions()
	if len(definitions) != 7 {
		t.Fatal("stable failure allocation incomplete")
	}
	if _, err := failure.Prepare(definitions...); err != nil {
		t.Fatal(err)
	}
	if _, err := i18n.Prepare(i18n.Component{Module: "fathomry", Name: "browser_chromedp", BaseLocale: "en", Resources: browser.Resources(), Directory: "resources", Definitions: definitions}); err != nil {
		t.Fatal(err)
	}
	definitions[0].Message = "modified"
	if browser.Definitions()[0].Message == "modified" {
		t.Fatal("definitions alias caller mutation")
	}
}
