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

package build

import (
	"bytes"
	"strings"
	"testing"

	"github.com/frost-leo/fathomry/failure/v1"
	"github.com/frost-leo/fathomry/i18n/v1"
)

func TestPresentation(t *testing.T) {
	for _, language := range []string{"en", "zh-CN", "fr"} {
		var output bytes.Buffer
		result := Result{Name: "health", Target: "linux/amd64", Artifact: "/private/line\n\x1b[31m", SHA256: strings.Repeat("a", 64), Published: true, Inspection: inspectionSchema}
		if err := Render(&output, result, language); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(output.String(), "F A T H O M R Y") || strings.Contains(output.String(), "\x1b") {
			t.Fatal("unsafe banner", output.String())
		}
		if language == "en" && !strings.Contains(output.String(), "inspection unavailable") {
			t.Fatal("legacy build implied inspected binding coverage", output.String())
		}
		output.Reset()
		result.Inspection = implementationInspectionSchema
		if err := Render(&output, result, language); err != nil || !strings.Contains(output.String(), "0 / 0 ("+implementationInspectionSchema+")") {
			t.Fatal("v2 coverage not reported", err)
		}
	}
}

func TestResources(t *testing.T) {
	if _, err := failure.Prepare(Definitions()...); err != nil {
		t.Fatal(err)
	}
	catalog, err := i18n.Prepare(Component())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.Inspect(); err != nil {
		t.Fatal(err)
	}
}
