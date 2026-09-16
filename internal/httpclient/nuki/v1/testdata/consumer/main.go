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

package main

import (
	"encoding/json"
	"os"

	"github.com/frost-leo/fathomry/internal/compatibility"
	nuki "github.com/frost-leo/fathomry/internal/httpclient/nuki/v1"
	sdk "github.com/nukilabs/tlsclient"
)

func main() {
	build, err := nuki.Build()
	if err != nil {
		panic(err)
	}
	report := struct {
		Go, Provider, Patch string
		Modules             map[string]string
		Replacements        map[string]bool
	}{Go: build.Go.Value, Provider: nuki.ProviderID, Patch: sdk.FathomryCompatibilityRevision, Modules: map[string]string{}, Replacements: map[string]bool{}}
	for _, module := range build.SDKs {
		report.Modules[module.Path.Value] = module.Version.Value
		report.Replacements[module.Path.Value] = module.Replacement.Kind == compatibility.LocalReplacement
	}
	if err := json.NewEncoder(os.Stdout).Encode(report); err != nil {
		panic(err)
	}
}
