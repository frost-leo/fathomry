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
	"github.com/frost-leo/fathomry/internal/compatibility"
	provider "github.com/frost-leo/fathomry/internal/httpclient/httpcloak/v1"
	"github.com/sardanioss/httpcloak/transport"
	"os"
)

func main() {
	build, err := provider.Build()
	if err != nil {
		os.Exit(1)
	}
	value := struct {
		Go, Provider, Framework, Patch string
		SDKs                           map[string]string
		Replacements                   map[string]bool
	}{
		Go: build.Go.Value, Provider: provider.ProviderID, Framework: build.Framework.Path.Value, Patch: transport.FathomryCompatibilityRevision, SDKs: make(map[string]string), Replacements: make(map[string]bool),
	}
	for _, module := range build.SDKs {
		value.SDKs[module.Path.Value] = module.Version.Value
		value.Replacements[module.Path.Value] = module.Replacement.Kind == compatibility.LocalReplacement
	}
	if err := json.NewEncoder(os.Stdout).Encode(value); err != nil {
		os.Exit(1)
	}
}
