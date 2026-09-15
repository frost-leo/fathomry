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

	sdk "github.com/bogdanfinn/tls-client"
	"github.com/frost-leo/fathomry/internal/compatibility"
	tlsclient "github.com/frost-leo/fathomry/internal/httpclient/tlsclient/v1"
)

func main() {
	build, err := tlsclient.Build()
	if err != nil {
		os.Exit(1)
	}
	value := struct {
		Go, Provider, Framework, Patch string
		SDKs                           map[string]string
		Replaced                       bool
	}{
		Go: build.Go.Value, Provider: tlsclient.ProviderID, Framework: build.Framework.Path.Value, Patch: sdk.FathomryCompatibilityRevision, SDKs: make(map[string]string),
	}
	for _, module := range build.SDKs {
		value.SDKs[module.Path.Value] = module.Version.Value
		if module.Path.Value == "github.com/bogdanfinn/tls-client" {
			value.Replaced = module.Replacement.Kind == compatibility.LocalReplacement
		}
	}
	if err := json.NewEncoder(os.Stdout).Encode(value); err != nil {
		os.Exit(1)
	}
}
