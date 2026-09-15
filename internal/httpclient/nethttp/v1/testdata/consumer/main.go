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

	nethttp "github.com/frost-leo/fathomry/internal/httpclient/nethttp/v1"
)

func main() {
	build, err := nethttp.Build()
	if err != nil {
		os.Exit(1)
	}
	value := struct {
		Go, Provider, Framework string
		SDKCount                int
	}{build.Go.Value, nethttp.ProviderID, build.Framework.Path.Value, len(build.SDKs)}
	if err := json.NewEncoder(os.Stdout).Encode(value); err != nil {
		os.Exit(1)
	}
}
