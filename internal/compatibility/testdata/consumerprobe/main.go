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

// This independent application fixture imports no internal inspector or bridge.
// Its parent test reads embedded metadata from the same executable that ran.
package main

import (
	"encoding/json"
	"os"
	"runtime"

	"go.yaml.in/yaml/v3"
)

var nativeAttempts int

func main() {
	var settings map[string]string
	if err := yaml.Unmarshal([]byte("dependency: yaml"), &settings); err != nil || settings["dependency"] != "yaml" {
		panic("native fixture dependency failed")
	}
	if err := json.NewEncoder(os.Stdout).Encode(struct {
		Go       string
		Attempts int
	}{runtime.Version(), nativeAttempts}); err != nil {
		panic("fixture output failed")
	}
}
