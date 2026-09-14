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
	minio "github.com/frost-leo/fathomry/internal/objectstore/minio/v7"
	"os"
)

func main() {
	_, _ = minio.Select(minio.OptionsV1{})
	build, err := compatibility.Inspect(compatibility.BuildRequest{SDKModules: []string{"github.com/minio/minio-go/v7"}})
	if err != nil {
		os.Exit(1)
	}
	versions := map[string]string{}
	for _, module := range build.SDKs {
		if module.Present {
			versions[module.Path.Value] = module.Version.Value
		}
	}
	if json.NewEncoder(os.Stdout).Encode(versions) != nil {
		os.Exit(1)
	}
}
