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

package redis

import (
	"embed"
	"io/fs"
)

//go:embed resources/*/*.json
var resources embed.FS

// CacheResources and MessagingResources are offline locale resources. Lookup
// never constructs clients or mutates the process-global native logger.
func CacheResources() fs.FS     { sub, _ := fs.Sub(resources, "resources/cache"); return sub }
func MessagingResources() fs.FS { sub, _ := fs.Sub(resources, "resources/messaging"); return sub }
