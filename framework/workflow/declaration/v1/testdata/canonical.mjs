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

import { readFileSync } from "node:fs";

function encode(value) {
  if (Array.isArray(value)) return "[" + value.map(encode).join(",") + "]";
  if (value !== null && typeof value === "object") {
    return "{" + Object.keys(value).sort().map(key => JSON.stringify(key) + ":" + encode(value[key])).join(",") + "}";
  }
  if (typeof value === "number" && (!Number.isSafeInteger(value) || Object.is(value, -0))) {
    throw new Error("outside admitted canonical number domain");
  }
  return JSON.stringify(value);
}
process.stdout.write(encode(JSON.parse(readFileSync(0, "utf8"))));
