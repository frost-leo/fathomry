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

// Package pkcs7 is the unchanged private CMS signing helper from go-mail v0.8.1,
// commit 2ac36f08567b2a215ebaffdfebcd8f82f165f256. Upstream MIT notices remain in
// pkcs7.go and LICENSE. It is retained in-module so the Mail integration can sign
// the exact bounded, corrected MIME entity without invoking the native serializer
// that misencodes 7bit and malformed explicit boundaries. The parent owns all
// input/key bounds and lifetime; this helper supplies no verification or runtime.
package pkcs7
