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

package surf

import utls "github.com/refraction-networking/utls"

// Dynamic callbacks remain cooperative: this checks the initial encoded input,
// not allocations or later native SNI/session/padding expansion by foreign code.
func fathomryValidateHello(spec utls.ClientHelloSpec, maximum int64) error {
	if maximum == 0 {
		return nil
	}
	size := int64(512 + 2*len(spec.CipherSuites) + len(spec.CompressionMethods))
	if len(spec.Extensions) > 256 || len(spec.CipherSuites) > 512 || len(spec.CompressionMethods) > 256 {
		return ErrFathomryProfileLimit
	}
	for _, extension := range spec.Extensions {
		if fathomryNil(extension) {
			return ErrFathomryProfileLimit
		}
		length := extension.Len()
		if length < 0 || int64(length) > maximum-size {
			return ErrFathomryProfileLimit
		}
		size += int64(length)
	}
	if size > maximum {
		return ErrFathomryProfileLimit
	}
	return nil
}
