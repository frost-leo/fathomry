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

package failure

import (
	"encoding/json"
	"strconv"
	"strings"
)

// Code is a 32-bit public identity with HRESULT's customer-failure layout:
// S=1, R=0, C=1, N=0, X=0, an 11-bit Facility and a 16-bit local Number.
// Both numeric fields must be nonzero. S means failure, not fatality, retryability
// or effect certainty. This is not a Windows system code or native SDK status.
// Published facility/number pairs must never be reused for a different meaning.
//
// The accepted layout is fixed; do not add code-layer, locale or SDK-version bits:
//
//	31 30 29 28 27 | 26........16 | 15................0
//	 S  R  C  N  X | Facility(11) | local Number(16)
//	 1  0  1  0  0 | 0x001..7FF  | 0x0001..FFFF
//
// C is set for both Fathomry and extensions: neither owns Microsoft system codes.
// Domain allocation comes from Domains, subsystem ownership from Allocations,
// and local numbers from the owning package's explicit constants. Native statuses
// remain original error causes; never truncate or cast them into this namespace.
type Code uint32

// ErrorPrefix is the fixed header for compile-time error constants. It is not a
// valid Code alone. Use ErrorPrefix | Code(facility)<<16 | number for a constant,
// or MakeCode for checked runtime composition; declaration admission checks both.
const ErrorPrefix Code = 0xA0000000

// MakeCode validates both fields before composing them. It never masks overflow
// into a different code. Encoding does not allocate a facility or register an error.
func MakeCode(facility Facility, number uint16) (Code, error) {
	if !facility.Valid() || number == 0 {
		return 0, reject(ErrCode)
	}
	return ErrorPrefix | Code(facility)<<16 | Code(number), nil
}

// Valid checks the bit layout, not whether this particular code is declared.
// Unknown well-formed codes remain queryable; foreign/zero/reserved layouts do not.
func (code Code) Valid() bool {
	return code&0xF8000000 == ErrorPrefix && code&0x07FF0000 != 0 && code&0xFFFF != 0
}

// Facility returns the stable logical owner number, or zero for an invalid code.
func (code Code) Facility() Facility {
	if !code.Valid() {
		return 0
	}
	return Facility(code >> 16 & 0x7FF)
}

// Number returns the owner's local error number, or zero for an invalid code.
func (code Code) Number() uint16 {
	if !code.Valid() {
		return 0
	}
	return uint16(code & 0xFFFF)
}

// String returns fixed-width hexadecimal, including the zero-value representation.
func (code Code) String() string {
	digits := strings.ToUpper(strconv.FormatUint(uint64(code), 16))
	return "0x" + strings.Repeat("0", 8-len(digits)) + digits
}

// Error makes a valid Code an errors.Is target; it is not an occurrence.
func (code Code) Error() string {
	if !code.Valid() {
		return "failure: invalid code"
	}
	return code.String()
}

// ParseCode accepts unsigned decimal or 0x/0X hexadecimal text without signs,
// whitespace, separators or implicit octal. The 32-bit layout is checked before
// returning; legacy uint64 spellings are not silently truncated or reinterpreted.
func ParseCode(text string) (Code, error) {
	base, digits, limit := 10, text, 10
	if strings.HasPrefix(text, "0x") || strings.HasPrefix(text, "0X") {
		base, digits, limit = 16, text[2:], 8
	}
	if len(digits) == 0 || len(digits) > limit {
		return 0, reject(ErrCode)
	}
	for _, digit := range digits {
		if !(digit >= '0' && digit <= '9' || base == 16 && (digit >= 'a' && digit <= 'f' || digit >= 'A' && digit <= 'F')) {
			return 0, reject(ErrCode)
		}
	}
	number, err := strconv.ParseUint(digits, base, 32)
	if err != nil || !Code(number).Valid() {
		return 0, reject(ErrCode)
	}
	return Code(number), nil
}

// MarshalText emits canonical eight-digit hexadecimal with a 0x prefix.
func (code Code) MarshalText() ([]byte, error) {
	if !code.Valid() {
		return nil, reject(ErrCode)
	}
	return []byte(code.String()), nil
}

// UnmarshalText accepts ParseCode's grammar and leaves the receiver on rejection.
func (code *Code) UnmarshalText(text []byte) error {
	if code == nil || len(text) > 10 {
		return reject(ErrCode)
	}
	value, err := ParseCode(string(text))
	if err != nil {
		return err
	}
	*code = value
	return nil
}

// MarshalJSON uses the canonical hexadecimal string, not a JSON number. Every
// uint32 fits JSON's common exact integer range; the string keeps one code spelling.
func (code Code) MarshalJSON() ([]byte, error) {
	if !code.Valid() {
		return nil, reject(ErrCode)
	}
	return json.Marshal(code.String())
}

// UnmarshalJSON accepts a bounded JSON string, never a number or null. The decoded
// string follows ParseCode. A failed decode never changes the receiver.
func (code *Code) UnmarshalJSON(data []byte) error {
	if code == nil || len(data) > 64 {
		return reject(ErrCode)
	}
	var text string
	if err := json.Unmarshal(data, &text); err != nil {
		return reject(ErrCode)
	}
	return code.UnmarshalText([]byte(text))
}

// Identifier is a readable, owner-qualified symbol, not an error or matching key.
type Identifier string

// Valid checks the bounded, lowercase qualified-symbol grammar, without lookup.
func (identifier Identifier) Valid() bool {
	return namespace(string(identifier), MaxIdentifierBytes) && strings.ContainsRune(string(identifier), '.')
}
