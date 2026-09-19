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

package version

import (
	"strings"
	"time"

	"github.com/frost-leo/fathomry/failure"
	"golang.org/x/mod/semver"
)

// Stable failure identities contain no input, filesystem paths or native causes.
const (
	InvalidVersion           failure.Code = "fathomry.version.invalid_version"
	Unordered                failure.Code = "fathomry.version.unordered"
	InvalidTimestamp         failure.Code = "fathomry.version.invalid_timestamp"
	InvalidRequest           failure.Code = "fathomry.version.invalid_request"
	InvalidMetadata          failure.Code = "fathomry.version.invalid_metadata"
	LimitExceeded            failure.Code = "fathomry.version.limit_exceeded"
	InvalidDeclaration       failure.Code = "fathomry.version.invalid_declaration"
	MissingDeclaration       failure.Code = "fathomry.version.missing_declaration"
	Conflict                 failure.Code = "fathomry.version.conflict"
	SerializationUnsupported failure.Code = "fathomry.version.serialization_unsupported"
)

// MaxVersionBytes bounds the complete spelling, including v and metadata.
const MaxVersionBytes = 128

// State classifies syntax, never release approval or source integrity.
type State string

const (
	Unknown     State = ""
	Development State = "development"
	Release     State = "release"
)

// Version is an immutable comparable value. The zero value is unknown.
// Equality includes every byte of prerelease and build metadata.
type Version struct {
	localValue
	text string
}

// Parse accepts empty (unknown), "(devel)", or a complete vMAJOR.MINOR.PATCH
// SemVer 2.0 value. The v prefix is mandatory; shorthand, surrounding whitespace
// and leading numeric zeros are rejected. Build metadata is preserved verbatim.
// A pseudo-version satisfies the syntax but does not prove an official release.
func Parse(text string) (Version, error) {
	if text == "" {
		return Version{}, nil
	}
	if text == "(devel)" {
		return Version{text: "(devel)"}, nil
	}
	if len(text) > MaxVersionBytes {
		return Version{}, problem(InvalidVersion)
	}
	core, _, _ := strings.Cut(text, "-")
	core, _, _ = strings.Cut(core, "+")
	if strings.Count(core, ".") != 2 || !semver.IsValid(text) {
		return Version{}, problem(InvalidVersion)
	}
	return Version{text: strings.Clone(text)}, nil
}

// String returns the exact machine spelling, including empty for unknown.
func (value Version) String() string { return value.text }

// State distinguishes unknown and development from ordered version syntax.
func (value Version) State() State {
	switch value.text {
	case "":
		return Unknown
	case "(devel)":
		return Development
	default:
		return Release
	}
}

// Equal compares exact identity, not precedence or artifact bytes.
func (value Version) Equal(other Version) bool { return value == other }

// Compare returns -1, 0 or +1 by SemVer precedence, ignoring build metadata.
// Unknown/development values return Unordered, including comparison to themselves.
func (value Version) Compare(other Version) (int, error) {
	if value.State() != Release || other.State() != Release {
		return 0, problem(Unordered)
	}
	return semver.Compare(value.text, other.text), nil
}

// Timestamp is an immutable optional UTC instant at whole-second resolution.
// Its zero value is absent, not the Unix epoch. It has no monotonic clock data.
type Timestamp struct {
	localValue
	text string
}

// ParseTimestamp accepts empty or exactly YYYY-MM-DDTHH:MM:SSZ, years 0001-9999.
// Offsets, fractional seconds, leap seconds and surrounding whitespace fail.
func ParseTimestamp(text string) (Timestamp, error) {
	if text == "" {
		return Timestamp{}, nil
	}
	if len(text) != 20 {
		return Timestamp{}, problem(InvalidTimestamp)
	}
	instant, err := time.Parse(time.RFC3339, text)
	if err != nil || instant.Year() < 1 || instant.UTC().Format(time.RFC3339) != text {
		return Timestamp{}, problem(InvalidTimestamp)
	}
	return Timestamp{text: strings.Clone(text)}, nil
}

// String returns the exact UTC spelling, or empty when absent.
func (value Timestamp) String() string { return value.text }

// Time returns the instant and whether it was present. Epoch zero is present.
func (value Timestamp) Time() (time.Time, bool) {
	if value.text == "" {
		return time.Time{}, false
	}
	instant, _ := time.Parse(time.RFC3339, value.text)
	return instant, true
}

type localValue struct{}

// MarshalJSON refuses to invent a persistent version/build record protocol.
func (localValue) MarshalJSON() ([]byte, error) {
	return nil, problem(SerializationUnsupported)
}

// UnmarshalJSON refuses reconstruction without changing the receiver.
func (*localValue) UnmarshalJSON([]byte) error {
	return problem(SerializationUnsupported)
}

func problem(code failure.Code) error { return failure.New(code, nil) }
