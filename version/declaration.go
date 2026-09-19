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

import "strings"

// These private string variables are the supported linker targets. Keep their
// names and constant/zero initializers stable; no runtime setters are provided.
var release, gitRevision, tree, buildTime string

// Declaration is a caller-owned, all-or-none main-application claim. Release must
// be ordered version syntax; GitRevision a full lowercase 40/64-hex ID; Tree clean
// or dirty; BuildTime a present UTC whole-second timestamp. Unknown/development
// labels and partial claims are not build declarations. No official-release,
// clean-dependency, signature, attestation or reproducibility guarantee is implied.
type Declaration struct {
	localValue
	Release     string
	GitRevision string
	Tree        TreeState
	BuildTime   string
}

// DeclarationOrigin distinguishes caller claims from values read from the four
// supported linker variables. Neither origin authenticates the declared facts.
type DeclarationOrigin string

const (
	NoDeclaration DeclarationOrigin = ""
	Caller        DeclarationOrigin = "caller"
	Linker        DeclarationOrigin = "linker"
)

// Claim is a validated complete declaration. Zero means absent. Release belongs
// to the application, independently of Main.Version and Framework.Version.
// BuildTime is a producer-selected build timestamp, not the source commit time.
type Claim struct {
	localValue
	Origin      DeclarationOrigin
	Release     Version
	GitRevision string
	Tree        TreeState
	BuildTime   Timestamp
}

// WithDeclaration returns a new Build with an explicit caller declaration.
// It never overwrites conflicting native main-source facts or an existing claim.
// Identical existing claims preserve their origin. On error the input is intact
// and the returned Build is zero. Caller claims are not artifact read-back.
func WithDeclaration(build Build, declaration Declaration) (Build, error) {
	return withDeclaration(build, declaration, Caller)
}

// CheckDeclaration validates and exactly compares the complete expected claim.
// Absent actual claims return MissingDeclaration; differing values return Conflict.
// A builder must check the running produced artifact, not attach the expected
// claim to file metadata and mistake that assertion for successful injection.
func (build Build) CheckDeclaration(expected Declaration) error {
	claim, err := parseDeclaration(expected, Caller)
	if err != nil {
		return err
	}
	actual := build.Snapshot().Declaration
	if actual.Origin == NoDeclaration {
		return problem(MissingDeclaration)
	}
	claim.Origin = actual.Origin
	if claim != actual {
		return problem(Conflict)
	}
	return nil
}

func withDeclaration(build Build, declaration Declaration, origin DeclarationOrigin) (Build, error) {
	claim, err := parseDeclaration(declaration, origin)
	if err != nil {
		return Build{}, err
	}
	result := build.Snapshot()
	if result.Declaration.Origin != NoDeclaration {
		if err := build.CheckDeclaration(declaration); err != nil {
			return Build{}, err
		}
		return build, nil
	}
	source := result.Source
	if source.VCS.Value != "" && source.VCS.Value != "git" ||
		source.Revision.Evidence == Reported && source.Revision.Value != claim.GitRevision ||
		source.Tree != UnknownTree && source.Tree != claim.Tree {
		return Build{}, problem(Conflict)
	}
	result.Declaration = claim
	return Build{data: &result}, nil
}

func parseDeclaration(input Declaration, origin DeclarationOrigin) (Claim, error) {
	release, err := Parse(input.Release)
	if err != nil || release.State() != Release || !gitObjectID(input.GitRevision) ||
		input.Tree != Clean && input.Tree != Dirty {
		return Claim{}, problem(InvalidDeclaration)
	}
	timestamp, err := ParseTimestamp(input.BuildTime)
	if err != nil || timestamp.String() == "" {
		return Claim{}, problem(InvalidDeclaration)
	}
	tree := Clean
	if input.Tree == Dirty {
		tree = Dirty
	}
	return Claim{
		Origin: origin, Release: release, GitRevision: strings.Clone(input.GitRevision),
		Tree: tree, BuildTime: timestamp,
	}, nil
}

func gitObjectID(value string) bool {
	if len(value) != 40 && len(value) != 64 {
		return false
	}
	for _, char := range value {
		if !(char >= '0' && char <= '9' || char >= 'a' && char <= 'f') {
			return false
		}
	}
	return true
}
