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
	"runtime/debug"
	"slices"
	"strings"

	"github.com/frost-leo/fathomry/internal/compatibility"
)

// FrameworkModule identifies the linked framework, not the main application's label.
const FrameworkModule = compatibility.FrameworkModule

// Normalization limits bound borrowed metadata and explicitly disclosed output.
const (
	MaxModules        = 4096
	MaxSettings       = 256
	MaxMetadataBytes  = 1 << 20
	MaxRequestedPaths = 64
	MaxPathBytes      = 256
)

// Evidence identifies how a field was obtained, never its authenticity.
type Evidence string

const (
	UnknownEvidence     Evidence = ""
	Reported            Evidence = "reported"
	Requested           Evidence = "requested"
	Redacted            Evidence = "redacted"
	DevelopmentEvidence Evidence = "development"
)

// Fact is a bounded scalar. Unknown/redacted/development have empty Value.
// Requested is used only for the path of an unavailable requested module.
type Fact struct {
	localValue
	Value    string
	Evidence Evidence
}

// Origin distinguishes a runtime read from caller-supplied data.
type Origin string

const (
	UnknownOrigin Origin = ""
	Runtime       Origin = "runtime"
	Supplied      Origin = "supplied"
)

// TreeState is explicit: its zero value does not mean a clean checkout.
type TreeState string

const (
	UnknownTree TreeState = ""
	Clean       TreeState = "clean"
	Dirty       TreeState = "dirty"
)

// Source describes ONLY the main application's source. Git revisions are full
// lowercase SHA-1/SHA-256 object IDs. Other VCS revisions are redacted.
// CommitTime is not a compilation timestamp. No branch/tag is inferred.
type Source struct {
	localValue
	VCS        Fact
	Revision   Fact
	Tree       TreeState
	CommitTime Timestamp
}

// ReplacementKind distinguishes selected module requirements from linked content.
type ReplacementKind string

const (
	NoReplacement     ReplacementKind = ""
	ModuleReplacement ReplacementKind = "module"
	LocalReplacement  ReplacementKind = "local"
)

// Replacement never exposes a local filesystem path or claims its source revision.
type Replacement struct {
	localValue
	Kind    ReplacementKind
	Path    Fact
	Version Fact
	Sum     Fact
}

// Module describes a contributing module. Present=false means metadata unavailable,
// not proof the source graph lacks the module. Selected Version and Replacement
// remain separate; no main-source facts are copied into dependencies.
type Module struct {
	localValue
	Path        Fact
	Present     bool
	Main        bool
	Version     Fact
	Sum         Fact
	Replacement Replacement
}

// Setting is an allowlisted, bounded non-path target/build setting.
type Setting struct {
	localValue
	Name  string
	Value string
}

// Request selects at most 64 dependencies and 64 additional disclosable module
// paths (each <=256 bytes). FrameworkModule is always selected/disclosable.
// The main path and versioned replacement paths need explicit disclosure.
// Local replacement paths, raw flags and unselected dependencies never escape.
type Request struct {
	localValue
	Dependencies  []string
	DisclosePaths []string
}

// Snapshot is an owned in-process record, not a durable or JSON schema.
// Copying this struct shares slices; use Build.Snapshot again for independent data.
// Missing facts remain zero. Origin describes metadata, Declaration has its own
// origin. Neither the Go version nor target is replaced with inspector host facts.
type Snapshot struct {
	localValue
	Origin       Origin
	Metadata     bool
	Go           Fact
	Main         Module
	Framework    Module
	Dependencies []Module
	Settings     []Setting
	Source       Source
	Declaration  Claim
}

// Build is an immutable handle supporting concurrent readers. Copying it shares
// only private immutable storage. Zero Build has unknown facts and no declaration.
type Build struct {
	localValue
	data *Snapshot
}

// Snapshot returns a detached copy. No slice in the result aliases Build.
func (build Build) Snapshot() Snapshot {
	if build.data == nil {
		return Snapshot{}
	}
	result := *build.data
	result.Dependencies = slices.Clone(result.Dependencies)
	result.Settings = slices.Clone(result.Settings)
	return result
}

// Inspect reads the consuming process and the optional linker declaration.
// An unstamped build is successful with no declaration; a partial, invalid or
// conflicting stamp returns a zero Build and stable failure. No defaults are
// supplied from runtime.Version, the host platform, environment, Git or a clock.
func Inspect(request Request) (Build, error) {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		info = nil
	}
	build, err := fromBuildInfo(info, request, Runtime)
	if err != nil {
		return Build{}, err
	}
	declaration := Declaration{Release: release, GitRevision: gitRevision, Tree: TreeState(tree), BuildTime: buildTime}
	if declaration == (Declaration{}) {
		return build, nil
	}
	return withDeclaration(build, declaration, Linker)
}

// FromBuildInfo accepts runtime/debug or debug/buildinfo data. nil means missing.
// Data/request slices and module pointers are borrowed only during the call and
// must not be concurrently mutated. Output retains no pointers to those inputs.
// This reads neither the inspector's facts nor its linker declaration; artifact
// readers should call debug/buildinfo.ReadFile then this function. Linker
// variables cannot be recovered or verified through Go BuildInfo.
func FromBuildInfo(info *debug.BuildInfo, request Request) (Build, error) {
	return fromBuildInfo(info, request, Supplied)
}

func fromBuildInfo(info *debug.BuildInfo, request Request, origin Origin) (Build, error) {
	if err := validateMetadata(info, request); err != nil {
		return Build{}, err
	}
	native, err := compatibility.FromBuildInfo(info, compatibility.BuildRequest{
		SDKModules: request.Dependencies, DisclosePaths: request.DisclosePaths,
	})
	if err != nil {
		return Build{}, problem(InvalidRequest)
	}
	if info != nil && info.GoVersion != "" && native.Go.Kind != compatibility.Observed && native.Go.Kind != compatibility.Development {
		return Build{}, problem(InvalidMetadata)
	}
	result := Snapshot{
		Origin: origin, Metadata: info != nil, Go: projectFact(native.Go),
		Main: projectModule(native.Main), Framework: projectModule(native.Framework),
	}
	for _, module := range native.SDKs {
		result.Dependencies = append(result.Dependencies, projectModule(module))
	}
	for _, setting := range native.Platform {
		result.Settings = append(result.Settings, Setting{Name: setting.Name, Value: strings.Clone(setting.Value)})
	}
	if info != nil {
		modules := map[string]*debug.Module{info.Main.Path: &info.Main}
		for _, module := range info.Deps {
			if module != nil {
				modules[module.Path] = module
			}
		}
		correctMissingVersion := func(module *Module) {
			raw := modules[module.Path.Value]
			if module.Main {
				raw = &info.Main
			}
			if module.Present && raw != nil && raw.Version == "" {
				module.Version = Fact{}
			}
		}
		correctMissingVersion(&result.Main)
		correctMissingVersion(&result.Framework)
		for index := range result.Dependencies {
			correctMissingVersion(&result.Dependencies[index])
		}
		result.Source, err = sourceFacts(info.Settings)
		if err != nil {
			return Build{}, err
		}
	}
	return Build{data: &result}, nil
}

func projectFact(input compatibility.Fact) Fact {
	evidence := UnknownEvidence
	switch input.Kind {
	case compatibility.Observed:
		evidence = Reported
	case compatibility.Redacted:
		evidence = Redacted
	case compatibility.Development:
		evidence = DevelopmentEvidence
	}
	return Fact{Value: strings.Clone(input.Value), Evidence: evidence}
}

func projectModule(input compatibility.Module) Module {
	result := Module{
		Path: projectFact(input.Path), Present: input.Present, Main: input.Main,
		Version: projectFact(input.Version), Sum: projectFact(input.Sum),
		Replacement: Replacement{
			Kind: ReplacementKind(input.Replacement.Kind), Path: projectFact(input.Replacement.Path),
			Version: projectFact(input.Replacement.Version), Sum: projectFact(input.Replacement.Sum),
		},
	}
	if !result.Present && result.Path.Value != "" {
		result.Path.Evidence = Requested
	}
	return result
}
