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

// Package compatibility reports available consuming-binary facts and assesses
// explicitly supplied, exact-combination test evidence. It owns no SDK selection,
// service probe, resource, global registry, deployment identity or retry policy.
// Build metadata is neither an integrity attestation nor a compatibility test.
package compatibility

import (
	"encoding/base64"
	"errors"
	"go/version"
	"runtime"
	"runtime/debug"
	"slices"
	"strings"

	"github.com/frost-leo/fathomry/internal/fault"
)

const FrameworkModule = "github.com/frost-leo/fathomry"

const ErrInvalid fault.Kind = "fathomry.compatibility.invalid"

// localValue prevents these process-local projections from accidentally becoming
// a persistent or Temporal protocol. A future format needs its own reader policy.
type localValue struct{}

func (localValue) MarshalJSON() ([]byte, error) {
	return nil, errors.New("compatibility: no serialization contract")
}
func (*localValue) UnmarshalJSON([]byte) error {
	return errors.New("compatibility: no reconstruction contract")
}

// Knowledge describes the evidence for a value, not compatibility. The zero value
// means unknown; an empty observed value is never produced by build inspection.
type Knowledge string

const (
	UnknownFact   Knowledge = ""
	Observed      Knowledge = "observed"
	Declared      Knowledge = "declared"
	Redacted      Knowledge = "redacted"
	Development   Knowledge = "development"
	NotApplicable Knowledge = "not-applicable"
)

// Fact is bounded, non-sensitive provenance. Declared and NotApplicable are
// owner assertions for mode/service profiles, never inferred from connectivity.
type Fact struct {
	localValue
	Value string
	Kind  Knowledge
}

// VCS describes only the main module's source tree. Modified is "true", "false"
// or unknown, not a claim about dependency checkouts, native libraries or deployment.
type VCS struct {
	localValue
	System   Fact
	Revision Fact
	Modified Fact
}

type ReplacementKind string

const (
	NoReplacement      ReplacementKind = ""
	ModuleReplacement  ReplacementKind = "module"
	LocalReplacement   ReplacementKind = "local"
	UnknownReplacement ReplacementKind = "unknown"
)

// Replacement separates the selected module from the actual replacement. Local
// paths are never retained, even if explicitly requested in the disclosure list.
type Replacement struct {
	localValue
	Kind    ReplacementKind
	Path    Fact
	Version Fact
	Sum     Fact
}

// Module describes one contributing module, not every entry in go.mod/go.sum.
// Present=false means no matching entry was available, not proof of absence from
// the source graph. Main VCS facts never propagate to dependency modules.
type Module struct {
	localValue
	Path        Fact
	Present     bool
	Main        bool
	Version     Fact
	Sum         Fact
	Replacement Replacement
	VCS         VCS
}

// Build contains safe available facts. SDKs includes explicitly requested module
// paths, not an inventory of private dependencies.
// All slices are caller-owned snapshots: copying the outer value alone shares
// slices; Clone provides an independent copy. Do not mutate concurrently with use.
// Platform includes only selected non-path build settings; omitted flags, native
// libraries and deployment identities require separate integration evidence.
type Build struct {
	localValue
	Metadata  Knowledge
	Go        Fact
	Main      Module
	Framework Module
	SDKs      []Module
	Platform  []Option
	// OpaqueSettings contains only fixed setting names (or "other"), never values.
	// Nonempty means available flags were redacted or Go deliberately omitted them;
	// they cannot establish exact tested-combination equivalence.
	OpaqueSettings []string
}

func (build Build) Clone() Build {
	build.SDKs = slices.Clone(build.SDKs)
	build.Platform = slices.Clone(build.Platform)
	build.OpaqueSettings = slices.Clone(build.OpaqueSettings)
	slices.SortFunc(build.SDKs, func(left, right Module) int { return strings.Compare(left.Path.Value, right.Path.Value) })
	sortOptions(build.Platform)
	return build
}

// BuildRequest selects relevant SDK modules and additional module paths safe to
// disclose (for example, a versioned fork or the main application). Disclosure
// does not imply that a path is independently required or contributes packages.
// Local filesystem paths are never eligible. Each list is limited to 64 paths.
type BuildRequest struct {
	localValue
	SDKModules    []string
	DisclosePaths []string
}

// Inspect reads the running binary, never the framework's go.mod or local VCS.
// Go remains available from runtime.Version even without module build metadata.
// At most 64 unique module paths may be explicitly selected for disclosure.
func Inspect(request BuildRequest) (Build, error) {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		info = nil
	}
	build, err := FromBuildInfo(info, request)
	if err != nil {
		return Build{}, err
	}
	build.Go = goFact(runtime.Version())
	return build, err
}

// FromBuildInfo normalizes metadata read by runtime/debug or debug/buildinfo.
// It does not verify its authenticity. nil means unavailable; no running-process
// facts are substituted when inspecting another binary. Input is borrowed only
// during this call and must not be concurrently mutated. Raw paths/flags are not
// retained. Only explicitly listed module paths and FrameworkModule are disclosed.
func FromBuildInfo(info *debug.BuildInfo, request BuildRequest) (Build, error) {
	if len(request.SDKModules) > 64 || len(request.DisclosePaths) > 64 {
		return Build{}, ErrInvalid.New(fault.Context{})
	}
	paths := slices.Clone(request.SDKModules)
	slices.Sort(paths)
	paths = slices.Compact(paths)
	allowed := map[string]bool{FrameworkModule: true}
	for _, path := range append(slices.Clone(paths), request.DisclosePaths...) {
		if !modulePath(path) {
			return Build{}, ErrInvalid.New(fault.Context{})
		}
		allowed[path] = true
	}
	build := Build{}
	find := func(path string) Module {
		missing := Module{Path: Fact{Value: path, Kind: Observed}}
		if info == nil {
			return missing
		}
		var found *debug.Module
		main := false
		if info.Main.Path == path {
			found, main = &info.Main, true
		}
		for _, dep := range info.Deps {
			if dep != nil && dep.Path == path {
				if found != nil {
					return missing // Ambiguous input is not a selected-version fact.
				}
				found = dep
			}
		}
		if found == nil {
			return missing
		}
		return normalizeModule(*found, main, allowed)
	}
	if info != nil {
		opaque := make(map[string]bool)
		build.Metadata, build.Go = Observed, goFact(info.GoVersion)
		if info.Main.Path != "" {
			build.Main = normalizeModule(info.Main, true, allowed)
			build.Main.VCS = vcsFacts(info.Settings)
		}
		platformKeys := []string{"GOOS", "GOARCH", "GOAMD64", "GOARM", "GOARM64", "GO386", "GOMIPS", "GOMIPS64", "GOPPC64", "GORISCV64", "CGO_ENABLED", "GOFIPS140", "-compiler", "-race", "-buildmode", "-asan", "-msan", "-cover", "-trimpath"}
		for _, key := range platformKeys {
			if fact := setting(info.Settings, key); fact.Kind == Observed {
				build.Platform = append(build.Platform, Option{Name: key, Value: fact.Value})
			}
		}
		sortOptions(build.Platform)
		for _, entry := range info.Settings {
			if slices.Contains(platformKeys, entry.Key) {
				if setting(info.Settings, entry.Key).Kind != Observed {
					opaque["invalid-platform-setting"] = true
				}
				if entry.Key == "-trimpath" && entry.Value == "true" {
					opaque["trimpath-omissions"] = true
				}
				continue
			}
			if entry.Key == "vcs" || entry.Key == "vcs.revision" || entry.Key == "vcs.modified" || entry.Key == "vcs.time" || entry.Value == "" {
				continue
			}
			name := "other"
			if opaqueSetting(entry.Key) {
				name = entry.Key
			}
			opaque[name] = true
		}
		for name := range opaque {
			build.OpaqueSettings = append(build.OpaqueSettings, name)
		}
		slices.Sort(build.OpaqueSettings)
	}
	build.Framework = find(FrameworkModule)
	if build.Framework.Main {
		build.Framework.VCS = build.Main.VCS
	}
	for _, path := range paths {
		if path != FrameworkModule {
			module := find(path)
			if module.Main {
				module.VCS = build.Main.VCS
			}
			build.SDKs = append(build.SDKs, module)
		}
	}
	return build, nil
}

func normalizeModule(input debug.Module, main bool, allowed map[string]bool) Module {
	module := Module{Present: true, Main: main, Path: disclosedPath(input.Path, allowed),
		Version: moduleVersion(input.Version), Sum: checksum(input.Sum)}
	if replacement := input.Replace; replacement != nil {
		module.Replacement.Kind = ModuleReplacement
		if replacement.Replace != nil {
			module.Replacement.Kind = UnknownReplacement
		} else if replacement.Version == "" || replacement.Version == "(devel)" {
			module.Replacement.Kind = LocalReplacement
			module.Replacement.Path = Fact{Kind: Redacted}
			module.Replacement.Version = Fact{Kind: Development}
		} else {
			module.Replacement.Path = disclosedPath(replacement.Path, allowed)
			module.Replacement.Version = moduleVersion(replacement.Version)
			module.Replacement.Sum = checksum(replacement.Sum)
		}
	}
	return module
}

func disclosedPath(path string, allowed map[string]bool) Fact {
	if path == "" {
		return Fact{}
	}
	if !allowed[path] || !modulePath(path) {
		return Fact{Kind: Redacted}
	}
	return Fact{Value: path, Kind: Observed}
}

func modulePath(value string) bool {
	if len(value) == 0 || len(value) > 256 || strings.HasPrefix(value, "/") || strings.HasPrefix(value, "~") || strings.Contains(value, "..") {
		return false
	}
	for _, part := range strings.Split(value, "/") {
		if part == "" || strings.HasPrefix(part, ".") {
			return false
		}
	}
	for _, char := range value {
		if !(char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' || strings.ContainsRune("-._~/", char)) {
			return false
		}
	}
	return true
}

func token(value string) bool {
	if len(value) == 0 || len(value) > 128 {
		return false
	}
	for _, char := range value {
		if !(char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' || strings.ContainsRune("-._+", char)) {
			return false
		}
	}
	return true
}

func moduleVersion(value string) Fact {
	if value == "" || value == "(devel)" {
		return Fact{Kind: Development}
	}
	if !token(value) || !strings.HasPrefix(value, "v") {
		return Fact{Kind: Redacted}
	}
	return Fact{Value: value, Kind: Observed}
}

func goFact(value string) Fact {
	if strings.HasPrefix(value, "devel") {
		return Fact{Kind: Development}
	}
	if !token(value) || !version.IsValid(value) {
		return Fact{}
	}
	return Fact{Value: value, Kind: Observed}
}

func checksum(value string) Fact {
	if value == "" {
		return Fact{}
	}
	if len(value) == 47 && strings.HasPrefix(value, "h1:") {
		decoded, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(value, "h1:"))
		if err == nil && len(decoded) == 32 && base64.StdEncoding.EncodeToString(decoded) == value[3:] {
			return Fact{Value: value, Kind: Observed}
		}
	}
	return Fact{Kind: Redacted}
}

func setting(settings []debug.BuildSetting, key string) Fact {
	var found Fact
	seen := false
	for _, entry := range settings {
		if entry.Key != key {
			continue
		}
		if seen || !token(entry.Value) {
			return Fact{}
		}
		seen, found = true, Fact{Value: entry.Value, Kind: Observed}
	}
	return found
}

func vcsFacts(settings []debug.BuildSetting) VCS {
	vcs := VCS{System: setting(settings, "vcs"), Revision: setting(settings, "vcs.revision"), Modified: setting(settings, "vcs.modified")}
	if vcs.System.Value != "git" && vcs.System.Value != "hg" && vcs.System.Value != "svn" && vcs.System.Value != "bzr" && vcs.System.Value != "fossil" {
		vcs.System = Fact{}
	}
	if vcs.Modified.Value != "true" && vcs.Modified.Value != "false" {
		vcs.Modified = Fact{}
	}
	for _, char := range vcs.Revision.Value {
		if !(char >= '0' && char <= '9' || char >= 'a' && char <= 'f') {
			vcs.Revision = Fact{Kind: Redacted}
			break
		}
	}
	return vcs
}

func validBuild(build Build) bool {
	if build.Metadata != UnknownFact && build.Metadata != Observed || !validFact(build.Go) || len(build.SDKs) > 64 || !validOptions(build.Platform) || len(build.OpaqueSettings) > 16 {
		return false
	}
	if build.Go.Kind == Observed && goFact(build.Go.Value) != build.Go || build.Go.Kind == Declared || build.Go.Kind == NotApplicable {
		return false
	}
	for _, name := range build.OpaqueSettings {
		if !opaqueSetting(name) && name != "other" && name != "trimpath-omissions" && name != "invalid-platform-setting" {
			return false
		}
	}
	modules := append([]Module{build.Main, build.Framework}, build.SDKs...)
	for _, module := range modules {
		validPath := func(fact Fact) bool {
			return fact.Kind == Observed && modulePath(fact.Value) || fact.Value == "" && (fact.Kind == UnknownFact || fact.Kind == Redacted)
		}
		validVersion := func(fact Fact) bool {
			return fact == (Fact{}) || fact.Kind == Development && fact.Value == "" || fact.Kind == Redacted && fact.Value == "" || fact.Kind == Observed && moduleVersion(fact.Value) == fact
		}
		validSum := func(fact Fact) bool {
			return fact == (Fact{}) || fact.Kind == Redacted && fact.Value == "" || fact.Kind == Observed && checksum(fact.Value) == fact
		}
		if !validPath(module.Path) || !validVersion(module.Version) || !validSum(module.Sum) {
			return false
		}
		vcs := module.VCS
		if !module.Main && vcs != (VCS{}) || !validFact(vcs.System) || !validFact(vcs.Revision) || !validFact(vcs.Modified) {
			return false
		}
		if vcs.Modified.Kind == Observed && vcs.Modified.Value != "true" && vcs.Modified.Value != "false" {
			return false
		}
		for _, char := range vcs.Revision.Value {
			if !(char >= '0' && char <= '9' || char >= 'a' && char <= 'f') {
				return false
			}
		}
		replacement := module.Replacement
		switch replacement.Kind {
		case NoReplacement:
			if replacement != (Replacement{}) {
				return false
			}
		case LocalReplacement:
			if replacement.Path != (Fact{Kind: Redacted}) || replacement.Version != (Fact{Kind: Development}) || replacement.Sum != (Fact{}) {
				return false
			}
		case ModuleReplacement:
			if !validPath(replacement.Path) || !validVersion(replacement.Version) || !validSum(replacement.Sum) {
				return false
			}
		case UnknownReplacement:
			if replacement.Path != (Fact{}) || replacement.Version != (Fact{}) || replacement.Sum != (Fact{}) {
				return false
			}
		default:
			return false
		}
	}
	seen := make(map[string]bool)
	for _, module := range build.SDKs {
		if module.Path.Kind != Observed || seen[module.Path.Value] {
			return false
		}
		seen[module.Path.Value] = true
	}
	return build.Framework.Path == (Fact{Kind: Observed, Value: FrameworkModule})
}

func opaqueSetting(key string) bool {
	switch key {
	case "-ldflags", "-gcflags", "-asmflags", "-gccgoflags", "-tags", "-pgo", "CGO_CFLAGS", "CGO_CPPFLAGS", "CGO_CXXFLAGS", "CGO_LDFLAGS", "GOEXPERIMENT", "DefaultGODEBUG":
		return true
	}
	return false
}
