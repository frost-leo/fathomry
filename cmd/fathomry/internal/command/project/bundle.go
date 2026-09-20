/*
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

package project

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"unicode"

	json "github.com/go-json-experiment/json"
	"golang.org/x/mod/modfile"
	"golang.org/x/mod/module"
)

const (
	BundleFormat       = "fathomry.sdk-bundle/v1"
	MaxManifestBytes   = 1 << 20
	MaxBundleModules   = 32
	MaxBundleFiles     = 8192
	MaxBundleFileBytes = 16 << 20
	MaxBundleBytes     = 128 << 20
)

// BundleManifest is an explicit artifact format, not project runtime settings.
// Files name all regular files relative to each module Directory and contain
// sha256: lowercase-hex digests of the corrected bytes. Upstream provenance is
// separate from corrected content, compatibility qualification and distribution
// permission. No hash here authenticates an untrusted producer.
type BundleManifest struct {
	Format           string         `json:"format"`
	FrameworkVersion string         `json:"framework_version"`
	Modules          []BundleModule `json:"modules"`
	License          []string       `json:"license,omitempty"`
}

// BundleModule retains the original module path/version and a non-secret local
// patch revision. Directory is a portable lowercase label, unique ignoring case.
// Every module includes go.mod, UPSTREAM.json and FATHOMRY.md in its inventory;
// preserve actual license files, without inventing a missing upstream grant.
type BundleModule struct {
	Path      string            `json:"path"`
	Version   string            `json:"version"`
	Revision  string            `json:"revision"`
	Directory string            `json:"directory"`
	Files     map[string]string `json:"files"`
}

type snapshot struct {
	root     *os.Root
	raw      []byte
	digest   string
	manifest BundleManifest
}

func digest(data []byte) string {
	hash := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(hash[:])
}
func validDigest(value string) bool {
	if len(value) != 71 || !strings.HasPrefix(value, "sha256:") {
		return false
	}
	data, err := hex.DecodeString(value[7:])
	return err == nil && len(data) == 32 && strings.ToLower(value) == value
}
func sortedKeys[T any](values map[string]T) []string {
	result := make([]string, 0, len(values))
	for key := range values {
		result = append(result, key)
	}
	slices.Sort(result)
	return result
}

func openSnapshot(ctx context.Context, directory, version string, destinationParent fs.FileInfo) (*snapshot, error) {
	root, err := os.OpenRoot(directory)
	if err != nil {
		return nil, problem(InvalidBundle)
	}
	fail := func(err error) (*snapshot, error) { _ = root.Close(); return nil, err }
	raw, err := readRegular(ctx, root, "manifest.json", MaxManifestBytes)
	if err != nil {
		return fail(err)
	}
	var manifest BundleManifest
	if json.Unmarshal(raw, &manifest, json.RejectUnknownMembers(true)) != nil ||
		manifest.Format != BundleFormat || manifest.FrameworkVersion != version ||
		len(manifest.Modules) == 0 || len(manifest.Modules) > MaxBundleModules {
		return fail(problem(InvalidBundle))
	}
	expected := map[string]bool{"manifest.json": true}
	paths, dirs := map[string]bool{}, map[string]bool{}
	spellings := map[string]string{}
	directories := map[string]bool{}
	total := int64(0)
	for _, entry := range manifest.Modules {
		if moduleOverlap(entry.Path, FrameworkModule) || module.Check(entry.Path, entry.Version) != nil ||
			module.CanonicalVersion(entry.Version) != entry.Version || len(entry.Path) > 256 ||
			!name(entry.Directory) || !name(entry.Revision) || paths[entry.Path] || dirs[entry.Directory] ||
			len(entry.Files) == 0 || len(entry.Files) > MaxBundleFiles ||
			entry.Files["go.mod"] == "" || entry.Files["UPSTREAM.json"] == "" || entry.Files["FATHOMRY.md"] == "" {
			return fail(problem(InvalidBundle))
		}
		paths[entry.Path], dirs[entry.Directory] = true, true
		for file, hash := range entry.Files {
			if !fs.ValidPath(file) || file == "." || len(file) > 512 || strings.Contains(file, "\\") ||
				file != "go.mod" && strings.HasSuffix(file, "/go.mod") ||
				strings.Contains("/"+file+"/", "/.git/") || module.CheckFilePath(file) != nil || !validDigest(hash) {
				return fail(problem(InvalidBundle))
			}
			target := entry.Directory + "/" + file
			if expected[target] || len(expected) > MaxBundleFiles {
				return fail(problem(InvalidBundle))
			}
			components := strings.Split(target, "/")
			for index := range components {
				prefix := strings.Join(components[:index+1], "/")
				key := foldPath(prefix)
				if previous, found := spellings[key]; found && previous != prefix {
					return fail(problem(InvalidBundle))
				}
				spellings[key] = prefix
				if index < len(components)-1 {
					if expected[prefix] {
						return fail(problem(InvalidBundle))
					}
					directories[prefix] = true
				} else if directories[prefix] {
					return fail(problem(InvalidBundle))
				}
			}
			expected[target] = true
			data, err := readSnapshotFile(ctx, root, target, hash)
			if err != nil {
				return fail(err)
			}
			total += int64(len(data))
			if total > MaxBundleBytes {
				return fail(problem(InvalidBundle))
			}
			if file == "go.mod" {
				parsed, err := modfile.Parse("go.mod", data, nil)
				if err != nil || parsed.Module == nil || parsed.Module.Mod.Path != entry.Path ||
					len(parsed.Replace) != 0 || len(parsed.Exclude) != 0 {
					return fail(problem(InvalidBundle))
				}
			}
		}
	}
	visited := 0
	err = fs.WalkDir(root.FS(), ".", func(file string, entry fs.DirEntry, err error) error {
		if err != nil {
			return problem(InvalidBundle)
		}
		if err := contextError(ctx); err != nil {
			return err
		}
		visited++
		if visited > MaxBundleFiles*2+MaxBundleModules || len(file) > 576 {
			return problem(InvalidBundle)
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return problem(InvalidBundle)
		}
		if entry.IsDir() {
			if destinationParent != nil {
				info, err := entry.Info()
				if err != nil || os.SameFile(info, destinationParent) {
					return problem(InvalidBundle)
				}
			}
			return nil
		}
		if !expected[file] {
			return problem(InvalidBundle)
		}
		return nil
	})
	if err != nil {
		return fail(err)
	}
	return &snapshot{root: root, raw: raw, digest: digest(raw), manifest: manifest}, nil
}

func foldPath(value string) string {
	var folded strings.Builder
	for _, character := range value {
		canonical := character
		for next := unicode.SimpleFold(character); next != character; next = unicode.SimpleFold(next) {
			canonical = min(canonical, next)
		}
		folded.WriteRune(canonical)
	}
	return folded.String()
}

func readRegular(ctx context.Context, root *os.Root, file string, maximum int64) ([]byte, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	info, err := root.Lstat(file)
	if err != nil || !info.Mode().IsRegular() || info.Size() > maximum {
		return nil, problem(InvalidBundle)
	}
	opened, err := root.Open(file)
	if err != nil {
		return nil, problem(InvalidBundle)
	}
	info, statErr := opened.Stat()
	if statErr != nil || !info.Mode().IsRegular() || info.Size() > maximum {
		_ = opened.Close()
		return nil, problem(InvalidBundle)
	}
	data, readErr := io.ReadAll(io.LimitReader(opened, maximum+1))
	closeErr := opened.Close()
	if readErr != nil || closeErr != nil || int64(len(data)) > maximum {
		return nil, problem(InvalidBundle)
	}
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	return data, nil
}

func readSnapshotFile(ctx context.Context, root *os.Root, file, hash string) ([]byte, error) {
	data, err := readRegular(ctx, root, file, MaxBundleFileBytes)
	if err != nil {
		return nil, err
	}
	if digest(data) != hash {
		return nil, problem(InvalidBundle)
	}
	return data, nil
}

// verifyDependencies checks a project's installed manifest/content and the exact
// root require/replace entries emitted for it. expected is the manifest digest
// captured from its validated input, not learned from the installed files.
// It neither invokes Go nor attests MVS selection, binary provenance, SDK behavior
// or license permission. The project source tree must remain stable during checking.
func verifyDependencies(ctx context.Context, directory, version, expected string) error {
	if ctx == nil || !absolute(directory) || !frameworkVersion(version) || !validDigest(expected) {
		return problem(InvalidInput)
	}
	bundle, err := openSnapshot(ctx, filepath.Join(directory, "third_party", "fathomry"), version, nil)
	if err != nil {
		return err
	}
	defer bundle.root.Close()
	if bundle.digest != expected {
		return problem(InvalidBundle)
	}
	root, err := os.OpenRoot(directory)
	if err != nil {
		return problem(InvalidBundle)
	}
	defer root.Close()
	data, err := readRegular(ctx, root, "go.mod", MaxManifestBytes)
	if err != nil {
		return err
	}
	parsed, err := modfile.Parse("go.mod", data, nil)
	if err != nil {
		return problem(InvalidBundle)
	}
	requirements := make(map[string]string)
	for _, entry := range parsed.Require {
		requirements[entry.Mod.Path] = entry.Mod.Version
	}
	if requirements[FrameworkModule] != version {
		return problem(InvalidBundle)
	}
	for _, entry := range bundle.manifest.Modules {
		if requirements[entry.Path] != entry.Version {
			return problem(InvalidBundle)
		}
		found := false
		for _, replacement := range parsed.Replace {
			if replacement.Old.Path == entry.Path {
				if found || replacement.Old.Version != "" || replacement.New.Version != "" ||
					replacement.New.Path != "./third_party/fathomry/"+entry.Directory {
					return problem(InvalidBundle)
				}
				found = true
			}
		}
		if !found {
			return problem(InvalidBundle)
		}
	}
	return contextError(ctx)
}
