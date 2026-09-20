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
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func fixtureBundle(t *testing.T) (string, BundleManifest) {
	t.Helper()
	root := t.TempDir()
	content := map[string]string{
		"go.mod":        moduleNotice + "module example.org/patched\n\ngo 1.27.0\n",
		"patched.go":    goNotice + "package patched\nfunc Revision() string{return \"corrected\"}\n",
		"UPSTREAM.json": `{"module":"example.org/patched","version":"v1.0.0","source":"synthetic"}`,
		"FATHOMRY.md":   textNotice + "# Synthetic test patch\nNot an upstream distribution.\n",
		"LICENSE":       "Synthetic test material belongs to Fathomry; see project notices.\n",
	}
	entry := BundleModule{Path: "example.org/patched", Version: "v1.0.0", Revision: "v1", Directory: "patched", Files: map[string]string{}}
	for file, value := range content {
		put(t, filepath.Join(root, entry.Directory, file), []byte(value))
		entry.Files[file] = digest([]byte(value))
	}
	manifest := BundleManifest{Format: BundleFormat, FrameworkVersion: "v0.0.0-gh83", Modules: []BundleModule{entry}}
	saveManifest(t, root, manifest)
	return root, manifest
}
func saveManifest(t *testing.T, root string, manifest BundleManifest) {
	t.Helper()
	data, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	put(t, filepath.Join(root, "manifest.json"), data)
}

func TestBundleInstallationAndIndependentVerification(t *testing.T) {
	root, manifest := fixtureBundle(t)
	options := optionsFor(t)
	options.Bundle = root
	result, err := Create(context.Background(), options)
	if err != nil || !result.Complete {
		t.Fatal("bundle creation", err)
	}
	raw, err := os.ReadFile(filepath.Join(root, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	expected := digest(raw)
	if err := verifyDependencies(context.Background(), options.Directory, options.FrameworkVersion, expected); err != nil {
		t.Fatal(err)
	}
	installed := filepath.Join(options.Directory, "third_party/fathomry", manifest.Modules[0].Directory, "patched.go")
	put(t, installed, []byte("modified"))
	if err := verifyDependencies(context.Background(), options.Directory, options.FrameworkVersion, expected); !errors.Is(err, InvalidBundle) {
		t.Fatal("modified installed SDK accepted", err)
	}
}

func TestMalformedBundlesRefuseBeforeTargetCreation(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*BundleManifest)
	}{
		{"format", func(m *BundleManifest) { m.Format = "unknown" }},
		{"framework", func(m *BundleManifest) { m.FrameworkVersion = "v0.0.0-other" }},
		{"empty", func(m *BundleManifest) { m.Modules = nil }},
		{"duplicate", func(m *BundleManifest) { m.Modules = append(m.Modules, m.Modules[0]) }},
		{"path", func(m *BundleManifest) { m.Modules[0].Directory = "../escape" }},
		{"framework-override", func(m *BundleManifest) { m.Modules[0].Path = FrameworkModule }},
		{"framework-child", func(m *BundleManifest) { m.Modules[0].Path = FrameworkModule + "/configuration" }},
		{"framework-parent", func(m *BundleManifest) { m.Modules[0].Path = "github.com/frost-leo" }},
		{"revision", func(m *BundleManifest) { m.Modules[0].Revision = "" }},
		{"hash", func(m *BundleManifest) { m.Modules[0].Files["patched.go"] = "wrong" }},
		{"provenance", func(m *BundleManifest) { delete(m.Modules[0].Files, "UPSTREAM.json") }},
		{"escape", func(m *BundleManifest) { m.Modules[0].Files["../../outside"] = digest(nil) }},
		{"absolute", func(m *BundleManifest) { m.Modules[0].Files["/outside"] = digest(nil) }},
		{"version", func(m *BundleManifest) { m.Modules[0].Version = "latest" }},
		{"case", func(m *BundleManifest) { m.Modules[0].Files["PATCHED.go"] = m.Modules[0].Files["patched.go"] }},
	}
	for _, item := range cases {
		t.Run(item.name, func(t *testing.T) {
			root, manifest := fixtureBundle(t)
			item.mutate(&manifest)
			saveManifest(t, root, manifest)
			options := optionsFor(t)
			options.Bundle = root
			result, err := Create(context.Background(), options)
			if !errors.Is(err, InvalidBundle) || result.Created {
				t.Fatal("invalid bundle acquired target", err)
			}
			if _, err := os.Stat(options.Directory); !errors.Is(err, fs.ErrNotExist) {
				t.Fatal("target exists")
			}
		})
	}
}

func TestBundleSourceInventoryAndModuleBoundaries(t *testing.T) {
	for _, mode := range []string{"changed", "missing", "extra", "symlink", "directory", "wrong-module", "nested-replace", "nested-module", "oversize", "duplicate-json", "unknown-json", "trailing-json"} {
		t.Run(mode, func(t *testing.T) {
			root, manifest := fixtureBundle(t)
			target := filepath.Join(root, "patched", "patched.go")
			switch mode {
			case "changed":
				put(t, target, []byte("different"))
			case "missing":
				if err := os.Remove(target); err != nil {
					t.Fatal(err)
				}
			case "extra":
				put(t, filepath.Join(root, "patched", "extra"), []byte("not inventoried"))
			case "symlink":
				if err := os.Remove(target); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(filepath.Join(root, "patched", "LICENSE"), target); err != nil {
					t.Skip("symlink fixture unavailable")
				}
			case "directory":
				if err := os.Remove(target); err != nil {
					t.Fatal(err)
				}
				if err := os.Mkdir(target, 0700); err != nil {
					t.Fatal(err)
				}
			case "wrong-module", "nested-replace":
				value := "module example.org/different\n\ngo 1.27.0\n"
				if mode == "nested-replace" {
					value = "module example.org/patched\n\ngo 1.27.0\nreplace example.org/child => ./child\n"
				}
				put(t, filepath.Join(root, "patched/go.mod"), []byte(value))
				manifest.Modules[0].Files["go.mod"] = digest([]byte(value))
				saveManifest(t, root, manifest)
			case "nested-module":
				value := []byte("module example.org/child\n\ngo 1.27.0\n")
				put(t, filepath.Join(root, "patched/child/go.mod"), value)
				manifest.Modules[0].Files["child/go.mod"] = digest(value)
				saveManifest(t, root, manifest)
			case "oversize":
				file, err := os.OpenFile(target, os.O_WRONLY, 0600)
				if err != nil {
					t.Fatal(err)
				}
				if err := file.Truncate(MaxBundleFileBytes + 1); err != nil {
					t.Fatal(err)
				}
				if err := file.Close(); err != nil {
					t.Fatal(err)
				}
			default:
				raw, err := os.ReadFile(filepath.Join(root, "manifest.json"))
				if err != nil {
					t.Fatal(err)
				}
				text := string(raw)
				switch mode {
				case "duplicate-json":
					text = strings.Replace(text, `"format":`, `"format":"ignored","format":`, 1)
				case "unknown-json":
					text = strings.Replace(text, "{", `{"unknown":true,`, 1)
				case "trailing-json":
					text += "{}"
				}
				put(t, filepath.Join(root, "manifest.json"), []byte(text))
			}
			options := optionsFor(t)
			options.Bundle = root
			result, err := Create(context.Background(), options)
			if !errors.Is(err, InvalidBundle) || result.Created {
				t.Fatal("unsupported source acquired a destination", err)
			}
		})
	}
}

func TestRootReplacementCannotBeRetargeted(t *testing.T) {
	root, _ := fixtureBundle(t)
	options := optionsFor(t)
	options.Bundle = root
	if _, err := Create(context.Background(), options); err != nil {
		t.Fatal(err)
	}
	manifest, err := os.ReadFile(filepath.Join(root, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	modPath := filepath.Join(options.Directory, "go.mod")
	original, err := os.ReadFile(modPath)
	if err != nil {
		t.Fatal(err)
	}
	changed := strings.Replace(string(original), "./third_party/fathomry/patched", "../unqualified", 1)
	put(t, modPath, []byte(changed))
	if err := verifyDependencies(context.Background(), options.Directory, options.FrameworkVersion, digest(manifest)); !errors.Is(err, InvalidBundle) {
		t.Fatal("retargeted root replacement accepted", err)
	}
}

func FuzzBundleMetadata(f *testing.F) {
	f.Add("sha256:" + strings.Repeat("0", 64))
	f.Add("../escape")
	f.Fuzz(func(t *testing.T, value string) {
		if len(value) > 4096 {
			t.Skip()
		}
		if validDigest(value) && (len(value) != 71 || !strings.HasPrefix(value, "sha256:")) {
			t.Fatal("invalid digest contract")
		}
	})
}

func TestBundleRejectsUnicodeCaseFoldAliases(t *testing.T) {
	root, manifest := fixtureBundle(t)
	for _, name := range []string{"sigma/σ.go", "sigma/ς.go"} {
		data := []byte("package sigma\n")
		put(t, filepath.Join(root, "patched", name), data)
		manifest.Modules[0].Files[name] = digest(data)
	}
	saveManifest(t, root, manifest)
	options := optionsFor(t)
	options.Bundle = root
	result, err := Create(context.Background(), options)
	if !errors.Is(err, InvalidBundle) || result.Created {
		t.Fatal("Unicode case aliases accepted", err)
	}
}

func TestBundleCannotReplaceTheConsumingModule(t *testing.T) {
	root, manifest := fixtureBundle(t)
	options := optionsFor(t)
	options.Module = manifest.Modules[0].Path
	options.Bundle = root
	result, err := Create(context.Background(), options)
	if !errors.Is(err, InvalidBundle) || result.Created {
		t.Fatal("bundle shadowed its consuming module", err)
	}
}

func TestBundleSourceCannotContainDestination(t *testing.T) {
	root, _ := fixtureBundle(t)
	options := optionsFor(t)
	options.Bundle = root
	options.Directory = filepath.Join(root, "generated")
	result, err := Create(context.Background(), options)
	if !errors.Is(err, InvalidBundle) || result.Created {
		t.Fatal("generation modified its input bundle", err)
	}
	alias := filepath.Join(t.TempDir(), "alias")
	if err := os.Symlink(filepath.Join(root, "patched"), alias); err != nil {
		t.Skip("symlink fixture unavailable")
	}
	options.Directory = filepath.Join(alias, "generated")
	result, err = Create(context.Background(), options)
	if !errors.Is(err, InvalidBundle) || result.Created {
		t.Fatal("source/destination alias accepted", err)
	}
}
