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

package nuki

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	// Native module test companions must be in the actual consumer test graph,
	// not satisfied accidentally by a developer's separately warmed SDK cache.
	_ "github.com/quic-go/go-ossfuzz-seeds"
	_ "github.com/stretchr/testify/require"
	_ "go.uber.org/mock/gomock"
)

func productRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("test source unavailable")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "../../../../"))
}
func nativeCommand(t *testing.T, environment []string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	goBinary := filepath.Join(runtime.GOROOT(), "bin", "go")
	args := []string{"test", "-mod=readonly", "-count=1", "-timeout=90s"}
	configuration := exec.CommandContext(ctx, goBinary, "env", "CGO_ENABLED")
	configuration.Env = environment
	cgo, err := configuration.Output()
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(string(cgo)) == "1" {
		args = append(args, "-race")
	}
	args = append(args, "github.com/nukilabs/tlsclient", "github.com/nukilabs/tlsclient/proxy", "github.com/nukilabs/socks",
		"github.com/nukilabs/qpack", "github.com/nukilabs/quic-go", "github.com/nukilabs/quic-go/http3", "github.com/nukilabs/quic-go/qlogwriter")
	command := exec.CommandContext(ctx, goBinary, args...)
	command.Dir = productRoot(t)
	command.Env = environment
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("native consumer-version controls: %v\n%s", err, output)
	}
}

func TestNativeCorrectionsUseConsumerVersions(t *testing.T) { nativeCommand(t, consumerEnvironment()) }

type cacheModule struct {
	Path, Version, Dir, GoMod string
	Replace                   *cacheModule
}
type cachePackage struct{ Module *cacheModule }

func TestNativeCorrectionsWithRestrictedModuleCache(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	root := productRoot(t)
	goBinary := filepath.Join(runtime.GOROOT(), "bin", "go")
	run := func(args ...string) []byte {
		command := exec.CommandContext(ctx, goBinary, args...)
		command.Dir = root
		command.Env = consumerEnvironment()
		output, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("cache inventory: %v\n%s", err, output)
		}
		return output
	}
	oldCache := strings.TrimSpace(string(run("env", "GOMODCACHE")))
	reduced := filepath.Join(t.TempDir(), "modcache")
	seen := map[string]bool{}
	packages := json.NewDecoder(bytes.NewReader(run("list", "-mod=readonly", "-deps", "-test", "-json", "./internal/httpclient/nuki/v1")))
	for {
		var item cachePackage
		err := packages.Decode(&item)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		module := item.Module
		if module == nil {
			continue
		}
		if module.Replace != nil {
			module = module.Replace
		}
		if module.Version == "" || module.Dir == "" || seen[module.Dir] {
			continue
		}
		relative, err := filepath.Rel(oldCache, module.Dir)
		if err != nil || strings.HasPrefix(relative, "..") {
			t.Fatal("module outside declared cache", err)
		}
		copyCacheTree(t, module.Dir, filepath.Join(reduced, relative))
		seen[module.Dir] = true
	}
	// Preserve cached graph metadata, but no extra module source trees or ZIPs.
	// Listing every lazy graph module would itself require unused dependencies
	// that a clean consumer build is not obliged to download.
	err := filepath.WalkDir(filepath.Join(oldCache, "cache", "download"), func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		suffix := filepath.Ext(path)
		if suffix != ".mod" && suffix != ".info" && suffix != ".ziphash" {
			return nil
		}
		relative, err := filepath.Rel(oldCache, path)
		if err != nil {
			return err
		}
		copyCacheFile(t, path, filepath.Join(reduced, relative))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(seen) == 0 {
		t.Fatal("restricted cache did not retain any actual dependencies")
	}
	nativeCommand(t, append(consumerEnvironment(), "GOMODCACHE="+reduced, "GOCACHE="+filepath.Join(t.TempDir(), "buildcache")))
}
func copyCacheTree(t *testing.T, source, target string) {
	t.Helper()
	err := filepath.WalkDir(source, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return os.MkdirAll(filepath.Join(target, relative), 0o700)
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return errors.New("unexpected dependency symlink")
		}
		copyCacheFile(t, path, filepath.Join(target, relative))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
func copyCacheFile(t *testing.T, source, target string) {
	t.Helper()
	input, err := os.Open(source)
	if err != nil {
		t.Fatal(err)
	}
	defer input.Close()
	if err := os.MkdirAll(filepath.Dir(target), 0o700); err != nil {
		t.Fatal(err)
	}
	output, err := os.Create(target)
	if err != nil {
		t.Fatal(err)
	}
	_, copyErr := io.Copy(output, input)
	closeErr := output.Close()
	if copyErr != nil || closeErr != nil {
		t.Fatal(copyErr, closeErr)
	}
}
