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

package project

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"go/format"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"

	"golang.org/x/mod/modfile"
	"golang.org/x/mod/module"
)

func repository(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs("../../../..")
	if err != nil {
		t.Fatal(err)
	}
	return root
}

func inputFor(t *testing.T) request {
	t.Helper()
	return request{filepath.Join(t.TempDir(), "app"), "example.org/collector", repository(t)}
}

func mustWrite(t *testing.T, path, value string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(value), 0o600); err != nil {
		t.Fatal(err)
	}
}

func mustMkdir(t *testing.T, path string) {
	t.Helper()
	if err := os.Mkdir(path, 0o700); err != nil {
		t.Fatal(err)
	}
}

func mustLink(t *testing.T, old, new string) {
	t.Helper()
	if err := os.Symlink(old, new); err != nil {
		t.Fatal(err)
	}
}

func assertAbsent(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("target must be absent: %s: %v", path, err)
	}
}

func readFiles(t *testing.T, directory string) map[string]string {
	t.Helper()
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	result := map[string]string{}
	for _, entry := range entries {
		if !entry.Type().IsRegular() {
			t.Fatalf("unexpected non-regular entry: %s", entry.Name())
		}
		data, err := os.ReadFile(filepath.Join(directory, entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		result[entry.Name()] = string(data)
	}
	return result
}

func assertComplete(t *testing.T, output prepared) {
	t.Helper()
	actual := readFiles(t, output.directory)
	if len(actual) != len(output.files) || len(actual) != 4 {
		t.Fatalf("unexpected files: %v", actual)
	}
	for _, file := range output.files {
		if actual[file.name] != string(file.data) {
			t.Errorf("%s differs from rendered bytes", file.name)
		}
	}
}

func TestCreateRendersExactlyFourFiles(t *testing.T) {
	input := inputFor(t)
	expected, err := prepare(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	assertAbsent(t, input.directory)
	effect, err := create(context.Background(), input)
	if effect != complete || err != nil {
		t.Fatalf("create: %v %v", effect, err)
	}
	assertComplete(t, expected)
	files := readFiles(t, input.directory)
	parsed, err := modfile.Parse("go.mod", []byte(files["go.mod"]), nil)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Module.Mod.Path != input.module || parsed.Go.Version != supportedGo ||
		len(parsed.Require) != 1 || parsed.Require[0].Mod.Path != frameworkModule || parsed.Require[0].Mod.Version != "v0.0.0" ||
		len(parsed.Replace) != 1 || parsed.Replace[0].Old.Path != frameworkModule || parsed.Replace[0].New.Version != "" {
		t.Fatalf("wrong generated module: %s", files["go.mod"])
	}
	source, err := filepath.EvalSymlinks(filepath.Join(input.directory, filepath.FromSlash(parsed.Replace[0].New.Path)))
	if err != nil || source != repository(t) {
		t.Fatalf("wrong source: %s %v", source, err)
	}
	formatted, err := format.Source([]byte(files["main.go"]))
	if err != nil || !bytes.Equal(formatted, []byte(files["main.go"])) {
		t.Fatalf("main.go not preformatted: %v", err)
	}
	var rules []string
	for line := range strings.SplitSeq(files[".gitignore"], "\n") {
		if line != "" && !strings.HasPrefix(line, "#") {
			rules = append(rules, line)
		}
	}
	wantRules := []string{"*.exe", "*.exe~", "*.dll", "*.so", "*.dylib", "*.test", "*.out", "coverage.*", "*.coverprofile", "profile.cov", "go.work", "go.work.sum", ".env", "/bin/"}
	if strings.Join(rules, "\n") != strings.Join(wantRules, "\n") {
		t.Fatalf("ignore rules: %q", rules)
	}
	for _, required := range []string{"cli.Main()", "cd -P .", "GOWORK=off go mod tidy", "go build -mod=readonly", "go run -mod=readonly", "go.sum", "live", "toolchain", "partial"} {
		if !strings.Contains(files["README.md"], required) {
			t.Errorf("README lacks %s", required)
		}
	}
}

func TestRelativePathsAndAliases(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink runtime acceptance is Linux")
	}
	root := t.TempDir()
	actual := filepath.Join(root, "physical")
	mustMkdir(t, actual)
	mustMkdir(t, filepath.Join(root, "invocation"))
	mustLink(t, actual, filepath.Join(root, "parent alias"))
	mustLink(t, repository(t), filepath.Join(root, "source \" alias"))
	t.Chdir(filepath.Join(root, "invocation"))
	input := request{"../parent alias/new", "example.org/project/v2", "../source \" alias"}
	effect, err := create(context.Background(), input)
	if effect != complete || err != nil {
		t.Fatalf("create: %v %v", effect, err)
	}
	data, err := os.ReadFile(filepath.Join(actual, "new", "go.mod"))
	if err != nil {
		t.Fatal(err)
	}
	metadata, err := modfile.Parse("go.mod", data, nil)
	if err != nil {
		t.Fatal(err)
	}
	if metadata.Module.Mod.Path != input.module || metadata.Replace[0].New.Path != "../../source \" alias" {
		t.Fatalf("wrong path/module: %s", data)
	}
}

func TestInvalidInputNeverCreates(t *testing.T) {
	for name, change := range map[string]func(*request){
		"empty-directory": func(input *request) { input.directory = "" },
		"empty-source":    func(input *request) { input.source = "" },
		"empty-module":    func(input *request) { input.module = "" },
		"relative-module": func(input *request) { input.module = "../module" },
		"space-module":    func(input *request) { input.module = "example.org/a b" },
		"module-v1":       func(input *request) { input.module = "example.org/project/v1" },
		"framework":       func(input *request) { input.module = frameworkModule },
		"cli":             func(input *request) { input.module = frameworkModule + "/cli" },
		"nul-source":      func(input *request) { input.source += "\x00" },
		"missing-source":  func(input *request) { input.source = filepath.Join(t.TempDir(), "absent") },
		"missing-parent":  func(input *request) { input.directory = filepath.Join(t.TempDir(), "absent", "app") },
	} {
		t.Run(name, func(t *testing.T) {
			input := inputFor(t)
			target := input.directory
			change(&input)
			effect, err := create(context.Background(), input)
			if effect != untouched || err == nil {
				t.Fatalf("accepted: %v %v", effect, err)
			}
			assertAbsent(t, target)
			if input.directory != "" && input.directory != target {
				assertAbsent(t, input.directory)
			}
		})
	}
}

func TestExistingTargetsNeverChange(t *testing.T) {
	for _, kind := range []string{"file", "empty", "nonempty", "symlink-file", "symlink-directory", "dangling"} {
		t.Run(kind, func(t *testing.T) {
			input := inputFor(t)
			switch kind {
			case "file":
				mustWrite(t, input.directory, "preserve")
			case "empty":
				mustMkdir(t, input.directory)
			case "nonempty":
				mustMkdir(t, input.directory)
				mustWrite(t, filepath.Join(input.directory, "keep"), "preserve")
			default:
				if runtime.GOOS == "windows" {
					t.Skip("symlink runtime acceptance is Linux")
				}
				other := filepath.Join(filepath.Dir(input.directory), "other")
				if kind == "symlink-file" {
					mustWrite(t, other, "preserve")
				} else if kind == "symlink-directory" {
					mustMkdir(t, other)
					mustWrite(t, filepath.Join(other, "keep"), "preserve")
				}
				mustLink(t, other, input.directory)
			}
			before, err := os.Lstat(input.directory)
			if err != nil {
				t.Fatal(err)
			}
			for range 2 {
				effect, err := create(context.Background(), input)
				if effect != untouched || !errors.Is(err, os.ErrExist) {
					t.Fatalf("wrong refusal: %v %v", effect, err)
				}
			}
			after, err := os.Lstat(input.directory)
			if err != nil || !os.SameFile(before, after) || before.Mode() != after.Mode() {
				t.Fatalf("entry replaced: %v", err)
			}
			switch kind {
			case "file", "symlink-file":
				data, err := os.ReadFile(input.directory)
				if err != nil || string(data) != "preserve" {
					t.Fatalf("file changed: %q %v", data, err)
				}
			case "nonempty", "symlink-directory":
				files := readFiles(t, input.directory)
				if len(files) != 1 || files["keep"] != "preserve" {
					t.Fatalf("directory changed: %v", files)
				}
			case "empty":
				if files := readFiles(t, input.directory); len(files) != 0 {
					t.Fatalf("directory populated: %v", files)
				}
			case "dangling":
				link, err := os.Readlink(input.directory)
				if err != nil || link != filepath.Join(filepath.Dir(input.directory), "other") {
					t.Fatalf("symlink changed: %s %v", link, err)
				}
			}
		})
	}
}

func TestSourceProfile(t *testing.T) {
	valid := "module " + frameworkModule + "\ngo 1.27.0\n"
	for name, contents := range map[string]string{
		"valid": valid, "toolchain-default": valid + "toolchain default\n", "toolchain-supported": valid + "toolchain go1.27.0\n",
		"wrong-module": "module example.org/not-fathomry\ngo 1.27.0\n",
		"no-module":    "go 1.27.0\n", "no-go": "module " + frameworkModule + "\n",
		"older-go":         strings.ReplaceAll(valid, "1.27.0", "1.26.4"),
		"future-go":        strings.ReplaceAll(valid, "1.27.0", "1.28.0"),
		"invalid-go":       strings.ReplaceAll(valid, "1.27.0", "1.27.0rc1"),
		"custom-toolchain": valid + "toolchain go1.custom\n",
		"newer-toolchain":  valid + "toolchain go1.27.1\n",
		"broken":           valid + "require (\n",
		"oversized":        valid + "//" + strings.Repeat("x", maxModuleBytes),
	} {
		t.Run(name, func(t *testing.T) {
			input := inputFor(t)
			input.source = t.TempDir()
			mustMkdir(t, filepath.Join(input.source, "cli"))
			mustWrite(t, filepath.Join(input.source, "go.mod"), contents)
			output, err := prepare(context.Background(), input)
			wantValid := name == "valid" || name == "toolchain-default" || name == "toolchain-supported"
			if (err == nil) != wantValid {
				t.Fatalf("prepare: %+v %v", output, err)
			}
			assertAbsent(t, input.directory)
			data, readErr := os.ReadFile(filepath.Join(input.source, "go.mod"))
			if readErr != nil || string(data) != contents {
				t.Fatal("source changed", readErr)
			}
		})
	}
	for _, kind := range []string{"missing-module", "directory-module", "missing-cli", "file-cli"} {
		t.Run(kind, func(t *testing.T) {
			input := inputFor(t)
			input.source = t.TempDir()
			switch kind {
			case "directory-module":
				mustMkdir(t, filepath.Join(input.source, "go.mod"))
			case "missing-cli", "file-cli":
				mustWrite(t, filepath.Join(input.source, "go.mod"), valid)
				if kind == "file-cli" {
					mustWrite(t, filepath.Join(input.source, "cli"), "not a directory")
				}
			}
			effect, err := create(context.Background(), input)
			if effect != untouched || err == nil {
				t.Fatalf("accepted: %v %v", effect, err)
			}
			assertAbsent(t, input.directory)
		})
	}
}

func TestSourceOverlapAndUnrenderablePath(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Linux symlink and literal-backslash path cases")
	}
	source := t.TempDir()
	mustMkdir(t, filepath.Join(source, "nested"))
	alias := filepath.Join(t.TempDir(), "source-alias")
	mustLink(t, source, alias)
	for _, pair := range [][2]string{
		{source, filepath.Join(source, "new")},
		{source, filepath.Join(alias, "nested", "new")},
		{alias, filepath.Join(source, "nested", "new")},
		{alias, filepath.Join(alias, "new")},
	} {
		input := request{pair[1], "example.org/collector", pair[0]}
		effect, err := create(context.Background(), input)
		if effect != untouched || !errors.Is(err, errOverlap) {
			t.Fatalf("overlap accepted: %v %v", effect, err)
		}
		assertAbsent(t, input.directory)
	}
	input := inputFor(t)
	sourceAlias := filepath.Join(t.TempDir(), "source\\name")
	mustLink(t, input.source, sourceAlias)
	input.source = sourceAlias
	effect, err := create(context.Background(), input)
	if effect != untouched || err == nil {
		t.Fatalf("unparseable replacement accepted: %v %v", effect, err)
	}
	assertAbsent(t, input.directory)
}

func TestSameTargetCompetition(t *testing.T) {
	input := inputFor(t)
	const count = 12
	plans := make([]prepared, count)
	for index := range plans {
		input.module = fmt.Sprintf("example.org/project%d", index)
		var err error
		plans[index], err = prepare(context.Background(), input)
		if err != nil {
			t.Fatal(err)
		}
	}
	var group sync.WaitGroup
	type result struct {
		index  int
		effect effect
		err    error
	}
	results := make(chan result, count)
	start := make(chan struct{})
	for index, plan := range plans {
		group.Go(func() {
			<-start
			effect, err := plan.write(context.Background(), exclusiveFile)
			results <- result{index, effect, err}
		})
	}
	close(start)
	group.Wait()
	close(results)
	winner := -1
	for result := range results {
		if result.err == nil {
			if winner >= 0 || result.effect != complete {
				t.Fatal("multiple or incomplete winners")
			}
			winner = result.index
		} else if result.effect != untouched || !errors.Is(result.err, os.ErrExist) {
			t.Fatalf("wrong loser: %+v", result)
		}
	}
	if winner < 0 {
		t.Fatal("no winner")
	}
	assertComplete(t, plans[winner])
}

type faultyFile struct {
	io.WriteCloser
	write func([]byte) (int, error)
	close func() error
}

func (file faultyFile) Write(data []byte) (int, error) {
	if file.write != nil {
		return file.write(data)
	}
	return file.WriteCloser.Write(data)
}

func (file faultyFile) Close() error {
	err := file.WriteCloser.Close()
	if file.close != nil {
		return errors.Join(err, file.close())
	}
	return err
}

func TestPartialFailuresPreserveFilesAndCauses(t *testing.T) {
	writeFailure, closeFailure, openFailure := errors.New("write sentinel"), errors.New("close sentinel"), errors.New("open sentinel")
	for _, kind := range []string{"open", "short-write", "write-and-close", "close-only", "cancel-after-open", "cancel-after-write", "cancel-and-close", "late-cancel", "file-race"} {
		t.Run(kind, func(t *testing.T) {
			input := inputFor(t)
			plan, err := prepare(context.Background(), input)
			if err != nil {
				t.Fatal(err)
			}
			neighbor := filepath.Join(filepath.Dir(input.directory), "keep")
			mustWrite(t, neighbor, "unrelated")
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			calls, closes := 0, 0
			open := func(path string) (io.WriteCloser, error) {
				calls++
				if kind == "open" && calls == 2 {
					return nil, openFailure
				}
				if kind == "file-race" && calls == 2 {
					mustWrite(t, path, "competitor")
				}
				file, err := exclusiveFile(path)
				if err != nil {
					return nil, err
				}
				wrapped := faultyFile{WriteCloser: file, close: func() error { closes++; return nil }}
				if kind == "late-cancel" {
					if calls == 4 {
						wrapped.close = func() error { closes++; cancel(); return nil }
					}
				} else if calls == 2 {
					switch kind {
					case "short-write", "write-and-close":
						wrapped.write = func(data []byte) (int, error) {
							count, err := file.Write(data[:7])
							if kind == "write-and-close" {
								err = errors.Join(err, writeFailure)
							}
							return count, err
						}
						if kind == "write-and-close" {
							wrapped.close = func() error { closes++; return closeFailure }
						}
					case "cancel-after-open":
						cancel()
					case "close-only":
						wrapped.close = func() error { closes++; return closeFailure }
					case "cancel-after-write", "cancel-and-close":
						wrapped.write = func(data []byte) (int, error) {
							count, err := file.Write(data)
							cancel()
							return count, err
						}
						if kind == "cancel-and-close" {
							wrapped.close = func() error { closes++; return closeFailure }
						}
					}
				}
				return wrapped, nil
			}
			effect, err := plan.write(ctx, open)
			if kind == "late-cancel" {
				if effect != complete || !errors.Is(err, context.Canceled) || calls != 4 || closes != 4 {
					t.Fatalf("late cancel: %v %v calls=%d closes=%d", effect, err, calls, closes)
				}
				assertComplete(t, plan)
			} else {
				if effect != partial || err == nil || calls != 2 {
					t.Fatalf("partial: %v %v calls=%d", effect, err, calls)
				}
				files := readFiles(t, plan.directory)
				if files["go.mod"] != string(plan.files[0].data) {
					t.Fatal("completed first file changed")
				}
				switch kind {
				case "open":
					if !errors.Is(err, openFailure) || len(files) != 1 || closes != 1 {
						t.Fatalf("open failure: %v %v closes=%d", err, files, closes)
					}
				case "file-race":
					if !errors.Is(err, os.ErrExist) || files["main.go"] != "competitor" || len(files) != 2 || closes != 1 {
						t.Fatal("exclusive file creation failed", err)
					}
				default:
					if len(files) != 2 || closes != 2 {
						t.Fatalf("files=%v closes=%d", files, closes)
					}
					if strings.HasPrefix(kind, "cancel") && !errors.Is(err, context.Canceled) {
						t.Fatal("cancellation lost", err)
					}
					if kind == "cancel-after-open" && files["main.go"] != "" {
						t.Fatal("wrote after observed cancellation")
					}
					if kind == "short-write" || kind == "write-and-close" {
						if !errors.Is(err, io.ErrShortWrite) || files["main.go"] != string(plan.files[1].data[:7]) {
							t.Fatal("short write lost", err)
						}
					}
					if kind == "write-and-close" && (!errors.Is(err, writeFailure) || !errors.Is(err, closeFailure)) ||
						kind == "cancel-and-close" && !errors.Is(err, closeFailure) {
						t.Fatal("joined cause lost", err)
					}
					if kind == "close-only" && (!errors.Is(err, closeFailure) || files["main.go"] != string(plan.files[1].data)) {
						t.Fatal("close failure lost or file discarded", err)
					}
				}
			}
			before := readFiles(t, plan.directory)
			retryEffect, retryErr := create(context.Background(), input)
			if retryEffect != untouched || !errors.Is(retryErr, os.ErrExist) {
				t.Fatal("retry not refused", retryEffect, retryErr)
			}
			after := readFiles(t, plan.directory)
			if fmt.Sprint(before) != fmt.Sprint(after) {
				t.Fatal("retry altered partial/complete output")
			}
			data, readErr := os.ReadFile(neighbor)
			if readErr != nil || string(data) != "unrelated" {
				t.Fatal("neighbor changed", readErr)
			}
		})
	}
}

func TestPreCanceledDoesNotInspectOrCreate(t *testing.T) {
	input := inputFor(t)
	input.source = filepath.Join(t.TempDir(), "missing")
	cause := errors.New("caller cause")
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(cause)
	effect, err := create(ctx, input)
	if effect != untouched || !errors.Is(err, context.Canceled) || !errors.Is(err, cause) || errors.Is(err, os.ErrNotExist) {
		t.Fatalf("wrong cancellation: %v %v", effect, err)
	}
	assertAbsent(t, input.directory)
}

func TestStaticValidationAndResources(t *testing.T) {
	command := New()
	if len(command.Aliases) != 0 || command.HasSubCommands() {
		t.Fatal("new must remain a project-only leaf")
	}
	if err := command.Flags().Set("module", "example.org/app"); err != nil {
		t.Fatal(err)
	}
	if err := command.Flags().Set("fathomry-source", "/missing/source"); err != nil {
		t.Fatal(err)
	}
	if err := command.ValidateArgs([]string{"/missing/parent/app"}); err != nil {
		t.Fatal("static validation inspected paths", err)
	}
	words := loadProse()
	for _, key := range []string{"new", "module", "source", "created", "notStarted", "partial", "completeDelivery"} {
		if words.english[key] == "" || words.chinese[key] == "" {
			t.Errorf("missing resource %s", key)
		}
	}
}

func FuzzModuleRendering(f *testing.F) {
	for _, seed := range []string{"example.org/app", "example.org/app/v2", "gopkg.in/app.v3", "github.com/frost-leo/fathomry", "../bad", "a b", "example.org/\"\n"} {
		f.Add(seed, "../source path")
	}
	f.Fuzz(func(t *testing.T, modulePath, replacement string) {
		if module.CheckPath(modulePath) != nil || len(modulePath)+len(replacement) > 4096 {
			return
		}
		files, err := render(modulePath, replacement)
		if err != nil {
			return
		}
		metadata, err := modfile.Parse("go.mod", files[0].data, nil)
		if err != nil || metadata.Module.Mod.Path != modulePath || len(metadata.Replace) != 1 ||
			metadata.Replace[0].New.Path != replacement {
			t.Fatalf("render not round-trippable: %v", err)
		}
	})
}
