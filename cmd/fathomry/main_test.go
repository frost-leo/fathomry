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

package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/cmd/fathomry/internal/command"
	"github.com/frost-leo/fathomry/failure/v1"
	"github.com/frost-leo/fathomry/i18n/v1"
	"github.com/frost-leo/fathomry/resource/v1"
	"github.com/frost-leo/fathomry/settings/v1"
	"github.com/spf13/cobra"
)

func binary(t *testing.T) string {
	t.Helper()
	name := "fathomry"
	if runtime.GOOS == "windows" {
		name += ".exe"
	}
	path := filepath.Join(t.TempDir(), name)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	build := exec.CommandContext(ctx, "go", "build", "-o", path, ".")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build executable: %v\n%s", err, output)
	}
	return path
}

func TestExecutable(t *testing.T) {
	path := binary(t)
	workdir := t.TempDir()
	for _, item := range []struct {
		name     string
		args     []string
		language string
		code     int
	}{
		{"help", []string{"--help"}, "zh-CN", 0},
		{"explain", []string{"error", "explain", "0xA0450001", "--lang", "en", "--output", "json"}, "private-invalid-locale", 0},
		{"missing", []string{"error", "explain", "0xA7FFFFFF", "--output", "json"}, "en", 1},
		{"version_deferred", []string{"--version"}, "en", 2},
	} {
		t.Run(item.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			run := exec.CommandContext(ctx, path, item.args...)
			run.Dir = workdir
			run.Env = append(os.Environ(), "FATHOMRY_LANG="+item.language)
			var output, diagnostic bytes.Buffer
			run.Stdout, run.Stderr = &output, &diagnostic
			err := run.Run()
			code := 0
			if err != nil {
				var exited *exec.ExitError
				if !errors.As(err, &exited) {
					t.Fatal(err)
				}
				code = exited.ExitCode()
			}
			if code != item.code {
				t.Fatalf("exit=%d want=%d: %s", code, item.code, diagnostic.String())
			}
			if strings.Contains(output.String()+diagnostic.String(), "private-invalid-locale") {
				t.Fatal("environment value leaked")
			}
			if item.code == 0 {
				if diagnostic.Len() != 0 {
					t.Fatal("success diagnostic")
				}
				if item.name == "help" && !strings.Contains(output.String(), "用法") {
					t.Fatal("entry did not capture environment preference")
				}
				if item.name == "explain" && !json.Valid(output.Bytes()) {
					t.Fatal("machine result malformed")
				}
			} else if output.Len() != 0 || diagnostic.Len() == 0 {
				t.Fatal("failed command streams mixed")
			}
		})
	}
	t.Run("closed_stdout_is_an_output_error_not_SIGPIPE", func(t *testing.T) {
		if runtime.GOOS == "windows" {
			t.Skip("POSIX signal acceptance")
		}
		reader, writer, err := os.Pipe()
		if err != nil {
			t.Fatal(err)
		}
		_ = reader.Close()
		defer writer.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		run := exec.CommandContext(ctx, path, "--help")
		var diagnostic bytes.Buffer
		run.Stdout, run.Stderr = writer, &diagnostic
		err = run.Run()
		var exited *exec.ExitError
		if !errors.As(err, &exited) || exited.ExitCode() != 1 || !strings.Contains(diagnostic.String(), command.ErrOutput.String()) {
			t.Fatalf("broken pipe bypassed process policy: %v / %s", err, diagnostic.String())
		}
	})
}

func TestProcessCancellation(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX signal acceptance")
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	process := exec.CommandContext(ctx, executable, "-test.run=^TestSignalHelper$", "-test.count=1")
	process.Env = append(os.Environ(), "FATHOMRY_CLI_TEST_CHILD=1")
	output, err := process.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var diagnostic bytes.Buffer
	process.Stderr = &diagnostic
	if err := process.Start(); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(output)
	line, err := reader.ReadString('\n')
	if err != nil || line != "ready\n" {
		_ = process.Process.Kill()
		_ = process.Wait()
		t.Fatal("helper readiness absent", err)
	}
	if err := process.Process.Signal(os.Interrupt); err != nil {
		_ = process.Process.Kill()
		_ = process.Wait()
		t.Fatal(err)
	}
	remaining, readErr := io.ReadAll(reader)
	waitErr := process.Wait()
	var exited *exec.ExitError
	if readErr != nil || !errors.As(waitErr, &exited) || exited.ExitCode() != 130 || string(remaining) != "cleaned\n" ||
		!strings.Contains(diagnostic.String(), command.ErrCanceled.String()) {
		t.Fatalf("signal/cleanup boundary failed: %v / %q / %s", waitErr, remaining, diagnostic.String())
	}
}

// TestSignalHelper adds a test-binary-only operation, never a shipped command.
func TestSignalHelper(t *testing.T) {
	if os.Getenv("FATHOMRY_CLI_TEST_CHILD") != "1" {
		return
	}
	ctx, stop := signalContext()
	defer stop()
	components := append(i18n.CoreComponents(), command.Component())
	var definitions []failure.Definition
	for _, component := range components {
		definitions = append(definitions, component.Definitions...)
	}
	errorsCatalog, err := failure.Prepare(definitions...)
	if err != nil {
		t.Fatal(err)
	}
	messages, err := i18n.Prepare(components...)
	if err != nil {
		t.Fatal(err)
	}
	var cleaned atomic.Bool
	err = command.Run(ctx, []string{"wait"}, command.Options{Input: os.Stdin, Output: os.Stdout, ErrorOutput: os.Stderr},
		command.Catalogs{Errors: errorsCatalog, Messages: messages}, func(invocation *command.Invocation) *cobra.Command {
			root := invocation.Group("fathomry", "fathomry.command_line.root")
			wait := &cobra.Command{Use: "wait", Short: "fathomry.command_line.error_list", Args: cobra.NoArgs}
			invocation.Bind(wait, func(ctx context.Context, _ []string) error {
				scope, err := invocation.Scope()
				if err != nil {
					return err
				}
				_, err = resource.Bind(scope, resource.Binding[int, int]{
					Name: "signal", Policy: resource.Fixed,
					Select: func(view settings.View) (int, error) {
						snapshot, err := settings.As[int](view)
						if err != nil {
							return 0, err
						}
						return snapshot.ValueCopy()
					},
					Clone: func(value int) int { return value },
					Build: func(context.Context, int) (*resource.Instance[int], error) {
						return &resource.Instance[int]{Value: 1, Release: func(cleanup context.Context) resource.ReleaseResult {
							if cleanup.Err() != nil {
								return resource.ReleaseResult{Err: cleanup.Err()}
							}
							_, err := fmt.Fprintln(os.Stdout, "cleaned")
							cleaned.Store(err == nil)
							return resource.ReleaseResult{Complete: true, Err: err}
						}}, nil
					},
				})
				if err != nil {
					return err
				}
				snapshot, err := settings.New(1, func(value int) int { return value })
				if err != nil {
					return err
				}
				update, err := scope.Apply(ctx, snapshot.View())
				if err != nil {
					return err
				}
				if err := update.Wait(ctx); err != nil {
					return err
				}
				if _, err := fmt.Fprintln(os.Stdout, "ready"); err != nil {
					return err
				}
				<-ctx.Done()
				return ctx.Err()
			})
			root.AddCommand(wait)
			return root
		})
	if !cleaned.Load() {
		t.Fatal("command returned before cleanup")
	}
	stop()
	os.Exit(command.ExitCode(err))
}
