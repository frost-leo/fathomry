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

package create

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/cmd/fathomry/internal/command"
	"github.com/frost-leo/fathomry/failure/v1"
	"github.com/frost-leo/fathomry/i18n/v1"
	"github.com/spf13/cobra"
)

func catalogs(t testing.TB) command.Catalogs {
	t.Helper()
	components := append(i18n.CoreComponents(), command.Component(), Component())
	var definitions []failure.Definition
	for _, component := range components {
		definitions = append(definitions, component.Definitions...)
	}
	failures, err := failure.Prepare(definitions...)
	if err != nil {
		t.Fatal(err)
	}
	messages, err := i18n.Prepare(components...)
	if err != nil {
		t.Fatal(err)
	}
	return command.Catalogs{Errors: failures, Messages: messages}
}

func TestLeafHelpAndRequiredInput(t *testing.T) {
	for _, args := range [][]string{
		{"project", "new", "--help"}, {"--lang=zh-CN", "help", "project", "new"},
		{"project", "new"}, {"project", "new", "positional"},
		{"project", "new", "--name=../private-canary", "--module=example.com/demo", "--framework-version=" + frameworkVersion},
	} {
		var output, diagnostic bytes.Buffer
		err := command.Run(context.Background(), args, command.Options{Input: strings.NewReader(""), Output: &output, ErrorOutput: &diagnostic},
			catalogs(t), func(inv *command.Invocation) *cobra.Command {
				root := inv.Group("fathomry", "fathomry.command_line.root")
				group := inv.Group("project", "fathomry.command_line.root")
				group.AddCommand(New(inv))
				root.AddCommand(group)
				return root
			})
		help := slices.Contains(args, "--help") || slices.Contains(args, "help")
		if help && (err != nil || output.Len() == 0 || diagnostic.Len() != 0) {
			t.Fatal(args, err)
		}
		if !help && (command.ExitCode(err) != 2 || output.Len() != 0) {
			t.Fatal("invalid input did not reject", args, err)
		}
		if strings.Contains(diagnostic.String(), "private-canary") {
			t.Fatal("invalid input leaked")
		}
	}
}

func TestCreationResourceCoverage(t *testing.T) {
	value := catalogs(t)
	for _, locale := range []string{"en", "zh-CN"} {
		coverage, err := value.Messages.Coverage(locale)
		if err != nil {
			t.Fatal(err)
		}
		for _, component := range coverage {
			if len(component.Missing) != 0 {
				t.Fatal("missing resource", component.Component, locale, component.Missing)
			}
		}
	}
}

type rejectedWriter struct{ err error }

func (writer rejectedWriter) Write([]byte) (int, error) { return 0, writer.err }

func TestMutationReportSurvivesFailureAndOutputLoss(t *testing.T) {
	for _, mode := range []string{"streams", "stdout_failed", "both_failed"} {
		t.Run(mode, func(t *testing.T) {
			parent, path := parentRoot(t)
			selected := prepared(t)
			fsCause := errors.New("private-filesystem-cause")
			outputCause := errors.New("private-output-cause")
			var output, diagnostic bytes.Buffer
			var writer io.Writer = &output
			var errorsWriter io.Writer = &diagnostic
			if mode != "streams" {
				writer = rejectedWriter{outputCause}
			}
			if mode == "both_failed" {
				errorsWriter = rejectedWriter{outputCause}
			}
			err := command.Run(context.Background(), []string{"--output=json", "probe"},
				command.Options{Input: strings.NewReader(""), Output: writer, ErrorOutput: errorsWriter}, catalogs(t),
				func(inv *command.Invocation) *cobra.Command {
					root := inv.Group("fathomry", "fathomry.command_line.root")
					leaf := &cobra.Command{Use: "probe", Short: "fathomry.project_creation.new"}
					inv.Bind(leaf, func(ctx context.Context, _ []string) error {
						result, cause := createInParent(ctx, parent, selected, func(rootFile *os.Root, name string) (io.WriteCloser, error) {
							file, err := openFile(rootFile, name)
							if err != nil {
								return nil, err
							}
							return &faultyFile{WriteCloser: file, short: true, writeError: fsCause}, nil
						})
						return stage(inv, result, cause)
					})
					root.AddCommand(leaf)
					return root
				})
			var execution *command.ExecutionError
			if err == nil || !errors.Is(err, fsCause) || !errors.As(err, &execution) {
				t.Fatal("failure/custody lost", err)
			}
			if mode != "streams" && !errors.Is(err, outputCause) {
				t.Fatal("output cause lost", err)
			}
			var envelope struct {
				Data report `json:"data"`
			}
			if json.Unmarshal(execution.ReportCopy(), &envelope) != nil || envelope.Data.State != "partial" ||
				len(envelope.Data.Directories) != 4 || len(envelope.Data.Files) != 1 ||
				envelope.Data.Files[0].BytesWritten != 7 || envelope.Data.Files[0].Complete {
				t.Fatal("partial report lost", string(execution.ReportCopy()))
			}
			if mode == "streams" && !bytes.Equal(output.Bytes(), execution.ReportCopy()) {
				t.Fatal("delivered/custodied report differs")
			}
			if bytes.Contains(diagnostic.Bytes(), []byte("private-")) || bytes.Contains(output.Bytes(), []byte("private-")) {
				t.Fatal("native cause leaked")
			}
			file, readErr := os.ReadFile(path + "/demo/go.mod")
			if readErr != nil || len(file) != 7 {
				t.Fatal("output failure undid filesystem effects", readErr)
			}
		})
	}
}

func TestCompleteEffectsSurviveCanceledReturn(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var output, diagnostic bytes.Buffer
	parent, _ := parentRoot(t)
	err := command.Run(ctx, []string{"--output=json", "probe"}, command.Options{Input: strings.NewReader(""), Output: &output, ErrorOutput: &diagnostic}, catalogs(t),
		func(inv *command.Invocation) *cobra.Command {
			root := inv.Group("fathomry", "fathomry.command_line.root")
			leaf := &cobra.Command{Use: "probe", Short: "fathomry.project_creation.new"}
			inv.Bind(leaf, func(ctx context.Context, _ []string) error {
				count := 0
				result, err := createInParent(ctx, parent, prepared(t), func(root *os.Root, path string) (io.WriteCloser, error) {
					file, err := openFile(root, path)
					if err != nil {
						return nil, err
					}
					count++
					if count == 6 {
						return &faultyFile{WriteCloser: file, afterClose: cancel}, nil
					}
					return file, nil
				})
				return stage(inv, result, err)
			})
			root.AddCommand(leaf)
			return root
		})
	var envelope struct {
		Data report `json:"data"`
	}
	if !errors.Is(err, context.Canceled) || command.ExitCode(err) != 130 || json.Unmarshal(output.Bytes(), &envelope) != nil || envelope.Data.State != "complete" {
		t.Fatal("complete effect/cancellation separation lost", err, output.String())
	}
}

func TestDeadlineCancellationExitPolicy(t *testing.T) {
	parent, _ := parentRoot(t)
	selected := prepared(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	count := 0
	result, creationError := createInParent(ctx, parent, selected, func(root *os.Root, path string) (io.WriteCloser, error) {
		file, err := openFile(root, path)
		if err != nil {
			return nil, err
		}
		count++
		if count == 6 {
			return &faultyFile{WriteCloser: file, afterClose: func() { <-ctx.Done() }}, nil
		}
		return file, nil
	})
	if result.State != "complete" || !errors.Is(creationError, context.DeadlineExceeded) {
		t.Fatal("late deadline setup", result, creationError)
	}
	var earlyOutput, earlyDiagnostic bytes.Buffer
	early := command.Run(ctx, []string{"--help"}, command.Options{Input: strings.NewReader(""), Output: &earlyOutput, ErrorOutput: &earlyDiagnostic}, catalogs(t),
		func(inv *command.Invocation) *cobra.Command {
			return inv.Group("fathomry", "fathomry.command_line.root")
		})
	if command.ExitCode(early) != 130 || !errors.Is(early, context.DeadlineExceeded) {
		t.Fatal("early deadline policy", early)
	}
	for _, failOutput := range []bool{false, true} {
		var output, diagnostic bytes.Buffer
		var sink io.Writer = &output
		outputError := errors.New("deadline-output-canary")
		if failOutput {
			sink = rejectedWriter{outputError}
		}
		late := command.Run(context.Background(), []string{"--output=json", "probe"}, command.Options{Input: strings.NewReader(""), Output: sink, ErrorOutput: &diagnostic}, catalogs(t),
			func(inv *command.Invocation) *cobra.Command {
				root := inv.Group("fathomry", "fathomry.command_line.root")
				leaf := &cobra.Command{Use: "probe", Short: "fathomry.project_creation.new"}
				inv.Bind(leaf, func(context.Context, []string) error { return stage(inv, result, creationError) })
				root.AddCommand(leaf)
				return root
			})
		want := 130
		if failOutput {
			want = 1
		}
		var retained *command.ExecutionError
		if command.ExitCode(late) != want || !errors.Is(late, context.DeadlineExceeded) || !errors.As(late, &retained) {
			t.Fatal("late deadline/custody policy", late)
		}
		if failOutput && !errors.Is(late, outputError) {
			t.Fatal("output failure precedence lost", late)
		}
		var envelope struct{ Data report }
		if json.Unmarshal(retained.ReportCopy(), &envelope) != nil || envelope.Data.State != "complete" || len(envelope.Data.Files) != 6 {
			t.Fatal("late deadline erased complete effects")
		}
	}
}
