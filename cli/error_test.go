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

package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/failure/v1"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

func ownedOccurrence(t *testing.T, err error, condition failure.Condition) *failure.Error {
	t.Helper()
	current, ok := failure.Inspect(err)
	if !ok || current.Diagnostic().Condition != condition || !errors.Is(err, condition) {
		t.Fatalf("missing %s occurrence: %T", condition, err)
	}
	return current
}

func TestPublicHostConditionsAndStatus(t *testing.T) {
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	deadline, cancelDeadline := context.WithDeadline(context.Background(), time.Unix(0, 0))
	defer cancelDeadline()
	for _, test := range []struct {
		name      string
		ctx       context.Context
		args      []string
		status    int
		condition failure.Condition
		cause     error
	}{
		{"success", context.Background(), nil, 0, "", nil},
		{"usage", context.Background(), []string{"missing"}, 2, ErrUsage, nil},
		{"language", context.Background(), []string{"--lang=PRIVATE-CANARY"}, 2, ErrLanguage, errLanguage},
		{"canceled", canceled, nil, 130, "", context.Canceled},
		{"deadline", deadline, nil, 1, "", context.DeadlineExceeded},
	} {
		t.Run(test.name, func(t *testing.T) {
			var previous *failure.Error
			for _, locale := range []string{"en", "zh-CN"} {
				var out, diagnostic bytes.Buffer
				args := append([]string{"--lang=" + locale}, test.args...)
				status, err := Run(test.ctx, args, Streams{strings.NewReader(""), &out, &diagnostic})
				if status != test.status || (err == nil) != (status == 0) {
					t.Fatal(status, err)
				}
				if test.condition != "" {
					current := ownedOccurrence(t, err, test.condition)
					if previous == current {
						t.Fatal("invocations reused occurrence")
					}
					previous = current
					if err.Error() != string(test.condition) {
						t.Fatal("unsafe owned error")
					}
				}
				if test.cause != nil && !errors.Is(err, test.cause) {
					t.Fatal("cause lost")
				}
				if strings.Contains(out.String()+diagnostic.String(), "PRIVATE-CANARY") {
					t.Fatal("unsafe diagnostic")
				}
				if test.name == "language" {
					var native *pflag.InvalidValueError
					if !errors.As(err, &native) || !strings.Contains(native.Error(), "PRIVATE-CANARY") {
						t.Fatal("parser evidence lost")
					}
				}
			}
		})
	}
}

func TestPublicProjectConditions(t *testing.T) {
	root, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	for _, locale := range []string{"en", "zh-CN"} {
		for _, kind := range []string{"source", "exists", "destination", "arguments", "identity"} {
			target := filepath.Join(t.TempDir(), "app")
			source, module := root, "example.org/app"
			condition, cause := ErrProjectSource, error(os.ErrNotExist)
			switch kind {
			case "source":
				source = filepath.Join(t.TempDir(), "missing")
			case "exists":
				condition, cause = ErrProjectExists, os.ErrExist
				if err := os.Mkdir(target, 0700); err != nil {
					t.Fatal(err)
				}
			case "destination":
				condition = ErrProjectDestination
				target = filepath.Join(t.TempDir(), "missing", "app")
			case "arguments":
				condition, cause = ErrProjectArguments, nil
				module = "invalid module"
			case "identity":
				condition, cause = ErrProjectIdentity, nil
				module = "github.com/frost-leo/fathomry"
			}
			args := []string{"new", target, "--module=" + module, "--fathomry-source=" + source, "--lang=" + locale}
			status, err := Run(context.Background(), args, Streams{strings.NewReader(""), io.Discard, io.Discard})
			ownedOccurrence(t, err, condition)
			expected := 1
			if kind == "arguments" || kind == "identity" {
				expected = 2
			}
			if status != expected || cause != nil && !errors.Is(err, cause) {
				t.Fatal(kind, status, err)
			}
			if kind != "exists" {
				if _, err := os.Stat(target); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("unexpected effect", kind, err)
				}
			}
		}
	}
}

func TestPublicInvalidInputsHaveConditionWithoutWrites(t *testing.T) {
	var nilWriter *bytes.Buffer
	var nilContext *embeddedContext
	for _, test := range []struct {
		ctx     context.Context
		streams Streams
	}{
		{nil, Streams{strings.NewReader(""), io.Discard, io.Discard}},
		{nilContext, Streams{strings.NewReader(""), io.Discard, io.Discard}},
		{context.Background(), Streams{nil, io.Discard, io.Discard}},
		{context.Background(), Streams{strings.NewReader(""), nilWriter, io.Discard}},
	} {
		writes := 0
		test.streams.Stderr = writerFunc(func([]byte) (int, error) { writes++; return 0, nil })
		status, err := Run(test.ctx, nil, test.streams)
		ownedOccurrence(t, err, ErrInputs)
		if status != 1 || writes != 0 {
			t.Fatal(status, err, writes)
		}
	}
}

type embeddedContext struct{ context.Context }

func TestSemanticSingletonAndUsageClassification(t *testing.T) {
	native := &os.PathError{Op: "read", Path: "PRIVATE-CANARY", Err: os.ErrPermission}
	core, _ := failure.New("example.command.refused", native, context.Canceled)
	for _, usage := range []bool{false, true} {
		status, err, _, diagnostic := capture(context.Background(), []string{"leaf"}, func(words *text) *cobra.Command {
			root := commands(words)
			leaf := &cobra.Command{Use: "leaf", Args: cobra.NoArgs, RunE: func(*cobra.Command, []string) error { return core }}
			if usage {
				leaf.Args = func(*cobra.Command, []string) error { return core }
				leaf.RunE = func(*cobra.Command, []string) error { t.Fatal("invalid operation ran"); return nil }
			}
			root.AddCommand(leaf)
			return root
		})
		expected := 1
		if usage {
			expected = 2
		}
		var pathError *os.PathError
		if status != expected || err != core || !errors.As(err, &pathError) || pathError != native || strings.Contains(diagnostic, "PRIVATE-CANARY") {
			t.Fatal(status, err)
		}
	}
	canceledCore, _ := failure.New("example.command.failed", context.Canceled)
	if exitStatus(canceledCore) != 1 || exitStatus(fmt.Errorf("context: %w", context.Canceled)) != 130 {
		t.Fatal("semantic failure became pure cancellation")
	}
}

func TestCompletionSlotsPreserveIdentityWithoutForeignHooks(t *testing.T) {
	native := errors.New("same native failure")
	output := hostFailure(ErrOutput, native)
	otherOutput := hostFailure(ErrOutput, native)
	if got := (completion{returned: output, output: output}).err(); got != output {
		t.Fatal("returned latch duplicated")
	}
	independent := (completion{output: output, diagnostics: otherOutput}).err()
	if _, ok := failure.Inspect(independent); ok {
		t.Fatal("aggregate acquired returned")
	}
	causes := independent.(interface{ Unwrap() []error }).Unwrap()
	if len(causes) != 2 || causes[0] != output || causes[1] != otherOutput {
		t.Fatal("same-code independent writes collapsed")
	}
	opaque := errors.Join(native, output)
	combined := (completion{returned: opaque, output: output}).err().(interface{ Unwrap() []error }).Unwrap()
	if len(combined) != 2 || combined[0] != opaque || combined[1] != output {
		t.Fatal("opaque graph flattened")
	}
	rich := &hostileOccurrence{core: output}
	if got := (completion{returned: rich}).err(); got != rich {
		t.Fatal("extension lost")
	}
	for _, foreign := range []error{rich, sliceError{1}, interfaceError{payload: []byte{2}}} {
		value := (completion{returned: foreign, output: output}).err()
		children := value.(interface{ Unwrap() []error }).Unwrap()
		if len(children) != 2 || children[1] != output {
			t.Fatal("foreign slot discarded")
		}
	}
	var nilCore *failure.Error
	zero := &failure.Error{}
	if (completion{}).err() != nil || (completion{returned: nilCore}).err() == nil || (completion{returned: zero}).err() != zero {
		t.Fatal("nil/zero normalization")
	}
	for _, value := range []completion{
		{returned: hostFailure(ErrUsage), usage: true, output: hostFailure(ErrOutput, context.Canceled)},
		{returned: hostFailure(ErrUsage), usage: true, cancellation: context.Canceled},
	} {
		if value.status() != 1 {
			t.Fatal("independent failure hidden")
		}
	}
}

type hostileOccurrence struct{ core *failure.Error }

func TestOutputOccurrenceFactsAndMethods(t *testing.T) {
	native := &os.PathError{Op: "write", Path: "PRIVATE-CANARY", Err: os.ErrPermission}
	first := &outputFailure{core: hostFailure(ErrOutput, native), stream: stdoutStream}
	second := &outputFailure{core: hostFailure(ErrOutput, native), stream: stderrStream}
	for _, selected := range []*outputFailure{first, second} {
		core, ok := failure.Inspect(selected)
		var cause *os.PathError
		if !ok || core != selected.core || !errors.As(selected, &cause) || cause != native || !errors.Is(selected, ErrOutput) {
			t.Fatal("selected output occurrence changed")
		}
		for _, value := range []any{selected, *selected} {
			for _, format := range []string{"%v", "%+v", "%#v", "%s", "%q", "%1000v"} {
				formatted := fmt.Sprintf(format, value)
				if strings.Contains(formatted, "PRIVATE-CANARY") || len(formatted) > 128 {
					t.Fatal("unsafe output projection")
				}
			}
			if _, err := json.Marshal(value); !errors.Is(err, failure.ErrSerialization) {
				t.Fatal("output serialized")
			}
		}
		var log bytes.Buffer
		slog.New(slog.NewJSONHandler(&log, nil)).Error("output", "error", selected)
		if strings.Contains(log.String(), "PRIVATE-CANARY") {
			t.Fatal("native cause logged")
		}
	}
	for _, roots := range [][]error{{first, second}, {second, first}} {
		joined := combine(roots...)
		if diagnosticKey(joined, 1) != "failed" {
			t.Fatal("aggregate primary inferred")
		}
		selected := roots[0].(*outputFailure)
		if selected.stream == unknownStream || diagnosticKey(selected, 1) != "output_failed" {
			t.Fatal("same-occurrence stream/identity lost")
		}
	}
	if (completion{returned: first, output: first}).err() != first {
		t.Fatal("output latch duplicated")
	}
	for _, invalid := range []*outputFailure{nil, {}, {core: &failure.Error{}}} {
		if _, ok := failure.Inspect(invalid); ok {
			t.Fatal("invalid occurrence")
		}
		_ = invalid.Error()
		_ = invalid.LogValue()
		_ = invalid.Unwrap()
		if err := invalid.UnmarshalJSON(nil); !errors.Is(err, failure.ErrSerialization) {
			t.Fatal(err)
		}
	}
}

func (*hostileOccurrence) Error() string           { panic("foreign Error") }
func (*hostileOccurrence) Failure() *failure.Error { panic("foreign Failure") }
func (*hostileOccurrence) Is(error) bool           { panic("foreign Is") }
func (*hostileOccurrence) Unwrap() error           { panic("foreign Unwrap") }

type sliceError []byte

func (sliceError) Error() string { panic("slice formatting") }

type interfaceError struct{ payload any }

func (interfaceError) Error() string { panic("interface formatting") }

func TestIndependentStreamOccurrencesWithSameNativeCause(t *testing.T) {
	native := errors.New("writer cause")
	stdout, stderr := &failedWriter{err: native}, &failedWriter{err: native}
	status, err := Run(context.Background(), nil, Streams{strings.NewReader(""), stdout, stderr})
	if status != 1 || stdout.calls != 1 || stderr.calls != 1 || !errors.Is(err, native) || !errors.Is(err, ErrOutput) {
		t.Fatal(status, stdout.calls, stderr.calls)
	}
	roots := err.(interface{ Unwrap() []error }).Unwrap()
	if len(roots) != 2 || roots[0] == roots[1] {
		t.Fatal("distinct stream observations collapsed")
	}
	for _, root := range roots {
		ownedOccurrence(t, root, ErrOutput)
	}
}

func TestIgnoredPresentationFailureRetainsSoleOccurrence(t *testing.T) {
	var observed error
	status, err, _, _ := capture(context.Background(), []string{"leaf"}, func(words *text) *cobra.Command {
		root := commands(words)
		root.AddCommand(&cobra.Command{Use: "leaf", Args: cobra.NoArgs, RunE: func(command *cobra.Command, _ []string) error {
			command.Annotations = map[string]string{"fathomry.short.id": "missing"}
			_ = command.Help()
			observed = words.failure()
			return nil
		}})
		return root
	})
	if status != 1 || observed == nil || err != observed {
		t.Fatal("retained presentation singleton lost")
	}
}

func TestOwnedPublicProjectionAndSerialization(t *testing.T) {
	_, err := Run(context.Background(), []string{"--lang=PRIVATE-CANARY"}, Streams{strings.NewReader(""), io.Discard, io.Discard})
	ownedOccurrence(t, err, ErrLanguage)
	for _, format := range []string{"%v", "%+v", "%#v", "%s", "%q"} {
		if value := fmt.Sprintf(format, err); strings.Contains(value, "PRIVATE-CANARY") || len(value) > 128 {
			t.Fatal("unsafe projection")
		}
	}
	var output bytes.Buffer
	slog.New(slog.NewJSONHandler(&output, nil)).Error("failed", "error", err)
	if strings.Contains(output.String(), "PRIVATE-CANARY") || !strings.Contains(output.String(), string(ErrLanguage)) {
		t.Fatal("unsafe log")
	}
	if data, encodeErr := json.Marshal(err); len(data) != 0 || !errors.Is(encodeErr, failure.ErrSerialization) {
		t.Fatal("runtime error encoded")
	}
}

func TestPublicFailureAfterEffectAndFailedDiagnostic(t *testing.T) {
	root, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(t.TempDir(), "app")
	outputCause, diagnosticCause := errors.New("output-CANARY"), errors.New("diagnostic-CANARY")
	writes, diagnostics := 0, 0
	status, err := Run(context.Background(), []string{"new", target, "--module=example.org/app", "--fathomry-source=" + root}, Streams{
		Stdin:  strings.NewReader(""),
		Stdout: writerFunc(func([]byte) (int, error) { writes++; return 0, outputCause }),
		Stderr: writerFunc(func([]byte) (int, error) { diagnostics++; return 0, diagnosticCause }),
	})
	if status != 1 || !errors.Is(err, ErrOutput) || errors.Is(err, ErrProjectCreation) || !errors.Is(err, outputCause) || !errors.Is(err, diagnosticCause) || writes != 1 || diagnostics != 1 {
		t.Fatal(status, err, writes, diagnostics)
	}
	for _, name := range []string{"go.mod", "main.go", "README.md", ".gitignore"} {
		if data, err := os.ReadFile(filepath.Join(target, name)); err != nil || len(data) == 0 {
			t.Fatal("effect lost", name, err)
		}
	}
}

func ExampleRun() {
	status, err := Run(context.Background(), []string{"missing"}, Streams{Stdin: strings.NewReader(""), Stdout: io.Discard, Stderr: io.Discard})
	current, ok := failure.Inspect(err)
	fmt.Println(status, ok, errors.Is(err, ErrUsage))
	fmt.Println(current.Diagnostic().Condition)
	// Output:
	// 2 true true
	// fathomry.cli.invalid_usage
}

func FuzzPublicHostConditions(f *testing.F) {
	for _, locale := range []string{"en", "zh-CN", "ZH-cn", "", "invalid", "PRIVATE-CANARY\x1b[31m"} {
		f.Add(locale)
	}
	f.Fuzz(func(t *testing.T, locale string) {
		if len(locale) > 2048 {
			t.Skip()
		}
		status, err := Run(context.Background(), []string{"--lang=" + locale}, Streams{strings.NewReader(""), io.Discard, io.Discard})
		valid := strings.EqualFold(locale, "en") || strings.EqualFold(locale, "zh-CN")
		if valid {
			if status != 0 || err != nil {
				t.Fatal("valid help acquired failure", status, err)
			}
		} else {
			ownedOccurrence(t, err, ErrLanguage)
			if status != 2 || err.Error() != string(ErrLanguage) {
				t.Fatal("unstable language rejection", status)
			}
		}
	})
}

func TestConditionValuesAreSharedStableIdentities(t *testing.T) {
	conditions := []failure.Condition{ErrInputs, ErrDefinition, ErrUsage, ErrLanguage, ErrOutput,
		ErrProjectArguments, ErrProjectIdentity, ErrProjectSource, ErrProjectDestination,
		ErrProjectExists, ErrProjectOverlap, ErrProjectPreparation, ErrProjectCreation, ErrProjectPresentation}
	seen := map[failure.Condition]bool{}
	for _, condition := range conditions {
		if !condition.Valid() || seen[condition] {
			t.Fatal("invalid or duplicate public identity", condition)
		}
		seen[condition] = true
	}
}
