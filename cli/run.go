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
	"context"
	"io"
	"reflect"

	"github.com/spf13/cobra"
)

// Streams contains borrowed invocation I/O. All fields must be non-nil, including
// their dynamic values. Use an empty reader or io.Discard explicitly if appropriate.
// Run never closes or flushes these streams. Callers own their concurrency,
// buffering and interruption.
type Streams struct {
	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer
}

// Run executes first-party commands with args excluding the executable name.
// Nil/empty args request root help, never os.Args. ctx and all streams are required.
// args are copied; streams are borrowed for the duration of the call.
//
// Status is 0 on success, 2 on invalid user input, 130 on otherwise pure caller
// cancellation, and 1 on configuration, execution, deadline, output or cleanup
// failure. Independent failures outrank cancellation. Host and project conditions
// identify their own failures; a sole semantic occurrence is retained unchanged.
// Independent failures are joined without inferring a primary. Raw/foreign joins
// and native causes may contain sensitive data: do not print them as diagnostics.
// A safe diagnostic is attempted once on failure; invalid invocation inputs
// return without writing. Success means the supplied writers
// accepted the bytes, not that caller-owned buffers were flushed or made durable.
// No automatic retry, rollback or remote-termination inference is performed.
func Run(ctx context.Context, args []string, streams Streams) (int, error) {
	return run(ctx, args, streams, commands)
}

func run(ctx context.Context, args []string, streams Streams, construct func(*text) *cobra.Command) (int, error) {
	if missing(ctx) || missing(streams.Stdin) || missing(streams.Stdout) || missing(streams.Stderr) {
		return 1, hostFailure(ErrInputs)
	}
	words := newText()
	output, diagnostics := &checkedWriter{writer: streams.Stdout, stream: stdoutStream}, &checkedWriter{writer: streams.Stderr, stream: stderrStream}
	var callerCancellation error
	observeCancellation := func() error {
		if callerCancellation == nil {
			callerCancellation = cancellation(ctx)
		}
		return callerCancellation
	}
	finish := func(err error, usage bool) (int, error) {
		var diagnosticRender error
		snapshot := func() completion {
			return completion{
				returned: err, usage: usage, presentation: words.failure(),
				output: output.failure(), diagnostics: diagnostics.failure(),
				diagnosticRender: diagnosticRender, cancellation: observeCancellation(),
			}
		}
		result := snapshot()
		status := result.status()
		if status != 0 {
			key := diagnosticKey(result.err(), status)
			message, renderErr := words.render("fathomry.cli:"+key, words.language)
			diagnosticRender = renderErr
			if renderErr != nil {
				message = "fathomry.cli.presentation_failed"
			}
			if diagnostics.failure() == nil {
				_, _ = io.WriteString(diagnostics, message+"\n")
			}
			result = snapshot()
		}
		return result.status(), result.err()
	}
	if observeCancellation() != nil {
		return finish(nil, false)
	}
	if err := words.failure(); err != nil {
		return finish(err, false)
	}

	root := construct(words)
	if err := prepare(root, words); err != nil {
		return finish(err, false)
	}
	root.SetIn(streams.Stdin)
	root.SetOut(output)
	root.SetErr(diagnostics)
	root.SetHelpFunc(func(command *cobra.Command, _ []string) {
		_ = renderHelp(output, command, words)
	})
	root.SetUsageFunc(func(command *cobra.Command) error {
		return renderHelp(output, command, words)
	})
	argv := append([]string{}, args...)
	selected, err := parseCommand(ctx, root, argv)
	if err != nil {
		return finish(usageFailure(err), true)
	}
	operands := selected.Flags().Args()
	help, _ := selected.Flags().GetBool("help")
	if selected.Name() == "help" {
		target, rest, err := root.Find(operands)
		if err != nil {
			return finish(usageFailure(err), true)
		}
		if len(rest) != 0 || target.Hidden {
			return finish(hostFailure(ErrUsage), true)
		}
		return finish(renderHelp(output, target, words), false)
	}
	if selected.RunE == nil {
		if len(operands) != 0 {
			return finish(hostFailure(ErrUsage), true)
		}
		return finish(renderHelp(output, selected, words), false)
	}
	if help {
		return finish(renderHelp(output, selected, words), false)
	}
	if err := selected.ValidateArgs(operands); err != nil {
		return finish(usageFailure(err), true)
	}
	if err := selected.ValidateRequiredFlags(); err != nil {
		return finish(usageFailure(err), true)
	}
	if err := selected.ValidateFlagGroups(); err != nil {
		return finish(usageFailure(err), true)
	}
	if observeCancellation() != nil {
		return finish(nil, false)
	}
	return finish(selected.RunE(selected, operands), false)
}

func parseCommand(ctx context.Context, command *cobra.Command, args []string) (*cobra.Command, error) {
	for {
		command.SetContext(ctx)
		flags := command.Flags()
		flags.SetInterspersed(!command.HasSubCommands())
		if err := command.ParseFlags(args); err != nil {
			return nil, err
		}
		operands := flags.Args()
		if !command.HasSubCommands() || len(operands) == 0 || flags.ArgsLenAtDash() >= 0 {
			return command, nil
		}
		// Find sees only a parsed command token, never an unconsumed flag value.
		next, rest, err := command.Find(operands[:1])
		if err != nil {
			return nil, err
		}
		if next == command || len(rest) != 0 {
			flags.SetInterspersed(true)
			if err := command.ParseFlags(operands); err != nil {
				return nil, err
			}
			return nil, errArguments
		}
		command, args = next, operands[1:]
	}
}

func missing(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	}
	return false
}

func cancellation(ctx context.Context) error {
	if ctx.Err() == nil {
		return nil
	}
	return combine(ctx.Err(), context.Cause(ctx))
}
