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
	"errors"
	"io"
	"unicode/utf8"

	"github.com/frost-leo/fathomry/failure"
	"github.com/frost-leo/fathomry/i18n"
	"github.com/spf13/cobra"
)

const (
	usageCode  failure.Code = "fathomry.cli.usage"
	outputCode failure.Code = "fathomry.cli.output"
	setupCode  failure.Code = "fathomry.cli.setup"
	cancelCode failure.Code = "fathomry.cli.cancelled"
	renderCode failure.Code = "fathomry.cli.render"
	execCode   failure.Code = "fathomry.cli.execution"
)

type commandError struct {
	status    int
	code      failure.Code
	cause     error
	resources []i18n.Resource
}

func (err commandError) Error() string { return string(err.code) }
func (err commandError) Unwrap() error { return err.cause }

// Usage classifies an explicitly rejected command option or argument.
func Usage() error { return commandError{status: 2, code: usageCode} }

// Execution classifies an operation failure without disclosing its native cause.
func Execution(code failure.Code, cause error, resources ...i18n.Resource) error {
	if !code.Valid() {
		code = renderCode
	}
	return commandError{status: 1, code: code, cause: cause, resources: resources}
}

// Render classifies a local rendering or projection failure.
func Render(cause error) error {
	return commandError{status: 1, code: renderCode, cause: cause}
}

type checkedOutput struct {
	destination io.Writer
	err         error
}

func (output *checkedOutput) Write(data []byte) (int, error) {
	written, err := output.destination.Write(data)
	if err == nil && written != len(data) {
		err = io.ErrShortWrite
	}
	if err != nil && output.err == nil {
		output.err = err
	}
	return written, err
}

// Run builds and executes one fresh command tree. Nil/empty supplied argv means
// zero arguments, never ambient process arguments. The caller retains streams.
// Only the outer executable owns signals and the final process exit.
func Run(ctx context.Context, args []string, in io.Reader, out, errOut io.Writer,
	build func(*Invocation) *cobra.Command,
) int {
	call := &Invocation{ctx: ctx, errOut: errOut, locale: "en"}
	if ctx == nil || in == nil || out == nil || errOut == nil || build == nil {
		return call.report(commandError{status: 1, code: setupCode})
	}
	if err := checkArgs(args); err != nil {
		return call.report(err)
	}
	if err := call.CheckContext(); err != nil {
		return call.report(err)
	}
	checked := &checkedOutput{destination: out}
	call.out = checked
	root := build(call)
	if root == nil {
		return call.report(commandError{status: 1, code: setupCode})
	}
	root.SetIn(in)
	root.SetOut(checked)
	root.SetErr(errOut)
	root.SetArgs(append(make([]string, 0, len(args)), args...))
	classifyActions(root)
	_, err := root.ExecuteContextC(ctx)
	if call.helpErr != nil {
		err = call.helpErr
	}
	if err == nil && checked.err != nil {
		err = commandError{status: 1, code: outputCode, cause: checked.err}
	}
	if err == nil {
		return 0
	}
	var classified commandError
	if !errors.As(err, &classified) {
		classified = commandError{status: 2, code: usageCode, cause: err}
	}
	return call.report(classified)
}

func classifyActions(command *cobra.Command) {
	wrap := func(run func(*cobra.Command, []string) error) func(*cobra.Command, []string) error {
		if run == nil {
			return nil
		}
		return func(command *cobra.Command, args []string) error {
			err := run(command, args)
			if err == nil {
				return nil
			}
			var classified commandError
			if errors.As(err, &classified) {
				return err
			}
			return commandError{status: 1, code: execCode, cause: err}
		}
	}
	command.RunE = wrap(command.RunE)
	command.PreRunE = wrap(command.PreRunE)
	command.PersistentPreRunE = wrap(command.PersistentPreRunE)
	command.PostRunE = wrap(command.PostRunE)
	command.PersistentPostRunE = wrap(command.PersistentPostRunE)
	for _, child := range command.Commands() {
		classifyActions(child)
	}
}

func checkArgs(args []string) error {
	if len(args) > 128 {
		return Usage()
	}
	bytes := 0
	for _, arg := range args {
		bytes += len(arg)
		if bytes > 64<<10 || !utf8.ValidString(arg) {
			return Usage()
		}
		for _, char := range arg {
			if char < 0x20 || char == 0x7f {
				return Usage()
			}
		}
	}
	return nil
}

// CheckContext reports cancellation at an explicit operation/output boundary.
// A callback or Writer that ignores Context cannot be forcibly interrupted.
func (call *Invocation) CheckContext() error {
	if err := call.ctx.Err(); err != nil {
		if errors.Is(err, context.Canceled) {
			return commandError{status: 130, code: cancelCode, cause: err}
		}
		return commandError{status: 1, code: cancelCode, cause: err}
	}
	return nil
}

// WriteFinite emits one prepared bounded document. Streaming commands may use
// Cobra's checked stdout directly and own their own protocol/limits.
func (call *Invocation) WriteFinite(value string, maxBytes int) error {
	if len(value) > maxBytes {
		return commandError{status: 1, code: renderCode}
	}
	if err := call.CheckContext(); err != nil {
		return err
	}
	if err := writeChecked(call.out, value); err != nil {
		return commandError{status: 1, code: outputCode, cause: err}
	}
	return nil
}

func writeChecked(writer io.Writer, value string) error {
	written, err := io.WriteString(writer, value)
	if err != nil {
		return err
	}
	if written != len(value) {
		return io.ErrShortWrite
	}
	return nil
}
