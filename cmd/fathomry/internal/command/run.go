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

// Package command supplies private invocation mechanisms for official commands.
// It owns no user plugin protocol, project loader or Framework runtime.
package command

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/frost-leo/fathomry/failure/v1"
	"github.com/frost-leo/fathomry/i18n/v1"
	"github.com/frost-leo/fathomry/resource/v1"
	"github.com/spf13/cobra"
)

// Invocation belongs to one execution. Command construction/flags/output are
// serial; only Scope access is concurrent-safe. Do not retain it after Run.
type Invocation struct {
	ctx               context.Context
	options           Options
	catalogs          Catalogs
	presenter         i18n.Presenter
	language          string
	format            string
	started           bool
	helperError       error
	presentationError error
	pending           []byte
	checkFailed       bool
	emitted           bool
	scopeMu           sync.Mutex
	scope             *resource.Scope
	closing           bool
}

// Run uses a fresh command tree. The builder registers official commands only;
// it must not perform operations or acquire resources during construction.
func Run(ctx context.Context, args []string, options Options, catalogs Catalogs, build func(*Invocation) *cobra.Command) error {
	if options.CleanupTimeout == 0 {
		options.CleanupTimeout = 5 * time.Second
	}
	if options.Language == "" {
		options.Language = "en"
	}
	if ctx == nil || build == nil || options.Input == nil || options.Output == nil || options.ErrorOutput == nil ||
		options.CleanupTimeout < time.Millisecond || options.CleanupTimeout > time.Minute {
		return Fail(ErrOptions)
	}
	presenter, err := i18n.NewPresenter(catalogs.Messages)
	if err == nil {
		presenter, err = presenter.WithLocale("en")
	}
	if _, problem := catalogs.Errors.Inspect(); err != nil || problem != nil {
		return Fail(ErrOptions, err, problem)
	}
	invocation := &Invocation{ctx: ctx, options: options, catalogs: catalogs, presenter: presenter, language: options.Language, format: "text"}
	if err := admit(args); err != nil {
		return invocation.finish(err, nil)
	}
	if err := ctx.Err(); err != nil {
		return invocation.finish(Fail(ErrCanceled, err), nil)
	}
	attemptedCleanup := false
	defer func() {
		if !attemptedCleanup {
			_ = invocation.close()
		}
	}()
	root := build(invocation)
	if root == nil {
		attemptedCleanup = true
		return invocation.finish(Fail(ErrOptions), invocation.close())
	}
	root.SetArgs(append([]string{}, args...))
	root.SetIn(options.Input)
	root.SetOut(io.Discard)
	root.SetErr(io.Discard)
	root.SilenceErrors, root.SilenceUsage = true, true
	root.CompletionOptions.DisableDefaultCmd = true
	// Cobra still installs its hidden completion RPC despite DisableDefaultCmd.
	root.PersistentPreRunE = func(cmd *cobra.Command, _ []string) error {
		if cmd.Name() == cobra.ShellCompRequestCmd {
			return Fail(ErrUsage)
		}
		return nil
	}
	root.SetFlagErrorFunc(func(_ *cobra.Command, cause error) error { return Fail(ErrUsage, cause) })
	root.SetHelpFunc(func(selected *cobra.Command, _ []string) { invocation.helperError = invocation.Help(selected) })
	root.SetUsageFunc(func(selected *cobra.Command) error { return invocation.Help(selected) })
	root.PersistentFlags().StringVar(&invocation.language, "lang", "en", "fathomry.command_line.flag_language")
	root.PersistentFlags().StringVar(&invocation.format, "output", "text", "fathomry.command_line.flag_output")
	// Keep the documented default independent of untrusted environment content.
	invocation.language = options.Language
	installHelp(root)
	help := &cobra.Command{Use: "help [command]", Short: "fathomry.command_line.help_command"}
	invocation.Bind(help, func(_ context.Context, args []string) error {
		selected, remaining, err := root.Find(args)
		if err != nil || len(remaining) != 0 {
			return Fail(ErrUsage, err)
		}
		return invocation.Help(selected)
	})
	help.Flags().BoolP("help", "h", false, "fathomry.command_line.flag_help")
	root.SetHelpCommand(help)
	err = root.ExecuteContext(ctx)
	if invocation.helperError != nil {
		err = errors.Join(err, invocation.helperError)
	}
	if err != nil {
		if _, known := failure.Inspect(err); !known {
			switch {
			case errors.Is(err, context.Canceled):
				err = Fail(ErrCanceled, err)
			case !invocation.started:
				err = Fail(ErrUsage, err)
			default:
				err = Fail(ErrExecution, err)
			}
		}
	}
	attemptedCleanup = true
	return invocation.finish(err, invocation.close())
}

func admit(args []string) error {
	if len(args) > MaxArguments {
		return Fail(ErrLimit)
	}
	size := 0
	for _, argument := range args {
		if len(argument) > MaxArgumentBytes-size {
			return Fail(ErrLimit)
		}
		size += len(argument)
		if !utf8.ValidString(argument) || strings.ContainsRune(argument, 0) {
			return Fail(ErrUsage)
		}
	}
	return nil
}

func installHelp(cmd *cobra.Command) {
	cmd.Flags().BoolP("help", "h", false, "fathomry.command_line.flag_help")
	for _, child := range cmd.Commands() {
		installHelp(child)
	}
}

// Bind supplies the common admission boundary without replacing Cobra's command
// declarations. Domain code returns errors; it never prints diagnostics or exits.
// The command's static Short resource must render before its handler is entered,
// including JSON paths that do not otherwise consume presentation resources.
func (invocation *Invocation) Bind(cmd *cobra.Command, run func(context.Context, []string) error) {
	cmd.RunE = func(_ *cobra.Command, args []string) error {
		if err := invocation.ctx.Err(); err != nil {
			return Fail(ErrCanceled, err)
		}
		if err := invocation.configure(); err != nil {
			return err
		}
		if _, err := invocation.presenter.Render(cmd.Short, nil, nil); err != nil {
			return Fail(ErrOptions, err)
		}
		invocation.started = true
		return run(invocation.ctx, args)
	}
}

// Group makes an official namespace show help instead of performing work.
func (invocation *Invocation) Group(use, description string) *cobra.Command {
	cmd := &cobra.Command{Use: use, Short: description, Args: cobra.NoArgs}
	invocation.Bind(cmd, func(context.Context, []string) error { return invocation.Help(cmd) })
	return cmd
}

func (invocation *Invocation) configure() error {
	if len(invocation.language) > 128 {
		return Fail(ErrUsage)
	}
	presenter, err := invocation.presenter.WithLocale(invocation.language)
	if err != nil {
		return Fail(ErrUsage, err)
	}
	if invocation.format != "text" && invocation.format != "json" {
		return Fail(ErrUsage)
	}
	invocation.presenter = presenter
	return nil
}

// Text renders only explicitly registered messages. Missing command resources
// fail the invocation instead of silently presenting a resource key as prose.
func (invocation *Invocation) Text(id string) string {
	value, err := invocation.presenter.Render(id, nil, nil)
	if err != nil {
		invocation.presentationError = errors.Join(invocation.presentationError, err)
		return ""
	}
	return value.Text
}

func (invocation *Invocation) Locale() string     { return invocation.language }
func (invocation *Invocation) Catalogs() Catalogs { return invocation.catalogs }

// Scope lazily creates public instance ownership. Resources acquired by a command
// belong here; the executor closes the scope even when its handler fails.
func (invocation *Invocation) Scope() (*resource.Scope, error) {
	invocation.scopeMu.Lock()
	defer invocation.scopeMu.Unlock()
	if invocation.closing || invocation.ctx.Err() != nil {
		return nil, Fail(ErrCanceled, invocation.ctx.Err())
	}
	if invocation.scope == nil {
		scope, err := resource.New(invocation.ctx, resource.Options{Name: "command", CleanupTimeout: invocation.options.CleanupTimeout})
		if err != nil {
			return nil, err
		}
		invocation.scope = scope
	}
	return invocation.scope, nil
}

func (invocation *Invocation) close() error {
	invocation.scopeMu.Lock()
	invocation.closing = true
	scope := invocation.scope
	invocation.scopeMu.Unlock()
	if scope == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(invocation.ctx), invocation.options.CleanupTimeout)
	defer cancel()
	return scope.Close(ctx)
}

// Result stages one bounded finite response. It is emitted only after successful
// execution and cleanup. Streaming needs its own explicit contract, not this API.
func (invocation *Invocation) Result(operation string, data any, text func(io.Writer) error) error {
	if invocation.emitted {
		return Fail(ErrOptions)
	}
	invocation.emitted = true
	buffer := &boundedBuffer{}
	var err error
	if invocation.format == "json" {
		err = encode(buffer, envelope{Schema: Schema, Command: operation, Data: data})
	} else {
		err = text(buffer)
	}
	if err != nil {
		return err
	}
	invocation.pending = bytes.Clone(buffer.Bytes())
	return nil
}

// CheckResult stages a completed check, not a failed execution. After successful
// cleanup and delivery, a failed check returns ErrCheck with its report intact.
// Handler, presentation, cleanup and output failures retain normal precedence.
func (invocation *Invocation) CheckResult(operation string, data any, passed bool, text func(io.Writer) error) error {
	if err := invocation.Result(operation, data, text); err != nil {
		return err
	}
	invocation.checkFailed = !passed
	return nil
}

// ExitCode is process policy, not a truncation of the 32-bit error identity.
func ExitCode(err error) int {
	switch {
	case err == nil:
		return 0
	case errors.Is(err, ErrOutput), errors.Is(err, ErrCleanup), errors.Is(err, ErrOptions), errors.Is(err, ErrExecution):
		return 1
	case errors.Is(err, ErrUsage), errors.Is(err, ErrLimit):
		return 2
	case errors.Is(err, ErrCanceled), errors.Is(err, context.Canceled):
		return 130
	default:
		return 1
	}
}
