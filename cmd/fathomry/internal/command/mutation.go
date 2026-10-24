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

package command

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"

	"github.com/frost-leo/fathomry/failure/v1"
	"github.com/frost-leo/fathomry/resource/v1"
)

// MutationSummary contains only bounded machine facts, never identifiers,
// credentials, arguments or native text. It is a diagnostic fallback, not output
// delivery acknowledgement or a substitute for the complete retained report.
type MutationSummary struct {
	Effect string `json:"effect"`
	State  string `json:"state"`
}

// ExecutionError preserves an online invocation's original failure core, bounded
// JSON mutation report and owned cleanup continuation. It is not a durable DTO.
// Copies share the scope; ReportCopy is detached and original failure stays fixed.
type ExecutionError struct {
	core   *failure.Error
	cause  error
	report []byte
	scope  *resource.Scope
}

func (value *ExecutionError) Error() string {
	if value == nil {
		return "command execution"
	}
	return value.core.Error()
}
func (value *ExecutionError) Failure() *failure.Error {
	if value == nil {
		return nil
	}
	return value.core
}
func (value *ExecutionError) Unwrap() error {
	if value == nil {
		return nil
	}
	return value.cause
}
func (value *ExecutionError) ReportCopy() []byte {
	if value == nil {
		return nil
	}
	return bytes.Clone(value.report)
}
func (value *ExecutionError) Close(ctx context.Context) error {
	if value == nil || ctx == nil {
		return Fail(ErrOptions)
	}
	if value.scope == nil {
		return nil
	}
	return value.scope.Close(ctx)
}
func (value *ExecutionError) CleanupComplete() bool {
	if value == nil {
		return false
	}
	if value.scope == nil {
		return true
	}
	return value.scope.ShutdownComplete()
}
func (value ExecutionError) Format(state fmt.State, _ rune) {
	_, _ = state.Write([]byte("command.ExecutionError"))
}
func (*ExecutionError) LogValue() slog.Value        { return slog.StringValue("command.ExecutionError") }
func (ExecutionError) MarshalJSON() ([]byte, error) { return nil, Fail(ErrOptions) }
func (*ExecutionError) UnmarshalJSON([]byte) error  { return Fail(ErrOptions) }

// Managed retains a resource-owning online command's cleanup on returned failure.
// Offline invocations keep their existing direct-error and success-output rules.
func (invocation *Invocation) Managed() { invocation.managed = true }

// MutationResult stages known mutation facts independently of a later handler or
// cleanup error. JSON custody is retained even if selected text rendering fails.
// It does not certify a successful write to either output stream.
func (invocation *Invocation) MutationResult(operation string, data any, summary MutationSummary, text func(io.Writer) error) error {
	if invocation.emitted || !machineToken(summary.Effect) || !machineToken(summary.State) {
		return Fail(ErrOptions)
	}
	invocation.emitted = true
	invocation.managed = true
	buffer := &boundedBuffer{}
	if err := encode(buffer, envelope{Schema: Schema, Command: operation, Data: data}); err != nil {
		return err
	}
	invocation.report = bytes.Clone(buffer.Bytes())
	invocation.mutation = &summary
	if invocation.format == "json" {
		invocation.pending = bytes.Clone(invocation.report)
		return nil
	}
	buffer = &boundedBuffer{}
	if err := text(buffer); err != nil {
		return err
	}
	invocation.pending = bytes.Clone(buffer.Bytes())
	return nil
}
func machineToken(value string) bool {
	if len(value) == 0 || len(value) > 64 {
		return false
	}
	for _, char := range value {
		if !(char >= 'a' && char <= 'z' || char == '_') {
			return false
		}
	}
	return true
}
func (invocation *Invocation) preserve(err error) error {
	if err == nil || !invocation.managed {
		return err
	}
	core, known := failure.Inspect(err)
	if !known {
		err = Fail(ErrExecution, err)
		core, _ = failure.Inspect(err)
	}
	return &ExecutionError{core: core, cause: err, report: bytes.Clone(invocation.report), scope: invocation.scope}
}
