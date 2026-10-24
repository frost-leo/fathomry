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
	"encoding/json"
	"fmt"
	"io"

	"github.com/frost-leo/fathomry/failure/v1"
	"github.com/frost-leo/fathomry/i18n/v1"
)

type boundedBuffer struct{ buffer bytes.Buffer }

func (buffer *boundedBuffer) Bytes() []byte { return buffer.buffer.Bytes() }
func (buffer *boundedBuffer) Write(data []byte) (int, error) {
	if len(data) > MaxOutputBytes-buffer.buffer.Len() {
		return 0, Fail(ErrLimit)
	}
	return buffer.buffer.Write(data)
}

type envelope struct {
	Schema  string `json:"schema"`
	Command string `json:"command"`
	Data    any    `json:"data"`
}
type diagnostic struct {
	Schema          string             `json:"schema"`
	Code            failure.Code       `json:"code"`
	Identifier      failure.Identifier `json:"identifier"`
	Message         string             `json:"message"`
	RequestedLocale string             `json:"requested_locale,omitempty"`
	Locale          string             `json:"locale"`
	Fallback        string             `json:"fallback,omitempty"`
	Mutation        *MutationSummary   `json:"mutation,omitempty"`
}

func encode(writer io.Writer, value any) error {
	encoder := json.NewEncoder(writer)
	encoder.SetEscapeHTML(true)
	return encoder.Encode(value)
}
func writeAll(writer io.Writer, value []byte) error {
	count, err := writer.Write(value)
	if err != nil {
		return err
	}
	if count != len(value) {
		return io.ErrShortWrite
	}
	return nil
}

func (invocation *Invocation) finish(err, cleanup error) (result error) {
	defer func() { result = invocation.preserve(result) }()
	if cleanup != nil {
		err = Fail(ErrCleanup, err, cleanup)
	}
	if invocation.presentationError != nil {
		err = Fail(ErrExecution, err, invocation.presentationError)
	}
	if len(invocation.pending) != 0 && (err == nil || invocation.mutation != nil && invocation.presentationError == nil) {
		if problem := writeAll(invocation.options.Output, invocation.pending); problem != nil {
			err = Fail(ErrOutput, err, problem)
		}
	}
	if err == nil && invocation.checkFailed {
		err = Fail(ErrCheck)
	}
	if err == nil {
		return nil
	}

	// Failed parsing may leave an invalid locale or format; diagnostics keep a
	// safe English presenter and never echo rejected preferences or native text.
	if len(invocation.language) <= 128 {
		if presenter, problem := invocation.presenter.WithLocale(invocation.language); problem == nil {
			invocation.presenter = presenter
		}
	}
	if _, found := failure.Inspect(err); !found {
		err = Fail(ErrExecution, err)
	}
	shown := invocation.presenter.Present(err)
	core, found := failure.Inspect(shown)
	if !found {
		return Fail(ErrExecution, err)
	}
	metadata := core.Diagnostic()
	value := diagnostic{Schema: Schema, Code: metadata.Definition.Code, Identifier: metadata.Definition.Identifier,
		Message: metadata.Definition.Message, Locale: "en", Mutation: invocation.mutation}
	if presented, ok := shown.(*i18n.Presented); ok {
		info := presented.Info()
		value.Message, value.Locale = info.Message.Text, info.Message.Locale
		value.RequestedLocale, value.Fallback = info.Selection.Requested, string(info.Selection.Fallback)
	}
	buffer := &boundedBuffer{}
	var problem error
	if invocation.format == "json" {
		problem = encode(buffer, value)
	} else {
		if value.Mutation != nil {
			_, problem = fmt.Fprintf(buffer, "%s: %s [effect=%s state=%s]\n", value.Code.String(), value.Message, value.Mutation.Effect, value.Mutation.State)
		} else {
			_, problem = fmt.Fprintf(buffer, "%s: %s\n", value.Code.String(), value.Message)
		}
	}
	if problem == nil {
		problem = writeAll(invocation.options.ErrorOutput, buffer.Bytes())
	}
	if problem != nil {
		return Fail(ErrOutput, err, problem)
	}
	return err
}

// Unavailable reports setup failure even when catalogs cannot be prepared.
// It emits only a known static definition; native causes remain on the return.
func Unavailable(writer io.Writer, cause error) error {
	err := Fail(ErrOptions, cause)
	core, _ := failure.Inspect(err)
	metadata := core.Diagnostic()
	if writer == nil {
		return err
	}
	if problem := writeAll(writer, []byte(metadata.Definition.Code.String()+": "+metadata.Definition.Message+"\n")); problem != nil {
		return Fail(ErrOutput, err, problem)
	}
	return err
}
