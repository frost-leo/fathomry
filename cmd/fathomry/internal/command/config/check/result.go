/*
fathomry
Copyright (C) 2026  Frost Leo
SPDX-License-Identifier: GPL-3.0-or-later

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.
*/

package check

import (
	"fmt"
	"log/slog"

	"github.com/frost-leo/fathomry/cmd/fathomry/internal/command"
	"github.com/frost-leo/fathomry/failure/v1"
	configuration "github.com/frost-leo/fathomry/framework/configuration/v1"
	"github.com/frost-leo/fathomry/settings/v1"
)

// Error retains the accepted configuration when a later
// configuration check, presentation, output or cleanup step fails. It preserves
// the original failure occurrence and cause chain. Use errors.As to obtain the
// existing ExecutionError for cleanup inspection/continuation; this value owns
// data, not another resource lifetime.
type Error struct {
	core          *failure.Error
	cause         error
	configuration configuration.Loaded
}

func (value *Error) Error() string {
	if value == nil || value.core == nil {
		return "configuration check"
	}
	return value.core.Error()
}

// Failure returns the original occurrence without assigning another error code.
func (value *Error) Failure() *failure.Error {
	if value == nil {
		return nil
	}
	return value.core
}

func (value *Error) Unwrap() error {
	if value == nil {
		return nil
	}
	return value.cause
}

// Configuration returns the exact accepted data capability, independently of
// output delivery and cleanup progress. InitialView remains frozen even if the
// reader saw later Watch updates. Explicit value access can expose secrets;
// ordinary formatting and serialization never expose this capability.
// A nil or zero error returns a zero Loaded.
func (value *Error) Configuration() configuration.Loaded {
	if value == nil {
		return configuration.Loaded{}
	}
	return value.configuration
}

func (Error) Format(state fmt.State, _ rune) {
	_, _ = state.Write([]byte("check.Error"))
}
func (*Error) LogValue() slog.Value {
	return slog.StringValue("check.Error")
}
func (Error) MarshalJSON() ([]byte, error) {
	return nil, command.Fail(command.ErrOptions)
}
func (*Error) UnmarshalJSON([]byte) error {
	return command.Fail(command.ErrOptions)
}

// Retain attaches accepted configuration to a failed command without owning its
// cleanup. The original ExecutionError remains reachable through errors.As.
func Retain(cause error, accepted settings.Reader) error {
	if cause == nil {
		return nil
	}
	view, err := accepted.Capture()
	if err != nil {
		return cause
	}
	snapshot, err := settings.As[configuration.Loaded](view)
	if err != nil {
		return cause
	}
	loaded, err := snapshot.ValueCopy()
	if err != nil {
		return cause
	}
	core, _ := failure.Inspect(cause)
	return &Error{core: core, cause: cause, configuration: loaded}
}
