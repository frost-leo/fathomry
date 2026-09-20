/*
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

package dotenv

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/frost-leo/fathomry/failure"
	"github.com/frost-leo/fathomry/framework/configuration"
)

// Limits bound one acquired file and its retained assignments.
const (
	MaxBytes   = 64 << 10
	MaxEntries = 128
)

// Values owns immutable literal strings. Its zero value is an empty file.
// Copies share immutable storage; concurrent lookup is supported. Values and
// names are private; intentional Lookup exposes a sensitive value to its caller.
type Values struct {
	private
	entries map[string]string
}

// Lookup reads one exact name from the file without process-environment access.
func (values Values) Lookup(name string) configuration.VariableValue {
	value, present := values.entries[name]
	if !present {
		return configuration.VariableValue{}
	}
	return configuration.VariableValue{Value: value, Present: true, Source: "dotenv"}
}

// LookupWithProcess reads one exact process name first. An explicitly empty
// process value overrides the file. Configuration loading captures each declared
// name once. Keep process values stable when consistency across names is needed.
func (values Values) LookupWithProcess(name string) configuration.VariableValue {
	value := configuration.ProcessVariable(name)
	if value.Present {
		return value
	}
	return values.Lookup(name)
}

// ReadFile acquires and closes one explicitly required file before returning.
// Missing, unreadable, invalid and oversized inputs fail with zero Values.
// Error inspection preserves cancellation and safe filesystem categories only.
func ReadFile(ctx context.Context, path string) (Values, error) {
	if ctx == nil || !filepath.IsAbs(path) || filepath.Clean(path) != path ||
		len(path) > 4096 || !utf8.ValidString(path) || strings.ContainsRune(path, 0) {
		return Values{}, problem(configuration.InvalidInput, nil)
	}
	if err := cancelled(ctx); err != nil {
		return Values{}, err
	}
	info, err := os.Stat(path)
	if err != nil {
		return Values{}, acquisitionError(err)
	}
	if !info.Mode().IsRegular() {
		return Values{}, problem(configuration.InvalidInput, nil)
	}
	if info.Size() > MaxBytes {
		return Values{}, problem(configuration.LimitExceeded, nil)
	}
	file, err := os.Open(path)
	if err != nil {
		return Values{}, acquisitionError(err)
	}
	values, readErr := read(ctx, file)
	closeErr := file.Close()
	if closeErr != nil {
		return Values{}, problem(configuration.Unavailable, readErr)
	}
	if readErr != nil {
		return Values{}, readErr
	}
	if err := cancelled(ctx); err != nil {
		return Values{}, err
	}
	return values, nil
}

func read(ctx context.Context, file *os.File) (Values, error) {
	info, err := file.Stat()
	if err != nil {
		return Values{}, acquisitionError(err)
	}
	if !info.Mode().IsRegular() {
		return Values{}, problem(configuration.InvalidInput, nil)
	}
	if err := cancelled(ctx); err != nil {
		return Values{}, err
	}
	raw, err := io.ReadAll(io.LimitReader(file, MaxBytes+1))
	if err != nil {
		return Values{}, acquisitionError(err)
	}
	if err := cancelled(ctx); err != nil {
		return Values{}, err
	}
	return parse(raw)
}

func parse(raw []byte) (Values, error) {
	if len(raw) > MaxBytes {
		return Values{}, problem(configuration.LimitExceeded, nil)
	}
	if !utf8.Valid(raw) || strings.ContainsRune(string(raw), 0) {
		return Values{}, problem(configuration.Invalid, nil)
	}
	entries := make(map[string]string)
	scanner := bufio.NewScanner(strings.NewReader(string(raw)))
	scanner.Buffer(make([]byte, 4096), MaxBytes+1)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if strings.HasPrefix(line, "export ") || strings.HasPrefix(line, "export\t") {
			line = strings.TrimSpace(line[6:])
		}
		key, value, ok := strings.Cut(line, "=")
		key = strings.TrimSpace(key)
		if !ok || !validName(key) {
			return Values{}, problem(configuration.Invalid, nil)
		}
		if _, exists := entries[key]; exists {
			return Values{}, problem(configuration.Invalid, nil)
		}
		if len(entries) == MaxEntries {
			return Values{}, problem(configuration.LimitExceeded, nil)
		}
		value, ok = literal(strings.TrimSpace(value))
		if !ok || !utf8.ValidString(value) || strings.ContainsRune(value, 0) {
			return Values{}, problem(configuration.Invalid, nil)
		}
		entries[key] = value
	}
	if scanner.Err() != nil {
		return Values{}, problem(configuration.Invalid, nil)
	}
	return Values{entries: entries}, nil
}

func literal(value string) (string, bool) {
	if value == "" {
		return "", true
	}
	if value[0] != '\'' && value[0] != '"' {
		for index := range value {
			if value[index] == '#' && (index == 0 || value[index-1] == ' ' || value[index-1] == '\t') {
				value = strings.TrimSpace(value[:index])
				break
			}
		}
		return value, true
	}
	quote := value[0]
	for index := 1; index < len(value); index++ {
		if quote == '"' && value[index] == '\\' {
			index++
			continue
		}
		if value[index] != quote {
			continue
		}
		tail := strings.TrimSpace(value[index+1:])
		if tail != "" && !strings.HasPrefix(tail, "#") {
			return "", false
		}
		if quote == '\'' {
			return value[1:index], true
		}
		decoded, err := strconv.Unquote(value[:index+1])
		return decoded, err == nil
	}
	return "", false
}

func validName(value string) bool {
	if value == "" || len(value) > 256 {
		return false
	}
	for index, char := range value {
		if !(char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char == '_' ||
			index > 0 && char >= '0' && char <= '9') {
			return false
		}
	}
	return true
}

func cancelled(ctx context.Context) error {
	if ctx.Err() == nil {
		return nil
	}
	return problem(configuration.Cancelled, errors.Join(ctx.Err(), context.Cause(ctx)))
}

func acquisitionError(err error) error {
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return problem(configuration.Unavailable, fs.ErrNotExist)
	case errors.Is(err, fs.ErrPermission):
		return problem(configuration.Unavailable, fs.ErrPermission)
	default:
		return problem(configuration.Unavailable, nil)
	}
}

func problem(code failure.Code, cause error) error {
	return failure.New(code, cause, failure.Attribute{Name: "provider", Value: "dotenv"})
}

type private struct{}

func (private) Format(state fmt.State, _ rune) { _, _ = io.WriteString(state, "dotenv[restricted]") }
func (private) LogValue() slog.Value           { return slog.StringValue("dotenv[restricted]") }
func (private) MarshalJSON() ([]byte, error)   { return nil, problem(configuration.InvalidInput, nil) }
func (*private) UnmarshalJSON([]byte) error    { return problem(configuration.InvalidInput, nil) }
