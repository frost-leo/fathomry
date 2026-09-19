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

package local

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"path/filepath"

	"github.com/frost-leo/fathomry/failure"
	"github.com/frost-leo/fathomry/framework/configuration"
	viper "github.com/frost-leo/fathomry/internal/configsource/viper/v1"
)

// ProviderID names this public acquisition adapter, not its SDK or data format.
const ProviderID = "local"

// Provider owns only frozen declarations. ReadConfiguration acquires and closes
// each file on its own stack; there is no Close method or retained native handle.
type Provider struct {
	private
	root   string
	format uint32
	files  []File
}

var _ configuration.Provider = (*Provider)(nil)

// ReadConfiguration returns original documents and explicit optional absences,
// or a zero input on any failure. Native paths, parser text and input values are
// withheld. Only safe filesystem categories and caller cancellation causes are
// exposed through errors.Is/As; catching an error does not produce usable data.
func (provider *Provider) ReadConfiguration(ctx context.Context) (configuration.Input, error) {
	fail := func(code failure.Code, name string, cause error) (configuration.Input, error) {
		return configuration.Input{}, problem(code, name, cause)
	}
	if provider == nil || provider.format == 0 || ctx == nil {
		return fail(configuration.InvalidInput, "", nil)
	}
	input := configuration.Input{Provider: ProviderID, SchemaVersion: provider.format}
	for _, file := range provider.files {
		if err := ctx.Err(); err != nil {
			return fail(configuration.Cancelled, file.Name, errors.Join(err, context.Cause(ctx)))
		}
		documents, err := viper.Load(ctx, []viper.LoadInput{{
			File:    filepath.Join(provider.root, filepath.FromSlash(file.Path)),
			Options: viper.OptionsV1{Encoding: file.Encoding},
		}})
		document := configuration.Document{Name: file.Name, Layer: file.Layer}
		if err != nil {
			switch {
			case ctx.Err() != nil:
				return fail(configuration.Cancelled, file.Name, errors.Join(ctx.Err(), context.Cause(ctx)))
			case errors.Is(err, fs.ErrNotExist) && !errors.Is(err, viper.ErrClose):
				if !file.Optional {
					return fail(configuration.Unavailable, file.Name, fs.ErrNotExist)
				}
				document.Absent = true
			case errors.Is(err, fs.ErrPermission):
				return fail(configuration.Unavailable, file.Name, fs.ErrPermission)
			case errors.Is(err, viper.ErrLimit):
				return fail(configuration.LimitExceeded, file.Name, nil)
			case errors.Is(err, viper.ErrDecode), errors.Is(err, viper.ErrInput):
				return fail(configuration.Invalid, file.Name, nil)
			default:
				return fail(configuration.Unavailable, file.Name, nil)
			}
		} else {
			document.Data = documents[0].RawCopy()
		}
		input.Documents = append(input.Documents, document)
	}
	if err := ctx.Err(); err != nil {
		return fail(configuration.Cancelled, "", errors.Join(err, context.Cause(ctx)))
	}
	return input, nil
}

func problem(code failure.Code, source string, cause error) error {
	attributes := []failure.Attribute{{Name: "provider", Value: ProviderID}}
	if source != "" {
		attributes = append(attributes, failure.Attribute{Name: "source", Value: source})
	}
	return failure.New(code, cause, attributes...)
}

type private struct{}

func (private) Format(state fmt.State, verb rune) {
	_, _ = io.WriteString(state, "local.Configuration[restricted]")
}
func (private) LogValue() slog.Value { return slog.StringValue("local.Configuration[restricted]") }
func (private) MarshalJSON() ([]byte, error) {
	return nil, problem(configuration.InvalidInput, "", nil)
}
func (*private) UnmarshalJSON([]byte) error {
	return problem(configuration.InvalidInput, "", nil)
}
