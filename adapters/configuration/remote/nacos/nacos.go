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

package nacos

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	"github.com/frost-leo/fathomry/failure"
	"github.com/frost-leo/fathomry/framework/configuration"
	native "github.com/frost-leo/fathomry/internal/configsource/nacos/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ProviderID identifies acquisition, independently of SDK and project versions.
const ProviderID = "nacos"

// Provider owns frozen bootstrap and bounded admission, never a persistent native
// client. Concurrent use is supported; copies share the same admission allowance.
// No Close is required: every call owns and joins its transient resources.
type Provider struct {
	private
	options native.OptionsV1
	sources []Source
	format  uint32
	slots   chan struct{}
}

var _ configuration.Provider = (*Provider)(nil)

// ReadConfiguration acquires fresh original documents in layer order. Each key
// uses a finite registered session, with one shared deadline for the whole load.
// Only observed missing optional input becomes Absent. No usable prefix, native
// diagnostic, endpoint/key inventory or stale cached value escapes a failure.
// Observed native context/RPC deadlines retain context.DeadlineExceeded even before
// the caller's context signals; this does not mean the caller context was canceled.
//
// Cleanup cancels and joins the native owner before returning, using a context
// without cancellation. Native DNS or caller trace hooks can therefore delay
// return beyond the request deadline. The admission slot remains owned until
// cleanup completes; no abandoned cleanup goroutine or persistent watch is used.
func (provider *Provider) ReadConfiguration(ctx context.Context) (input configuration.Input, result error) {
	if provider == nil || provider.slots == nil || ctx == nil {
		return configuration.Input{}, problem(configuration.InvalidInput, "", nil)
	}
	work, cancel := context.WithTimeout(ctx, provider.options.RequestTimeout)
	defer cancel()
	if work.Err() != nil {
		return configuration.Input{}, cancelled(work)
	}
	select {
	case provider.slots <- struct{}{}:
		defer func() { <-provider.slots }()
	default:
		return configuration.Input{}, problem(configuration.LimitExceeded, "", nil)
	}
	client, err := native.Open(work, provider.options)
	if err != nil {
		return configuration.Input{}, classify(work, "", err)
	}
	defer func() {
		if err := client.Close(context.WithoutCancel(ctx)); err != nil {
			input = configuration.Input{}
			result = problem(configuration.Unavailable, "", result)
		}
		if work.Err() != nil {
			input = configuration.Input{}
			result = problem(configuration.Cancelled, "", errors.Join(result, work.Err(), context.Cause(work)))
		}
	}()
	input = configuration.Input{Provider: ProviderID, SchemaVersion: provider.format}
	for _, source := range provider.sources {
		document, err := client.Read(work, native.KeyV1{Group: source.Group, DataID: source.DataID})
		selected := configuration.Document{Name: source.Name, Layer: source.Layer}
		if work.Err() != nil {
			return configuration.Input{}, cancelled(work)
		}
		if err != nil {
			if source.Optional && errors.Is(err, native.ErrMissing) {
				selected.Absent = true
			} else {
				return configuration.Input{}, classify(work, source.Name, err)
			}
		} else {
			selected.Data = document.RawCopy()
		}
		input.Documents = append(input.Documents, selected)
	}
	return input, nil
}

func classify(ctx context.Context, name string, err error) error {
	if ctx.Err() != nil {
		return cancelled(ctx)
	}
	switch {
	case errors.Is(err, context.DeadlineExceeded) || status.Code(err) == codes.DeadlineExceeded:
		return problem(configuration.Cancelled, name, context.DeadlineExceeded)
	case errors.Is(err, native.ErrMissing):
		return problem(configuration.Unavailable, name, configuration.Missing)
	case errors.Is(err, native.ErrDenied):
		return problem(configuration.Unavailable, name, configuration.Denied)
	case errors.Is(err, native.ErrLimit):
		return problem(configuration.LimitExceeded, name, nil)
	case errors.Is(err, native.ErrEmpty), errors.Is(err, native.ErrDecode), errors.Is(err, native.ErrUnsupported):
		return problem(configuration.Invalid, name, nil)
	default:
		return problem(configuration.Unavailable, name, nil)
	}
}

func cancelled(ctx context.Context) error {
	return problem(configuration.Cancelled, "", errors.Join(ctx.Err(), context.Cause(ctx)))
}

func problem(code failure.Code, source string, cause error) error {
	attributes := []failure.Attribute{{Name: "provider", Value: ProviderID}}
	if source != "" {
		attributes = append(attributes, failure.Attribute{Name: "source", Value: source})
	}
	return failure.New(code, cause, attributes...)
}

type private struct{}

func (private) Format(state fmt.State, _ rune) {
	_, _ = io.WriteString(state, "nacos.Configuration[restricted]")
}
func (private) LogValue() slog.Value { return slog.StringValue("nacos.Configuration[restricted]") }
func (private) MarshalJSON() ([]byte, error) {
	return nil, problem(configuration.InvalidInput, "", nil)
}
func (*private) UnmarshalJSON([]byte) error { return problem(configuration.InvalidInput, "", nil) }
