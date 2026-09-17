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

package redis

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	"github.com/frost-leo/fathomry/internal/fault"
)

// ProviderID identifies the independently selected go-redis integration.
const ProviderID = "cache.redis.v9"

const (
	ErrInput       fault.Kind = "fathomry." + ProviderID + ".input"
	ErrAuthority   fault.Kind = "fathomry." + ProviderID + ".authority"
	ErrUnsupported fault.Kind = "fathomry." + ProviderID + ".unsupported"
	ErrLimit       fault.Kind = "fathomry." + ProviderID + ".limit"
	ErrProtocol    fault.Kind = "fathomry." + ProviderID + ".protocol"
	ErrCommand     fault.Kind = "fathomry." + ProviderID + ".command"
	ErrState       fault.Kind = "fathomry." + ProviderID + ".state"
	ErrCleanup     fault.Kind = "fathomry." + ProviderID + ".cleanup"
)

func failure(kind fault.Kind, operation string, causes ...error) error {
	return kind.New(fault.Context{Provider: ProviderID, Operation: operation}, causes...)
}
func nativeFailure(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	return failure(ErrCommand, "execute", err, ctx.Err(), context.Cause(ctx))
}

type private struct{}

func (private) String() string             { return "redis[restricted]" }
func (private) GoString() string           { return "redis[restricted]" }
func (private) Format(s fmt.State, _ rune) { _, _ = io.WriteString(s, "redis[restricted]") }
func (private) LogValue() slog.Value       { return slog.StringValue("redis[restricted]") }
func (private) MarshalJSON() ([]byte, error) {
	return nil, errors.New("redis: runtime serialization unsupported")
}
func (*private) UnmarshalJSON([]byte) error {
	return errors.New("redis: runtime reconstruction unsupported")
}
func (*OptionsV1) LogValue() slog.Value           { return slog.StringValue("redis[restricted]") }
func (*Client) LogValue() slog.Value              { return slog.StringValue("redis[restricted]") }
func (*Source) LogValue() slog.Value              { return slog.StringValue("redis[restricted]") }
func (*Result) LogValue() slog.Value              { return slog.StringValue("redis[restricted]") }
func (*Value) LogValue() slog.Value               { return slog.StringValue("redis[restricted]") }
func (*Command) LogValue() slog.Value             { return slog.StringValue("redis[restricted]") }
func (*Reply) LogValue() slog.Value               { return slog.StringValue("redis[restricted]") }
func (*Pair) LogValue() slog.Value                { return slog.StringValue("redis[restricted]") }
func (*Password) LogValue() slog.Value            { return slog.StringValue("redis[restricted]") }
func (*Session) LogValue() slog.Value             { return slog.StringValue("redis[restricted]") }
func (*Subscription) LogValue() slog.Value        { return slog.StringValue("redis[restricted]") }
func (*SubscriptionOptions) LogValue() slog.Value { return slog.StringValue("redis[restricted]") }
