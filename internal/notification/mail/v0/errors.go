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

package mail

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
)

// ProviderID identifies the independently selected go-mail integration.
const ProviderID = "notification.mail.v0"

const (
	ErrInput       fault.Kind = "fathomry." + ProviderID + ".input"
	ErrLimit       fault.Kind = "fathomry." + ProviderID + ".limit"
	ErrUnsupported fault.Kind = "fathomry." + ProviderID + ".unsupported"
	ErrSend        fault.Kind = "fathomry." + ProviderID + ".send"
	ErrCleanup     fault.Kind = "fathomry." + ProviderID + ".cleanup"
)

func failure(kind fault.Kind, operation string, causes ...error) error {
	return kind.New(fault.Context{Provider: ProviderID, Operation: operation}, causes...)
}

func phaseFailure(kind fault.Kind, operation string, ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	var expired error
	if findCause(err, func(cause error) bool { return cause == os.ErrDeadlineExceeded }) != nil {
		if deadline, bounded := ctx.Deadline(); bounded && !time.Now().Before(deadline) {
			// Socket deadlines can fire before the context timer is scheduled.
			expired = context.DeadlineExceeded
		}
	}
	return failure(kind, operation, err, ctx.Err(), context.Cause(ctx), expired)
}

// Inspection is bounded even when a borrowed callback supplies a cyclic graph.
// Original causes remain intact for deliberate external errors.Is/As access.
func findCause(root error, match func(error) bool) error {
	queue := []error{root}
	for index := 0; index < len(queue) && index < 128; index++ {
		err := queue[index]
		if err == nil {
			continue
		}
		if match(err) {
			return err
		}
		switch value := err.(type) {
		case interface{ Unwrap() []error }:
			children := value.Unwrap()
			queue = append(queue, children[:min(len(children), 128-len(queue))]...)
		case interface{ Unwrap() error }:
			if len(queue) < 128 {
				queue = append(queue, value.Unwrap())
			}
		}
	}
	return nil
}

type private struct{}

func (private) String() string                 { return "mail[restricted]" }
func (private) GoString() string               { return "mail[restricted]" }
func (private) Format(state fmt.State, _ rune) { _, _ = io.WriteString(state, "mail[restricted]") }
func (private) LogValue() slog.Value           { return slog.StringValue("mail[restricted]") }
func (private) MarshalJSON() ([]byte, error) {
	return nil, errors.New("mail: runtime serialization unsupported")
}
func (*private) UnmarshalJSON([]byte) error {
	return errors.New("mail: runtime reconstruction unsupported")
}
func (*OptionsV1) LogValue() slog.Value  { return slog.StringValue("mail[restricted]") }
func (*Content) LogValue() slog.Value    { return slog.StringValue("mail[restricted]") }
func (*Inline) LogValue() slog.Value     { return slog.StringValue("mail[restricted]") }
func (*Attachment) LogValue() slog.Value { return slog.StringValue("mail[restricted]") }
func (*Message) LogValue() slog.Value    { return slog.StringValue("mail[restricted]") }
func (*Source) LogValue() slog.Value     { return slog.StringValue("mail[restricted]") }
func (*Client) LogValue() slog.Value     { return slog.StringValue("mail[restricted]") }
func (*Result) LogValue() slog.Value     { return slog.StringValue("mail[restricted]") }
func (*Delivery) LogValue() slog.Value   { return slog.StringValue("mail[restricted]") }
func (*Recipient) LogValue() slog.Value  { return slog.StringValue("mail[restricted]") }
