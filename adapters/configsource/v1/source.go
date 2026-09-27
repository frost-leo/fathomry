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

package configsource

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
)

// Per-owner ceilings do not constitute process or fleet quotas.
const (
	MaxDocuments     = 16
	MaxDocumentBytes = 1 << 20
	MaxBatchBytes    = 4 << 20
)

// Description is safe, caller-owned selection metadata. Names are explicitly
// declared non-secret aliases, never native paths, endpoints or keys.
type Description struct {
	Name       string
	Module     string
	Documents  []string
	Observable bool
}

// Selection is a deferred immutable source. Select validates/copies bootstrap;
// each Capture/Observe creates independent ownership, without implicit sharing.
// Implementations must honor complete bounded batches and diagnostic guards.
type Selection interface {
	Description() (Description, error)
	Capture(context.Context) (Batch, error)
	Observe(context.Context) (Observer, error)
	fmt.Formatter
	slog.LogValuer
	json.Marshaler
}

// Presence separates positive absence from present empty content and failure.
type Presence uint8

const (
	Missing Presence = iota + 1
	Present
)

// Document is safe per-slot metadata; Bytes is decoded raw size, not a revision.
type Document struct {
	Name     string
	Presence Presence
	Bytes    int
}

// Batch is an immutable complete capture; nil is invalid. RawCopy explicitly
// exposes sensitive owned UTF-8 bytes. Unknown slots return ErrValue.
// A batch does not assert a common-time or cross-document transaction.
type Batch interface {
	Documents() []Document
	RawCopy(string) ([]byte, Presence, error)
	fmt.Formatter
	slog.LogValuer
	json.Marshaler
}

// Cursor is an opaque, owner-bound ephemeral position returned by Current/Next.
// A nil cursor requests current state. Implementations reject foreign values,
// including arbitrary implementations of this interface; it is not a wire token.
type Cursor interface {
	fmt.Formatter
	slog.LogValuer
	json.Marshaler
}

// Status describes raw acquisition, not application validation or remote truth.
type Status uint8

const (
	Pending Status = iota + 1
	Available
	Degraded
	Closing
	Closed
)

// State is one coherent observation. Batch is nil until the first complete
// capture; failures retain the old Batch. Generation changes only with raw data,
// whereas Cursor also advances on status changes. Failure may retain sensitive
// causes through deliberate errors.Is/As; ordinary State formatting is redacted.
type State struct {
	Batch      Batch
	Generation uint64
	Status     Status
	Failure    error
	Cursor     Cursor
}

// Observer owns one live acquisition/recovery loop. Current permits concurrent
// readers; one Next waiter is admitted at a time. Wait cancellation never cancels
// ownership. Close begins shutdown even if its wait expires; retry on the same
// handle. Closed requires joined work; no new batch publishes after Closing.
type Observer interface {
	Current() (State, error)
	Next(context.Context, Cursor) (State, error)
	Close(context.Context) error
	fmt.Formatter
	slog.LogValuer
	json.Marshaler
}

func (State) Format(state fmt.State, _ rune) {
	_, _ = io.WriteString(state, "configsource.State[restricted]")
}
func (*State) LogValue() slog.Value        { return slog.StringValue("configsource.State[restricted]") }
func (State) MarshalJSON() ([]byte, error) { return nil, ErrValue }
func (*State) UnmarshalJSON([]byte) error  { return ErrValue }
