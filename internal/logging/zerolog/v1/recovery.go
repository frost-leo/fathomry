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

package zerolog

import (
	"context"
	"errors"
	"io"
)

// RecordState is a qualified structured-destination outcome, not retry policy
// or proof of export/backend durability.
type RecordState string

const (
	RecordAccepted RecordState = "accepted"
	RecordRejected RecordState = "rejected"
	RecordStopped  RecordState = "stopped"
)

// RecordOutcome distinguishes independent-event refusal from damaged/closed or
// uncertain destination state. Accepted requires nil Err; Rejected and Stopped
// require non-nil Err. Typed-nil errors and malformed outcomes are stopped.
type RecordOutcome struct {
	private
	State RecordState
	Err   error
}

// ManagedRecordWriter is an explicit opt-in borrowed destination for independent
// records. Rejected fails only this event; Stopped prevents later output entry.
// No failed event is retried, acknowledged or silently treated as accepted.
// Calls must return normally and honor their declared cooperative lifetime.
// The logger never Syncs or Closes a borrowed managed destination.
type ManagedRecordWriter interface {
	WriteManagedRecord(context.Context, Record) RecordOutcome
}

func (sink borrowedSink) write(ctx context.Context, record Record, report *SinkResult) {
	returned := false
	defer func() {
		_ = recover()
		if !returned {
			report.Accepted = false
			report.Stopped = true
			report.WriteError = failure(ErrWrite, "sink-panic")
		}
	}()
	if sink.managed != nil {
		outcome := sink.managed.WriteManagedRecord(ctx, record)
		switch {
		case outcome.Err != nil && nilHandle(outcome.Err):
			report.WriteError = failure(ErrWrite, "managed-record-outcome", outcome.Err)
			report.Stopped = true
		case outcome.State == RecordAccepted && outcome.Err == nil:
			report.Accepted = true
		case outcome.State == RecordRejected && outcome.Err != nil:
			report.WriteError = failure(ErrWrite, "record-rejected", outcome.Err)
		case outcome.State == RecordStopped && outcome.Err != nil:
			report.WriteError = failure(ErrWrite, "record-stopped", outcome.Err)
			report.Stopped = true
		default:
			report.WriteError = failure(ErrWrite, "managed-record-outcome", outcome.Err)
			report.Stopped = true
		}
	} else if sink.writer != nil {
		data := record.JSONCopy()
		count, err := sink.writer.Write(data)
		report.Written, report.BytesKnown = count, count >= 0 && count <= len(data)
		if count != len(data) {
			err = errors.Join(err, io.ErrShortWrite)
		}
		report.WriteError = joined(ErrWrite, "writer", err)
		report.Accepted = report.BytesKnown && count == len(data) && err == nil
		report.Stopped = report.WriteError != nil
	} else {
		err := sink.records.WriteRecord(ctx, record)
		report.WriteError = joined(ErrWrite, "record-writer", err)
		report.Accepted = err == nil
		report.Stopped = err != nil
	}
	returned = true
}
