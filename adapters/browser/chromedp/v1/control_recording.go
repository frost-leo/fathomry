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

package chromedp

import (
	"context"
	"errors"

	"github.com/chromedp/cdproto/page"
	browser "github.com/frost-leo/fathomry/adapters/browser/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	native "github.com/frost-leo/fathomry/internal/browser/chromedp/v0"
)

// StopRecording waits for the native recorder to flush its stream. It uses the
// existing Control reservation, including at ordinary command/Inbox saturation.
// A timeout ends only the wait. Repeated calls join the same native stop rather
// than submitting it again. A stop reply is not video-format validation.
func (control *Control) StopRecording(ctx context.Context) (page.StopScreenRecordingResult, error) {
	if control == nil || control.state == nil || ctx == nil {
		return page.StopScreenRecordingResult{}, fail(ErrInput, "recording-stop")
	}
	result, err := control.state.native.StopRecording(ctx)
	return result, translate(err, "recording-stop")
}

// ReadRecording consumes bounded IO only after StopRecording confirms producer
// completion. The recorder's stream stays private to Control. Each chunk has an
// independent receipt; Control.Close stops ignored recording and closes its IO.
func (control *Control) ReadRecording(ctx context.Context, size int) (chunk Chunk, receipt *adapters.Receipt[Result], err error) {
	return control.readRecording(ctx, size, nil)
}

// ReadRecordingAt seeks within confirmed recording output. It changes subsequent
// read position and shares the cumulative byte limit with ReadRecording.
func (control *Control) ReadRecordingAt(ctx context.Context, size int, offset int64) (Chunk, *adapters.Receipt[Result], error) {
	return control.readRecording(ctx, size, &offset)
}

func (control *Control) readRecording(ctx context.Context, size int, offset *int64) (chunk Chunk, receipt *adapters.Receipt[Result], err error) {
	if control == nil || control.state == nil || ctx == nil {
		return chunk, nil, fail(ErrInput, "recording-read")
	}
	state, family := control.state, control.state.context
	var primary error
	receipt, err = family.endpoint.Child(ctx, state.call.Scope(), request("recording_read", family.id, 0, family.budget.EvidenceBytes), func(call *adapters.Call[Result]) {
		finish, err := state.page.beginOperation()
		if err != nil {
			primary = err
			_ = call.Resolve(adapters.Outcome[Result]{Primary: err})
			return
		}
		defer finish()
		var value native.Chunk
		if offset == nil {
			value, err = state.native.ReadRecording(call.Context(), size)
		} else {
			value, err = state.native.ReadRecordingAt(call.Context(), size, *offset)
		}
		chunk, primary = Chunk{native: value}, translate(err, "recording-read")
		info, _ := call.Receipt().Snapshot()
		_ = call.Resolve(adapters.Outcome[Result]{Present: true, Value: Result{payload: value.DataCopy(), attribution: attribution(info.Info()), observation: browser.Observation{Returned: err == nil, EOF: value.EOF(), Effect: browser.Unknown}}, Primary: primary})
	})
	return chunk, receipt, errors.Join(err, receiptError(receipt, primary))
}
