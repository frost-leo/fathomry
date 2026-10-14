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
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
	"testing"

	"github.com/chromedp/cdproto/cdp"
	"github.com/chromedp/cdproto/tracing"
	sdk "github.com/chromedp/chromedp"
)

func traceParameters() tracing.StartParams {
	return tracing.StartParams{StreamFormat: tracing.StreamFormatJSON, TracingBackend: tracing.BackendChrome,
		TraceConfig: &tracing.TraceConfig{TraceBufferSizeInKb: 1024, IncludedCategories: []string{"blink.user_timing"}}}
}

func TestLiveTraceStreamsNativeDataAndClosesItsBrowserOriginIO(t *testing.T) {
	for _, compression := range []tracing.StreamCompression{tracing.StreamCompressionNone, tracing.StreamCompressionGzip} {
		t.Run(string(compression), func(t *testing.T) {
			owner, retained, dependencies := controlFixture(t, "data:text/html,<p>trace fixture</p>", 8)
			params := traceParameters()
			params.StreamCompression = compression
			trace, receipt, err := owner.Trace(retained.Context(), params)
			if err != nil {
				t.Fatal(err)
			}
			if params.TransferMode != "" {
				t.Fatal("trace input mutated")
			}
			params.TraceConfig.IncludedCategories[0] = "changed-after-copy"
			if _, _, err := RunAction(bounded(t), retained.Page(), Evaluate[bool]("performance.mark('fathomry-trace-marker');true")); err != nil {
				t.Fatal(err)
			}
			drain(t, dependencies, 1)
			completion, err := trace.Stop(bounded(t))
			if err != nil || completion.Stream == "" || completion.TraceFormat != tracing.StreamFormatJSON || completion.StreamCompression != compression {
				t.Fatal("native trace completion missing or changed", err)
			}
			if repeat, err := trace.Stop(bounded(t)); err != nil || repeat.Stream != completion.Stream {
				t.Fatal("stop was not observational on repetition", err)
			}
			var body []byte
			eof := false
			for range 512 {
				chunk, _, err := trace.Read(bounded(t), 8192)
				if err != nil {
					t.Fatal(err)
				}
				body = append(body, chunk.DataCopy()...)
				drain(t, dependencies, 1)
				if chunk.EOF() {
					eof = true
					break
				}
			}
			if !eof {
				t.Fatal("bounded fixture did not observe EOF")
			}
			consumed := len(body)
			if compression == tracing.StreamCompressionGzip {
				reader, err := gzip.NewReader(bytes.NewReader(body))
				if err != nil {
					t.Fatal(err)
				}
				body, err = io.ReadAll(io.LimitReader(reader, 4<<20))
				if err != nil {
					t.Fatal(err)
				}
				if err := reader.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if !json.Valid(body) || !bytes.Contains(body, []byte("fathomry-trace-marker")) {
				t.Fatal("incremental trace bytes lost the native synthetic workload")
			}
			if err := trace.Close(bounded(t)); err != nil || !trace.Closed() {
				t.Fatal("trace IO cleanup failed", err)
			}
			snapshot, err := receipt.WaitReleased(bounded(t))
			if err != nil || snapshot.Err() != nil {
				t.Fatal("trace evidence did not release", err)
			}
			result, _ := snapshot.ValueCopy()
			facts := result.Trace()
			if !facts.Completed || !facts.StreamClosed || !facts.EOF || facts.Bytes != int64(consumed) || facts.DataLossOccurred != completion.DataLossOccurred {
				t.Fatal("trace completion/EOF/loss/cleanup evidence was conflated")
			}
			drain(t, dependencies, 1)
			if _, _, err := trace.Read(bounded(t), 1); err == nil {
				t.Fatal("closed trace stream revived")
			}
			if err := owner.Close(bounded(t)); err != nil || !owner.ShutdownComplete() {
				t.Fatal(err)
			}
			status, _ := dependencies.Evidence.Inspect()
			drain(t, dependencies, status.Outstanding)
		})
	}
}

func TestLiveTraceStopAndIgnoredStreamCleanupUseExistingReservation(t *testing.T) {
	owner, retained, dependencies := controlFixture(t, "data:text/html,<p>trace saturation</p>", 1)
	trace, receipt, err := owner.Trace(retained.Context(), traceParameters())
	if err != nil {
		t.Fatal(err)
	}
	fillControlInbox(t, retained, dependencies)
	if _, err := trace.Stop(bounded(t)); err != nil {
		t.Fatal("stop needed a new command or evidence slot", err)
	}
	if err := trace.Close(bounded(t)); err != nil || !trace.Closed() {
		t.Fatal("ignored trace stream was not closed", err)
	}
	snapshot, err := receipt.WaitReleased(bounded(t))
	if err != nil {
		t.Fatal(err)
	}
	result, _ := snapshot.ValueCopy()
	if !result.Trace().Completed || !result.Trace().StreamClosed || result.Trace().EOF || result.Trace().Bytes != 0 {
		t.Fatal("ignored stream invented byte consumption/EOF")
	}
	if err := owner.Close(bounded(t)); err != nil || !owner.ShutdownComplete() {
		t.Fatal(err)
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}

func TestLiveTraceNeverStoppedCancellationAndConcurrentStart(t *testing.T) {
	owner, retained, dependencies := controlFixture(t, "data:text/html,<p>unconsumed trace</p>", 4)
	ctx, cancel := context.WithCancel(retained.Context())
	trace, receipt, err := owner.Trace(ctx, traceParameters())
	if err != nil {
		t.Fatal(err)
	}
	if second, _, err := owner.Trace(retained.Context(), traceParameters()); second != nil || !errors.Is(err, ErrState) {
		t.Fatal("concurrent trace producer was accepted", err)
	}
	drain(t, dependencies, 1)
	cancel()
	if _, err := receipt.WaitReleased(bounded(t)); err != nil || !trace.Closed() {
		t.Fatal("never-read trace cancellation retained its native producer", err)
	}
	if err := owner.Close(bounded(t)); err != nil || !owner.ShutdownComplete() {
		t.Fatal(err)
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}

func TestLiveBorrowedBrowserCannotStartGlobalTrace(t *testing.T) {
	owner, retained, peer, dependencies := borrowedControlFixture(t)
	if trace, receipt, err := owner.Trace(bounded(t), traceParameters()); trace != nil || receipt == nil || !errors.Is(err, ErrState) {
		t.Fatal("borrowed connection acquired global tracing", err)
	}
	drain(t, dependencies, 1)
	if _, _, err := RunAction(bounded(t), retained.Page(), Command(tracing.Start, traceParameters())); !errors.Is(err, ErrUnsupported) {
		t.Fatal("ordinary page obtained global tracing", err)
	}
	drain(t, dependencies, 1)
	if _, _, err := RunAction(bounded(t), retained.Page(), Command(tracing.End, cdp.Empty{})); !errors.Is(err, ErrUnsupported) {
		t.Fatal("ordinary page obtained global trace stop", err)
	}
	drain(t, dependencies, 1)
	_ = owner.Close(bounded(t))
	if !owner.ShutdownComplete() {
		t.Fatal("borrowed trace refusal retained the source")
	}
	if value, err := sdk.Run(peer, sdk.Evaluate[int]("43")); err != nil || value != 43 {
		t.Fatal("trace refusal affected independent browser peer", err)
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}
