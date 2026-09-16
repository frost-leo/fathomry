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

package nuki

import (
	"errors"
	"sync/atomic"
	"testing"

	"github.com/nukilabs/quic-go/qlogwriter"
)

type traceOutput struct {
	writeErr error
	closeErr error
	fail     atomic.Bool
	closed   atomic.Bool
}

func (output *traceOutput) Write(data []byte) (int, error) {
	if output.writeErr != nil {
		return 0, output.writeErr
	}
	return len(data), nil
}
func (output *traceOutput) Close() error {
	if output.fail.Load() {
		return output.closeErr
	}
	output.closed.Store(true)
	return nil
}

func TestProviderNativeTraceErrorsRetainReleaseEvidence(t *testing.T) {
	for _, failingClose := range []bool{true, false} {
		options := providerOptions()
		owner, err := newOwner(defaults(options), options.Native)
		if err != nil {
			t.Fatal(err)
		}
		cause := errors.New("qlog-output-failure")
		output := &traceOutput{closeErr: cause}
		output.fail.Store(failingClose)
		if !failingClose {
			output.writeErr = cause
		}
		trace := qlogwriter.NewFileSeq(output)
		done := make(chan struct{})
		go func() { defer close(done); trace.Run() }()
		guard := guardedTrace{owner: owner, Trace: trace}
		if guard.AddProducer() == nil {
			t.Fatal("trace producer missing")
		}
		t.Cleanup(func() { output.fail.Store(false); _ = output.Close(); <-done })
		result := owner.release(testContext(t))
		if failingClose {
			for range 2 {
				if result.Released || result.Quiescent || result.Continue == nil || !errors.Is(result.Err, cause) || output.closed.Load() {
					t.Fatal("unclosed qlog output lost ownership/error", result.Err)
				}
				result = result.Continue(testContext(t))
			}
			output.fail.Store(false)
			result = result.Continue(testContext(t))
		}
		if !result.Released || !result.Quiescent || !errors.Is(result.Err, cause) || !output.closed.Load() {
			t.Fatal("trace history conflated with physical release", result.Err)
		}
		owner.mu.Lock()
		remaining := len(owner.recorders)
		owner.mu.Unlock()
		if remaining != 0 {
			t.Fatal("released producer retained charge")
		}
	}
}
