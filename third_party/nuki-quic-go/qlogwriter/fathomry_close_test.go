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

package qlogwriter

import (
	"bytes"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nukilabs/quic-go/qlogwriter/jsontext"
)

type correctionOutput struct {
	bytes.Buffer
	writeErr  error
	short     bool
	shortAt   int
	writes    int
	closeErr  error
	failClose atomic.Bool
	closed    atomic.Bool
	closes    atomic.Int32
}

func (output *correctionOutput) Write(data []byte) (int, error) {
	output.writes++
	if output.writeErr != nil {
		return 0, output.writeErr
	}
	if output.short || output.shortAt != 0 && output.writes == output.shortAt {
		return 0, nil
	}
	return output.Buffer.Write(data)
}

func TestEventPayloadShortWriteIsReportedAfterRelease(t *testing.T) {
	output := &correctionOutput{shortAt: 3}
	trace := NewFileSeq(output)
	producer := trace.AddProducer().(*Writer)
	go trace.Run()
	producer.RecordEvent(testEvent{message: "payload"})
	if err := producer.Close(); !errors.Is(err, io.ErrShortWrite) {
		t.Fatal("JSON token short write disappeared", err)
	}
	if !producer.ReleaseConfirmed() || !output.closed.Load() {
		t.Fatal("closed output retained ownership after a write error")
	}
}

type failingEvent struct{ cause error }

func (event failingEvent) Name() string                              { return "test:failure" }
func (event failingEvent) Encode(*jsontext.Encoder, time.Time) error { return event.cause }

func TestEventFailureDoesNotOverwriteEarlierWriteFailure(t *testing.T) {
	cause := errors.New("event-encoding-failed")
	output := &correctionOutput{shortAt: 3}
	trace := NewFileSeq(output)
	producer := trace.AddProducer().(*Writer)
	go trace.Run()
	producer.RecordEvent(failingEvent{cause: cause})
	err := producer.Close()
	if !errors.Is(err, cause) || !errors.Is(err, io.ErrShortWrite) || !producer.ReleaseConfirmed() {
		t.Fatal("combined encoding failures or positive release were lost", err)
	}
}
func (output *correctionOutput) Close() error {
	output.closes.Add(1)
	if output.failClose.Load() {
		return output.closeErr
	}
	output.closed.Store(true)
	return nil
}

func TestOutputFailureAndReleaseAreSeparate(t *testing.T) {
	cause := errors.New("qlog-write-failure")
	for _, output := range []*correctionOutput{{writeErr: cause}, {short: true}} {
		trace := NewFileSeq(output)
		producer := trace.AddProducer().(*Writer)
		go trace.Run()
		expected := cause
		if output.short {
			expected = io.ErrShortWrite
		}
		if err := producer.Close(); !errors.Is(err, expected) {
			t.Fatal("output error lost", err)
		}
		if !producer.ReleaseConfirmed() || !output.closed.Load() {
			t.Fatal("historical write failure retained ownership")
		}
		if err := producer.Close(); !errors.Is(err, expected) || output.closes.Load() != 1 {
			t.Fatal("repeated Close changed evidence", err)
		}
	}
}

func TestOutputCloseRetriesWithoutRemovingProducerAgain(t *testing.T) {
	cause := errors.New("qlog-close-failure")
	output := &correctionOutput{closeErr: cause}
	output.failClose.Store(true)
	trace := NewFileSeq(output)
	first, last := trace.AddProducer().(*Writer), trace.AddProducer().(*Writer)
	go trace.Run()
	for range 2 {
		if err := first.Close(); err != nil {
			t.Fatal(err)
		}
		if !first.ReleaseConfirmed() || output.closes.Load() != 0 {
			t.Fatal("non-final producer closed shared output")
		}
	}
	for range 2 {
		if err := last.Close(); !errors.Is(err, cause) {
			t.Fatal("Close error lost", err)
		}
		if last.ReleaseConfirmed() || output.closed.Load() {
			t.Fatal("failed Close fabricated release")
		}
	}
	output.failClose.Store(false)
	if err := last.Close(); !errors.Is(err, cause) {
		t.Fatal("retry discarded historical failure", err)
	}
	if !last.ReleaseConfirmed() || !output.closed.Load() || output.closes.Load() != 3 {
		t.Fatal("successful retry did not release output")
	}
	if trace.AddProducer() != nil {
		t.Fatal("closed trace admitted another producer")
	}
}
