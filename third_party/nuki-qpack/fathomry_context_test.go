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

package qpack

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"
)

func TestFathomryCanceledSectionKeepsAnotherSectionAlive(t *testing.T) {
	decoder := NewDecoder(WithMaxTableCapacity(64))
	if err := decoder.dt.setCapacity(64); err != nil {
		t.Fatal(err)
	}
	defer decoder.Close()
	firstCtx, cancel := context.WithCancelCause(context.Background())
	defer cancel(context.Canceled)
	secondCtx, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	block := []byte{2, 0, 0x80}
	first, second := make(chan error, 1), make(chan error, 1)
	go func() { _, err := decoder.DecodeForStreamContext(firstCtx, 0, block, 64)(); first <- err }()
	go func() {
		field, err := decoder.DecodeForStreamContext(secondCtx, 4, block, 64)()
		if err == nil && (field.Name != "x" || field.Value != "value") {
			err = errors.New("wrong dynamic field")
		}
		second <- err
	}()
	cause := errors.New("synthetic-cancellation")
	cancel(cause)
	select {
	case err := <-first:
		if !errors.Is(err, cause) {
			t.Fatal("cause lost", err)
		}
	case <-secondCtx.Done():
		t.Fatal("cancel did not wake decoder")
	}
	select {
	case <-second:
		t.Fatal("another stream was terminated")
	default:
	}
	if err := decoder.applyInsert("x", "value"); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-second:
		if err != nil {
			t.Fatal(err)
		}
	case <-secondCtx.Done():
		t.Fatal("healthy section did not finish")
	}
}

func TestFathomryDynamicFieldListIsBoundedBeforeAccumulation(t *testing.T) {
	decoder := NewDecoder(WithMaxTableCapacity(64))
	if err := decoder.dt.setCapacity(64); err != nil {
		t.Fatal(err)
	}
	if err := decoder.applyInsert("x", "value"); err != nil {
		t.Fatal(err)
	}
	data := append([]byte{2, 0}, bytes.Repeat([]byte{0x80}, 1<<16)...)
	_, err := decoder.DecodeForStreamContext(context.Background(), 4, data, 64)()
	if !errors.Is(err, ErrHeaderLimit) {
		t.Fatal("unbounded eager decoding", err)
	}
}

func TestFathomryEncoderLiteralIsBoundedBeforeAllocation(t *testing.T) {
	prefix := appendVarInt(nil, 7, 1<<30)
	reader := bufio.NewReader(bytes.NewReader(prefix[1:]))
	_, err := readStringFrom(prefix[0], 7, reader, 64)
	if !errors.Is(err, ErrHeaderLimit) {
		t.Fatal("oversized allocation not refused", err)
	}
}

type failingWriter struct{ cause error }

func (writer failingWriter) Write([]byte) (int, error) { return 0, writer.cause }

func TestFathomryDecoderStreamWriteCauseIsNotDiscarded(t *testing.T) {
	cause := errors.New("synthetic-control-write")
	decoder := NewDecoder(WithMaxTableCapacity(64), WithDecoderStream(failingWriter{cause}))
	if err := decoder.dt.setCapacity(64); err != nil {
		t.Fatal(err)
	}
	if err := decoder.applyInsert("x", "value"); err != nil {
		t.Fatal(err)
	}
	_, err := decoder.DecodeForStreamContext(context.Background(), 4, []byte{2, 0, 0x80}, 64)()
	if !errors.Is(err, cause) {
		t.Fatal("control-stream cause lost", err)
	}
}

func TestFathomryStaticContextDecoderKeepsEOF(t *testing.T) {
	decode := NewDecoder().DecodeForStreamContext(context.Background(), 0, []byte{0, 0, 0xd9}, 64)
	field, err := decode()
	if err != nil || field.Name != ":status" || field.Value != "200" {
		t.Fatal("static field changed", err)
	}
	if _, err := decode(); err != io.EOF {
		t.Fatal("EOF changed", err)
	}
}
