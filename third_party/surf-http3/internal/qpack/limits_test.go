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

package qpackdecoder

import (
	"bytes"
	"context"
	"errors"
	"io"
	"runtime"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/http2/hpack"
)

func reviewCodec(t *testing.T, capacity, blocked uint64) *Decoder {
	t.Helper()
	decoder, err := New(capacity, blocked)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { decoder.Close(nil) })
	return decoder
}

func reviewApply(t *testing.T, decoder *Decoder, encoded []byte) {
	t.Helper()
	if err := decoder.ParseEncoder(bytes.NewReader(encoded), func(uint64) error { return nil }); !errors.Is(err, io.EOF) {
		t.Fatal("legal encoder instructions refused", err)
	}
}

func reviewInsert(name, value string, huffman bool) []byte {
	nameBytes, valueBytes := []byte(name), []byte(value)
	nameMask, valueMask := byte(0x40), byte(0)
	if huffman {
		nameBytes, valueBytes = hpack.AppendHuffmanString(nil, name), hpack.AppendHuffmanString(nil, value)
		nameMask, valueMask = 0x60, 0x80
	}
	encoded := append(AppendInstruction(5, nameMask, uint64(len(nameBytes))), nameBytes...)
	return append(append(encoded, AppendInstruction(7, valueMask, uint64(len(valueBytes)))...), valueBytes...)
}

func reviewBlocked(t *testing.T, decoder *Decoder, expected int) {
	t.Helper()
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		decoder.mu.Lock()
		count := len(decoder.blocked)
		decoder.mu.Unlock()
		if count == expected {
			return
		}
		select {
		case <-ticker.C:
		case <-deadline.C:
			t.Fatalf("blocked records=%d, want %d", count, expected)
		}
	}
}

func TestFathomryQPACKBlockedCountCancellationAndClose(t *testing.T) {
	decoder := reviewCodec(t, 64, 1)
	reviewApply(t, decoder, AppendInstruction(5, 0x20, 64))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	first := make(chan error, 1)
	go func() { _, _, err := decoder.Decode(ctx, 0, []byte{2, 0, 0x80}, 64); first <- err }()
	reviewBlocked(t, decoder, 1)
	if _, _, err := decoder.Decode(context.Background(), 4, []byte{2, 0, 0x80}, 64); !errors.Is(err, ErrEncoding) {
		t.Fatal("second blocked stream exceeded advertised1 without refusal", err)
	}
	cancel()
	if err := <-first; !errors.Is(err, context.Canceled) {
		t.Fatal("blocked cancellation lost", err)
	}
	reviewBlocked(t, decoder, 0)
	second := make(chan error, 1)
	go func() { _, _, err := decoder.Decode(context.Background(), 4, []byte{2, 0, 0x80}, 64); second <- err }()
	reviewBlocked(t, decoder, 1)
	reviewApply(t, decoder, reviewInsert("x", "value", false))
	if err := <-second; err != nil {
		t.Fatal("released blocked slot not reusable", err)
	}
	reviewBlocked(t, decoder, 0)
	decoder.Close(errors.New("fixture shutdown"))
	if _, _, err := decoder.Decode(context.Background(), 8, []byte{2, 0, 0x80}, 64); !errors.Is(err, ErrClosed) {
		t.Fatal("ready dynamic block decoded after Close", err)
	}
	if decoder.table.entries != nil || decoder.table.size != 0 || len(decoder.blocked) != 0 {
		t.Fatal("Close retained table or waiter records")
	}
	closedWaiter := reviewCodec(t, 64, 1)
	third := make(chan error, 1)
	go func() {
		_, _, err := closedWaiter.Decode(context.Background(), 0, []byte{2, 0, 0x80}, 64)
		third <- err
	}()
	reviewBlocked(t, closedWaiter, 1)
	closedWaiter.Close(nil)
	select {
	case err := <-third:
		if !errors.Is(err, ErrClosed) {
			t.Fatal("source stop failed to wake blocked decode", err)
		}
	case <-time.After(time.Second):
		t.Fatal("source stop left a blocked decode alive")
	}
}

type reviewPrefixReader struct {
	*bytes.Reader
	reads int
}

func (reader *reviewPrefixReader) Read(data []byte) (int, error) {
	reader.reads++
	if reader.Reader.Len() == 0 {
		return 0, errors.New("unexpected read after rejecting instruction prefix")
	}
	return reader.Reader.Read(data)
}

func TestFathomryQPACKLiteralBoundsBeforeAllocation(t *testing.T) {
	decoder := reviewCodec(t, 64, 1)
	reviewApply(t, decoder, AppendInstruction(5, 0x20, 64))
	value := strings.Repeat("\xff", 31)
	if len(hpack.AppendHuffmanString(nil, value)) <= 64 {
		t.Fatal("fixture lacks longer-encoded-than-table Huffman control")
	}
	reviewApply(t, decoder, reviewInsert("x", value, true))
	fields, required, err := decoder.Decode(context.Background(), 0, []byte{2, 0, 0x80}, 64)
	if err != nil || required != 1 || len(fields) != 1 || fields[0].Name != "x" || fields[0].Value != value || decoder.table.size != 64 {
		t.Fatal("exact decoded table limit rejected legal long Huffman bytes", err, decoder.table.size)
	}
	bad := reviewInsert("x", strings.Repeat("\xff", 32), true)
	if err := decoder.ParseEncoder(bytes.NewReader(bad), func(uint64) error { return nil }); !errors.Is(err, ErrLimit) || decoder.table.size != 64 || decoder.table.insertCount != 1 {
		t.Fatal("decoded one-over was installed", err, decoder.table.size, decoder.table.insertCount)
	}
	prefix := append([]byte{0x41, 'x'}, AppendInstruction(7, 0x80, 1<<61)...)
	input := &reviewPrefixReader{Reader: bytes.NewReader(prefix)}
	if err := decoder.ParseEncoder(input, func(uint64) error { return nil }); !errors.Is(err, ErrLimit) || input.reads != 1 {
		t.Fatal("large literal length reached body read/allocation", err, input.reads)
	}
	fieldPrefix := append([]byte{0, 0, 0x21, 'x'}, AppendInstruction(7, 0x80, 32<<20)...)
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	_, _, err = decoder.Decode(context.Background(), 4, fieldPrefix, 64<<20)
	runtime.ReadMemStats(&after)
	allocated := after.TotalAlloc - before.TotalAlloc
	t.Logf("truncated field literal allocated=%d bytes", allocated)
	if !errors.Is(err, io.ErrUnexpectedEOF) || allocated > 64<<10 {
		t.Fatal("tiny truncated field section allocated its announced32MiB literal", err, allocated)
	}
}

func TestFathomryQPACKTableResidenceAndZeroControls(t *testing.T) {
	zero := reviewCodec(t, 0, 0)
	if fields, _, err := zero.Decode(context.Background(), 0, []byte{0, 0, 0xd9}, 64); err != nil || len(fields) != 1 {
		t.Fatal("static control failed with explicit zero capacity", err)
	}
	if _, _, err := zero.Decode(context.Background(), 4, []byte{2, 0, 0x80}, 64); !errors.Is(err, ErrEncoding) {
		t.Fatal("zero-capacity peer created a blocked dynamic reference", err)
	}
	for _, values := range [][2]uint64{{64<<20 + 1, 1}, {64, 1025}} {
		if decoder, err := New(values[0], values[1]); decoder != nil || !errors.Is(err, ErrLimit) {
			t.Fatal("oversized codec configuration admitted", err)
		}
	}
	decoder := reviewCodec(t, 256, 0)
	reviewApply(t, decoder, AppendInstruction(5, 0x20, 256))
	for range 4096 {
		reviewApply(t, decoder, reviewInsert("", "", false))
		if decoder.table.size > 256 || len(decoder.table.entries) > 8 || cap(decoder.table.entries) > 16 {
			t.Fatal("table backing grew beyond bounded resident entries", decoder.table.size, len(decoder.table.entries), cap(decoder.table.entries))
		}
	}
	if decoder.table.insertCount != 4096 {
		t.Fatal("empty opaque table entries incorrectly rejected")
	}
	decoder.Close(nil)
	if decoder.table.entries != nil || decoder.table.size != 0 {
		t.Fatal("evicting table survived closed source")
	}
}
