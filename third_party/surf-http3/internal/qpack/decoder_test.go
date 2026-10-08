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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/quic-go/qpack"
	"golang.org/x/net/http2/hpack"
)

func reviewCandidateInteger(prefix uint, mask byte, value uint64) []byte {
	maximum := uint64(1<<prefix) - 1
	if value < maximum {
		return []byte{mask | byte(value)}
	}
	result := []byte{mask | byte(maximum)}
	for value -= maximum; value >= 128; value >>= 7 {
		result = append(result, byte(value&127)|128)
	}
	return append(result, byte(value))
}

func reviewCandidateInsert(field qpack.HeaderField, huffman bool) []byte {
	result := reviewCandidateInteger(5, 0x40, uint64(len(field.Name)))
	result = append(result, field.Name...)
	value, mask := []byte(field.Value), byte(0)
	if huffman {
		value, mask = hpack.AppendHuffmanString(nil, field.Value), 0x80
	}
	result = append(result, reviewCandidateInteger(7, mask, uint64(len(value)))...)
	return append(result, value...)
}

func reviewCandidateDecoder(t *testing.T, capacity, blocked uint64, fields ...qpack.HeaderField) *Decoder {
	t.Helper()
	decoder, err := New(capacity, blocked)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { decoder.Close(nil) })
	data := reviewCandidateInteger(5, 0x20, capacity)
	for _, field := range fields {
		data = append(data, reviewCandidateInsert(field, false)...)
	}
	if err := decoder.ParseEncoder(bytes.NewReader(data), func(uint64) error { return nil }); !errors.Is(err, io.EOF) {
		t.Fatal("fixture instructions", err)
	}
	return decoder
}

func reviewCandidateWait(t *testing.T, label string, check func() bool) {
	t.Helper()
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for !check() {
		select {
		case <-deadline.C:
			t.Fatal(label)
		case <-ticker.C:
		}
	}
}

func TestFathomryCandidateQPACKRICAndCheckedIndices(t *testing.T) {
	decoder := reviewCandidateDecoder(t, 128, 1, qpack.HeaderField{Name: "x", Value: "v"}, qpack.HeaderField{Name: "y", Value: "w"})
	for _, control := range []struct {
		name  string
		block []byte
		want  qpack.HeaderField
		ric   uint64
	}{
		{"relative", []byte{2, 0, 0x80}, qpack.HeaderField{Name: "x", Value: "v"}, 1},
		{"post-base", []byte{3, 0x80, 0x10}, qpack.HeaderField{Name: "y", Value: "w"}, 2},
		{"static-nonzero-base", []byte{0, 1, 0xd9}, qpack.HeaderField{Name: ":status", Value: "200"}, 0},
	} {
		t.Run(control.name, func(t *testing.T) {
			fields, required, err := decoder.Decode(context.Background(), 0, control.block, 128)
			if err != nil || required != control.ric || len(fields) != 1 || fields[0] != control.want {
				t.Fatal("valid control changed", fields, required, err)
			}
		})
	}
	tooLargeBase := append([]byte{2}, reviewCandidateInteger(7, 0, maxInteger)...)
	tooLargeBase = append(tooLargeBase, 0x80)
	postOverflow := append([]byte{2}, reviewCandidateInteger(7, 0, maxInteger-1)...)
	postOverflow = append(postOverflow, 0x11)
	legacyWrap := append([]byte{2}, reviewCandidateInteger(7, 0, (uint64(1)<<63)-1)...)
	legacyWrap = append(legacyWrap, reviewCandidateInteger(4, 0x10, uint64(1)<<63)...)
	for _, invalid := range []struct {
		name  string
		block []byte
	}{
		{"zero-ric-dynamic", []byte{0, 1, 0x80}},
		{"understated-ric", []byte{2, 1, 0x80}},
		{"overstated-ric", []byte{3, 0, 0x81}},
		{"relative-underflow", []byte{2, 0, 0x81}},
		{"post-base-outside-ric", []byte{2, 0, 0x10}},
		{"negative-base", []byte{2, 0x81, 0x80}},
		{"base-addition-limit", tooLargeBase},
		{"post-base-addition-limit", postOverflow},
		{"old-uint64-wrap", legacyWrap},
	} {
		t.Run(invalid.name, func(t *testing.T) {
			fields, required, err := decoder.Decode(context.Background(), 0, invalid.block, 128)
			if !errors.Is(err, ErrEncoding) || len(fields) != 0 || required != 0 {
				t.Fatal("invalid reference/arithmetic yielded fields", fields, required, err)
			}
		})
	}
	evicted := reviewCandidateDecoder(t, 64, 0, qpack.HeaderField{Name: "x", Value: "v"}, qpack.HeaderField{Name: "y", Value: "w"})
	if fields, _, err := evicted.Decode(context.Background(), 0, []byte{2, 0, 0x80}, 128); !errors.Is(err, ErrEncoding) || len(fields) != 0 {
		t.Fatal("evicted entry remained referenceable", fields, err)
	}
}

func TestFathomryCandidateQPACKCloseRejectsReadyAndBlockedSections(t *testing.T) {
	decoder := reviewCandidateDecoder(t, 128, 1, qpack.HeaderField{Name: "x", Value: "v"})
	kept, _, err := decoder.Decode(context.Background(), 0, []byte{2, 0, 0x80}, 128)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, _, err := decoder.Decode(ctx, 4, []byte{3, 0, 0x80}, 128); done <- err }()
	reviewCandidateWait(t, "missing blocked section", func() bool {
		decoder.mu.Lock()
		defer decoder.mu.Unlock()
		return len(decoder.blocked) == 1
	})
	cause := errors.New("synthetic decoder shutdown")
	decoder.Close(cause)
	select {
	case err := <-done:
		if !errors.Is(err, ErrClosed) || !errors.Is(err, cause) {
			t.Fatal("blocked close cause", err)
		}
	case <-ctx.Done():
		t.Fatal("close left a blocked decoder alive")
	}
	if fields, _, err := decoder.Decode(context.Background(), 8, []byte{2, 0, 0x80}, 128); !errors.Is(err, ErrClosed) || !errors.Is(err, cause) || len(fields) != 0 {
		t.Fatal("ready decode survived terminal close", fields, err)
	}
	decoder.mu.Lock()
	retained := decoder.table.entries != nil || decoder.table.size != 0 || decoder.table.capacity != 0 || len(decoder.blocked) != 0
	decoder.mu.Unlock()
	if retained || len(kept) != 1 || kept[0] != (qpack.HeaderField{Name: "x", Value: "v"}) {
		t.Fatal("close retained table memory or mutated already detached fields")
	}
}

type reviewCandidatePartialReader struct {
	input   *bytes.Reader
	blocked chan struct{}
	release chan struct{}
	once    sync.Once
}

func (reader *reviewCandidatePartialReader) Read(data []byte) (int, error) {
	if reader.input.Len() > 0 {
		return reader.input.Read(data)
	}
	reader.once.Do(func() { close(reader.blocked) })
	<-reader.release
	return 0, io.EOF
}

func TestFathomryCandidateQPACKPartialInstructionNeverHoldsTableLock(t *testing.T) {
	for _, partial := range []struct {
		name string
		data []byte
	}{
		{"literal-value", []byte{0x41, 'y', 5}},
		{"referenced-name-value", []byte{0x80, 5}},
		{"capacity-varint", []byte{0x3f}},
	} {
		t.Run(partial.name, func(t *testing.T) {
			decoder := reviewCandidateDecoder(t, 64, 1, qpack.HeaderField{Name: "x", Value: "v"})
			reader := &reviewCandidatePartialReader{input: bytes.NewReader(partial.data), blocked: make(chan struct{}), release: make(chan struct{})}
			unblock := sync.OnceFunc(func() { close(reader.release) })
			parsed := make(chan error, 1)
			go func() {
				parsed <- decoder.ParseEncoder(reader, func(uint64) error { return errors.New("partial insert committed") })
			}()
			joined := false
			t.Cleanup(func() {
				unblock()
				if !joined {
					select {
					case <-parsed:
					case <-time.After(2 * time.Second):
						t.Error("partial parser did not terminate")
					}
				}
			})
			select {
			case <-reader.blocked:
			case <-time.After(2 * time.Second):
				t.Fatal("fixture did not enter partial instruction read")
			}
			ready := make(chan error, 1)
			go func() {
				fields, _, err := decoder.Decode(context.Background(), 0, []byte{2, 0, 0x80}, 128)
				if err == nil && (len(fields) != 1 || fields[0] != (qpack.HeaderField{Name: "x", Value: "v"})) {
					err = errors.New("ready field changed")
				}
				ready <- err
			}()
			select {
			case err := <-ready:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("partial encoder I/O held the table lock against ready decoding")
			}
			closed := make(chan struct{})
			go func() { decoder.Close(nil); close(closed) }()
			select {
			case <-closed:
			case <-time.After(2 * time.Second):
				t.Fatal("partial encoder I/O held the table lock against Close")
			}
			unblock()
			select {
			case err := <-parsed:
				joined = true
				if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
					t.Fatal("partial instruction had an unexpected terminal result", err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("released instruction reader survived")
			}
		})
	}
}

func TestFathomryCandidateQPACKHuffmanEncodedBytesCanExceedDecodedCapacity(t *testing.T) {
	field := qpack.HeaderField{Name: "x", Value: strings.Repeat("\xff", 64)}
	capacity := uint64(len(field.Name) + len(field.Value) + 32)
	encoded := hpack.AppendHuffmanString(nil, field.Value)
	if uint64(len(encoded)) <= capacity {
		t.Fatal("fixture does not distinguish encoded and decoded limits")
	}
	for _, accepted := range []bool{false, true} {
		name := "decoded-one-byte-over"
		maximum := capacity - 1
		if accepted {
			name, maximum = "legal-long-encoding", capacity
		}
		t.Run(name, func(t *testing.T) {
			decoder := reviewCandidateDecoder(t, maximum, 0)
			inserted := uint64(0)
			err := decoder.ParseEncoder(bytes.NewReader(reviewCandidateInsert(field, true)), func(count uint64) error { inserted = count; return nil })
			if !accepted {
				if !errors.Is(err, ErrLimit) || inserted != 0 || decoder.table.insertCount != 0 || decoder.table.size != 0 {
					t.Fatal("decoded overflow installed an entry", inserted, err)
				}
				return
			}
			if !errors.Is(err, io.EOF) || inserted != 1 || decoder.table.size != maximum {
				t.Fatal("legal encoded literal was rejected by a decoded-byte ceiling", inserted, err)
			}
			fields, required, err := decoder.Decode(context.Background(), 0, []byte{2, 0, 0x80}, maximum)
			if err != nil || required != 1 || len(fields) != 1 || fields[0] != field {
				t.Fatal("exact decoded field limit", required, err)
			}
			if fields, _, err := decoder.Decode(context.Background(), 4, []byte{2, 0, 0x80}, maximum-1); !errors.Is(err, ErrLimit) || len(fields) != 0 {
				t.Fatal("decoded field overflow escaped bound", err)
			}
		})
	}
}
