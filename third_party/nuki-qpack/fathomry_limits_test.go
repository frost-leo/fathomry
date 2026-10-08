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
	"strings"
	"testing"
	"time"
)

func TestFathomryCloseDropsTableStorage(t *testing.T) {
	for _, ended := range []bool{false, true} {
		t.Run(map[bool]string{false: "live-encoder", true: "finite-encoder-eof"}[ended], func(t *testing.T) {
			decoder := NewDecoder(WithMaxTableCapacity(256))
			defer decoder.Close()
			encoded := append(encSetCapacity(256), encInsertLiteralName("x", strings.Repeat("v", 128))...)
			if ended {
				if err := decoder.ParseEncoderStream(bytes.NewReader(encoded)); err != nil {
					t.Fatal(err)
				}
			} else {
				reader := bufio.NewReader(bytes.NewReader(encoded))
				for range 2 {
					first, err := reader.ReadByte()
					if err != nil {
						t.Fatal(err)
					}
					if err := decoder.parseEncoderInstruction(first, reader); err != nil {
						t.Fatal(err)
					}
				}
			}
			field, err := decoder.Decode([]byte{2, 0, 0x80})()
			if err != nil || field.Name != "x" || len(field.Value) != 128 || decoder.dt.size == 0 {
				t.Fatal("finite encoder input lost its usable dynamic table", field, err)
			}
			cause := decoder.closeErr
			if err := decoder.Close(); err != nil {
				t.Fatal(err)
			}
			if decoder.dt.entries != nil || decoder.dt.size != 0 || decoder.dt.maxCapacity != 256 || decoder.dt.insertCount != 1 {
				t.Fatal("terminal Close retained table storage or changed immutable bounds/counters")
			}
			if cause != nil && !errors.Is(decoder.closeErr, cause) {
				t.Fatal("terminal Close overwrote the encoder cause")
			}
			for _, instruction := range [][]byte{encSetCapacity(256), encInsertLiteralName("late", "value"), encInsertNameRef(true, 0, "value"), encDuplicate(0)} {
				reader := bufio.NewReader(bytes.NewReader(instruction))
				first, err := reader.ReadByte()
				if err != nil {
					t.Fatal(err)
				}
				if err := decoder.parseEncoderInstruction(first, reader); err == nil {
					t.Fatal("instruction repopulated a terminal decoder")
				}
			}
			if decoder.dt.entries != nil || decoder.dt.size != 0 || decoder.dt.insertCount != 1 {
				t.Fatal("terminal decoder reacquired table storage")
			}
		})
	}
}

func TestFathomryStaticBaseSemantics(t *testing.T) {
	for _, base := range []byte{0, 1, 126} {
		field, err := NewDecoder().Decode([]byte{0, base, 0xd9})()
		if err != nil || field.Name != ":status" {
			t.Fatal("valid static Base rejected", base, field, err)
		}
	}
	if _, err := NewDecoder().Decode([]byte{0, 0x80, 0xd9})(); err == nil || err == io.EOF {
		t.Fatal("negative static Base admitted")
	}
}
func TestFathomryBlockedLimitReleasesOnCancel(t *testing.T) {
	decoder := NewDecoder(WithMaxTableCapacity(64), WithMaxBlockedStreams(1))
	defer decoder.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := decoder.DecodeForStreamContext(ctx, 4, []byte{2, 0, 0x80}, 1024)(); done <- err }()
	until := time.NewTimer(time.Second)
	defer until.Stop()
	poll := time.NewTicker(time.Millisecond)
	defer poll.Stop()
	for decoder.BlockedStreams() != 1 {
		select {
		case <-poll.C:
		case <-until.C:
			t.Fatal("first section never blocked")
		}
	}
	other, otherCancel := context.WithTimeout(context.Background(), time.Second)
	defer otherCancel()
	if _, err := decoder.DecodeForStreamContext(other, 8, []byte{2, 0, 0x80}, 1024)(); err == nil || errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("advertised blocked limit was not enforced", err)
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal("blocked cancellation lost", err)
		}
	case <-until.C:
		t.Fatal("blocked decoder remained alive")
	}
	if decoder.BlockedStreams() != 0 {
		t.Fatal("canceled section kept blocked reservation")
	}
}
