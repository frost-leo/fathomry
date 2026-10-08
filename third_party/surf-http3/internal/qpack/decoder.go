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
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"sync"

	"github.com/quic-go/qpack"
)

// Decoder is connection-local. Callers serialize each stream's sections and own
// feedback ordering; table strings are immutable and decoded fields are detached.
type Decoder struct {
	mu         sync.Mutex
	table      *dynamicTable
	maxBlocked uint64
	blocked    map[uint64]bool
	changed    chan struct{}
	closed     error
}

func New(capacity, blocked uint64) (*Decoder, error) {
	if capacity > 64<<20 || blocked > 1024 {
		return nil, ErrLimit
	}
	return &Decoder{table: newDynamicTable(capacity), maxBlocked: blocked, blocked: make(map[uint64]bool), changed: make(chan struct{})}, nil
}

func (decoder *Decoder) signal() {
	close(decoder.changed)
	decoder.changed = make(chan struct{})
}

func (decoder *Decoder) Close(reason error) {
	decoder.mu.Lock()
	defer decoder.mu.Unlock()
	if decoder.closed == nil {
		decoder.closed = errors.Join(ErrClosed, reason)
		decoder.table.entries = nil
		decoder.table.size = 0
		decoder.table.capacity = 0
		decoder.blocked = nil
		decoder.signal()
	}
}

func (decoder *Decoder) ParseEncoder(reader io.Reader, inserted func(uint64) error) error {
	input := bufio.NewReader(reader)
	for {
		first, err := input.ReadByte()
		if err != nil {
			return err
		}
		count, changed, err := decoder.instruction(first, input)
		if err != nil {
			return err
		}
		if changed {
			if err := inserted(count); err != nil {
				return err
			}
		}
	}
}

func (decoder *Decoder) instruction(first byte, input *bufio.Reader) (uint64, bool, error) {
	decoder.mu.Lock()
	if decoder.closed != nil {
		err := decoder.closed
		decoder.mu.Unlock()
		return 0, false, err
	}
	capacity := decoder.table.capacity
	if decoder.table.maxCapacity == 0 {
		decoder.mu.Unlock()
		return 0, false, ErrEncoding
	}
	decoder.mu.Unlock()
	var name, value string
	var index uint64
	var err error
	switch {
	case first&0x80 != 0:
		index, err = integer(first, 6, input)
		if err != nil {
			return 0, false, err
		}
		if first&0x40 != 0 {
			field, problem := staticField(index)
			if problem != nil {
				return 0, false, problem
			}
			name = field.Name
		} else {
			decoder.mu.Lock()
			entry, found := decoder.table.atRelative(index)
			decoder.mu.Unlock()
			if !found {
				return 0, false, ErrEncoding
			}
			name = entry.name
		}
		if capacity < 32 || uint64(len(name)) > capacity-32 {
			return 0, false, ErrLimit
		}
		next, problem := input.ReadByte()
		if problem != nil {
			return 0, false, problem
		}
		value, err = literal(next, 7, input, capacity-32-uint64(len(name)))
	case first&0x40 != 0:
		if capacity < 32 {
			return 0, false, ErrLimit
		}
		name, err = literal(first, 5, input, capacity-32)
		if err != nil {
			return 0, false, err
		}
		next, problem := input.ReadByte()
		if problem != nil {
			return 0, false, problem
		}
		value, err = literal(next, 7, input, capacity-32-uint64(len(name)))
	default:
		index, err = integer(first, 5, input)
	}
	if err != nil {
		return 0, false, err
	}
	decoder.mu.Lock()
	defer decoder.mu.Unlock()
	if decoder.closed != nil {
		return 0, false, decoder.closed
	}
	before := decoder.table.insertCount
	switch {
	case first&0xc0 != 0:
		err = decoder.table.insert(name, value)
	case first&0x20 != 0:
		err = decoder.table.setCapacity(index)
	default:
		err = decoder.table.duplicate(index)
	}
	if err != nil {
		return 0, false, err
	}
	changed := before != decoder.table.insertCount
	if changed {
		decoder.signal()
	}
	return decoder.table.insertCount, changed, nil
}

// Decode checks both encoded and decoded field bytes. Its wait uses receive-side
// caller cancellation, not the QUIC stream's completed send-half context.
func (decoder *Decoder) Decode(ctx context.Context, stream uint64, data []byte, limit uint64) ([]qpack.HeaderField, uint64, error) {
	if ctx == nil || limit == 0 || limit > 1<<30 || uint64(len(data)) > limit {
		return nil, 0, ErrLimit
	}
	input := bytes.NewReader(data)
	first, err := input.ReadByte()
	if err != nil {
		return nil, 0, err
	}
	encoded, err := integer(first, 8, input)
	if err != nil {
		return nil, 0, err
	}
	first, err = input.ReadByte()
	if err != nil {
		return nil, 0, err
	}
	delta, err := integer(first, 7, input)
	if err != nil {
		return nil, 0, err
	}
	decoder.mu.Lock()
	defer decoder.mu.Unlock()
	if decoder.closed != nil {
		return nil, 0, decoder.closed
	}
	if ctx.Err() != nil {
		return nil, 0, context.Cause(ctx)
	}
	required, err := requiredCount(encoded, decoder.table.maxCapacity/32, decoder.table.insertCount)
	if err != nil {
		return nil, 0, err
	}
	var base uint64
	if first&0x80 == 0 {
		if delta > maxInteger-required {
			return nil, 0, ErrEncoding
		}
		base = required + delta
	} else {
		if delta >= required {
			return nil, 0, ErrEncoding
		}
		base = required - delta - 1
	}
	if decoder.table.insertCount < required {
		if decoder.blocked[stream] || uint64(len(decoder.blocked)) >= decoder.maxBlocked {
			return nil, 0, ErrEncoding
		}
		decoder.blocked[stream] = true
		defer func() { delete(decoder.blocked, stream) }()
		for decoder.table.insertCount < required && decoder.closed == nil && ctx.Err() == nil {
			changed := decoder.changed
			decoder.mu.Unlock()
			select {
			case <-changed:
			case <-ctx.Done():
			}
			decoder.mu.Lock()
		}
	}
	if ctx.Err() != nil {
		return nil, 0, context.Cause(ctx)
	}
	if decoder.closed != nil {
		return nil, 0, decoder.closed
	}
	fields, referenced, err := decoder.fields(ctx, input, base, required, limit)
	if err != nil {
		return nil, 0, err
	}
	if referenced != required {
		return nil, 0, ErrEncoding
	}
	return fields, required, nil
}

func (decoder *Decoder) fields(ctx context.Context, input *bytes.Reader, base, required, limit uint64) ([]qpack.HeaderField, uint64, error) {
	var fields []qpack.HeaderField
	var size, referenced uint64
	resolve := func(index uint64, static, post bool) (qpack.HeaderField, error) {
		if static {
			return staticField(index)
		}
		var absolute uint64
		if post {
			if index > maxInteger-base {
				return qpack.HeaderField{}, ErrEncoding
			}
			absolute = base + index
		} else {
			if index >= base {
				return qpack.HeaderField{}, ErrEncoding
			}
			absolute = base - index - 1
		}
		if absolute >= required {
			return qpack.HeaderField{}, ErrEncoding
		}
		entry, found := decoder.table.at(absolute)
		if !found {
			return qpack.HeaderField{}, ErrEncoding
		}
		referenced = max(referenced, absolute+1)
		return qpack.HeaderField{Name: entry.name, Value: entry.value}, nil
	}
	for {
		if ctx.Err() != nil {
			return nil, 0, context.Cause(ctx)
		}
		first, err := input.ReadByte()
		if err == io.EOF {
			return fields, referenced, nil
		}
		if err != nil {
			return nil, 0, err
		}
		if limit-size < 32 {
			return nil, 0, ErrLimit
		}
		remaining := limit - size - 32
		var field qpack.HeaderField
		valueLiteral := false
		switch {
		case first&0x80 != 0:
			index, err := integer(first, 6, input)
			if err != nil {
				return nil, 0, err
			}
			field, err = resolve(index, first&0x40 != 0, false)
			if err != nil {
				return nil, 0, err
			}
		case first&0x40 != 0:
			index, err := integer(first, 4, input)
			if err != nil {
				return nil, 0, err
			}
			field, err = resolve(index, first&0x10 != 0, false)
			if err != nil {
				return nil, 0, err
			}
			valueLiteral = true
		case first&0x20 != 0:
			field.Name, err = literal(first, 3, input, remaining)
			if err != nil {
				return nil, 0, err
			}
			valueLiteral = true
		case first&0x10 != 0:
			index, err := integer(first, 4, input)
			if err != nil {
				return nil, 0, err
			}
			field, err = resolve(index, false, true)
			if err != nil {
				return nil, 0, err
			}
		default:
			index, err := integer(first, 3, input)
			if err != nil {
				return nil, 0, err
			}
			field, err = resolve(index, false, true)
			if err != nil {
				return nil, 0, err
			}
			valueLiteral = true
		}
		if uint64(len(field.Name)) > remaining {
			return nil, 0, ErrLimit
		}
		if valueLiteral {
			first, err = input.ReadByte()
			if err != nil {
				return nil, 0, err
			}
			field.Value, err = literal(first, 7, input, remaining-uint64(len(field.Name)))
			if err != nil {
				return nil, 0, err
			}
		}
		count := uint64(len(field.Name)) + uint64(len(field.Value)) + 32
		if count > limit-size {
			return nil, 0, ErrLimit
		}
		size += count
		fields = append(fields, field)
	}
}

// Reconstruction follows the already-selected MIT decoder's absolute-index
// model, with explicit 62-bit bounds before every addition.
func requiredCount(encoded, entries, total uint64) (uint64, error) {
	if encoded == 0 {
		return 0, nil
	}
	if entries == 0 || entries > maxInteger/2 || total > maxInteger-entries {
		return 0, ErrEncoding
	}
	cycle := 2 * entries
	if encoded > cycle {
		return 0, ErrEncoding
	}
	ceiling := total + entries
	wrapped := ceiling / cycle * cycle
	if encoded-1 > maxInteger-wrapped {
		return 0, ErrEncoding
	}
	required := wrapped + encoded - 1
	if required > ceiling {
		if required <= cycle {
			return 0, ErrEncoding
		}
		required -= cycle
	}
	if required == 0 {
		return 0, ErrEncoding
	}
	return required, nil
}

// ReadInstructionInteger is shared by the native decoder-stream validator.
func ReadInstructionInteger(first byte, prefix uint, reader io.ByteReader) (uint64, error) {
	return integer(first, prefix, reader)
}

// AppendInstruction encodes one bounded feedback instruction.
func AppendInstruction(prefix uint, mask byte, value uint64) []byte {
	result := appendInteger(nil, prefix, value)
	result[0] |= mask
	return result
}
