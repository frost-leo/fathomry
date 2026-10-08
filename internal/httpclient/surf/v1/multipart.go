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

package surf

import (
	"context"
	"errors"
	"io"
	"mime"
	"reflect"
	"slices"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/enetx/g"
	http "github.com/enetx/http"
	sdk "github.com/enetx/surf"
)

// MaxMultipartParts is the bounded combined native field/file input count.
const MaxMultipartParts = 128

// Multipart is bounded runtime input, not a durable DTO. Native ordered unique
// fields precede files; repeated field names replace values at their first slot.
type Multipart struct {
	private
	Fields []Field
	Parts  []Part
}
type Field struct {
	private
	Name, Value string
}

// Part borrows exactly one Input or Open. Open returns a fresh independently
// closable reader; a reader plus error still transfers cleanup responsibility.
type Part struct {
	private
	Name, FileName, ContentType string
	Input                       io.ReadCloser
	Open                        func(context.Context) (io.ReadCloser, error)
}

func copyMultipart(input *Multipart, request *http.Request, value settings) (*Multipart, bool, error) {
	if input == nil {
		return nil, false, nil
	}
	if request.Body != nil && request.Body != http.NoBody || request.GetBody != nil || len(input.Fields) > MaxMultipartParts || len(input.Parts) > MaxMultipartParts-len(input.Fields) {
		return nil, false, failure(ErrInput, "multipart")
	}
	result := &Multipart{Fields: slices.Clone(input.Fields), Parts: slices.Clone(input.Parts)}
	seen := make(map[io.ReadCloser]bool)
	replayable := true
	var metadata, fields int64
	for _, field := range result.Fields {
		if field.Name == "" || !utf8.ValidString(field.Name) || !fieldValue(field.Name) {
			return nil, false, failure(ErrInput, "multipart-field")
		}
		metadata += int64(len(field.Name)) + 64
		fields += int64(len(field.Value))
	}
	for _, part := range result.Parts {
		if part.Name == "" || part.FileName == "" || nilLike(part.Input) || (part.Input == nil) == (part.Open == nil) {
			return nil, false, failure(ErrInput, "multipart-part")
		}
		for _, text := range []string{part.Name, part.FileName, part.ContentType} {
			if !utf8.ValidString(text) || !fieldValue(text) {
				return nil, false, failure(ErrInput, "multipart-metadata")
			}
			metadata += int64(len(text)) + 64
		}
		if part.ContentType != "" {
			if _, _, err := mime.ParseMediaType(part.ContentType); err != nil {
				return nil, false, failure(ErrInput, "multipart-content-type", err)
			}
		}
		if part.Input != nil {
			replayable = false
			if reflect.TypeOf(part.Input).Comparable() {
				if seen[part.Input] {
					return nil, false, failure(ErrInput, "multipart-aliased-input")
				}
				seen[part.Input] = true
			}
		}
	}
	if metadata > value.MaxHeaderBytes || fields > value.MaxRequestBytes {
		return nil, false, failure(ErrLimit, "multipart-metadata")
	}
	if !replayable && value.NativeRetries > 0 {
		return nil, false, failure(ErrInput, "multipart-retry-requires-factories")
	}
	return result, replayable, nil
}
func (op *operation) makeMultipart(replay bool) (*sdk.Multipart, error) {
	if replay {
		if !op.work.enter() {
			return nil, failure(ErrState, "multipart-replay")
		}
		defer op.work.leave()
		op.mu.Lock()
		if op.closing || op.replays >= op.client.owner.settings.MaxReplays {
			op.mu.Unlock()
			return nil, failure(ErrLimit, "multipart-replays")
		}
		op.replays++
		op.mu.Unlock()
	}
	result := sdk.NewMultipart()
	for _, field := range op.multipart.Fields {
		result.Field(g.String(field.Name), g.String(field.Value))
	}
	var created []*multipartInput
	for _, part := range op.multipart.Parts {
		ctx, cancel := context.WithCancel(op.ctx)
		input := &multipartInput{op: op, ctx: ctx, cancel: cancel, open: part.Open, ready: make(chan struct{})}
		if part.Input != nil {
			input.started = true
			input.raw = part.Input
			input.initErr = op.rememberMultipart(part.Input)
			close(input.ready)
		}
		op.mu.Lock()
		op.parts = append(op.parts, input)
		op.mu.Unlock()
		created = append(created, input)
		if replay {
			if err := input.prepare(); err != nil {
				var closing sync.WaitGroup
				for _, acquired := range created {
					closing.Go(func() { _ = acquired.Close() })
				}
				closing.Wait()
				return nil, err
			}
		}
		result.FileReader(g.String(part.Name), g.String(part.FileName), input)
		if part.ContentType != "" {
			result.ContentType(g.String(part.ContentType))
		}
	}
	return result, nil
}
func (op *operation) rememberMultipart(input io.ReadCloser) error {
	if !reflect.TypeOf(input).Comparable() {
		return nil
	}
	op.mu.Lock()
	defer op.mu.Unlock()
	if op.seenParts == nil {
		op.seenParts = make(map[io.ReadCloser]bool)
	}
	if op.seenParts[input] {
		return failure(ErrInput, "multipart-aliased-replay")
	}
	op.seenParts[input] = true
	return nil
}

type multipartInput struct {
	op                         *operation
	ctx                        context.Context
	cancel                     context.CancelFunc
	open                       func(context.Context) (io.ReadCloser, error)
	mu                         sync.Mutex
	started, closed            bool
	ready                      chan struct{}
	raw                        io.ReadCloser
	initErr, readErr, closeErr error
	once                       sync.Once
}

func (input *multipartInput) prepare() error {
	input.mu.Lock()
	if input.closed {
		input.mu.Unlock()
		return io.ErrClosedPipe
	}
	if err := input.ctx.Err(); err != nil {
		input.mu.Unlock()
		return errors.Join(err, context.Cause(input.ctx))
	}
	start := !input.started
	input.started = true
	input.mu.Unlock()
	if start {
		var raw io.ReadCloser
		err := invoke("multipart-input-open", func() error { var err error; raw, err = input.open(input.ctx); return err })
		if raw == nil || nilLike(raw) {
			raw = nil
			if err == nil {
				err = failure(ErrInput, "multipart-nil-input")
			}
		}
		if raw != nil {
			if alias := input.op.rememberMultipart(raw); alias != nil {
				raw = nil
				err = errors.Join(err, alias)
			}
		}
		input.mu.Lock()
		input.raw, input.initErr = raw, err
		input.mu.Unlock()
		close(input.ready)
	} else {
		<-input.ready
	}
	input.mu.Lock()
	defer input.mu.Unlock()
	if input.closed {
		return io.ErrClosedPipe
	}
	return input.initErr
}
func (input *multipartInput) Read(buffer []byte) (int, error) {
	if err := input.prepare(); err != nil {
		return 0, err
	}
	input.mu.Lock()
	raw := input.raw
	input.mu.Unlock()
	var count int
	err := invoke("multipart-input-read", func() error { var err error; count, err = raw.Read(buffer); return err })
	if count < 0 || count > len(buffer) {
		count = 0
		err = failure(ErrIntegrity, "multipart-input-count")
	}
	if err != nil && err != io.EOF {
		input.mu.Lock()
		if input.readErr == nil {
			input.readErr = err
		}
		input.mu.Unlock()
	}
	return count, err
}
func (input *multipartInput) Close() error {
	input.once.Do(func() {
		input.cancel()
		input.mu.Lock()
		input.closed = true
		started := input.started
		input.mu.Unlock()
		if started {
			<-input.ready
			input.mu.Lock()
			raw := input.raw
			input.mu.Unlock()
			if raw != nil {
				input.closeErr = invoke("multipart-input-close", raw.Close)
			}
		}
	})
	return input.closeErr
}
func multipartHeader(name string) bool { return strings.EqualFold(name, "Content-Type") }
