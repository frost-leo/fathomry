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
	"mime/multipart"
	"reflect"
	"strings"
	"sync"

	"github.com/enetx/g"
	"github.com/enetx/surf/header"
)

type fathomryMultipart struct {
	mu       sync.Mutex
	replay   func() (*Multipart, error)
	track    func(io.ReadCloser) io.ReadCloser
	boundary string
	previous io.ReadCloser
}

// FathomryMultipart uses the native encoder without materialization. FileReader
// inputs must be closable, independently owned and immutable after transfer.
// replay must acquire fresh inputs before returning; track registers EVERY body,
// including redirect replays that a callback may decline to send. No file paths
// or native Retry buffering are admitted through this controlled entry.
func (req *Request) FathomryMultipart(value *Multipart, replay func() (*Multipart, error), track func(io.ReadCloser) io.ReadCloser) *Request {
	if req.err != nil {
		return req
	}
	if value == nil || track == nil || value.retry {
		req.err = errors.New("surf: invalid controlled multipart")
		return req
	}
	req.multipart = value
	req.managedMultipart = &fathomryMultipart{replay: replay, track: track}
	return req
}
func (req *Request) prepareFathomryMultipart() {
	selection := req.managedMultipart
	body, contentType, boundary, err := req.multipart.prepareManaged(req.request.Context(), req.cli.fathomry, req.cli.boundary)
	if err != nil {
		req.err = err
		return
	}
	selection.boundary = boundary
	req.request.Body = selection.track(body)
	selection.previous = req.request.Body
	req.request.Header.Set(header.CONTENT_TYPE, contentType)
	if selection.replay != nil {
		req.request.GetBody = func() (io.ReadCloser, error) {
			selection.mu.Lock()
			defer selection.mu.Unlock()
			if err := selection.previous.Close(); err != nil {
				return nil, err
			}
			value, err := selection.replay()
			if err != nil {
				return nil, err
			}
			if value == nil || value.retry {
				return nil, errors.New("surf: invalid multipart replay")
			}
			body, _, _, err := value.prepareManaged(req.request.Context(), req.cli.fathomry, func() g.String { return g.String(boundary) })
			if err != nil {
				return nil, err
			}
			selection.previous = selection.track(body)
			return selection.previous, nil
		}
	} else {
		req.request.GetBody = nil
	}
}
func (req *Request) validateFathomryMultipart() error {
	var content []string
	for name, values := range req.request.Header {
		if strings.EqualFold(name, header.CONTENT_TYPE) {
			content = append(content, values...)
		}
	}
	if len(content) != 1 {
		return errors.New("surf: multipart content type is ambiguous")
	}
	kind, parameters, err := mime.ParseMediaType(content[0])
	if err != nil || kind != "multipart/form-data" || parameters["boundary"] != req.managedMultipart.boundary {
		return errors.New("surf: multipart boundary changed")
	}
	return nil
}

type fathomryMultipartBody struct {
	reader      *io.PipeReader
	writer      *io.PipeWriter
	inputs      []io.Closer
	produced    chan struct{}
	done        chan struct{}
	once        sync.Once
	err         error
	producerErr error
	stop        func() bool
	release     func()
}

func (body *fathomryMultipartBody) Read(buffer []byte) (int, error) { return body.reader.Read(buffer) }

// FathomryProducerError is available after Close and reports encoding/input
// termination separately from an early response or the caller's cleanup wait.
func (body *fathomryMultipartBody) FathomryProducerError() error {
	<-body.produced
	return body.producerErr
}
func (body *fathomryMultipartBody) Close() error {
	body.once.Do(func() {
		body.stop()
		_ = body.reader.Close()
		_ = body.writer.CloseWithError(io.ErrClosedPipe)
		body.err = closeMultipartInputs(body.inputs)
		<-body.produced
		body.release()
		close(body.done)
	})
	<-body.done
	return body.err
}
func closeMultipartInputs(inputs []io.Closer) error {
	var pending sync.WaitGroup
	errorsByInput := make([]error, len(inputs))
	for index, input := range inputs {
		pending.Go(func() { errorsByInput[index] = fathomryInvoke(input.Close) })
	}
	pending.Wait()
	return errors.Join(errorsByInput...)
}
func (value *Multipart) prepareManaged(ctx context.Context, state *fathomryState, boundary func() g.String) (io.ReadCloser, string, string, error) {
	var inputs []io.Closer
	seen := make(map[io.Closer]bool)
	var invalid error
	for _, file := range value.files {
		if file == nil || file.file != nil {
			invalid = errors.New("surf: controlled multipart requires reader inputs")
			continue
		}
		input, ok := file.reader.(io.Closer)
		if !ok || fathomryNil(input) {
			invalid = errors.New("surf: controlled multipart input is not closable")
			continue
		}
		if reflect.TypeOf(input).Comparable() {
			if seen[input] {
				invalid = errors.New("surf: aliased multipart input")
				continue
			}
			seen[input] = true
		}
		inputs = append(inputs, input)
	}
	if invalid != nil {
		return nil, "", "", fathomryFailure(invalid, closeMultipartInputs(inputs))
	}
	release, err := state.enter(ctx)
	if err != nil {
		return nil, "", "", fathomryFailure(err, closeMultipartInputs(inputs))
	}
	reader, pipe := io.Pipe()
	encoder := multipart.NewWriter(pipe)
	if boundary != nil {
		err = fathomryInvoke(func() error { return encoder.SetBoundary(boundary().Std()) })
	}
	if err != nil {
		_ = reader.Close()
		_ = pipe.Close()
		cleanup := closeMultipartInputs(inputs)
		release()
		return nil, "", "", fathomryFailure(err, cleanup)
	}
	body := &fathomryMultipartBody{reader: reader, writer: pipe, inputs: inputs, produced: make(chan struct{}), done: make(chan struct{}), release: release}
	// Registration is serialized before cancellation can call Close.
	ready := make(chan struct{})
	body.stop = context.AfterFunc(ctx, func() { <-ready; _ = body.Close() })
	close(ready)
	go func() {
		err := fathomryInvoke(func() error { return value.writeMultipart(encoder) })
		body.producerErr = err
		_ = pipe.CloseWithError(err)
		close(body.produced)
	}()
	return body, encoder.FormDataContentType(), encoder.Boundary(), nil
}
