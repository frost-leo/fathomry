/*
fathomry
Copyright (C) 2026  Frost Leo
SPDX-License-Identifier: GPL-3.0-or-later

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.
*/

package mail

import (
	"bytes"
	"context"
	"errors"
	ht "html/template"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	tt "text/template"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
)

// Input selects exactly one byte, borrowed Reader, explicitly authorized path,
// or FS/name source. Bytes nil with no other selection is an empty input.
// Owned files are always closed. Borrowed readers remain caller-owned and must
// cooperate with cancellation; their actual return, not a deadline, ends use.
type Input struct {
	private
	Bytes  []byte
	Reader io.Reader
	Write  func(context.Context, io.Writer) error
	Path   string
	FS     fs.FS
	Name   string
}

// FileInput adds a bounded attachment or CID resource during admitted preparation.
type FileInput struct {
	private
	Input  Input
	File   Attachment
	Inline bool
}

// PartInput acquires one body/alternative through the same bounded input routes.
type PartInput struct {
	private
	Input Input
	Part  Part
}

// TemplateInput executes exactly one supplied template. Name empty executes its
// root. File nil adds a body part; a non-nil File adds an attachment or CID asset.
// Template functions/data are borrowed until return and must not mutate concurrently.
type TemplateInput struct {
	private
	Text   *tt.Template
	HTML   *ht.Template
	Name   string
	Data   any
	Part   Part
	File   *Attachment
	Inline bool
}

// Transform receives detached content, in FIFO order. Returned data is checked
// and frozen before the next step. No native owner or source authority escapes.
type Transform struct {
	private
	Name  string
	Apply func(context.Context, Content) (Content, error)
}

// Preparation is admitted message work, distinct from offline source preparation.
// Each stage must cooperate and return before its work/evidence can release.
type Preparation struct {
	private
	Files         []FileInput
	Parts         []PartInput
	Templates     []TemplateInput
	Transforms    []Transform
	SkipTransform string
	DKIM          *DKIM
	SMIME         *SMIME
}

func readInput(ctx context.Context, input Input, limit int) (data []byte, err error) {
	if input.Reader != nil && nilInterface(input.Reader) || input.FS != nil && nilInterface(input.FS) {
		return nil, failure(ErrInput, "nil-input")
	}
	count := 0
	if input.Bytes != nil {
		count++
	}
	if input.Reader != nil {
		count++
	}
	if input.Write != nil {
		count++
	}
	if input.Path != "" {
		count++
	}
	if input.FS != nil {
		count++
	}
	if count > 1 || input.FS == nil && input.Name != "" {
		return nil, failure(ErrInput, "input-source")
	}
	if ctx.Err() != nil {
		return nil, errors.Join(ctx.Err(), context.Cause(ctx))
	}
	if input.Write != nil {
		writer := &scopedWriter{writer: &boundedWriter{ctx: ctx, limit: limit}}
		err := input.Write(ctx, writer)
		writer.mu.Lock()
		writer.closed = true
		data := bytes.Clone(writer.writer.buffer.Bytes())
		err = errors.Join(err, writer.err, ctx.Err(), context.Cause(ctx))
		writer.mu.Unlock()
		return data, err
	}
	reader := input.Reader
	var closer io.Closer
	if input.Path != "" {
		if !filepath.IsAbs(input.Path) {
			return nil, failure(ErrInput, "input-path")
		}
		file, openErr := os.Open(input.Path)
		if openErr != nil {
			return nil, openErr
		}
		reader, closer = file, file
	}
	if input.FS != nil {
		if !fs.ValidPath(input.Name) {
			return nil, failure(ErrInput, "fs-name")
		}
		file, openErr := input.FS.Open(input.Name)
		if file != nil {
			closer = file
		}
		if openErr != nil {
			if closer != nil {
				if closeErr := closer.Close(); closeErr != nil {
					openErr = &inputCloseError{primary: openErr, cleanup: closeErr}
				}
			}
			return nil, openErr
		}
		if file == nil {
			return nil, failure(ErrInput, "fs-open")
		}
		reader = file
	}
	if closer != nil {
		defer func() {
			if closeErr := closer.Close(); closeErr != nil {
				err = &inputCloseError{primary: err, cleanup: closeErr}
			}
		}()
	}
	if reader == nil {
		if len(input.Bytes) > limit {
			return nil, failure(ErrLimit, "input")
		}
		return bytes.Clone(input.Bytes), nil
	}
	writer := &boundedWriter{ctx: ctx, limit: limit}
	// LimitReader bounds acquisition even if a foreign Reader ignores context.
	_, err = io.Copy(writer, io.LimitReader(reader, int64(limit)+1))
	if err == nil && ctx.Err() != nil {
		err = errors.Join(ctx.Err(), context.Cause(ctx))
	}
	return writer.buffer.Bytes(), err
}

type inputCloseError struct{ primary, cleanup error }

func (*inputCloseError) Error() string       { return "mail: input cleanup failed" }
func (err *inputCloseError) Unwrap() []error { return []error{err.primary, err.cleanup} }

func (client *Client) PrepareMessage(ctx context.Context, id fault.Correlation, message Message, plan Preparation) (*invocation.Receipt[Result], error) {
	if message.content == nil || len(plan.Files) > 32 || len(plan.Parts) > 32 || len(plan.Templates) > 32 || len(plan.Transforms) > 32 {
		return nil, failure(ErrInput, "prepare-message")
	}
	return client.execute(ctx, id, "prepare-message", func(work context.Context) (Result, error, error) {
		limits := client.owner.settings
		if message.bytes > limits.MaxMessageBytes {
			return Result{}, failure(ErrLimit, "message"), nil
		}
		if message.wire != nil && (len(plan.Files)+len(plan.Parts)+len(plan.Templates)+len(plan.Transforms) != 0 || plan.SMIME != nil) {
			return Result{}, failure(ErrInput, "frozen-mime-transform"), nil
		}
		current := message
		addFile := func(file Attachment, inline bool) error {
			content := current.ContentCopy()
			if inline {
				content.Inline = append(content.Inline, Inline{ID: file.ID, Name: file.Name, ContentType: file.ContentType, Data: file.Data, Encoding: file.Encoding, Description: file.Description, Fields: file.Fields})
			} else {
				content.Attachments = append(content.Attachments, file)
			}
			frozen, err := NewMessage(content)
			if err == nil && frozen.bytes > limits.MaxMessageBytes {
				err = failure(ErrLimit, "message")
			}
			if err == nil {
				current = frozen
			}
			return err
		}
		for _, file := range plan.Files {
			data, err := readInput(work, file.Input, limits.MaxMessageBytes-current.bytes)
			if err != nil {
				return Result{}, err, nil
			}
			value := file.File
			value.Data = data
			if err := addFile(value, file.Inline); err != nil {
				return Result{}, err, nil
			}
		}
		for _, input := range plan.Parts {
			data, err := readInput(work, input.Input, limits.MaxMessageBytes-current.bytes)
			if err != nil {
				return Result{}, err, nil
			}
			content := current.ContentCopy()
			promoteBodies(&content)
			part := input.Part
			part.Data = data
			content.Parts = append(content.Parts, part)
			current, err = NewMessage(content)
			if err != nil {
				return Result{}, err, nil
			}
			if current.bytes > limits.MaxMessageBytes {
				return Result{}, failure(ErrLimit, "message"), nil
			}
		}
		for _, input := range plan.Templates {
			if (input.Text == nil) == (input.HTML == nil) {
				return Result{}, failure(ErrInput, "template"), nil
			}
			writer := &boundedWriter{ctx: work, limit: limits.MaxMessageBytes - current.bytes}
			var err error
			if input.Text != nil {
				if input.Name == "" {
					err = input.Text.Execute(writer, input.Data)
				} else {
					err = input.Text.ExecuteTemplate(writer, input.Name, input.Data)
				}
			} else {
				if input.Name == "" {
					err = input.HTML.Execute(writer, input.Data)
				} else {
					err = input.HTML.ExecuteTemplate(writer, input.Name, input.Data)
				}
			}
			if err != nil {
				return Result{}, err, nil
			}
			if input.File != nil {
				file := *input.File
				file.Data = writer.buffer.Bytes()
				if err = addFile(file, input.Inline); err != nil {
					return Result{}, err, nil
				}
			} else {
				content := current.ContentCopy()
				if len(content.Parts) == 0 {
					if content.Text != "" {
						content.Parts = append(content.Parts, Part{ContentType: "text/plain", Data: []byte(content.Text)})
					}
					if content.HTML != "" {
						content.Parts = append(content.Parts, Part{ContentType: "text/html", Data: []byte(content.HTML)})
					}
					content.Text, content.HTML = "", ""
				}
				part := input.Part
				part.Data = writer.buffer.Bytes()
				if part.ContentType == "" {
					part.ContentType = "text/plain"
					if input.HTML != nil {
						part.ContentType = "text/html"
					}
				}
				content.Parts = append(content.Parts, part)
				current, err = NewMessage(content)
				if err != nil {
					return Result{}, err, nil
				}
			}
		}
		names := map[string]bool{}
		for _, transform := range plan.Transforms {
			if transform.Name == "" || !token(transform.Name, 64) || names[transform.Name] || transform.Apply == nil {
				return Result{}, failure(ErrInput, "transform"), nil
			}
			names[transform.Name] = true
			if transform.Name == plan.SkipTransform {
				continue
			}
			if work.Err() != nil {
				return Result{}, work.Err(), nil
			}
			content, err := transform.Apply(work, current.ContentCopy())
			if err != nil {
				return Result{}, err, nil
			}
			current, err = NewMessage(content)
			if err != nil {
				return Result{}, err, nil
			}
			if current.bytes > limits.MaxMessageBytes {
				return Result{}, failure(ErrLimit, "message"), nil
			}
		}
		wire, err := compose(work, current, limits)
		if err != nil {
			return Result{}, err, nil
		}
		if plan.SMIME != nil {
			wire, err = plan.SMIME.sign(work, wire, limits.MaxMIMEBytes, "")
			if err != nil {
				return Result{}, err, nil
			}
		}
		if plan.DKIM != nil {
			if current.unsigned != nil {
				wire = current.unsigned
			}
			current.unsigned = bytes.Clone(wire)
			wire, err = plan.DKIM.sign(work, wire, limits.MaxMIMEBytes)
		}
		if err != nil {
			return Result{}, err, nil
		}
		if work.Err() != nil {
			return Result{}, work.Err(), nil
		}
		current.wire = bytes.Clone(wire)
		return Result{message: current}, nil, nil
	})
}

func promoteBodies(content *Content) {
	if len(content.Parts) != 0 {
		return
	}
	if content.Text != "" {
		content.Parts = append(content.Parts, Part{ContentType: "text/plain", Data: []byte(content.Text)})
	}
	if content.HTML != "" {
		content.Parts = append(content.Parts, Part{ContentType: "text/html", Data: []byte(content.HTML)})
	}
	content.Text, content.HTML = "", ""
}

type scopedWriter struct {
	mu     sync.Mutex
	writer *boundedWriter
	closed bool
	err    error
}

func (writer *scopedWriter) Write(data []byte) (int, error) {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	if writer.closed {
		return 0, failure(ErrInput, "closed-writer")
	}
	if writer.err != nil {
		return 0, writer.err
	}
	count, err := writer.writer.Write(data)
	writer.err = err
	return count, err
}

// Export writes prepared or newly composed MIME to a borrowed writer. It never
// closes that writer; partial counts/errors remain independently observable.
func (client *Client) Export(ctx context.Context, id fault.Correlation, message Message, writer io.Writer) (*invocation.Receipt[Result], error) {
	if message.content == nil || nilInterface(writer) {
		return nil, failure(ErrInput, "export")
	}
	return client.execute(ctx, id, "export", func(work context.Context) (Result, error, error) {
		wire, err := compose(work, message, client.owner.settings)
		if err != nil {
			return Result{}, err, nil
		}
		count, err := writer.Write(bytes.Clone(wire))
		if count < 0 || count > len(wire) {
			return Result{}, failure(ErrInput, "writer-count"), nil
		}
		if err == nil && count != len(wire) {
			err = io.ErrShortWrite
		}
		return Result{written: int64(count)}, errors.Join(err, work.Err(), context.Cause(work)), nil
	})
}
