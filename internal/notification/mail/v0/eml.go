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
	"encoding/base64"
	"errors"
	"io"
	"mime"
	"mime/multipart"
	"mime/quotedprintable"
	stdmail "net/mail"
	"net/textproto"
	"slices"
	"sort"
	"strings"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
)

// ImportOptions overrides only the envelope, never visible MIME recipients.
// Empty From/nil Recipients derive from headers; "<>" selects a null sender.
type ImportOptions struct {
	private
	From       string
	Recipients []string
	DKIM       *DKIM
	SMIME      *SMIME
}

// HeaderChange reports top-level changes without a second private value copy.
type HeaderChange struct {
	private
	Name                    string
	Dropped, Added, Changed bool
}

func (result Result) HeaderChanges() []HeaderChange { return slices.Clone(result.changes) }

// Import uses the selected native retained-header profile with bounded standard
// MIME parsing, avoiding native filename slicing panics and stale boundaries.
// Reply-To/unknown headers have explicit loss reporting; arbitrary signed-message
// or byte-for-byte round-tripping is not promised.
func (client *Client) Import(ctx context.Context, id fault.Correlation, input Input, options ImportOptions) (*invocation.Receipt[Result], error) {
	if len(options.Recipients) > 100 {
		return nil, failure(ErrLimit, "import-envelope")
	}
	return client.execute(ctx, id, "import", func(work context.Context) (Result, error, error) {
		limits := client.owner.settings
		raw, err := readInput(work, input, limits.MaxMessageBytes)
		if err != nil {
			return Result{}, err, nil
		}
		before, err := stdmail.ReadMessage(bytes.NewReader(raw))
		if err != nil {
			return Result{}, err, nil
		}
		content, err := importContent(work, before, limits.MaxMessageBytes)
		if err != nil {
			return Result{}, err, nil
		}
		content.EnvelopeFrom = options.From
		message, err := NewMessage(content)
		if err != nil {
			return Result{}, err, nil
		}
		if options.Recipients != nil {
			message.recipients = make([]string, len(options.Recipients))
			for index, addr := range options.Recipients {
				message.recipients[index], err = address(addr)
				if err != nil {
					return Result{}, err, nil
				}
			}
		}
		wire, err := compose(work, message, limits)
		if err != nil {
			return Result{}, err, nil
		}
		if options.SMIME != nil {
			wire, err = options.SMIME.sign(work, wire, limits.MaxMIMEBytes, "")
			if err != nil {
				return Result{}, err, nil
			}
		}
		if options.DKIM != nil {
			message.unsigned = bytes.Clone(wire)
			wire, err = options.DKIM.sign(work, wire, limits.MaxMIMEBytes)
			if err != nil {
				return Result{}, err, nil
			}
		}
		after, err := stdmail.ReadMessage(bytes.NewReader(wire))
		if err != nil {
			return Result{}, err, nil
		}
		message.wire = bytes.Clone(wire)
		return Result{message: message, changes: headerChanges(before.Header, after.Header)}, nil, nil
	})
}
func importContent(ctx context.Context, message *stdmail.Message, limit int) (Content, error) {
	var result Content
	if err := validateEMLHeaders(textproto.MIMEHeader(message.Header)); err != nil {
		return result, err
	}
	addresses := func(name string) ([]string, error) {
		var result []string
		for _, value := range message.Header[textproto.CanonicalMIMEHeaderKey(name)] {
			if value == "" {
				continue
			}
			parsed, err := stdmail.ParseAddressList(value)
			if err != nil {
				return nil, err
			}
			if len(parsed) > 100-len(result) {
				return nil, failure(ErrLimit, "eml-addresses")
			}
			for _, addr := range parsed {
				result = append(result, addr.String())
			}
		}
		return result, nil
	}
	from, err := addresses("From")
	if err != nil {
		return result, err
	}
	if len(from) > 1 {
		return result, failure(ErrInput, "eml-from")
	}
	if len(from) == 1 {
		result.From = from[0]
	}
	if result.To, err = addresses("To"); err != nil {
		return result, err
	}
	if result.Cc, err = addresses("Cc"); err != nil {
		return result, err
	}
	if result.Bcc, err = addresses("Bcc"); err != nil {
		return result, err
	}
	if date, err := message.Header.Date(); err == nil {
		result.Date = date
	} else if !errors.Is(err, stdmail.ErrHeaderNotPresent) {
		return result, err
	}
	result.ID = strings.Trim(message.Header.Get("Message-ID"), "<>")
	result.Subject = message.Header.Get("Subject")
	result.SubjectPreformatted = true
	for _, name := range []string{"Importance", "In-Reply-To", "List-Unsubscribe", "List-Unsubscribe-Post", "Organization", "Precedence", "Priority", "References", "User-Agent", "X-Mailer", "X-MsMail-Priority", "X-Priority"} {
		if value := message.Header.Get(name); value != "" {
			result.Fields = append(result.Fields, Header{Name: name, Values: []string{value}, Preformatted: true})
		}
	}
	count, remaining := 0, limit
	var visit func(textproto.MIMEHeader, io.Reader, int) error
	visit = func(headers textproto.MIMEHeader, body io.Reader, depth int) error {
		count++
		if count > 64 || depth > 8 {
			return failure(ErrLimit, "eml-structure")
		}
		if err := validateEMLHeaders(headers); err != nil {
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		contentType := headers.Get("Content-Type")
		if contentType == "" {
			contentType = "text/plain; charset=US-ASCII"
		}
		media, params, err := mime.ParseMediaType(contentType)
		if err != nil {
			return err
		}
		if strings.HasPrefix(media, "multipart/") {
			if params["boundary"] == "" {
				return failure(ErrInput, "eml-boundary")
			}
			reader := multipart.NewReader(body, params["boundary"])
			for {
				part, err := reader.NextRawPart()
				if err == io.EOF {
					return nil
				}
				if err != nil {
					return err
				}
				if err = visit(part.Header, part, depth+1); err != nil {
					return err
				}
			}
		}
		encoding := strings.ToLower(headers.Get("Content-Transfer-Encoding"))
		if encoding == "" {
			encoding = "7bit"
		}
		switch encoding {
		case "base64":
			body = base64.NewDecoder(base64.StdEncoding, body)
		case "quoted-printable":
			body = quotedprintable.NewReader(body)
		case "7bit", "8bit":
		default:
			return failure(ErrUnsupported, "eml-encoding")
		}
		writer := &boundedWriter{ctx: ctx, limit: remaining}
		if _, err = io.Copy(writer, body); err != nil {
			return err
		}
		data := writer.buffer.Bytes()
		remaining -= len(data)
		disposition, attributes := "", map[string]string{}
		if value := headers.Get("Content-Disposition"); value != "" {
			disposition, attributes, err = mime.ParseMediaType(value)
			if err != nil {
				return err
			}
		}
		if disposition == "attachment" || disposition == "inline" {
			name := attributes["filename"]
			if name == "" {
				name = params["name"]
			}
			if name == "" {
				name = "generic.attachment"
			}
			delete(params, "name")
			delete(params, "filename")
			delete(params, "boundary")
			file := Attachment{Name: name, ContentType: mime.FormatMediaType(media, params), Data: data, Encoding: encoding, ID: strings.Trim(headers.Get("Content-ID"), "<>"), Description: headers.Get("Content-Description")}
			if disposition == "inline" {
				if file.ID == "" {
					file.ID = file.Name
				}
				result.Inline = append(result.Inline, Inline{Name: file.Name, ContentType: file.ContentType, Data: file.Data, Encoding: file.Encoding, ID: file.ID, Description: file.Description})
			} else {
				result.Attachments = append(result.Attachments, file)
			}
		} else {
			charset := params["charset"]
			delete(params, "charset")
			result.Parts = append(result.Parts, Part{ContentType: mime.FormatMediaType(media, params), Charset: charset, Encoding: encoding, Description: headers.Get("Content-Description"), Data: data})
		}
		return nil
	}
	if err = visit(textproto.MIMEHeader(message.Header), message.Body, 0); err != nil {
		return Content{}, err
	}
	return result, nil
}
func validateEMLHeaders(header textproto.MIMEHeader) error {
	if len(header) > 64 {
		return failure(ErrLimit, "eml-header")
	}
	total := 0
	for key, values := range header {
		total += len(key)
		if !fieldName(key) || len(values) > 32 {
			return failure(ErrLimit, "eml-header")
		}
		for _, value := range values {
			total += len(value)
			if !line(value, 4096) {
				return failure(ErrInput, "eml-header")
			}
		}
	}
	if total > 64<<10 {
		return failure(ErrLimit, "eml-header")
	}
	return nil
}
func headerChanges(before, after stdmail.Header) []HeaderChange {
	names := map[string]bool{}
	for name := range before {
		names[name] = true
	}
	for name := range after {
		names[name] = true
	}
	var changed []HeaderChange
	for name := range names {
		original, retained := before[name], after[name]
		if !slices.Equal(original, retained) {
			changed = append(changed, HeaderChange{Name: name, Dropped: len(retained) == 0, Added: len(original) == 0, Changed: len(original) > 0 && len(retained) > 0})
		}
	}
	sort.Slice(changed, func(i, j int) bool { return changed[i].Name < changed[j].Name })
	return changed
}
