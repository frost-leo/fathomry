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

package mail

import (
	"bytes"
	"context"
	"encoding/base64"
	"mime"
	stdmail "net/mail"
	"strings"
	"unicode"
	"unicode/utf8"

	sdk "github.com/wneessen/go-mail"
)

// Content is borrowed only by NewMessage; callers must not mutate it concurrently.
// ID is an explicit bare ASCII Message-ID (without brackets), not a deduplication
// guarantee. Address fields accept one mailbox with an optional UTF-8 display
// name per entry. EnvelopeFrom is a bare mailbox, defaulting to From; null reverse
// paths and non-ASCII envelope addresses are not supported in this profile.
//
// Text and HTML are UTF-8: at least one must be nonempty; both produce MIME
// alternatives. HTML is explicitly caller-authored content, not sanitized or
// executed by this package. Callers own CID references, image/attachment validity,
// remote-content/privacy policy, accessibility and recipient rendering. Headers
// permits only bounded custom X- headers, never standard/envelope overrides.
type Content struct {
	private
	ID, From, EnvelopeFrom, ReplyTo, Subject, Text, HTML string
	To, Cc, Bcc                                          []string
	Headers                                              map[string]string
	Inline                                               []Inline
	Attachments                                          []Attachment
}

// Inline supplies a MIME-related resource and bare Content-ID, unique within the
// message. Name is a leaf filename; ContentType is explicit. Data is copied by
// NewMessage; nil and empty both mean an empty resource. MIME transport does not
// validate file formats or supply a chart renderer.
type Inline struct {
	private
	ID, Name, ContentType string
	Data                  []byte
}

// Attachment is a caller-provided file, copied by NewMessage. Name is a leaf
// filename and ContentType an explicit MIME media type. Filename parameters are
// Provider-owned; ContentType cannot set name, filename or boundary. Nil and empty Data both
// mean an empty file. No file path, reader, callback or native File escapes.
type Attachment struct {
	private
	Name, ContentType string
	Data              []byte
}

// Message is an immutable concurrently readable input; copies share frozen data.
// NewMessage enforces absolute limits before copying; source-specific limits and
// encoded MIME bounds are enforced under admission before sending.
type Message struct {
	private
	content    *Content
	sender     string
	recipients []string
	bytes      int
}

// NewMessage freezes at most 16 MiB total input, 100 recipients, 32 custom headers,
// 16 inline resources and 16 attachments. It does not allocate a native Msg or
// perform I/O. Caller-owned frozen messages are outside the source's call quota.
func NewMessage(input Content) (Message, error) {
	if !identifier(input.ID) || !line(input.Subject, 998) || input.Subject == "" || strings.TrimSpace(input.Subject) != input.Subject ||
		input.Text == "" && input.HTML == "" ||
		len(input.To)+len(input.Cc)+len(input.Bcc) < 1 || len(input.To)+len(input.Cc)+len(input.Bcc) > 100 ||
		len(input.Inline) > 16 || len(input.Attachments) > 16 || len(input.Headers) > 32 {
		return Message{}, failure(ErrInput, "message")
	}
	total := 0
	add := func(size int) bool {
		if size > 16<<20-total {
			return false
		}
		total += size
		return true
	}
	for _, value := range []string{input.ID, input.From, input.EnvelopeFrom, input.ReplyTo, input.Subject, input.Text, input.HTML} {
		if !add(len(value)) {
			return Message{}, failure(ErrLimit, "message")
		}
	}
	if !utf8.ValidString(input.Text) || !utf8.ValidString(input.HTML) ||
		strings.ContainsRune(input.Text, 0) || strings.ContainsRune(input.HTML, 0) {
		return Message{}, failure(ErrInput, "body")
	}
	sender, err := address(input.From)
	if err != nil {
		return Message{}, err
	}
	if input.EnvelopeFrom != "" {
		parsed, parseErr := address(input.EnvelopeFrom)
		if parseErr != nil || parsed != input.EnvelopeFrom {
			return Message{}, failure(ErrInput, "envelope")
		}
		sender = parsed
	}
	if input.ReplyTo != "" {
		if _, err = address(input.ReplyTo); err != nil {
			return Message{}, err
		}
	}
	recipients := make([]string, 0, len(input.To)+len(input.Cc)+len(input.Bcc))
	seen := map[string]bool{}
	for _, group := range [][]string{input.To, input.Cc, input.Bcc} {
		for _, value := range group {
			if !add(len(value)) {
				return Message{}, failure(ErrLimit, "message")
			}
			mailbox, parseErr := address(value)
			if parseErr != nil {
				return Message{}, parseErr
			}
			if seen[mailbox] {
				return Message{}, failure(ErrInput, "duplicate-recipient")
			}
			seen[mailbox] = true
			recipients = append(recipients, mailbox)
		}
	}
	headerNames := map[string]bool{}
	for name, value := range input.Headers {
		if !headerName(name) || !line(value, 998) {
			return Message{}, failure(ErrInput, "header")
		}
		lower := strings.ToLower(name)
		if headerNames[lower] {
			return Message{}, failure(ErrInput, "header")
		}
		headerNames[lower] = true
		if !add(len(name)) || !add(len(value)) {
			return Message{}, failure(ErrLimit, "headers")
		}
	}
	ids := map[string]bool{}
	for _, inline := range input.Inline {
		if !identifier(inline.ID) || ids[inline.ID] || !fileMetadata(inline.Name, inline.ContentType) {
			return Message{}, failure(ErrInput, "inline")
		}
		ids[inline.ID] = true
		for _, size := range []int{len(inline.ID), len(inline.Name), len(inline.ContentType), len(inline.Data)} {
			if !add(size) {
				return Message{}, failure(ErrLimit, "inline")
			}
		}
	}
	names := map[string]bool{}
	for _, attachment := range input.Attachments {
		if !fileMetadata(attachment.Name, attachment.ContentType) || names[attachment.Name] {
			return Message{}, failure(ErrInput, "attachment")
		}
		names[attachment.Name] = true
		for _, size := range []int{len(attachment.Name), len(attachment.ContentType), len(attachment.Data)} {
			if !add(size) {
				return Message{}, failure(ErrLimit, "attachment")
			}
		}
	}
	input.To = append([]string(nil), input.To...)
	input.Cc = append([]string(nil), input.Cc...)
	input.Bcc = append([]string(nil), input.Bcc...)
	headers := make(map[string]string, len(input.Headers))
	for name, value := range input.Headers {
		headers[name] = value
	}
	input.Headers = headers
	input.Inline = append([]Inline(nil), input.Inline...)
	for index := range input.Inline {
		input.Inline[index].Data = bytes.Clone(input.Inline[index].Data)
	}
	input.Attachments = append([]Attachment(nil), input.Attachments...)
	for index := range input.Attachments {
		input.Attachments[index].Data = bytes.Clone(input.Attachments[index].Data)
	}
	return Message{content: &input, sender: sender, recipients: recipients, bytes: total}, nil
}
func line(value string, limit int) bool {
	if len(value) > limit || !utf8.ValidString(value) {
		return false
	}
	for _, char := range value {
		if unicode.IsControl(char) {
			return false
		}
	}
	return true
}
func identifier(value string) bool {
	if len(value) < 3 || len(value) > 254 || strings.Count(value, "@") != 1 {
		return false
	}
	parts := strings.SplitN(value, "@", 2)
	if strings.Contains(parts[1], ":") || !hostValid(parts[1]) || parts[0] == "" || parts[0][0] == '.' || parts[0][len(parts[0])-1] == '.' || strings.Contains(parts[0], "..") {
		return false
	}
	for _, char := range parts[0] {
		if !(char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' || strings.ContainsRune("._+-", char)) {
			return false
		}
	}
	return true
}
func address(value string) (string, error) {
	if !line(value, 512) {
		return "", failure(ErrInput, "address")
	}
	parsed, err := stdmail.ParseAddress(value)
	if err != nil || !identifier(parsed.Address) {
		return "", failure(ErrInput, "address")
	}
	return parsed.Address, nil
}
func headerName(name string) bool {
	if len(name) < 3 || len(name) > 64 || !strings.HasPrefix(strings.ToLower(name), "x-") {
		return false
	}
	for _, char := range name {
		if !(char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' || char == '-') {
			return false
		}
	}
	return true
}
func fileMetadata(name, contentType string) bool {
	if name == "" || name == "." || name == ".." || !line(name, 128) || strings.ContainsAny(name, "/\\\"") ||
		contentType == "" || !line(contentType, 256) {
		return false
	}
	media, params, err := mime.ParseMediaType(contentType)
	if err != nil || !strings.Contains(media, "/") || strings.Contains(media, "*") || strings.HasPrefix(media, "multipart/") {
		return false
	}
	for key := range params {
		if key == "name" || key == "filename" || key == "boundary" {
			return false
		}
	}
	return true
}
func (value settings) check(messages []Message) error {
	if len(messages) < 1 || len(messages) > value.MaxMessages {
		return failure(ErrLimit, "batch")
	}
	ids := map[string]bool{}
	for _, message := range messages {
		if message.content == nil {
			return failure(ErrInput, "message")
		}
		if message.bytes > value.MaxMessageBytes || len(message.recipients) > value.MaxRecipients {
			return failure(ErrLimit, "message")
		}
		if ids[message.content.ID] {
			return failure(ErrInput, "duplicate-message-id")
		}
		ids[message.content.ID] = true
	}
	return nil
}
func compose(ctx context.Context, message Message, limits settings) ([]byte, error) {
	input := message.content
	native := sdk.NewMsg(sdk.WithNoDefaultUserAgent())
	if err := native.From(input.From); err != nil {
		return nil, failure(ErrInput, "mime", err)
	}
	for _, group := range []struct {
		values []string
		set    func(...string) error
	}{
		{input.To, native.To}, {input.Cc, native.Cc}, {input.Bcc, native.Bcc}} {
		if len(group.values) > 0 {
			if err := group.set(group.values...); err != nil {
				return nil, failure(ErrInput, "mime", err)
			}
		}
	}
	if input.ReplyTo != "" {
		if err := native.ReplyTo(input.ReplyTo); err != nil {
			return nil, failure(ErrInput, "mime", err)
		}
	}
	native.SetMessageIDWithValue(input.ID)
	native.SetGenHeader(sdk.HeaderSubject, encodeSubject(input.Subject))
	for name, value := range input.Headers {
		native.SetGenHeader(sdk.Header(name), value)
	}
	if input.Text != "" {
		native.SetBodyString(sdk.TypeTextPlain, input.Text)
		if input.HTML != "" {
			native.AddAlternativeString(sdk.TypeTextHTML, input.HTML)
		}
	} else {
		native.SetBodyString(sdk.TypeTextHTML, input.HTML)
	}
	for _, inline := range input.Inline {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if err := native.EmbedReader(inline.Name, bytes.NewReader(inline.Data), sdk.WithFileContentID("<"+inline.ID+">"), fileHeaders(inline.Name, inline.ContentType, "inline")); err != nil {
			return nil, failure(ErrInput, "inline", err)
		}
	}
	for _, attachment := range input.Attachments {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if err := native.AttachReader(attachment.Name, bytes.NewReader(attachment.Data), fileHeaders(attachment.Name, attachment.ContentType, "attachment")); err != nil {
			return nil, failure(ErrInput, "attachment", err)
		}
	}
	writer := &boundedWriter{ctx: ctx, limit: limits.MaxMIMEBytes}
	if _, err := native.WriteTo(writer); err != nil {
		return nil, failure(ErrInput, "mime", err)
	}
	for remaining := writer.buffer.Bytes(); len(remaining) > 0; {
		line, rest, _ := bytes.Cut(remaining, []byte("\r\n"))
		if len(line) > 998 {
			return nil, failure(ErrLimit, "mime-line")
		}
		remaining = rest
	}
	return writer.buffer.Bytes(), nil
}

func fileHeaders(name, contentType, disposition string) sdk.FileOption {
	media, params, _ := mime.ParseMediaType(contentType)
	params["name"] = name
	typeHeader := mime.FormatMediaType(media, params)
	dispositionHeader := mime.FormatMediaType(disposition, map[string]string{"filename": name})
	return func(file *sdk.File) {
		// Native defaults rewrite filenames and use encoded-words in parameters.
		// RFC 2231 encoding preserves their values through standard MIME parsers.
		file.Header.Set(sdk.HeaderContentType.String(), typeHeader)
		file.Header.Set(sdk.HeaderContentDisposition.String(), dispositionHeader)
	}
}
func encodeSubject(subject string) string {
	// Encode ASCII too: literal encoded-words must not become a different subject.
	var encoded strings.Builder
	for subject != "" {
		end := min(45, len(subject))
		for end < len(subject) && !utf8.RuneStart(subject[end]) {
			end--
		}
		if encoded.Len() > 0 {
			encoded.WriteByte(' ')
		}
		encoded.WriteString("=?utf-8?b?")
		encoded.WriteString(base64.StdEncoding.EncodeToString([]byte(subject[:end])))
		encoded.WriteString("?=")
		subject = subject[end:]
	}
	return encoded.String()
}

type boundedWriter struct {
	ctx    context.Context
	limit  int
	buffer bytes.Buffer
}

func (writer *boundedWriter) Write(value []byte) (int, error) {
	if writer.ctx.Err() != nil {
		return 0, writer.ctx.Err()
	}
	if len(value) > writer.limit-writer.buffer.Len() {
		return 0, failure(ErrLimit, "mime")
	}
	return writer.buffer.Write(value)
}
