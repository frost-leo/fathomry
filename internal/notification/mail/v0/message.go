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
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"mime"
	"mime/multipart"
	stdmail "net/mail"
	"path/filepath"
	"slices"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	sdk "github.com/wneessen/go-mail"
)

// Content is borrowed only by NewMessage; callers must not mutate it concurrently.
// ID is a bare ASCII Message-ID (without brackets), not a deduplication
// guarantee. Address fields accept one mailbox with an optional UTF-8 display
// name per entry. Empty ID/date generate once. EnvelopeFrom empty derives from
// From; "<>" is the null reverse path. International envelopes require SMTPUTF8.
// SubjectPreformatted preserves an already encoded RFC 2047 field, including its
// charset label; otherwise Subject is literal text even if it resembles encoded words.
//
// Text and HTML are UTF-8 and may both be empty; both nonempty produce MIME
// alternatives. HTML is explicitly caller-authored content, not sanitized or
// executed by this package. Callers own CID references, image/attachment validity,
// remote-content/privacy policy, accessibility and recipient rendering. Headers
// permits bounded non-structural standard/custom fields, never envelope overrides.
type Content struct {
	private
	ID, From, EnvelopeFrom, ReplyTo, Subject, Text, HTML string
	SubjectPreformatted                                  bool
	To, Cc, Bcc                                          []string
	Headers                                              map[string]string
	Inline                                               []Inline
	Attachments                                          []Attachment
	Date                                                 time.Time
	Charset, Encoding, MIMEVersion, Boundary             string
	PGP                                                  int
	PGPHash                                              string
	Parts                                                []Part
	Fields                                               []Header
	ReplyToList, MDN                                     []string
}

// Part is immutable after NewMessage; Data contains bytes in the declared
// charset, not automatically transcoded text. Empty encoding inherits the message.
type Part struct {
	private
	ContentType, Charset, Encoding, Description string
	Data                                        []byte
}

// Header supplies repeated non-structural fields. Preformatted bypasses native
// encoded-word processing but still rejects control characters and line overflow.
type Header struct {
	private
	Name         string
	Values       []string
	Preformatted bool
}

// Inline supplies a MIME-related resource and bare Content-ID, unique within the
// message. IDs are bounded opaque labels (also without @); brackets, controls and
// path separators are refused. Name is a leaf filename; empty ContentType uses native extension
// lookup during admitted construction, falling back to application/octet-stream. Data is copied by
// NewMessage; nil and empty both mean an empty resource. MIME transport does not
// validate file formats or supply a chart renderer.
type Inline struct {
	private
	ID, Name, ContentType string
	Data                  []byte
	Encoding, Description string
	Fields                []Header
}

// Attachment is a caller-provided file, copied by NewMessage. Name is a leaf
// filename and optional ContentType an explicit MIME media type. Filename parameters are
// Provider-owned; ContentType cannot set name, filename or boundary. Nil and empty Data both
// mean an empty file. No file path, reader, callback or native File escapes.
type Attachment struct {
	private
	Name, ContentType         string
	Data                      []byte
	Encoding, Description, ID string
	Fields                    []Header
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
	wire       []byte
	unsigned   []byte
}

// NewMessage freezes at most 16 MiB total input, 100 recipients, 32 custom headers,
// 16 inline resources and 16 attachments. It does not allocate a native Msg or
// perform I/O. Caller-owned frozen messages are outside the source's call quota.
func NewMessage(input Content) (Message, error) {
	if input.ID == "" {
		random := make([]byte, 16)
		if _, err := rand.Read(random); err != nil {
			return Message{}, err
		}
		input.ID = hex.EncodeToString(random) + "@localhost"
	}
	if input.Date.IsZero() {
		input.Date = time.Now()
	}
	if !messageID(input.ID) || !line(input.Subject, 998) ||
		len(input.To)+len(input.Cc)+len(input.Bcc) > 100 ||
		len(input.Inline) > 16 || len(input.Attachments) > 16 || len(input.Headers)+len(input.Fields) > 32 || len(input.Parts) > 32 || len(input.ReplyToList) > 100 || len(input.MDN) > 100 {
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
	if input.PGP < 0 || input.PGP > 2 || !token(input.Charset, 64) || !encodingValid(input.Encoding) || !token(input.MIMEVersion, 16) {
		return Message{}, failure(ErrInput, "mime-options")
	}
	if input.PGP != 0 && (len(input.Parts) != 2 || len(input.Inline)+len(input.Attachments) != 0 || input.Text != "" || input.HTML != "") {
		return Message{}, failure(ErrInput, "pgp-structure")
	}
	if input.PGP == 2 && (!token(input.PGPHash, 64) || input.PGPHash == "") {
		return Message{}, failure(ErrInput, "pgp-micalg")
	}
	if !add(len(input.PGPHash)) {
		return Message{}, failure(ErrLimit, "pgp-micalg")
	}
	if input.Boundary != "" {
		writer := multipart.NewWriter(&bytes.Buffer{})
		if err := writer.SetBoundary(input.Boundary); err != nil {
			return Message{}, failure(ErrInput, "boundary", err)
		}
	}
	for _, text := range []string{input.Charset, input.Encoding, input.MIMEVersion, input.Boundary} {
		if !add(len(text)) {
			return Message{}, failure(ErrLimit, "mime-options")
		}
	}
	if len(input.Parts) != 0 && (input.Text != "" || input.HTML != "") {
		return Message{}, failure(ErrInput, "body-selection")
	}
	for _, part := range input.Parts {
		if part.ContentType == "" || !fileMetadata("part", part.ContentType) || !token(part.Charset, 64) || !encodingValid(part.Encoding) || !line(part.Description, 998) {
			return Message{}, failure(ErrInput, "part")
		}
		for _, size := range []int{len(part.ContentType), len(part.Charset), len(part.Encoding), len(part.Description), len(part.Data)} {
			if !add(size) {
				return Message{}, failure(ErrLimit, "part")
			}
		}
		encoding := part.Encoding
		if encoding == "" {
			encoding = input.Encoding
		}
		if encoding == "7bit" {
			for _, char := range part.Data {
				if char > 127 {
					return Message{}, failure(ErrInput, "7bit")
				}
			}
		}
	}
	if !utf8.ValidString(input.Text) || !utf8.ValidString(input.HTML) ||
		strings.ContainsRune(input.Text, 0) || strings.ContainsRune(input.HTML, 0) {
		return Message{}, failure(ErrInput, "body")
	}
	var sender string
	var err error
	if input.From != "" {
		sender, err = address(input.From)
		if err != nil {
			return Message{}, err
		}
	}
	if input.EnvelopeFrom == "<>" {
		sender = ""
	} else if input.EnvelopeFrom != "" {
		parsed, parseErr := address(input.EnvelopeFrom)
		if parseErr != nil || strings.ContainsAny(input.EnvelopeFrom, "<>") {
			return Message{}, failure(ErrInput, "envelope")
		}
		sender = parsed
	}
	if input.ReplyTo != "" && len(input.ReplyToList) > 0 {
		return Message{}, failure(ErrInput, "reply-to")
	}
	for _, group := range [][]string{input.ReplyToList, input.MDN} {
		for _, addr := range group {
			if _, err := address(addr); err != nil {
				return Message{}, err
			}
			if !add(len(addr)) {
				return Message{}, failure(ErrLimit, "address")
			}
		}
	}
	if input.ReplyTo != "" {
		if _, err = address(input.ReplyTo); err != nil {
			return Message{}, err
		}
	}
	recipients := make([]string, 0, len(input.To)+len(input.Cc)+len(input.Bcc))
	for _, group := range [][]string{input.To, input.Cc, input.Bcc} {
		for _, value := range group {
			if !add(len(value)) {
				return Message{}, failure(ErrLimit, "message")
			}
			mailbox, parseErr := address(value)
			if parseErr != nil {
				return Message{}, parseErr
			}
			recipients = append(recipients, mailbox)
		}
	}
	headerNames := map[string]bool{}
	for _, header := range input.Fields {
		if !headerName(header.Name) || len(header.Values) > 32 || header.Preformatted && len(header.Values) != 1 {
			return Message{}, failure(ErrInput, "header")
		}
		lower := strings.ToLower(header.Name)
		if headerNames[lower] {
			return Message{}, failure(ErrInput, "header")
		}
		headerNames[lower] = true
		if !add(len(header.Name)) {
			return Message{}, failure(ErrLimit, "header")
		}
		for _, text := range header.Values {
			if !line(text, 998) {
				return Message{}, failure(ErrInput, "header")
			}
			if !add(len(text)) {
				return Message{}, failure(ErrLimit, "header")
			}
		}
	}
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
		fieldBytes, err := validateFileFields(inline.Fields)
		if err != nil {
			return Message{}, err
		}
		if !add(fieldBytes) {
			return Message{}, failure(ErrLimit, "file-fields")
		}
		if !identifier(inline.ID) || ids[inline.ID] || !fileMetadata(inline.Name, inline.ContentType) || !encodingValid(inline.Encoding) || !line(inline.Description, 998) {
			return Message{}, failure(ErrInput, "inline")
		}
		ids[inline.ID] = true
		for _, size := range []int{len(inline.ID), len(inline.Name), len(inline.ContentType), len(inline.Data), len(inline.Encoding), len(inline.Description)} {
			if !add(size) {
				return Message{}, failure(ErrLimit, "inline")
			}
		}
	}
	for _, attachment := range input.Attachments {
		fieldBytes, err := validateFileFields(attachment.Fields)
		if err != nil {
			return Message{}, err
		}
		if !add(fieldBytes) {
			return Message{}, failure(ErrLimit, "file-fields")
		}
		if !fileMetadata(attachment.Name, attachment.ContentType) || !encodingValid(attachment.Encoding) || !line(attachment.Description, 998) || attachment.ID != "" && !identifier(attachment.ID) {
			return Message{}, failure(ErrInput, "attachment")
		}
		for _, size := range []int{len(attachment.Name), len(attachment.ContentType), len(attachment.Data), len(attachment.ID), len(attachment.Encoding), len(attachment.Description)} {
			if !add(size) {
				return Message{}, failure(ErrLimit, "attachment")
			}
		}
	}
	input.Parts = slices.Clone(input.Parts)
	for index := range input.Parts {
		input.Parts[index].Data = bytes.Clone(input.Parts[index].Data)
	}
	input.Fields = slices.Clone(input.Fields)
	for index := range input.Fields {
		input.Fields[index].Values = slices.Clone(input.Fields[index].Values)
	}
	input.ReplyToList, input.MDN = slices.Clone(input.ReplyToList), slices.Clone(input.MDN)
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
		input.Inline[index].Fields = cloneFields(input.Inline[index].Fields)
	}
	input.Attachments = append([]Attachment(nil), input.Attachments...)
	for index := range input.Attachments {
		input.Attachments[index].Data = bytes.Clone(input.Attachments[index].Data)
		input.Attachments[index].Fields = cloneFields(input.Attachments[index].Fields)
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
	if value == "" || !line(value, 254) || strings.ContainsAny(value, "<>/\\") {
		return false
	}
	if strings.Contains(value, "@") {
		return messageID(value)
	}
	return true
}
func address(value string) (string, error) {
	if !line(value, 512) {
		return "", failure(ErrInput, "address")
	}
	parsed, err := stdmail.ParseAddress(value)
	if err != nil || parsed == nil || !strings.Contains(parsed.Address, "@") {
		return "", failure(ErrInput, "address")
	}
	bare := (&stdmail.Address{Address: parsed.Address}).String()
	return strings.TrimSuffix(strings.TrimPrefix(bare, "<"), ">"), nil
}
func headerName(name string) bool {
	if !fieldName(name) {
		return false
	}
	switch strings.ToLower(name) {
	case "from", "to", "cc", "bcc", "resent-bcc", "reply-to", "envelope-from", "subject", "date", "message-id", "mime-version", "content-type", "content-transfer-encoding":
		return false
	}
	return true
}
func fieldName(name string) bool {
	if len(name) < 1 || len(name) > 64 {
		return false
	}
	for _, char := range name {
		if char < 33 || char > 126 || char == ':' {
			return false
		}
	}
	return true
}
func fileMetadata(name, contentType string) bool {
	if name == "" || name == "." || name == ".." || !line(name, 128) || strings.ContainsAny(name, "/\\\"") ||
		!line(contentType, 256) {
		return false
	}
	if contentType == "" {
		return true
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
	for _, message := range messages {
		if message.content == nil {
			return failure(ErrInput, "message")
		}
		if len(message.recipients) == 0 || message.content.From == "" && message.content.EnvelopeFrom == "" {
			return failure(ErrInput, "envelope")
		}
		if message.bytes > value.MaxMessageBytes || len(message.recipients) > value.MaxRecipients {
			return failure(ErrLimit, "message")
		}
		if len(message.wire) > value.MaxMIMEBytes {
			return failure(ErrLimit, "mime")
		}
	}
	return nil
}
func compose(ctx context.Context, message Message, limits settings) ([]byte, error) {
	if message.content == nil {
		return nil, failure(ErrInput, "message")
	}
	if message.bytes > limits.MaxMessageBytes {
		return nil, failure(ErrLimit, "message")
	}
	if message.wire != nil {
		if len(message.wire) > limits.MaxMIMEBytes {
			return nil, failure(ErrLimit, "mime")
		}
		return message.wire, nil
	}
	if message.content.PGP != 0 {
		return composePGP(ctx, message, limits)
	}
	native, err := buildNative(ctx, message)
	if err != nil {
		return nil, err
	}
	wire, err := writeNative(ctx, native, 3*limits.MaxMIMEBytes)
	if err != nil {
		return nil, err
	}
	return normalizeMIME(ctx, wire, limits.MaxMIMEBytes, message.content.Boundary)
}

func buildNative(ctx context.Context, message Message) (*sdk.Msg, error) {
	input := message.content
	native := sdk.NewMsg(sdk.WithNoDefaultUserAgent())
	if input.Charset != "" {
		native.SetCharset(sdk.Charset(input.Charset))
	}
	if input.Encoding != "" {
		native.SetEncoding(sdk.Encoding(input.Encoding))
	}
	if input.MIMEVersion != "" {
		native.SetMIMEVersion(sdk.MIMEVersion(input.MIMEVersion))
	}
	native.SetPGPType(sdk.PGPType(input.PGP))
	if input.From != "" {
		if err := native.From(input.From); err != nil {
			return nil, failure(ErrInput, "mime", err)
		}
	} else if input.EnvelopeFrom != "" && input.EnvelopeFrom != "<>" {
		if err := native.EnvelopeFrom(input.EnvelopeFrom); err != nil {
			return nil, err
		}
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
	if len(input.ReplyToList) > 0 {
		if err := native.SetAddrHeader(sdk.HeaderReplyTo, input.ReplyToList...); err != nil {
			return nil, failure(ErrInput, "reply-to", err)
		}
	}
	if len(input.MDN) > 0 {
		if err := native.RequestMDNTo(input.MDN...); err != nil {
			return nil, failure(ErrInput, "mdn", err)
		}
	}
	native.SetMessageIDWithValue(input.ID)
	native.SetDateWithValue(input.Date)
	if input.SubjectPreformatted {
		native.SetGenHeaderPreformatted(sdk.HeaderSubject, input.Subject)
	} else {
		native.SetGenHeader(sdk.HeaderSubject, encodeSubject(input.Subject))
	}
	for name, value := range input.Headers {
		native.SetGenHeader(sdk.Header(name), value)
	}
	for _, header := range input.Fields {
		if header.Preformatted {
			native.SetGenHeaderPreformatted(sdk.Header(header.Name), header.Values[0])
		} else {
			native.SetGenHeader(sdk.Header(header.Name), slices.Clone(header.Values)...)
		}
	}
	if len(input.Parts) > 0 {
		for index, part := range input.Parts {
			options := []sdk.PartOption{sdk.WithPartContentDescription(part.Description)}
			if part.Charset != "" {
				options = append(options, sdk.WithPartCharset(sdk.Charset(part.Charset)))
			}
			if part.Encoding != "" {
				options = append(options, sdk.WithPartEncoding(sdk.Encoding(part.Encoding)))
			}
			if index == 0 {
				native.SetBodyString(sdk.ContentType(part.ContentType), string(part.Data), options...)
			} else {
				native.AddAlternativeString(sdk.ContentType(part.ContentType), string(part.Data), options...)
			}
		}
	} else if input.Text != "" || input.HTML == "" {
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
		if err := native.EmbedReader(inline.Name, bytes.NewReader(inline.Data), sdk.WithFileContentID("<"+inline.ID+">"), fileHeaders(inline.Name, inline.ContentType, "inline"), fileOptions(inline.Encoding, inline.Description, ""), extraFileFields(inline.Fields)); err != nil {
			return nil, failure(ErrInput, "inline", err)
		}
	}
	for _, attachment := range input.Attachments {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if err := native.AttachReader(attachment.Name, bytes.NewReader(attachment.Data), fileHeaders(attachment.Name, attachment.ContentType, "attachment"), fileOptions(attachment.Encoding, attachment.Description, attachment.ID), extraFileFields(attachment.Fields)); err != nil {
			return nil, failure(ErrInput, "attachment", err)
		}
	}
	return native, nil
}

func writeNative(ctx context.Context, native *sdk.Msg, limit int) ([]byte, error) {
	writer := &boundedWriter{ctx: ctx, limit: limit}
	if _, err := native.WriteTo(writer); err != nil {
		return nil, failure(ErrInput, "mime", err)
	}
	if err := checkMIME(writer.buffer.Bytes()); err != nil {
		return nil, err
	}
	return writer.buffer.Bytes(), nil
}

func checkMIME(data []byte) error {
	for remaining := data; len(remaining) > 0; {
		line, rest, _ := bytes.Cut(remaining, []byte("\r\n"))
		if len(line) > 998 {
			return failure(ErrLimit, "mime-line")
		}
		remaining = rest
	}
	return nil
}

func fileHeaders(name, contentType, disposition string) sdk.FileOption {
	if contentType == "" {
		contentType = mime.TypeByExtension(filepath.Ext(name))
		if contentType == "" {
			contentType = "application/octet-stream"
		}
	}
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
