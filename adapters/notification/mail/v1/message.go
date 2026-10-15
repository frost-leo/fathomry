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
	native "github.com/frost-leo/fathomry/internal/notification/mail/v0"
	"time"
)

// Content is borrowed only by NewMessage. Empty ID/date generate stable values
// once; subject and body may be empty. From/recipients are required by Send, not
// MIME preparation. EnvelopeFrom "<>" explicitly selects the null reverse path.
// Bodies are caller-authored; the adapter does not sanitize or render reports.
// SubjectPreformatted explicitly preserves encoded words instead of literal text;
// it does not permit control characters or bypass header bounds.
type Content struct {
	private
	ID, From, EnvelopeFrom, ReplyTo, Subject, Text, HTML string
	SubjectPreformatted                                  bool
	To, Cc, Bcc                                          []string
	Headers                                              map[string]string
	Date                                                 time.Time
	Charset, Encoding, MIMEVersion, Boundary             string
	PGP                                                  int
	PGPHash                                              string
	Parts                                                []Part
	Fields                                               []Header
	ReplyToList, MDN                                     []string
	Inline                                               []Inline
	Attachments                                          []Attachment
}

// Part.Data contains bytes in the declared charset; no transcoding is inferred.
type Part struct {
	private
	ContentType, Charset, Encoding, Description string
	Data                                        []byte
}

// Header is a repeated non-structural MIME field. Address/MIME controls have their
// own fields; Bcc/Resent-Bcc and all control-character injection are refused.
type Header struct {
	private
	Name         string
	Values       []string
	Preformatted bool
}
type Inline struct {
	private
	ID, Name, ContentType string
	Data                  []byte
	Encoding, Description string
	Fields                []Header
}
type Attachment struct {
	private
	Name, ContentType         string
	Data                      []byte
	Encoding, Description, ID string
	Fields                    []Header
}

// Message is immutable and concurrent-readable. Copies share only frozen data.
// Prepared MIME contains private content; explicit EMLCopy returns detached bytes.
type Message struct {
	private
	native native.Message
}

func NewMessage(input Content) (Message, error) {
	content, err := nativeContent(input)
	if err != nil {
		return Message{}, err
	}
	value, err := native.NewMessage(content)
	return Message{native: value}, translate(err, "message")
}
func (message Message) ContentCopy() Content { return publicContent(message.native.ContentCopy()) }
func (message Message) EMLCopy() []byte      { return message.native.EMLCopy() }

func nativeContent(value Content) (native.Content, error) {
	if len(value.Parts) > 32 || len(value.Fields)+len(value.Headers) > 32 || len(value.Inline) > 16 || len(value.Attachments) > 16 || len(value.To)+len(value.Cc)+len(value.Bcc) > 100 || len(value.ReplyToList) > 100 || len(value.MDN) > 100 {
		return native.Content{}, fail(ErrLimit, "content")
	}
	result := native.Content{ID: value.ID, From: value.From, EnvelopeFrom: value.EnvelopeFrom, ReplyTo: value.ReplyTo, Subject: value.Subject, SubjectPreformatted: value.SubjectPreformatted, Text: value.Text, HTML: value.HTML, To: value.To, Cc: value.Cc, Bcc: value.Bcc, Headers: value.Headers, Date: value.Date, Charset: value.Charset, Encoding: value.Encoding, MIMEVersion: value.MIMEVersion, Boundary: value.Boundary, PGP: value.PGP, PGPHash: value.PGPHash, ReplyToList: value.ReplyToList, MDN: value.MDN}
	for _, item := range value.Parts {
		result.Parts = append(result.Parts, native.Part{ContentType: item.ContentType, Charset: item.Charset, Encoding: item.Encoding, Description: item.Description, Data: item.Data})
	}
	for _, item := range value.Fields {
		result.Fields = append(result.Fields, native.Header{Name: item.Name, Values: item.Values, Preformatted: item.Preformatted})
	}
	for _, item := range value.Inline {
		if len(item.Fields) > 32 {
			return native.Content{}, fail(ErrLimit, "file-fields")
		}
		result.Inline = append(result.Inline, native.Inline{ID: item.ID, Name: item.Name, ContentType: item.ContentType, Data: item.Data, Encoding: item.Encoding, Description: item.Description, Fields: nativeFields(item.Fields)})
	}
	for _, item := range value.Attachments {
		if len(item.Fields) > 32 {
			return native.Content{}, fail(ErrLimit, "file-fields")
		}
		result.Attachments = append(result.Attachments, nativeAttachment(item))
	}
	return result, nil
}
func publicContent(value native.Content) Content {
	result := Content{ID: value.ID, From: value.From, EnvelopeFrom: value.EnvelopeFrom, ReplyTo: value.ReplyTo, Subject: value.Subject, SubjectPreformatted: value.SubjectPreformatted, Text: value.Text, HTML: value.HTML, To: value.To, Cc: value.Cc, Bcc: value.Bcc, Headers: value.Headers, Date: value.Date, Charset: value.Charset, Encoding: value.Encoding, MIMEVersion: value.MIMEVersion, Boundary: value.Boundary, PGP: value.PGP, PGPHash: value.PGPHash, ReplyToList: value.ReplyToList, MDN: value.MDN}
	for _, item := range value.Parts {
		result.Parts = append(result.Parts, Part{ContentType: item.ContentType, Charset: item.Charset, Encoding: item.Encoding, Description: item.Description, Data: item.Data})
	}
	for _, item := range value.Fields {
		result.Fields = append(result.Fields, Header{Name: item.Name, Values: item.Values, Preformatted: item.Preformatted})
	}
	for _, item := range value.Inline {
		result.Inline = append(result.Inline, Inline{ID: item.ID, Name: item.Name, ContentType: item.ContentType, Data: item.Data, Encoding: item.Encoding, Description: item.Description, Fields: publicFields(item.Fields)})
	}
	for _, item := range value.Attachments {
		result.Attachments = append(result.Attachments, Attachment{Name: item.Name, ContentType: item.ContentType, Data: item.Data, Encoding: item.Encoding, Description: item.Description, ID: item.ID, Fields: publicFields(item.Fields)})
	}
	return result
}
func nativeAttachment(value Attachment) native.Attachment {
	return native.Attachment{Name: value.Name, ContentType: value.ContentType, Data: value.Data, Encoding: value.Encoding, Description: value.Description, ID: value.ID, Fields: nativeFields(value.Fields)}
}

func nativeFields(values []Header) []native.Header {
	result := make([]native.Header, len(values))
	for index, value := range values {
		result[index] = native.Header{Name: value.Name, Values: value.Values, Preformatted: value.Preformatted}
	}
	return result
}
func publicFields(values []native.Header) []Header {
	result := make([]Header, len(values))
	for index, value := range values {
		result[index] = Header{Name: value.Name, Values: value.Values, Preformatted: value.Preformatted}
	}
	return result
}
func nativePart(value Part) native.Part {
	return native.Part{ContentType: value.ContentType, Charset: value.Charset, Encoding: value.Encoding, Description: value.Description, Data: value.Data}
}
