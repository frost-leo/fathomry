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
	"errors"
	"io"
	"mime"
	"mime/multipart"
	stdmail "net/mail"
	"strings"
	"testing"
)

func TestAttachmentMetadataRoundTrip(t *testing.T) {
	for _, name := range []string{"temperature-°C.txt", "report:final.txt", "=?utf-8?b?c2VjcmV0?=.txt"} {
		t.Run(name, func(t *testing.T) {
			input := mailContent(t)
			input.Inline = nil
			input.Attachments = []Attachment{{Name: name, ContentType: "text/plain; charset=utf-8", Data: []byte("payload")}}
			message, err := NewMessage(input)
			if err != nil {
				t.Fatal(err)
			}
			encoded, err := compose(context.Background(), message, defaults(OptionsV1{}))
			if err != nil {
				t.Fatal(err)
			}
			_, parts := parseMIME(t, encoded)
			found := 0
			for _, part := range parts {
				if part.filename != "" {
					found++
				}
				if part.filename != "" && part.filename != name {
					t.Fatal("attachment filename did not round trip through standard MIME parsing")
				}
			}
			if found != 1 {
				t.Fatal("attachment metadata missing")
			}
		})
	}
}
func TestContentTypeCannotOverrideFilename(t *testing.T) {
	for _, contentType := range []string{`text/plain; name="different.txt"`, `text/plain; name*=utf-8''different.txt`} {
		input := mailContent(t)
		input.Attachments[0].ContentType = contentType
		if _, err := NewMessage(input); err == nil {
			t.Fatal("conflicting Content-Type filename accepted")
		}
	}
}

func TestMessageIdentifiersRejectUnbracketedIPv6(t *testing.T) {
	for _, field := range []string{"message", "inline"} {
		input := mailContent(t)
		if field == "message" {
			input.ID = "mail@::1"
		} else {
			input.Inline[0].ID = "asset@::1"
		}
		if _, err := NewMessage(input); !errors.Is(err, ErrInput) {
			t.Fatal("invalid unbracketed IPv6 identifier accepted")
		}
	}
}
func TestHeaderWireLineLimit(t *testing.T) {
	input := mailContent(t)
	input.Headers = map[string]string{"X-Long": strings.Repeat("a", 998)}
	message, err := NewMessage(input)
	if err != nil {
		return
	}
	encoded, err := compose(context.Background(), message, defaults(OptionsV1{}))
	if err != nil {
		if errors.Is(err, ErrLimit) {
			return
		}
		t.Fatal(err)
	}
	for _, line := range bytes.Split(encoded, []byte("\r\n")) {
		if len(line) > 998 {
			t.Fatal("encoded message exceeded the RFC 5322 hard line limit")
		}
	}
}
func TestOversizedBodyCannotBecomeTruncatedSuccess(t *testing.T) {
	input := mailContent(t)
	input.Text = strings.Repeat("a", 10000)
	input.HTML = "<p>small alternative</p>"
	message, err := NewMessage(input)
	if err != nil {
		t.Fatal(err)
	}
	limits := defaults(OptionsV1{})
	for _, limit := range []int{1024, 2048, 4096, 8192} {
		limits.MaxMIMEBytes = limit
		if _, err := compose(context.Background(), message, limits); !errors.Is(err, ErrLimit) {
			t.Fatal("oversized MIME did not retain its original failure")
		}
	}
}
func TestUnicodeMIMEParameter(t *testing.T) {
	input := mailContent(t)
	input.Inline = nil
	input.Attachments = []Attachment{{Name: "data.txt", ContentType: `text/plain; title="Temperature °C"`, Data: []byte("payload")}}
	message, err := NewMessage(input)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := compose(context.Background(), message, defaults(OptionsV1{}))
	if err != nil {
		t.Fatal(err)
	}
	top, err := stdmail.ReadMessage(bytes.NewReader(encoded))
	if err != nil {
		t.Fatal(err)
	}
	_, params, err := mime.ParseMediaType(top.Header.Get("Content-Type"))
	if err != nil {
		t.Fatal(err)
	}
	reader := multipart.NewReader(top.Body, params["boundary"])
	found := 0
	for {
		part, err := reader.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if part.Header.Get("Content-Disposition") == "" {
			continue
		}
		found++
		header := part.Header.Get("Content-Type")
		for _, char := range header {
			if char > 127 {
				t.Fatal("unencoded non-ASCII MIME parameter on wire")
			}
		}
		_, params, err := mime.ParseMediaType(header)
		if err != nil || params["title"] != "Temperature °C" {
			t.Fatal("MIME parameter value changed")
		}
	}
	if found != 1 {
		t.Fatal("MIME parameter part missing")
	}
}

func FuzzFileMetadataRoundTrip(f *testing.F) {
	for _, name := range []string{"report.csv", "temperature-°C.txt", "report:final.txt", "=?utf-8?b?c2VjcmV0?=.txt", "bad\r\nname.txt"} {
		f.Add(name)
	}
	f.Fuzz(func(t *testing.T, name string) {
		if len(name) > 256 {
			return
		}
		input := Content{ID: "file@fixture.test", From: "sender@fixture.test", To: []string{"to@fixture.test"}, Subject: "File metadata", Text: "text",
			Attachments: []Attachment{{Name: name, ContentType: "application/octet-stream", Data: []byte{0, 1, 255}}}}
		message, err := NewMessage(input)
		if err != nil {
			return
		}
		encoded, err := compose(context.Background(), message, defaults(OptionsV1{}))
		if err != nil {
			t.Fatal(err)
		}
		_, parts := parseMIME(t, encoded)
		found := false
		for _, part := range parts {
			if part.kind == "application/octet-stream" {
				found = true
				if part.filename != name || !bytes.Equal(part.data, []byte{0, 1, 255}) {
					t.Fatal("file metadata or bytes changed")
				}
			}
		}
		if !found {
			t.Fatal("file missing")
		}
	})
}
