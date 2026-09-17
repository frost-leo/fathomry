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
	"errors"
	"image"
	"image/color"
	"image/png"
	"io"
	"mime"
	"mime/multipart"
	"mime/quotedprintable"
	stdmail "net/mail"
	"strings"
	"testing"
)

func inlinePNG(t testing.TB) []byte {
	t.Helper()
	bitmap := image.NewRGBA(image.Rect(0, 0, 2, 2))
	bitmap.Set(0, 0, color.RGBA{40, 100, 200, 255})
	bitmap.Set(1, 1, color.RGBA{20, 160, 140, 255})
	var buffer bytes.Buffer
	if err := png.Encode(&buffer, bitmap); err != nil {
		t.Fatal(err)
	}
	return buffer.Bytes()
}
func mailContent(t testing.TB) Content {
	return Content{ID: "message@fixture.test", From: "Sender <sender@fixture.test>", EnvelopeFrom: "bounce@fixture.test",
		ReplyTo: "reply@fixture.test", To: []string{"Recipient <to@fixture.test>"}, Cc: []string{"cc@fixture.test"}, Bcc: []string{"hidden@fixture.test"},
		Subject: "Notification – synthetic report", Text: "Synthetic data: A=20, B=30, C=50 units.",
		HTML:        "<!doctype html><html><body><p>Synthetic data: A=20, B=30, C=50 units.</p><img src=\"cid:logo@fixture.test\" alt=\"Fixture icon\"></body></html>",
		Headers:     map[string]string{"X-Report-Type": "synthetic"},
		Inline:      []Inline{{ID: "logo@fixture.test", Name: "logo.png", ContentType: "image/png", Data: inlinePNG(t)}},
		Attachments: []Attachment{{Name: "report.csv", ContentType: "text/csv", Data: []byte("label,units\nA,20\nB,30\nC,50\n")}}}
}

type mimePart struct {
	kind, cid, filename string
	data                []byte
}

func parseMIME(t testing.TB, data []byte) (*stdmail.Message, []mimePart) {
	t.Helper()
	message, err := stdmail.ReadMessage(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	var parts []mimePart
	var visit func(string, string, string, string, io.Reader)
	visit = func(contentType, transfer, cid, disposition string, reader io.Reader) {
		kind, params, err := mime.ParseMediaType(contentType)
		if err != nil {
			t.Fatal(err)
		}
		if strings.HasPrefix(kind, "multipart/") {
			multipartReader := multipart.NewReader(reader, params["boundary"])
			for {
				part, err := multipartReader.NextRawPart()
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Fatal(err)
				}
				visit(part.Header.Get("Content-Type"), part.Header.Get("Content-Transfer-Encoding"), part.Header.Get("Content-ID"), part.Header.Get("Content-Disposition"), part)
				_ = part.Close()
			}
			return
		}
		switch strings.ToLower(transfer) {
		case "base64":
			reader = base64.NewDecoder(base64.StdEncoding, reader)
		case "quoted-printable":
			reader = quotedprintable.NewReader(reader)
		}
		decoded, err := io.ReadAll(reader)
		if err != nil {
			t.Fatal(err)
		}
		filename := ""
		if disposition != "" {
			_, params, err := mime.ParseMediaType(disposition)
			if err != nil {
				t.Fatal(err)
			}
			filename = params["filename"]
		}
		parts = append(parts, mimePart{kind: kind, cid: cid, filename: filename, data: decoded})
	}
	visit(message.Header.Get("Content-Type"), message.Header.Get("Content-Transfer-Encoding"), "", "", message.Body)
	return message, parts
}
func TestMIMEExactAlternativesInlineAttachmentsAndPrivacy(t *testing.T) {
	input := mailContent(t)
	pngBefore := bytes.Clone(input.Inline[0].Data)
	htmlBefore := input.HTML
	message, err := NewMessage(input)
	if err != nil {
		t.Fatal(err)
	}
	input.To[0] = "wrong@fixture.test"
	input.Inline[0].Data[0] = 0
	input.Attachments[0].Data[0] = 'X'
	input.Headers["X-Report-Type"] = "modified"
	encoded, err := compose(context.Background(), message, defaults(OptionsV1{}))
	if err != nil {
		t.Fatal(err)
	}
	parsed, parts := parseMIME(t, encoded)
	if parsed.Header.Get("Bcc") != "" || bytes.Contains(encoded, []byte("hidden@fixture.test")) ||
		parsed.Header.Get("Message-ID") != "<message@fixture.test>" || strings.Contains(parsed.Header.Get("To"), "wrong@fixture.test") ||
		parsed.Header.Get("X-Report-Type") != "synthetic" {
		t.Fatal("identity, copying or BCC secrecy failed")
	}
	subject, err := (&mime.WordDecoder{}).DecodeHeader(parsed.Header.Get("Subject"))
	if err != nil || subject != "Notification – synthetic report" {
		t.Fatal("UTF-8 subject changed")
	}
	seen := map[string]int{}
	for _, part := range parts {
		seen[part.kind]++
		switch part.kind {
		case "image/png":
			if part.cid != "<logo@fixture.test>" || !bytes.Equal(part.data, pngBefore) {
				t.Fatal("inline bytes or CID changed")
			}
		case "text/plain":
			if !bytes.Contains(part.data, []byte("A=20, B=30, C=50 units.")) {
				t.Fatal("plain body changed")
			}
		case "text/html":
			if string(part.data) != htmlBefore {
				t.Fatal("caller-authored HTML changed")
			}
		case "text/csv":
			if part.filename != "report.csv" || !bytes.Equal(part.data, []byte("label,units\nA,20\nB,30\nC,50\n")) {
				t.Fatal("attachment changed")
			}
		}
	}
	for _, kind := range []string{"image/png", "text/plain", "text/html", "text/csv"} {
		if seen[kind] != 1 {
			t.Fatalf("missing/duplicate MIME kind: %s", kind)
		}
	}
}
func TestMessageRejectsMalformedAndOversizedInputs(t *testing.T) {
	cases := map[string]func(*Content){
		"subject-injection":   func(c *Content) { c.Subject = "hello\r\nBcc: victim@fixture.test" },
		"from-injection":      func(c *Content) { c.From = "sender@fixture.test\nX: injected" },
		"address-injection":   func(c *Content) { c.To = []string{"to@fixture.test\r\nBcc: hidden@fixture.test"} },
		"unicode-envelope":    func(c *Content) { c.EnvelopeFrom = "séndér@fixture.test" },
		"envelope-display":    func(c *Content) { c.EnvelopeFrom = "Name <bounce@fixture.test>" },
		"id-brackets":         func(c *Content) { c.ID = "<message@fixture.test>" },
		"cid-brackets":        func(c *Content) { c.Inline[0].ID = "<logo@fixture.test>" },
		"cid-path":            func(c *Content) { c.Inline[0].ID = "x/y@fixture.test" },
		"duplicate-cid":       func(c *Content) { c.Inline = append(c.Inline, c.Inline[0]) },
		"no-body":             func(c *Content) { c.Text = ""; c.HTML = "" },
		"empty-recipient":     func(c *Content) { c.To = nil; c.Cc = nil; c.Bcc = nil },
		"duplicate-recipient": func(c *Content) { c.Bcc = []string{"to@fixture.test"} },
		"file-path":           func(c *Content) { c.Attachments[0].Name = "../report.csv" },
		"file-injection":      func(c *Content) { c.Attachments[0].Name = "report\n.csv" },
		"mime-injection":      func(c *Content) { c.Attachments[0].ContentType = "text/plain\r\nX: inject" },
		"mime-malformed":      func(c *Content) { c.Inline[0].ContentType = "invalid" },
		"mime-multipart":      func(c *Content) { c.Attachments[0].ContentType = "multipart/mixed" },
		"header-name":         func(c *Content) { c.Headers["X-New\nHeader"] = "value" },
		"header-injection":    func(c *Content) { c.Headers["X-Report-Type"] = "value\r\nBcc: leak" },
		"header-override":     func(c *Content) { c.Headers["Bcc"] = "other@fixture.test" },
		"duplicate-header":    func(c *Content) { c.Headers["x-report-type"] = "other" },
		"oversized":           func(c *Content) { c.Text = strings.Repeat("x", 16<<20) },
		"many-recipients":     func(c *Content) { c.To = make([]string, 101) },
		"many-inline":         func(c *Content) { c.Inline = make([]Inline, 17) },
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			content := mailContent(t)
			change(&content)
			if _, err := NewMessage(content); err == nil {
				t.Fatal("invalid input accepted")
			}
		})
	}
}
func TestMIMEEncodedExpansionAndCancellation(t *testing.T) {
	input := mailContent(t)
	message, err := NewMessage(input)
	if err != nil {
		t.Fatal(err)
	}
	limits := defaults(OptionsV1{})
	limits.MaxMIMEBytes = 1024
	if _, err = compose(context.Background(), message, limits); !errors.Is(err, ErrLimit) {
		t.Fatal("encoded MIME budget ignored")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err = compose(ctx, message, defaults(OptionsV1{})); !errors.Is(err, context.Canceled) {
		t.Fatal("composition ignored cancellation")
	}
}
func TestNativeBodyModesAndOpaqueAttachments(t *testing.T) {
	for _, mode := range []string{"text", "html", "alternatives"} {
		t.Run(mode, func(t *testing.T) {
			content := mailContent(t)
			content.Inline = nil
			if mode == "text" {
				content.HTML = ""
			}
			if mode == "html" {
				content.Text = ""
			}
			content.Attachments = []Attachment{{Name: "empty.bin", ContentType: "application/octet-stream"},
				{Name: "opaque.bin", ContentType: "application/octet-stream", Data: []byte{0, 1, 2, 255}}}
			message, err := NewMessage(content)
			if err != nil {
				t.Fatal(err)
			}
			encoded, err := compose(context.Background(), message, defaults(OptionsV1{}))
			if err != nil {
				t.Fatal(err)
			}
			_, parts := parseMIME(t, encoded)
			seen := map[string]int{}
			files := 0
			for _, part := range parts {
				seen[part.kind]++
				if part.filename == "empty.bin" {
					files++
					if len(part.data) != 0 {
						t.Fatal("empty attachment changed")
					}
				}
				if part.filename == "opaque.bin" {
					files++
					if !bytes.Equal(part.data, []byte{0, 1, 2, 255}) {
						t.Fatal("opaque attachment changed")
					}
				}
			}
			if files != 2 || mode == "text" && seen["text/html"] != 0 || mode == "html" && seen["text/plain"] != 0 ||
				mode == "alternatives" && (seen["text/plain"] != 1 || seen["text/html"] != 1) {
				t.Fatal("body mode changed")
			}
		})
	}
}
func FuzzMessageHeaders(f *testing.F) {
	for _, value := range []string{"Normal subject", "Hello\r\nBcc: x@y.test", "Temperature: 20 °C", "\x00", "<script>", "=?utf-8?b?SGVsbG8=?="} {
		f.Add(value)
	}
	f.Fuzz(func(t *testing.T, subject string) {
		if len(subject) > 2048 {
			return
		}
		message, err := NewMessage(Content{ID: "fuzz@fixture.test", From: "from@fixture.test", To: []string{"to@fixture.test"}, Subject: subject, Text: "text"})
		if err != nil {
			return
		}
		encoded, err := compose(context.Background(), message, defaults(OptionsV1{}))
		if err != nil {
			t.Fatal(err)
		}
		parsed, _ := parseMIME(t, encoded)
		decoded, err := (&mime.WordDecoder{}).DecodeHeader(parsed.Header.Get("Subject"))
		if err != nil || decoded != subject {
			t.Fatal("subject round trip changed")
		}
	})
}
func BenchmarkComposeMIME(b *testing.B) {
	message, err := NewMessage(mailContent(b))
	if err != nil {
		b.Fatal(err)
	}
	limits := defaults(OptionsV1{})
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := compose(context.Background(), message, limits); err != nil {
			b.Fatal(err)
		}
	}
}
