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
	"io"
	"mime"
	stdmail "net/mail"
	"strings"
	"testing"

	"github.com/frost-leo/fathomry/internal/fault"
	sdk "github.com/wneessen/go-mail"
)

func TestRecheckedPGPCollisions(t *testing.T) {
	for _, payload := range []string{"ciphertext\r\n--collision--\r\ntrailing", "--collision\r\ntrailing"} {
		for _, mode := range []int{1, 2} {
			for _, encoding := range []string{"7bit", "base64"} {
				parts := []Part{
					{ContentType: "application/pgp-encrypted", Encoding: "7bit", Data: []byte("Version: 1\r\n")},
					{ContentType: "application/octet-stream", Encoding: encoding, Data: []byte(payload)},
				}
				if mode == 2 {
					parts[0].ContentType = "text/plain"
					parts[1].ContentType = "application/pgp-signature"
				}
				for _, boundary := range []string{"collision", "unrelated-boundary"} {
					message := mustMessage(t, Content{PGP: mode, PGPHash: "pgp-sha256", Boundary: boundary, Parts: parts})
					wire, err := compose(context.Background(), message, defaults(OptionsV1{}))
					if encoding == "7bit" && boundary == "collision" {
						if !errors.Is(err, ErrInput) {
							t.Fatal("PGP delimiter collision was not refused", err)
						}
						continue
					}
					if err != nil {
						t.Fatal("non-colliding control rejected", err)
					}
					_, decoded := parseMIME(t, wire)
					if len(decoded) != 2 || !bytes.Equal(decoded[1].data, []byte(payload)) {
						t.Fatal("PGP ciphertext changed")
					}
				}
			}
		}
	}
}

func TestRecheckedEMLOptionalFilenamesAndEncodedHeaders(t *testing.T) {
	bound := localBound(t)
	defer drain(t, bound.inbox)
	for _, disposition := range []string{"attachment", "inline"} {
		raw := "From: sender@example.invalid\r\nTo: to@example.invalid\r\nContent-Type: multipart/mixed; boundary=example\r\n\r\n--example\r\nContent-Type: application/octet-stream\r\nContent-Disposition: " + disposition + "\r\nContent-Transfer-Encoding: base64\r\n\r\nYQ==\r\n--example--\r\n"
		native, err := sdk.EMLToMsgFromReader(strings.NewReader(raw))
		if err != nil {
			t.Fatal("native optional filename control", err)
		}
		files := native.GetAttachments()
		if disposition == "inline" {
			files = native.GetEmbeds()
		}
		if len(files) != 1 || files[0].Name != "generic.attachment" {
			t.Fatal("selected native default changed")
		}
		receipt, err := bound.client.Import(context.Background(), fault.Correlation{Call: "import-optional-" + disposition}, Input{Bytes: []byte(raw)}, ImportOptions{})
		result := resolved(t, receipt, err)
		if result.Err() != nil {
			t.Fatal(result.Err())
		}
		message, _ := result.Outcome.Value.PreparedMessage()
		_, parts := parseMIME(t, message.EMLCopy())
		if len(parts) != 2 || parts[1].filename != "generic.attachment" || string(parts[1].data) != "a" {
			t.Fatalf("optional filename/body not retained: %+v", parts)
		}
		if disposition == "inline" && message.ContentCopy().Inline[0].ID != "generic.attachment" {
			t.Fatal("inline fallback Content-ID changed")
		}
	}
	decoder := &mime.WordDecoder{CharsetReader: func(label string, input io.Reader) (io.Reader, error) {
		raw, err := io.ReadAll(input)
		if err != nil {
			return nil, err
		}
		switch label {
		case "windows-1252":
			if !bytes.Equal(raw, []byte{'c', 'a', 'f', 0xe9}) {
				t.Fatal("Windows-1252 octets changed")
			}
			return strings.NewReader("café"), nil
		case "gbk":
			if !bytes.Equal(raw, []byte{0xd6, 0xd0, 0xce, 0xc4}) {
				t.Fatal("GBK octets changed")
			}
			return strings.NewReader("中文"), nil
		default:
			return nil, errors.New("unknown test charset")
		}
	}}
	for _, test := range []struct{ encoded, want string }{
		{"=?windows-1252?Q?caf=E9?=", "café"},
		{"=?gbk?B?1tDOxA==?=", "中文"},
		{"=?utf-8?B?5Lit5paH?=", "中文"},
		{"=?x-private-charset?Q?opaque?=", ""},
	} {
		raw := "From: sender@example.invalid\r\nTo: to@example.invalid\r\nSubject: " + test.encoded + "\r\nOrganization: " + test.encoded + "\r\nContent-Type: text/plain\r\n\r\nbody\r\n"
		receipt, err := bound.client.Import(context.Background(), fault.Correlation{Call: "import-encoded"}, Input{Bytes: []byte(raw)}, ImportOptions{})
		result := resolved(t, receipt, err)
		if result.Err() != nil {
			t.Fatal(result.Err())
		}
		message, _ := result.Outcome.Value.PreparedMessage()
		if !message.ContentCopy().SubjectPreformatted {
			t.Fatal("imported encoded Subject not explicitly represented")
		}
		parsed, err := stdmail.ReadMessage(bytes.NewReader(message.EMLCopy()))
		if err != nil {
			t.Fatal(err)
		}
		for _, name := range []string{"Subject", "Organization"} {
			if parsed.Header.Get(name) != test.encoded {
				t.Fatal("encoded header was not preserved")
			}
			if test.want != "" {
				value, err := decoder.DecodeHeader(parsed.Header.Get(name))
				if err != nil || value != test.want {
					t.Fatal("decoded header changed", value, err)
				}
			}
		}
	}
	if _, err := NewMessage(Content{SubjectPreformatted: true, Subject: "subject\r\nBcc: hidden@example.invalid"}); !errors.Is(err, ErrInput) {
		t.Fatal("preformatted Subject injection allowed")
	}
	normal, err := compose(context.Background(), mustMessage(t, Content{Subject: "=?utf-8?Q?literal?="}), defaults(OptionsV1{}))
	if err != nil {
		t.Fatal(err)
	}
	parsed, _ := stdmail.ReadMessage(bytes.NewReader(normal))
	literal, err := new(mime.WordDecoder).DecodeHeader(parsed.Header.Get("Subject"))
	if err != nil || literal != "=?utf-8?Q?literal?=" {
		t.Fatal("ordinary literal Subject interpreted as encoded words")
	}
}

func TestRecheckedSuccessfulServerResponseSurvivesCleanup(t *testing.T) {
	for _, cleanupFailure := range []bool{false, true} {
		options := peerOptions{dataReply: "250 2.0.0 relay-private-queue-identifier"}
		if cleanupFailure {
			options.resetReply = "550 reset rejected"
		}
		peer := newPeer(t, options)
		bound := bindTest(t, testOptions(peer), 1)
		result := sendTest(t, bound.client, "server-response", plainMessage(t, "response"))
		delivery := result.Outcome.Value.Messages()[0]
		if delivery.Effect() != Accepted || delivery.Code() != 250 || delivery.ServerResponse() != "2.0.0 relay-private-queue-identifier" {
			t.Fatal("successful DATA reply lost")
		}
		if (result.Err() != nil) != cleanupFailure {
			t.Fatal("cleanup outcome missing")
		}
		drain(t, bound.inbox)
	}
	peer := newPeer(t, peerOptions{dropAck: true})
	bound := bindTest(t, testOptions(peer), 1)
	result := sendTest(t, bound.client, "missing-response", plainMessage(t, "missing"))
	delivery := result.Outcome.Value.Messages()[0]
	if delivery.Effect() != Unknown || delivery.ServerResponse() != "" {
		t.Fatal("invented a successful response without ACK")
	}
	drain(t, bound.inbox)
}

func TestRecheckedOpaqueCIDAndHeaderNames(t *testing.T) {
	for _, id := range []string{"logo", "logo.png", "report 2026.pdf", "café.txt", "tag!value@example.invalid"} {
		message := mustMessage(t, Content{Headers: map[string]string{"X_Trace": "opaque"}, Inline: []Inline{{ID: id, Name: "data.txt", Data: []byte("opaque")}}})
		wire, err := compose(context.Background(), message, defaults(OptionsV1{}))
		if err != nil {
			t.Fatal(err)
		}
		parsed, parts := parseMIME(t, wire)
		if parsed.Header.Get("X_Trace") != "opaque" || len(parts) != 2 || parts[1].cid != "<"+id+">" {
			t.Fatalf("opaque CID/header changed: %+v", parts)
		}
	}
	for _, id := range []string{"<logo>", "x/y", "logo\r\nBcc: hidden@example.invalid", ""} {
		if _, err := NewMessage(Content{Inline: []Inline{{ID: id, Name: "file.txt"}}}); !errors.Is(err, ErrInput) {
			t.Fatal("unsafe CID permitted")
		}
	}
	for _, id := range []string{"mail@[IPv6:::1]", "mail@[127.0.0.1]", `"quoted local"@example.invalid`} {
		message := mustMessage(t, Content{ID: id})
		wire, err := compose(context.Background(), message, defaults(OptionsV1{}))
		if err != nil {
			t.Fatal(err)
		}
		parsed, _ := stdmail.ReadMessage(bytes.NewReader(wire))
		if parsed.Header.Get("Message-ID") != "<"+id+">" {
			t.Fatal("protocol-valid Message-ID changed")
		}
	}
}
