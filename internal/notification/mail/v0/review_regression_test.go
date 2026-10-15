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
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"io"
	"mime"
	"mime/multipart"
	stdmail "net/mail"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/frost-leo/fathomry/internal/fault"
)

func TestReviewedMIMEEncodingsBoundariesAndFrozenHeaders(t *testing.T) {
	for _, encoding := range []string{"7bit", "quoted-printable", "base64", "8bit"} {
		for _, route := range []string{"text", "part", "attachment", "inline"} {
			t.Run(encoding+"/"+route, func(t *testing.T) {
				payload := []byte("left=right =41 \t\r\n")
				input := Content{}
				switch route {
				case "text":
					input.Text, input.Encoding = string(payload), encoding
				case "part":
					input.Parts = []Part{{ContentType: "text/plain", Encoding: encoding, Data: payload}}
				case "attachment":
					input.Attachments = []Attachment{{Name: "data.txt", ContentType: "text/plain", Encoding: encoding, Data: payload}}
				case "inline":
					input.Inline = []Inline{{ID: "fixture@example.invalid", Name: "data.txt", ContentType: "text/plain", Encoding: encoding, Data: payload}}
				}
				message := mustMessage(t, input)
				wire, err := compose(context.Background(), message, defaults(OptionsV1{}))
				if err != nil {
					t.Fatal(err)
				}
				parsed, parts := parseMIME(t, wire)
				if !bytes.Equal(parts[len(parts)-1].data, payload) {
					t.Fatalf("decoded content changed: %q", parts[len(parts)-1].data)
				}
				if route == "text" || route == "part" {
					if parsed.Header.Get("Content-Transfer-Encoding") != encoding {
						t.Fatal("declared encoding lost")
					}
				}
				if route == "attachment" || route == "inline" {
					parsed, err = stdmail.ReadMessage(bytes.NewReader(wire))
					if err != nil {
						t.Fatal(err)
					}
					_, parameters, err := mime.ParseMediaType(parsed.Header.Get("Content-Type"))
					if err != nil {
						t.Fatal(err)
					}
					reader := multipart.NewReader(parsed.Body, parameters["boundary"])
					found := false
					for {
						part, err := reader.NextRawPart()
						if err == io.EOF {
							break
						}
						if err != nil {
							t.Fatal(err)
						}
						if part.FileName() == "data.txt" {
							found = true
							if part.Header.Get("Content-Transfer-Encoding") != encoding {
								t.Fatal("file encoding changed")
							}
						}
					}
					if !found {
						t.Fatal("file missing")
					}
				}
			})
		}
	}
	for _, boundary := range []string{"fixed-boundary", "boundary with space"} {
		input := mailContent(t)
		input.Boundary = boundary
		wire, err := compose(context.Background(), mustMessage(t, input), defaults(OptionsV1{}))
		if err != nil {
			t.Fatal(err)
		}
		parsed, parts := parseMIME(t, wire)
		_, params, err := mime.ParseMediaType(parsed.Header.Get("Content-Type"))
		if err != nil || params["boundary"] != boundary || len(parts) != 4 {
			t.Fatal("outer/nested boundary contract", err)
		}
	}
	collision := Content{Boundary: "collision", Text: "\r\n--collision\r\n", HTML: "alternative"}
	if _, err := compose(context.Background(), mustMessage(t, collision), defaults(OptionsV1{})); !errors.Is(err, ErrInput) {
		t.Fatal("boundary collision accepted", err)
	}
	input := Content{Fields: []Header{{Name: "X-Long", Values: []string{strings.Repeat("汉", 300)}}}, Text: "body"}
	message := mustMessage(t, input)
	original := message.ContentCopy().Fields[0].Values[0]
	var workers sync.WaitGroup
	for i := 0; i < 16; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			if _, err := compose(context.Background(), message, defaults(OptionsV1{})); err != nil {
				t.Error(err)
			}
		}()
	}
	workers.Wait()
	if message.ContentCopy().Fields[0].Values[0] != original || input.Fields[0].Values[0] != original {
		t.Fatal("frozen header mutation")
	}
}

func TestReviewedEMLNestedFilenamesAndSevenBit(t *testing.T) {
	bound := localBound(t)
	defer drain(t, bound.inbox)
	for _, filename := range []string{"filename=a", `filename="semi;colon.txt"`, "filename*=utf-8''caf%C3%A9.txt"} {
		want := map[string]string{"filename=a": "a", `filename="semi;colon.txt"`: "semi;colon.txt", "filename*=utf-8''caf%C3%A9.txt": "café.txt"}[filename]
		raw := "From: sender@example.invalid\r\nTo: to@example.invalid\r\nContent-Type: multipart/mixed; boundary=outer\r\n\r\n--outer\r\nContent-Type: multipart/alternative; boundary=inner\r\n\r\n--inner\r\nContent-Type: text/plain\r\nContent-Transfer-Encoding: 7bit\r\n\r\nleft=right =41 \t\r\n--inner\r\nContent-Type: text/html\r\n\r\n<p>html</p>\r\n--inner--\r\n--outer\r\nContent-Type: text/plain\r\nContent-Disposition: attachment; " + filename + "\r\nContent-Transfer-Encoding: 7bit\r\n\r\nfile=41\r\n--outer--\r\n"
		receipt, err := bound.client.Import(context.Background(), fault.Correlation{Call: "import-fixed"}, Input{Bytes: []byte(raw)}, ImportOptions{})
		out := resolved(t, receipt, err)
		if out.Err() != nil {
			t.Fatal(out.Err())
		}
		message, ok := out.Outcome.Value.PreparedMessage()
		if !ok {
			t.Fatal("imported message absent")
		}
		_, parts := parseMIME(t, message.EMLCopy())
		if len(parts) != 3 || string(parts[0].data) != "left=right =41 \t" || parts[2].filename != want || string(parts[2].data) != "file=41" {
			t.Fatalf("import fidelity: %+v", parts)
		}
		repeated := prepareTest(t, bound.client, message, Preparation{})
		if !bytes.Equal(repeated.EMLCopy(), message.EMLCopy()) {
			t.Fatal("repeated preparation redecoded true 7bit")
		}
	}
}
func TestReviewedDKIMMetadataAndSourcePrecedence(t *testing.T) {
	_, keyPEM, _ := testSigningKey(t)
	for _, auid := range []string{"sender@example.invalid; d=second.invalid", "sender@unrelated.invalid"} {
		if _, err := NewDKIM(DKIMOptions{Domain: "example.invalid", Selector: "test", AUID: auid}, keyPEM); err == nil {
			t.Fatal("invalid AUID accepted")
		}
	}
	for _, auid := range []string{"@example.invalid", "tag=value@example.invalid", `"a; b"@example.invalid`} {
		value, err := NewDKIM(DKIMOptions{Domain: "example.invalid", Selector: "test", AUID: auid}, keyPEM)
		if err != nil {
			t.Fatal("valid AUID refused", auid, err)
		}
		wire, err := compose(context.Background(), plainMessage(t, "auid"), defaults(OptionsV1{}))
		if err != nil {
			t.Fatal(err)
		}
		wire, err = value.sign(context.Background(), wire, 128<<10)
		if err != nil {
			t.Fatal(err)
		}
		parsed, _ := stdmail.ReadMessage(bytes.NewReader(wire))
		header := parsed.Header.Get("DKIM-Signature")
		if strings.Count(header, "d=example.invalid") != 1 || strings.Contains(header, "i=tag=value") || strings.Contains(header, "i=\"a; b\"") {
			t.Fatal("AUID tag injection/encoding")
		}
	}
	perMessage, err := NewDKIM(DKIMOptions{Domain: "example.invalid", Selector: "message"}, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	sourceSigner, err := NewDKIM(DKIMOptions{Domain: "example.invalid", Selector: "source"}, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	local := localBound(t)
	prepared := prepareTest(t, local.client, plainMessage(t, "source-sign"), Preparation{DKIM: perMessage})
	drain(t, local.inbox)
	frozen := prepared.EMLCopy()
	peer := newPeer(t, peerOptions{})
	settings := testOptions(peer)
	settings.Native.AlwaysDKIM = sourceSigner
	owned := bindTest(t, settings, 1)
	out := sendTest(t, owned.client, "source-sign", prepared)
	if out.Err() != nil {
		t.Fatal(out.Err())
	}
	drain(t, owned.inbox)
	captures, _ := peer.snapshot()
	parsed, _ := stdmail.ReadMessage(bytes.NewReader(captures[0].data))
	if len(parsed.Header["Dkim-Signature"]) != 1 || !strings.Contains(parsed.Header.Get("DKIM-Signature"), "s=source;") || !bytes.Equal(frozen, prepared.EMLCopy()) {
		t.Fatal("source precedence changed immutable message or stacked signatures")
	}
}
func TestReviewedSMIMESignsCorrectedEntity(t *testing.T) {
	bound := localBound(t)
	defer drain(t, bound.inbox)
	_, rsaPEM, certificatePEM := testSigningKey(t)
	for _, keyType := range []string{"rsa", "ecdsa"} {
		keyPEM, certPEM := rsaPEM, certificatePEM
		if keyType == "ecdsa" {
			key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
			if err != nil {
				t.Fatal(err)
			}
			certBlock, _ := pem.Decode(certificatePEM)
			template, err := x509.ParseCertificate(certBlock.Bytes)
			if err != nil {
				t.Fatal(err)
			}
			template.SignatureAlgorithm = x509.ECDSAWithSHA256
			template.PublicKey, template.PublicKeyAlgorithm = &key.PublicKey, x509.ECDSA
			der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
			if err != nil {
				t.Fatal(err)
			}
			encoded, err := x509.MarshalECPrivateKey(key)
			if err != nil {
				t.Fatal(err)
			}
			keyPEM = pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: encoded})
			certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
		}
		signer, err := NewSMIME(certPEM, keyPEM)
		if err != nil {
			t.Fatal(err)
		}
		for _, kind := range []string{"sevenbit", "attachment", "pgp"} {
			t.Run(keyType+"/"+kind, func(t *testing.T) {
				input := Content{From: "sender@example.invalid", To: []string{"to@example.invalid"}, Encoding: "7bit", Text: "left=right =41 \t\r\n"}
				if kind == "attachment" {
					input.Attachments = []Attachment{{Name: "data.txt", Encoding: "7bit", Data: []byte("file=bytes \t\r\n")}}
					input.HTML = "<p>html</p>"
					input.Boundary = "chosen boundary"
				}
				if kind == "pgp" {
					input.Text = ""
					input.PGP = 1
					input.Parts = []Part{{ContentType: "application/pgp-encrypted", Encoding: "7bit", Data: []byte("Version: 1\r\n")}, {ContentType: "application/octet-stream", Encoding: "7bit", Data: []byte("opaque=41\r\n")}}
				}
				message := prepareTest(t, bound.client, mustMessage(t, input), Preparation{SMIME: signer})
				directory := t.TempDir()
				path := filepath.Join(directory, "signed.eml")
				if err := os.WriteFile(path, message.EMLCopy(), 0600); err != nil {
					t.Fatal(err)
				}
				target := filepath.Join(directory, "verified")
				if output, err := exec.Command("openssl", "smime", "-verify", "-noverify", "-in", path, "-out", target).CombinedOutput(); err != nil {
					t.Fatalf("independent %s CMS: %v %s", kind, err, output)
				}
				extracted, err := os.ReadFile(target)
				if err != nil {
					t.Fatal(err)
				}
				parsed, err := stdmail.ReadMessage(bytes.NewReader(message.EMLCopy()))
				if err != nil {
					t.Fatal(err)
				}
				_, params, err := mime.ParseMediaType(parsed.Header.Get("Content-Type"))
				if err != nil {
					t.Fatal(err)
				}
				// A raw entity begins immediately after the first multipart delimiter.
				_, rest, _ := bytes.Cut(message.EMLCopy(), []byte("--"+params["boundary"]+"\r\n"))
				exact, _, _ := bytes.Cut(rest, []byte("\r\n--"+params["boundary"]+"\r\n"))
				if !bytes.Equal(extracted, exact) {
					t.Fatal("CMS verified a different entity than the emitted bytes")
				}
				if _, parts := parseMIME(t, extracted); len(parts) == 0 {
					t.Fatal("signed entity malformed")
				}
			})
		}
	}
}
