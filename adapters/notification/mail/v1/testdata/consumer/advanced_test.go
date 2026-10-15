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

package consumer

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	ht "html/template"
	"io"
	"mime"
	"mime/multipart"
	stdmail "net/mail"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"

	mail "github.com/frost-leo/fathomry/adapters/notification/mail/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
)

func TestPublicAdvancedPreparationImportAndLocalDelivery(t *testing.T) {
	ctx, cancel := waitContext(t)
	defer cancel()
	config := mail.Settings{Name: "advanced", Host: "localhost", Port: 465, Hello: "test.invalid", TLSMode: "implicit", Auth: "none", MaxActive: 1, MaxMessageBytes: 64 << 10, MaxMIMEBytes: 128 << 10}
	policy, err := mail.Recommend(config)
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := adapters.New(ctx, policy.Runtime)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close(context.Background())
	inbox, err := adapters.NewInbox[mail.Result](policy.Evidence)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := mail.Open(ctx, config, mail.Dependencies{Runtime: runtime, Evidence: inbox})
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close(context.Background())
	ack := func() {
		entry, _ := take(t, ctx, inbox)
		if err = entry.Ack(); err != nil {
			t.Fatal(err)
		}
	}
	input, err := mail.NewMessage(mail.Content{From: "sender@example.invalid", To: []string{"to@example.invalid"}, Text: "base"})
	if err != nil {
		t.Fatal(err)
	}
	result, err := owner.Client().PrepareMessage(ctx, input, mail.Preparation{
		Parts: []mail.PartInput{{Part: mail.Part{ContentType: "text/plain"}, Input: mail.Input{Write: func(_ context.Context, writer io.Writer) error {
			_, err := io.WriteString(writer, "writer-body")
			return err
		}}}},
		Files:     []mail.FileInput{{Input: mail.Input{FS: fstest.MapFS{"asset.txt": &fstest.MapFile{Data: []byte("asset")}}, Name: "asset.txt"}, File: mail.Attachment{Name: "asset.txt", Fields: []mail.Header{{Name: "X-Part", Values: []string{"preserved"}}}}}},
		Templates: []mail.TemplateInput{{HTML: ht.Must(ht.New("supplied").Parse("<p>{{.}}</p>")), Data: "<private>"}},
		Transforms: []mail.Transform{{Name: "caller", Apply: func(_ context.Context, value mail.Content) (mail.Content, error) {
			value.Headers = map[string]string{"Organization": "transformed"}
			return value, nil
		}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	prepared, ok := result.PreparedMessage()
	if !ok {
		t.Fatal("missing advanced message")
	}
	ack()
	wire := prepared.EMLCopy()
	if !bytes.Contains(wire, []byte("X-Part: preserved")) || !bytes.Contains(wire, []byte("&lt;private&gt;")) {
		t.Fatal("file/header/template mapping")
	}
	imported, err := owner.Client().Import(ctx, mail.Input{Bytes: []byte("From: sender@example.invalid\r\nTo: to@example.invalid\r\nReply-To: lost@example.invalid\r\nSubject: imported\r\nContent-Type: text/plain\r\n\r\ncontent\r\n")}, mail.ImportOptions{From: "bounce@example.invalid", Recipients: []string{"hidden@example.invalid"}})
	if err != nil {
		t.Fatal(err)
	}
	ack()
	normalized, ok := imported.PreparedMessage()
	if !ok || len(imported.HeaderChanges()) == 0 {
		t.Fatal("import mapping")
	}
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	dkim, err := mail.NewDKIM(mail.DKIMOptions{Domain: "example.invalid", Selector: "fixture", SignHeaders: []string{"From", "Subject"}, OversignHeaders: []string{"From"}, BodyCanonicalization: "relaxed", HeaderCanonicalization: "relaxed"}, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}))
	if err != nil {
		t.Fatal(err)
	}
	signed, err := owner.Client().PrepareMessage(ctx, input, mail.Preparation{DKIM: dkim})
	if err != nil {
		t.Fatal(err)
	}
	ack()
	signedMessage, _ := signed.PreparedMessage()
	parsed, err := stdmail.ReadMessage(bytes.NewReader(signedMessage.EMLCopy()))
	if err != nil || !strings.Contains(parsed.Header.Get("DKIM-Signature"), "a=ed25519-sha256") {
		t.Fatal("public signing mapping")
	}
	if _, err = owner.Client().PrepareMessage(ctx, input, mail.Preparation{DKIM: &mail.DKIM{}}); err == nil {
		t.Fatal("zero signer silently omitted")
	}
	ack()
	pgp, err := mail.NewMessage(mail.Content{PGP: 1, Parts: []mail.Part{{ContentType: "application/pgp-encrypted", Data: []byte("Version: 1")}, {ContentType: "application/octet-stream", Data: []byte("ciphertext")}}})
	if err != nil {
		t.Fatal(err)
	}
	encrypted, err := owner.Client().PrepareMessage(ctx, pgp, mail.Preparation{})
	if err != nil {
		t.Fatal(err)
	}
	ack()
	pgpMessage, _ := encrypted.PreparedMessage()
	parsed, err = stdmail.ReadMessage(bytes.NewReader(pgpMessage.EMLCopy()))
	if err != nil {
		t.Fatal(err)
	}
	kind, parameters, err := mime.ParseMediaType(parsed.Header.Get("Content-Type"))
	if err != nil || kind != "multipart/encrypted" {
		t.Fatal("PGP mapping")
	}
	reader := multipart.NewReader(parsed.Body, parameters["boundary"])
	parts := 0
	for {
		part, err := reader.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if _, err = io.Copy(io.Discard, part); err != nil {
			t.Fatal(err)
		}
		parts++
	}
	if parts != 2 {
		t.Fatal("PGP structure")
	}
	directory := t.TempDir()
	executable := filepath.Join(directory, "fake-sendmail")
	if err = os.WriteFile(executable, []byte("#!/bin/sh\ndirectory=$1\nshift\nprintf '%s\\n' \"$@\" > \"$directory/argv\"\ncat > \"$directory/mime\"\n"), 0700); err != nil {
		t.Fatal(err)
	}
	local, err := owner.Client().Sendmail(ctx, normalized, mail.LocalDelivery{Executable: executable, Arguments: []string{directory}})
	if err != nil {
		t.Fatal(err)
	}
	ack()
	if !local.ProcessInfo().Started || !local.ProcessInfo().ExitKnown || local.ProcessInfo().ExitCode != 0 {
		t.Fatal("local result mapping")
	}
	arguments, err := os.ReadFile(filepath.Join(directory, "argv"))
	if err != nil || string(arguments) != "-oi\n-f\nbounce@example.invalid\n--\nhidden@example.invalid\n" {
		t.Fatal("imported envelope mapping")
	}
	captured, err := os.ReadFile(filepath.Join(directory, "mime"))
	if err != nil || !bytes.Equal(captured, normalized.EMLCopy()) {
		t.Fatal("local MIME mapping")
	}
}
