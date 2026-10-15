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
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"io"
	"io/fs"
	"math/big"
	"mime"
	stdmail "net/mail"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"testing/fstest"
	tt "text/template"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
)

func TestExpandedMessageFidelityAndRefusal(t *testing.T) {
	input := Content{From: "o'hara@example.invalid", To: []string{"\"quoted local\"@example.invalid", "u@example.invalid", "u@example.invalid"}, EnvelopeFrom: "<>",
		Headers: map[string]string{"Organization": "Fixture", "References": "<old@example.invalid>"}, Fields: []Header{{Name: "List-Unsubscribe", Values: []string{"<https://example.invalid/unsubscribe>"}, Preformatted: true}}}
	message, err := NewMessage(input)
	if err != nil {
		t.Fatal(err)
	}
	if message.sender != "" || len(message.recipients) != 3 || message.recipients[0] != "\"quoted local\"@example.invalid" {
		t.Fatal("envelope fidelity")
	}
	wire, err := compose(context.Background(), message, defaults(OptionsV1{}))
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := stdmail.ReadMessage(bytes.NewReader(wire))
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Header.Get("Organization") != "Fixture" || parsed.Header.Get("List-Unsubscribe") != "<https://example.invalid/unsubscribe>" || parsed.Header.Get("Subject") != "" {
		t.Fatal("header fidelity")
	}
	if _, err := NewMessage(Content{}); err != nil {
		t.Fatal("empty MIME preparation rejected", err)
	}
	if err := defaults(OptionsV1{}).check([]Message{mustMessage(t, Content{})}); err == nil {
		t.Fatal("empty sending envelope accepted")
	}
	for _, name := range []string{"Bcc", "bCc", "Resent-Bcc", "Content-Type", "Subject"} {
		bad := input
		bad.Headers = map[string]string{name: "hidden@example.invalid"}
		if _, err := NewMessage(bad); err == nil {
			t.Fatalf("reserved header %s accepted", name)
		}
	}
	unicode := input
	unicode.EnvelopeFrom = "séndér@example.invalid"
	unicode.To = []string{"用户@example.invalid"}
	unicodeMessage, err := NewMessage(unicode)
	if err != nil || !unicodeMessage.requiresUTF8() {
		t.Fatal("SMTPUTF8 input unavailable", err)
	}
	input.Parts = []Part{{ContentType: "text/plain", Charset: "ISO-8859-1", Encoding: "base64", Data: []byte{0xe9}}}
	binary := mustMessage(t, input)
	wire, err = compose(context.Background(), binary, defaults(OptionsV1{}))
	if err != nil {
		t.Fatal(err)
	}
	_, parts := parseMIME(t, wire)
	if len(parts) != 1 || !bytes.Equal(parts[0].data, []byte{0xe9}) {
		t.Fatal("declared charset bytes changed")
	}
}

func mustMessage(t testing.TB, input Content) Message {
	t.Helper()
	result, err := NewMessage(input)
	if err != nil {
		t.Fatal(err)
	}
	return result
}
func localBound(t testing.TB) *bound {
	return bindTest(t, OptionsV1{Name: "local", Host: "localhost", Port: 465, Hello: "fixture.test", TLSMode: "implicit", Auth: "none", MaxActive: 1, MaxMessageBytes: 64 << 10, MaxMIMEBytes: 128 << 10}, 16)
}
func prepareTest(t testing.TB, client *Client, message Message, plan Preparation) Message {
	t.Helper()
	receipt, err := client.PrepareMessage(context.Background(), fault.Correlation{Call: "prepare"}, message, plan)
	out := resolved(t, receipt, err)
	if out.Err() != nil {
		t.Fatal(out.Err())
	}
	value, ok := out.Outcome.Value.PreparedMessage()
	if !ok {
		t.Fatal("no prepared message")
	}
	return value
}

type trackingFS struct {
	base              fs.FS
	opens, closes     int
	readErr, closeErr error
}
type trackingFile struct {
	fs.File
	owner *trackingFS
}

func (owner *trackingFS) Open(name string) (fs.File, error) {
	file, err := owner.base.Open(name)
	if err != nil {
		return nil, err
	}
	owner.opens++
	return &trackingFile{file, owner}, nil
}
func (file *trackingFile) Read(data []byte) (int, error) {
	if file.owner.readErr != nil {
		return 0, file.owner.readErr
	}
	return file.File.Read(data)
}
func (file *trackingFile) Close() error {
	file.owner.closes++
	return errors.Join(file.File.Close(), file.owner.closeErr)
}

func TestTrackedFSAllPathsAndBoundedTemplates(t *testing.T) {
	bound := localBound(t)
	defer drain(t, bound.inbox)
	for _, kind := range []string{"success", "read", "close", "over"} {
		t.Run(kind, func(t *testing.T) {
			payload := []byte("independent bytes")
			if kind == "over" {
				payload = bytes.Repeat([]byte("x"), 65<<10)
			}
			witness := errors.New("controlled-" + kind)
			filesystem := &trackingFS{base: fstest.MapFS{"fixture.txt": &fstest.MapFile{Data: payload}}}
			if kind == "read" {
				filesystem.readErr = witness
			}
			if kind == "close" {
				filesystem.closeErr = witness
			}
			receipt, err := bound.client.PrepareMessage(context.Background(), fault.Correlation{Call: kind}, plainMessage(t, kind), Preparation{Files: []FileInput{{Input: Input{FS: filesystem, Name: "fixture.txt"}, File: Attachment{Name: "fixture.txt", ContentType: "text/plain"}}}})
			outcome := resolved(t, receipt, err)
			if filesystem.opens != 1 || filesystem.closes != 1 {
				t.Fatalf("unjoined FS %d/%d", filesystem.opens, filesystem.closes)
			}
			if kind == "success" {
				if outcome.Err() != nil {
					t.Fatal(outcome.Err())
				}
				message, _ := outcome.Outcome.Value.PreparedMessage()
				_, parts := parseMIME(t, message.EMLCopy())
				if !bytes.Equal(parts[len(parts)-1].data, payload) {
					t.Fatal("attachment changed")
				}
			} else if kind == "over" {
				if !errors.Is(outcome.Err(), ErrLimit) {
					t.Fatal("overflow not refused")
				}
			} else if !errors.Is(outcome.Err(), witness) {
				t.Fatal("original read/close error lost")
			}
		})
	}
	template := tt.Must(tt.New("bounded").Parse("{{.}}"))
	message := prepareTest(t, bound.client, plainMessage(t, "template"), Preparation{Templates: []TemplateInput{{Text: template, Data: "rendered"}}, Transforms: []Transform{{Name: "add", Apply: func(_ context.Context, value Content) (Content, error) {
		value.Headers = map[string]string{"Organization": "transformed"}
		return value, nil
	}}}})
	parsed, parts := parseMIME(t, message.EMLCopy())
	if parsed.Header.Get("Organization") != "transformed" || !bytes.Contains(parts[1].data, []byte("rendered")) {
		t.Fatal("template/transform not applied")
	}
	receipt, err := bound.client.PrepareMessage(context.Background(), fault.Correlation{Call: "large-template"}, plainMessage(t, "template"), Preparation{Templates: []TemplateInput{{Text: template, Data: strings.Repeat("x", 65<<10)}}})
	if out := resolved(t, receipt, err); !errors.Is(out.Err(), ErrLimit) {
		t.Fatal("template exceeded pre-materialization budget")
	}
}

func TestNativeEMLLossIsReported(t *testing.T) {
	bound := localBound(t)
	defer drain(t, bound.inbox)
	raw := "From: sender@example.invalid\r\nTo: to@example.invalid\r\nBcc: hidden@example.invalid\r\nReply-To: reply@example.invalid\r\nX-Private: value\r\nSubject: kept\r\nContent-Type: text/plain\r\n\r\nbody\r\n"
	receipt, err := bound.client.Import(context.Background(), fault.Correlation{Call: "eml"}, Input{Bytes: []byte(raw)}, ImportOptions{})
	out := resolved(t, receipt, err)
	if out.Err() != nil {
		t.Fatal(out.Err())
	}
	message, ok := out.Outcome.Value.PreparedMessage()
	if !ok {
		t.Fatal("no MIME")
	}
	parsed, err := stdmail.ReadMessage(bytes.NewReader(message.EMLCopy()))
	if err != nil {
		t.Fatal(err)
	}
	subject, decodeErr := (&mime.WordDecoder{}).DecodeHeader(parsed.Header.Get("Subject"))
	if decodeErr != nil || subject != "kept" || parsed.Header.Get("Reply-To") != "" || parsed.Header.Get("X-Private") != "" || parsed.Header.Get("Bcc") != "" || len(message.recipients) != 2 {
		t.Fatal("native preservation/loss/envelope changed")
	}
	for _, name := range []string{"Reply-To", "X-Private", "Bcc"} {
		found := false
		for _, change := range out.Outcome.Value.HeaderChanges() {
			found = found || change.Name == name && change.Dropped
		}
		if !found {
			t.Fatal("unreported header loss", name)
		}
	}
}

func TestSendmailCapturesEnvelopeAndJoinsFailure(t *testing.T) {
	bound := localBound(t)
	defer drain(t, bound.inbox)
	directory := t.TempDir()
	command := filepath.Join(directory, "fake-sendmail")
	script := "#!/bin/sh\ndirectory=$1\nshift\nprintf '%s\\n' \"$@\" > \"$directory/argv\"\nhead -c 32768 /dev/zero >&2\ncat > \"$directory/mime\"\nexit 0\n"
	if err := os.WriteFile(command, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	message := mustMessage(t, Content{From: "from@example.invalid", EnvelopeFrom: "bounce@example.invalid", To: []string{"to@example.invalid"}, Bcc: []string{"hidden@example.invalid"}, Text: "synthetic"})
	receipt, err := bound.client.Sendmail(context.Background(), fault.Correlation{Call: "local"}, message, LocalDelivery{Executable: command, Arguments: []string{directory}})
	out := resolved(t, receipt, err)
	if out.Err() != nil {
		t.Fatal(out.Err())
	}
	process := out.Outcome.Value.ProcessInfo()
	if !process.Started || !process.ExitKnown || process.ExitCode != 0 || !process.StderrTruncated || len(process.StderrCopy()) != 16<<10 {
		t.Fatal("process facts", process)
	}
	argv, err := os.ReadFile(filepath.Join(directory, "argv"))
	if err != nil {
		t.Fatal(err)
	}
	if string(argv) != "-oi\n-f\nbounce@example.invalid\n--\nto@example.invalid\nhidden@example.invalid\n" {
		t.Fatalf("argv %q", argv)
	}
	wire, err := os.ReadFile(filepath.Join(directory, "mime"))
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := stdmail.ReadMessage(bytes.NewReader(wire))
	if err != nil || parsed.Header.Get("Bcc") != "" {
		t.Fatal("BCC leak")
	}
	if process.InputBytes != int64(len(wire)) {
		t.Fatal("stdin count mismatch")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if receipt, err := bound.client.Sendmail(ctx, fault.Correlation{Call: "canceled"}, message, LocalDelivery{Executable: command}); err == nil && receipt != nil {
		out, _ := receipt.Result()
		if out.Outcome.Value.ProcessInfo().Started {
			t.Fatal("pre-canceled process started")
		}
	}
}

func testSigningKey(t testing.TB) (*rsa.PrivateKey, []byte, []byte) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	cert := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "fixture.invalid"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature}
	der, err := x509.CreateCertificate(rand.Reader, cert, cert, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return key, keyPEM, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}
func TestDKIMAndSMIMEIndependentVerification(t *testing.T) {
	bound := localBound(t)
	defer drain(t, bound.inbox)
	key, keyPEM, certPEM := testSigningKey(t)
	signer, err := NewDKIM(DKIMOptions{Domain: "example.invalid", Selector: "fixture", HeaderCanonicalization: "relaxed", BodyCanonicalization: "simple", SignHeaders: []string{"from", "subject", "date"}}, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	signed := prepareTest(t, bound.client, plainMessage(t, "dkim"), Preparation{DKIM: signer})
	verify := func(wire []byte) error {
		parsed, err := stdmail.ReadMessage(bytes.NewReader(wire))
		if err != nil {
			return err
		}
		body, err := io.ReadAll(parsed.Body)
		if err != nil {
			return err
		}
		tags := map[string]string{}
		signatureHeader := parsed.Header.Get("DKIM-Signature")
		for _, part := range strings.Split(signatureHeader, ";") {
			name, value, _ := strings.Cut(strings.TrimSpace(part), "=")
			tags[name] = value
		}
		digest := sha256.Sum256(append(bytes.TrimRight(body, "\r\n"), '\r', '\n'))
		if base64.StdEncoding.EncodeToString(digest[:]) != tags["bh"] {
			return errors.New("independent body digest mismatch")
		}
		canonical := func(name, value string) string {
			return strings.ToLower(name) + ":" + strings.Join(strings.Fields(value), " ")
		}
		var headers strings.Builder
		for _, name := range strings.Split(tags["h"], ":") {
			headers.WriteString(canonical(name, parsed.Header.Get(name)) + "\r\n")
		}
		position := strings.LastIndex(signatureHeader, "b=")
		if position < 0 {
			return errors.New("missing b")
		}
		headers.WriteString(canonical("DKIM-Signature", signatureHeader[:position+2]))
		signature, err := base64.StdEncoding.DecodeString(strings.Join(strings.Fields(tags["b"]), ""))
		if err != nil {
			return err
		}
		hash := sha256.Sum256([]byte(headers.String()))
		return rsa.VerifyPKCS1v15(&key.PublicKey, crypto.SHA256, hash[:], signature)
	}
	wire := signed.EMLCopy()
	if err := verify(wire); err != nil {
		t.Fatal(err)
	}
	broken := bytes.Replace(wire, []byte("Synthetic"), []byte("Corrupted"), 1)
	if err := verify(broken); err == nil {
		t.Fatal("DKIM corruption control passed")
	}
	smime, err := NewSMIME(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	smimeMessage := prepareTest(t, bound.client, plainMessage(t, "smime"), Preparation{SMIME: smime})
	directory := t.TempDir()
	path := filepath.Join(directory, "signed.eml")
	if err := os.WriteFile(path, smimeMessage.EMLCopy(), 0600); err != nil {
		t.Fatal(err)
	}
	command := exec.Command("openssl", "smime", "-verify", "-noverify", "-in", path, "-out", filepath.Join(directory, "verified"))
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("independent CMS verification %v: %s", err, output)
	}
	corrupted := bytes.Replace(smimeMessage.EMLCopy(), []byte("Synthetic"), []byte("Corrupted"), 1)
	if bytes.Equal(corrupted, smimeMessage.EMLCopy()) {
		t.Fatal("corruption fixture did not change body")
	}
	if err := os.WriteFile(path, corrupted, 0600); err != nil {
		t.Fatal(err)
	}
	if output, err := exec.Command("openssl", "smime", "-verify", "-noverify", "-in", path, "-out", filepath.Join(directory, "corrupt")).CombinedOutput(); err == nil {
		t.Fatalf("CMS corruption control passed: %s", output)
	}
	keyPEM[0] = 'x'
	if _, err := signer.sign(context.Background(), signed.EMLCopy(), 128<<10); err != nil {
		t.Fatal("key input aliases", err)
	}
}

func TestContinueBatchDoesNotResendEarlierPosition(t *testing.T) {
	peer := newPeer(t, peerOptions{rejectMessage: 2})
	bound := bindTest(t, testOptions(peer), 1)
	messages := []Message{plainMessage(t, "first"), plainMessage(t, "second"), plainMessage(t, "third")}
	receipt, err := bound.client.SendBatchWithOptions(context.Background(), fault.Correlation{Call: "continue"}, messages, BatchOptions{ContinueOnError: true})
	result := resolved(t, receipt, err)
	if result.Err() == nil {
		t.Fatal("missing partial error")
	}
	deliveries := result.Outcome.Value.Messages()
	if !slices.Equal([]Effect{deliveries[0].Effect(), deliveries[1].Effect(), deliveries[2].Effect()}, []Effect{Accepted, NotAccepted, Accepted}) {
		t.Fatal("continuation effects")
	}
	captured, _ := peer.snapshot()
	if len(captured) != 2 || result.Attempts.Observed != 3 {
		t.Fatal("continuation resent or lost positions")
	}
	drain(t, bound.inbox)
}
