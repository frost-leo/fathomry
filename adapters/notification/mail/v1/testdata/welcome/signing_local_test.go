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

package main

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
	"math/big"
	"mime"
	stdmail "net/mail"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	mail "github.com/frost-leo/fathomry/adapters/notification/mail/v1"
)

func TestWelcomeCompositeDKIMAndSMIMEVerification(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	certificate := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "welcome.fixture.test"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature}
	der, err := x509.CreateCertificate(rand.Reader, certificate, certificate, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	dkim, err := mail.NewDKIM(mail.DKIMOptions{Domain: "fixture.test", Selector: "welcome", HeaderCanonicalization: "relaxed", BodyCanonicalization: "simple", SignHeaders: []string{"from", "subject", "date"}}, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	smime, err := mail.NewSMIME(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	session, _ := testMailSession(t, localSettings())
	content := reportWelcome(t, "signed-welcome")
	signed := preparedWelcome(t, session, content, func(plan *mail.Preparation) { plan.DKIM, plan.SMIME = dkim, smime })
	wire := signed.EMLCopy()
	if err := verifyWelcomeDKIM(wire, &key.PublicKey); err != nil {
		t.Fatal(err)
	}
	corrupted := bytes.Replace(wire, []byte("Welcome to Fathomry"), []byte("Corrupt to Fathomry"), 1)
	if bytes.Equal(corrupted, wire) {
		t.Fatal("signature negative control did not modify payload")
	}
	if err := verifyWelcomeDKIM(corrupted, &key.PublicKey); err == nil {
		t.Fatal("corrupted DKIM body verified")
	}
	directory := t.TempDir()
	path := filepath.Join(directory, "signed.eml")
	if err := os.WriteFile(path, wire, 0600); err != nil {
		t.Fatal(err)
	}
	verified := filepath.Join(directory, "verified.eml")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if output, err := exec.CommandContext(ctx, "openssl", "smime", "-verify", "-noverify", "-in", path, "-out", verified).CombinedOutput(); err != nil {
		t.Fatalf("independent welcome CMS verification: %v\n%s", err, output)
	}
	extracted, err := os.ReadFile(verified)
	if err != nil {
		t.Fatal(err)
	}
	root, err := stdmail.ReadMessage(bytes.NewReader(wire))
	if err != nil {
		t.Fatal(err)
	}
	_, params, err := mime.ParseMediaType(root.Header.Get("Content-Type"))
	if err != nil {
		t.Fatal(err)
	}
	_, rest, ok := bytes.Cut(wire, []byte("--"+params["boundary"]+"\r\n"))
	if !ok {
		t.Fatal("signed entity absent")
	}
	exact, _, ok := bytes.Cut(rest, []byte("\r\n--"+params["boundary"]+"\r\n"))
	if !ok || !bytes.Equal(extracted, exact) {
		t.Fatal("CMS verified bytes other than the transmitted entity")
	}
	_, parts := parseWelcome(t, extracted)
	images, files := 0, 0
	for _, part := range parts {
		if part.cid != "" {
			images++
		} else if part.filename != "" {
			files++
		}
	}
	if images != 4 || files != 5 {
		t.Fatal("signing lost welcome MIME components")
	}
	if err := os.WriteFile(path, corrupted, 0600); err != nil {
		t.Fatal(err)
	}
	if output, err := exec.CommandContext(ctx, "openssl", "smime", "-verify", "-noverify", "-in", path, "-out", filepath.Join(directory, "corrupt")).CombinedOutput(); err == nil {
		t.Fatalf("corrupted CMS verified: %s", output)
	}
}

func verifyWelcomeDKIM(wire []byte, key *rsa.PublicKey) error {
	message, err := stdmail.ReadMessage(bytes.NewReader(wire))
	if err != nil {
		return err
	}
	body, err := io.ReadAll(message.Body)
	if err != nil {
		return err
	}
	header := message.Header.Get("DKIM-Signature")
	tags := map[string]string{}
	for _, part := range strings.Split(header, ";") {
		name, value, _ := strings.Cut(strings.TrimSpace(part), "=")
		tags[name] = value
	}
	if tags["a"] != "rsa-sha256" || tags["c"] != "relaxed/simple" || tags["d"] != "fixture.test" || tags["s"] != "welcome" {
		return errors.New("unexpected signing profile")
	}
	bodyHash := sha256.Sum256(append(bytes.TrimRight(body, "\r\n"), '\r', '\n'))
	if base64.StdEncoding.EncodeToString(bodyHash[:]) != tags["bh"] {
		return errors.New("independent DKIM body hash mismatch")
	}
	canonical := func(name, value string) string {
		return strings.ToLower(name) + ":" + strings.Join(strings.Fields(value), " ")
	}
	var headers strings.Builder
	for _, name := range strings.Split(tags["h"], ":") {
		headers.WriteString(canonical(name, message.Header.Get(name)) + "\r\n")
	}
	position := strings.LastIndex(header, "b=")
	if position < 0 {
		return errors.New("signature value missing")
	}
	headers.WriteString(canonical("DKIM-Signature", header[:position+2]))
	signature, err := base64.StdEncoding.DecodeString(strings.Join(strings.Fields(tags["b"]), ""))
	if err != nil {
		return err
	}
	hash := sha256.Sum256([]byte(headers.String()))
	return rsa.VerifyPKCS1v15(key, crypto.SHA256, hash[:], signature)
}

func TestWelcomeFakeSendmailPreservesEnvelopeAndMIME(t *testing.T) {
	session, _ := testMailSession(t, localSettings())
	content := reportWelcome(t, "local-welcome")
	content.EnvelopeFrom = "bounce@fixture.test"
	content.Bcc = []string{"hidden@fixture.test"}
	message := preparedWelcome(t, session, content, nil)
	directory := t.TempDir()
	executable := filepath.Join(directory, "fake-sendmail")
	script := "#!/bin/sh\ndirectory=$1\nshift\nprintf '%s\\n' \"$@\" > \"$directory/argv\"\ncat > \"$directory/mime\"\n"
	if err := os.WriteFile(executable, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	result, operationErr := session.owner.Client().Sendmail(context.Background(), message, mail.LocalDelivery{Executable: executable, Arguments: []string{directory}})
	result, err := session.accept("sendmail", result, operationErr)
	if err != nil {
		t.Fatal(err)
	}
	process := result.ProcessInfo()
	if !process.Started || !process.ExitKnown || process.ExitCode != 0 {
		t.Fatal("local process outcome not observed")
	}
	argv, err := os.ReadFile(filepath.Join(directory, "argv"))
	if err != nil || string(argv) != "-oi\n-f\nbounce@fixture.test\n--\nrecipient@fixture.test\nhidden@fixture.test\n" {
		t.Fatal("local envelope mapping changed")
	}
	wire, err := os.ReadFile(filepath.Join(directory, "mime"))
	if err != nil || !bytes.Equal(wire, message.EMLCopy()) || process.InputBytes != int64(len(wire)) {
		t.Fatal("local MIME/input byte count changed")
	}
	root, parts := parseWelcome(t, wire)
	if root.Header.Get("Bcc") != "" || bytes.Contains(wire, []byte("hidden@fixture.test")) || len(parts) != 11 {
		t.Fatal("local welcome composition/privacy changed")
	}
}
