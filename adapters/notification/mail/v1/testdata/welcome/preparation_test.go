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
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"mime"
	"mime/multipart"
	"mime/quotedprintable"
	stdmail "net/mail"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	mail "github.com/frost-leo/fathomry/adapters/notification/mail/v1"
)

type welcomePart struct {
	kind, filename, cid, encoding string
	header                        stdmail.Header
	data                          []byte
}

func parseWelcome(t testing.TB, wire []byte) (*stdmail.Message, []welcomePart) {
	t.Helper()
	message, err := stdmail.ReadMessage(bytes.NewReader(wire))
	if err != nil {
		t.Fatal(err)
	}
	var parts []welcomePart
	var visit func(stdmail.Header, io.Reader, int)
	visit = func(header stdmail.Header, reader io.Reader, depth int) {
		if depth > 8 {
			t.Fatal("excessive MIME nesting")
		}
		kind, params, err := mime.ParseMediaType(header.Get("Content-Type"))
		if err != nil {
			t.Fatal(err)
		}
		if strings.HasPrefix(kind, "multipart/") {
			children := multipart.NewReader(reader, params["boundary"])
			for {
				part, err := children.NextRawPart()
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Fatal(err)
				}
				visit(stdmail.Header(part.Header), part, depth+1)
				if err := part.Close(); err != nil {
					t.Fatal(err)
				}
			}
			return
		}
		encoding := header.Get("Content-Transfer-Encoding")
		switch encoding {
		case "base64":
			reader = base64.NewDecoder(base64.StdEncoding, reader)
		case "quoted-printable":
			reader = quotedprintable.NewReader(reader)
		case "7bit", "8bit", "":
		default:
			t.Fatal("unknown MIME encoding")
		}
		data, err := io.ReadAll(io.LimitReader(reader, 16<<20))
		if err != nil {
			t.Fatal(err)
		}
		filename := ""
		if disposition := header.Get("Content-Disposition"); disposition != "" {
			_, attrs, err := mime.ParseMediaType(disposition)
			if err != nil {
				t.Fatal(err)
			}
			filename = attrs["filename"]
		}
		parts = append(parts, welcomePart{kind: kind, filename: filename, cid: header.Get("Content-ID"), encoding: encoding, header: header, data: data})
	}
	visit(message.Header, message.Body, 0)
	return message, parts
}
func localSettings() mail.Settings {
	return mail.Settings{Name: "welcome-control", Host: "localhost", Port: 465, Hello: "welcome.fixture.test", TLSMode: "implicit", Auth: "none", MaxActive: 1, MaxMessages: 3, MaxRecipients: 4, Timeout: 3 * time.Second, CloseTimeout: time.Second}
}
func testMailSession(t testing.TB, settings mail.Settings) (*mailSession, *submission) {
	t.Helper()
	summary := new(submission)
	session, err := openMail(context.Background(), settings, summary)
	if err != nil {
		if session != nil {
			session.close()
		}
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = session.close() })
	return session, summary
}
func preparedWelcome(t testing.TB, session *mailSession, content mail.Content, change func(*mail.Preparation)) mail.Message {
	t.Helper()
	input, plan, err := prepareWelcome(content)
	if err != nil {
		t.Fatal(err)
	}
	if change != nil {
		change(&plan)
	}
	result, operationErr := session.owner.Client().PrepareMessage(context.Background(), input, plan)
	result, err = session.accept("prepare-message", result, operationErr)
	if err != nil {
		t.Fatal(err)
	}
	message, present := result.PreparedMessage()
	if !present {
		t.Fatal("prepared message absent")
	}
	if content.Headers["X-Fathomry-Welcome"] != "" {
		t.Fatal("preparation mutated caller headers")
	}
	return message
}
func basicWelcome(t testing.TB, id string) mail.Content {
	t.Helper()
	content, err := welcomeContent("sender@fixture.test", "recipient@fixture.test", id+"@fixture.test")
	if err != nil {
		t.Fatal(err)
	}
	return content
}
func reportWelcome(t testing.TB, id string) mail.Content {
	t.Helper()
	content := basicWelcome(t, id)
	snapshot, now := sampleRepositorySnapshot()
	snapshot.Branch = "welcome-{{.NotTemplateData}}"
	assets := map[string][]byte{}
	for _, key := range []string{"commits", "stars", "languages", "issues"} {
		assets[key] = samplePNG(t)
	}
	result, err := repositoryContent(content, snapshot, assets, now)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

type countedFS struct {
	fs.FS
	opened, closed int
}
type countedFile struct {
	fs.File
	owner *countedFS
}

func (value *countedFS) Open(name string) (fs.File, error) {
	file, err := value.FS.Open(name)
	if file == nil {
		return nil, err
	}
	value.opened++
	return &countedFile{File: file, owner: value}, err
}
func (value *countedFile) Close() error { value.owner.closed++; return value.File.Close() }

func TestWelcomePackAndReportThroughPublicPreparation(t *testing.T) {
	for _, report := range []bool{false, true} {
		t.Run(map[bool]string{false: "welcome", true: "repository"}[report], func(t *testing.T) {
			session, summary := testMailSession(t, localSettings())
			content := basicWelcome(t, "pack")
			if report {
				content = reportWelcome(t, "report-pack")
			}
			var messages packMessages
			raw, _ := welcomePack.ReadFile("pack/messages.json")
			if err := json.Unmarshal(raw, &messages); err != nil {
				t.Fatal(err)
			}
			content.Subject = messages.International
			fsControl := &countedFS{FS: welcomePack}
			message := preparedWelcome(t, session, content, func(plan *mail.Preparation) {
				plan.Files[0].Input.FS = fsControl
				data := plan.Templates[2].Data.(welcomeData)
				data.Audience = "<strong>curious & careful</strong>"
				plan.Templates[2].Data = data
			})
			if fsControl.opened != 1 || fsControl.closed != 1 {
				t.Fatal("FS acquisition was not joined")
			}
			root, parts := parseWelcome(t, message.EMLCopy())
			subject, err := new(mime.WordDecoder).DecodeHeader(root.Header.Get("Subject"))
			if err != nil || subject != messages.International || root.Header.Get("X-Fathomry-Welcome") != "public-api" {
				t.Fatal("header/transform mapping")
			}
			plain, html, images, files := 0, 0, 0, map[string]welcomePart{}
			for _, part := range parts {
				switch {
				case part.cid != "":
					images++
					found := false
					for _, asset := range content.Inline {
						if part.cid == "<"+asset.ID+">" {
							found = bytes.Equal(part.data, asset.Data) && part.kind == asset.ContentType
						}
					}
					if !found {
						t.Fatal("CID bytes/type/reference changed")
					}
				case part.filename != "":
					files[part.filename] = part
				case part.kind == "text/plain":
					plain++
					if !strings.Contains(string(part.data), "Welcome to Fathomry") || !strings.Contains(string(part.data), messages.International) {
						t.Fatal("plain template omitted welcome data")
					}
				case part.kind == "text/html":
					html++
					if strings.ReplaceAll(string(part.data), "\r\n", "\n") != content.HTML {
						t.Fatal("authored welcome HTML was reinterpreted")
					}
				}
			}
			if plain != 1 || html != 1 || images != len(content.Inline) || len(files) != 5 {
				t.Fatal("welcome MIME composition incomplete")
			}
			guide, _ := welcomePack.ReadFile("pack/guide.txt")
			if !bytes.Equal(files[messages.GuideName].data, guide) {
				t.Fatal("UTF-8 filename/FS bytes changed")
			}
			csv := content.Attachments[0]
			if !bytes.Equal(files[csv.Name].data, csv.Data) {
				t.Fatal("borrowed reader bytes changed")
			}
			// Decode explicit JSON names independently of the producer's anonymous type.
			var fields map[string]any
			if json.Unmarshal(files["welcome.json"].data, &fields) != nil || fields["schema"] != "fathomry.welcome/v1" || fields["message_id"] != content.ID || fields["synthetic"] != true {
				t.Fatal("scoped writer metadata changed")
			}
			if !strings.Contains(string(files["getting-started.txt"].data), messages.International) {
				t.Fatal("named text attachment missing")
			}
			note := files["welcome-note.html"]
			if note.encoding != "quoted-printable" || !strings.Contains(string(note.data), "&lt;strong&gt;curious &amp; careful&lt;/strong&gt;") || strings.Contains(string(note.data), "<strong>curious") {
				t.Fatal("HTML template did not escape values")
			}
			var exported bytes.Buffer
			result, operationErr := session.owner.Client().Export(context.Background(), message, &exported)
			result, err = session.accept("export", result, operationErr)
			if err != nil || result.BytesWritten() != int64(exported.Len()) || !bytes.Equal(exported.Bytes(), message.EMLCopy()) {
				t.Fatal("export changed frozen message", err)
			}
			if err := session.close(); err != nil || !summary.SourceReleased || !summary.RuntimeClosed || summary.EvidenceOutstanding != 0 || summary.EvidenceRecords != 3 {
				t.Fatal("welcome owner/evidence cleanup", err)
			}
		})
	}
}

func TestWelcomePathImportAndWriterFailure(t *testing.T) {
	session, _ := testMailSession(t, localSettings())
	content := reportWelcome(t, "import")
	content.ReplyTo = "reply@fixture.test"
	path := filepath.Join(t.TempDir(), "welcome-local.txt")
	expected := []byte("A caller-selected local welcome resource.\n")
	if err := os.WriteFile(path, expected, 0600); err != nil {
		t.Fatal(err)
	}
	message := preparedWelcome(t, session, content, func(plan *mail.Preparation) {
		plan.Files = append(plan.Files, mail.FileInput{Input: mail.Input{Path: path}, File: mail.Attachment{Name: "welcome-local.txt", ContentType: "text/plain", Encoding: "base64"}})
	})
	_, before := parseWelcome(t, message.EMLCopy())
	found := false
	for _, part := range before {
		if part.filename == "welcome-local.txt" {
			found = bytes.Equal(part.data, expected)
		}
	}
	if !found {
		t.Fatal("explicit path not acquired")
	}
	imported, operationErr := session.owner.Client().Import(context.Background(), mail.Input{Reader: bytes.NewReader(message.EMLCopy())}, mail.ImportOptions{From: "bounce@fixture.test", Recipients: []string{"envelope@fixture.test"}})
	imported, err := session.accept("import", imported, operationErr)
	if err != nil {
		t.Fatal(err)
	}
	normalized, present := imported.PreparedMessage()
	if !present {
		t.Fatal("imported message missing")
	}
	root, after := parseWelcome(t, normalized.EMLCopy())
	if root.Header.Get("Reply-To") != "" || root.Header.Get("X-Fathomry-Welcome") != "" {
		t.Fatal("native import loss profile was silently changed")
	}
	for _, name := range []string{"Reply-To", "X-Fathomry-Welcome"} {
		dropped := false
		for _, change := range imported.HeaderChanges() {
			if change.Name == name && change.Dropped {
				dropped = true
			}
		}
		if !dropped {
			t.Fatal("import header loss was not reported", name)
		}
	}
	if len(before) != len(after) {
		t.Fatal("import changed welcome part count")
	}
	for index := range before {
		if before[index].kind != after[index].kind || before[index].filename != after[index].filename || !bytes.Equal(before[index].data, after[index].data) {
			t.Fatal("import changed decoded welcome content")
		}
	}
	result, operationErr := session.owner.Client().Export(context.Background(), message, shortWelcomeWriter{})
	_, err = session.accept("export", result, operationErr)
	if !errors.Is(err, io.ErrShortWrite) || result.BytesWritten() != 7 {
		t.Fatal("partial writer outcome lost", err)
	}
	input, plan, err := prepareWelcome(content)
	if err != nil {
		t.Fatal(err)
	}
	plan.Files = append(plan.Files, mail.FileInput{Input: mail.Input{Path: "relative.txt"}, File: mail.Attachment{Name: "relative.txt"}})
	result, operationErr = session.owner.Client().PrepareMessage(context.Background(), input, plan)
	_, err = session.accept("prepare-message", result, operationErr)
	if !errors.Is(err, mail.ErrInput) {
		t.Fatal("relative input path accepted", err)
	}
}

type shortWelcomeWriter struct{}

func (shortWelcomeWriter) Write(data []byte) (int, error) { return min(7, len(data)), nil }

func TestWelcomeCancellationAndSourceFirstEvidence(t *testing.T) {
	for _, sourceFirst := range []bool{false, true} {
		t.Run(map[bool]string{false: "canceled-callback", true: "source-first"}[sourceFirst], func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			summary := new(submission)
			session, err := openMail(ctx, localSettings(), summary)
			if err != nil {
				t.Fatal(err)
			}
			defer session.close()
			input, plan, err := prepareWelcome(basicWelcome(t, "cancel"))
			if err != nil {
				t.Fatal(err)
			}
			if !sourceFirst {
				original := plan.Files[1].Input.Write
				plan.Files[1].Input.Write = func(ctx context.Context, writer io.Writer) error { err := original(ctx, writer); cancel(); return err }
			}
			result, operationErr := session.owner.Client().PrepareMessage(ctx, input, plan)
			if sourceFirst {
				if err := session.owner.Close(context.Background()); err != nil {
					t.Fatal(err)
				}
			}
			_, err = session.accept("prepare-message", result, operationErr)
			if !sourceFirst && !errors.Is(err, context.Canceled) || sourceFirst && err != nil {
				t.Fatal("operation cancellation distinction", err)
			}
			if err := session.close(); err != nil || !summary.SourceReleased || !summary.RuntimeClosed || summary.EvidenceOutstanding != 0 || summary.EvidenceRecords != 2 {
				t.Fatal("canceled/source-first custody leaked", err)
			}
		})
	}
}
