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
	"embed"
	"encoding/json"
	"errors"
	htmltemplate "html/template"
	"io"
	"strings"
	texttemplate "text/template"

	mail "github.com/frost-leo/fathomry/adapters/notification/mail/v1"
)

//go:embed pack/*
var welcomePack embed.FS

type packMessages struct {
	Audience, Greeting, International string
	GuideName                         string `json:"guide_name"`
}
type welcomeData struct {
	packMessages
	Body  htmltemplate.HTML
	Plain string
	ID    string
}

func prepareWelcome(content mail.Content) (mail.Message, mail.Preparation, error) {
	frozen, err := mail.NewMessage(content)
	if err != nil {
		return mail.Message{}, mail.Preparation{}, err
	}
	content = frozen.ContentCopy()
	raw, err := welcomePack.ReadFile("pack/messages.json")
	if err != nil {
		return mail.Message{}, mail.Preparation{}, err
	}
	var messages packMessages
	if err := json.Unmarshal(raw, &messages); err != nil {
		return mail.Message{}, mail.Preparation{}, err
	}
	data := welcomeData{packMessages: messages, Plain: content.Text, ID: content.ID}
	// This HTML is authored here or produced by the already-escaped report renderer.
	// Keep it as data: report labels must never become Go template source.
	data.Body = htmltemplate.HTML(content.HTML)
	html, err := htmltemplate.New("welcome").Parse(`{{define "body"}}{{.Body}}{{end}}{{define "note"}}<!doctype html>
<html lang="en"><head><meta charset="utf-8"><title>Welcome to Fathomry</title></head>
<body><h1>{{.Greeting}}</h1><p>For {{.Audience}}.</p><p>{{.International}}</p>
<p>Your welcome pack includes {{.GuideName}} and a small synthetic data example.</p>
<p>Keep source ownership, evidence acknowledgement and recipient delivery separate.</p></body></html>
{{end}}`)
	if err != nil {
		return mail.Message{}, mail.Preparation{}, err
	}
	plain, err := texttemplate.New("welcome").Parse(`{{define "plain"}}{{.Plain}}
A small welcome pack is attached: a guide, starter notes, metadata and sample data.
{{.International}}
{{end}}{{define "starter"}}{{.Greeting}}
For {{.Audience}}.

1. Prepare explicit Settings and caller-owned Runtime/Inbox budgets.
2. Open a source Owner; use its non-owning public Client.
3. Prepare and export an immutable message before an authorized send.
4. Observe finite evidence, close the source, acknowledge its record, then close Runtime.

{{.International}}
{{end}}`)
	if err != nil {
		return mail.Message{}, mail.Preparation{}, err
	}
	plan := mail.Preparation{
		Parts: []mail.PartInput{{Input: mail.Input{Write: func(ctx context.Context, writer io.Writer) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			return plain.ExecuteTemplate(writer, "plain", data)
		}}, Part: mail.Part{ContentType: "text/plain", Charset: "UTF-8", Encoding: "quoted-printable"}}},
		Templates: []mail.TemplateInput{
			{HTML: html, Name: "body", Data: data, Part: mail.Part{ContentType: "text/html", Charset: "UTF-8", Encoding: "quoted-printable"}},
			{Text: plain, Name: "starter", Data: data, File: &mail.Attachment{Name: "getting-started.txt", ContentType: "text/plain; charset=utf-8", Encoding: "base64"}},
			{HTML: html, Name: "note", Data: data, File: &mail.Attachment{Name: "welcome-note.html", ContentType: "text/html; charset=utf-8", Encoding: "quoted-printable"}},
		},
		Files: []mail.FileInput{
			{Input: mail.Input{FS: welcomePack, Name: "pack/guide.txt"}, File: mail.Attachment{Name: messages.GuideName, ContentType: "text/plain; charset=utf-8", Encoding: "base64"}},
			{Input: mail.Input{Write: func(ctx context.Context, writer io.Writer) error {
				if err := ctx.Err(); err != nil {
					return err
				}
				return json.NewEncoder(writer).Encode(struct {
					Schema    string `json:"schema"`
					Greeting  string `json:"greeting"`
					MessageID string `json:"message_id"`
					Synthetic bool   `json:"synthetic"`
				}{"fathomry.welcome/v1", messages.Greeting, content.ID, true})
			}}, File: mail.Attachment{Name: "welcome.json", ContentType: "application/json", Encoding: "base64"}},
		},
		Transforms: []mail.Transform{{Name: "welcome-metadata", Apply: func(ctx context.Context, value mail.Content) (mail.Content, error) {
			if err := ctx.Err(); err != nil {
				return value, err
			}
			if value.Headers == nil {
				value.Headers = map[string]string{}
			}
			value.Headers["Organization"] = "Fathomry"
			value.Headers["User-Agent"] = "Fathomry public Mail welcome example"
			value.Headers["X-Fathomry-Welcome"] = "public-api"
			return value, nil
		}}},
	}
	for _, file := range content.Attachments {
		metadata := file
		metadata.Data = nil
		if metadata.Encoding == "" {
			metadata.Encoding = "base64"
		}
		plan.Files = append(plan.Files, mail.FileInput{Input: mail.Input{Reader: bytes.NewReader(file.Data)}, File: metadata})
	}
	for _, file := range content.Inline {
		plan.Files = append(plan.Files, mail.FileInput{Inline: true, Input: mail.Input{Bytes: file.Data}, File: mail.Attachment{ID: file.ID, Name: file.Name, ContentType: file.ContentType, Encoding: "base64", Description: file.Description, Fields: file.Fields}})
	}
	content.Text, content.HTML = "", ""
	content.Attachments, content.Inline = nil, nil
	message, err := mail.NewMessage(content)
	if err != nil {
		return mail.Message{}, mail.Preparation{}, err
	}
	return message, plan, nil
}

func preparedHTML(message mail.Message) (string, error) {
	content := message.ContentCopy()
	for _, part := range content.Parts {
		if part.ContentType == "text/html" {
			return string(part.Data), nil
		}
	}
	if content.HTML != "" {
		return content.HTML, nil
	}
	return "", errors.New("prepared welcome HTML is missing")
}
func previewHTML(message mail.Message) (string, error) {
	html, err := preparedHTML(message)
	if err != nil {
		return "", err
	}
	for _, asset := range message.ContentCopy().Inline {
		html = strings.ReplaceAll(html, "cid:"+asset.ID, inlineDataURI(asset))
	}
	return html, nil
}
