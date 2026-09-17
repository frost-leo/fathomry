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

package main

import (
	_ "embed"
	"errors"
	stdmail "net/mail"

	mail "github.com/frost-leo/fathomry/internal/notification/mail/v0"
)

//go:embed welcome.html
var welcomeHTML string

const welcomePlain = `Welcome to Fathomry.

A place for ideas to find their rhythm.

Explore Fathomry:
https://github.com/frost-leo/fathomry

Start with a spark. Build something that lasts.

There is room for what you imagine. We're glad you're here.

Read the documentation:
https://github.com/frost-leo/fathomry/blob/develop/docs/README.md

Fathomry / Made for the work ahead.
Welcome preview
`

func welcomeContent(from, to, id string) (mail.Content, error) {
	sender, err := stdmail.ParseAddress(from)
	if err != nil {
		return mail.Content{}, errors.New("welcome sender is invalid")
	}
	sender.Name = "Fathomry"
	return mail.Content{ID: id, From: sender.String(), To: []string{to}, Subject: "Welcome to Fathomry",
		Text: welcomePlain, HTML: welcomeHTML, Headers: map[string]string{"X-Fathomry-Test": "welcome-preview"},
		Attachments: []mail.Attachment{{Name: "synthetic-report.csv", ContentType: "text/csv", Data: []byte("category,units\nA,20\nB,30\nC,50\n")}}}, nil
}
