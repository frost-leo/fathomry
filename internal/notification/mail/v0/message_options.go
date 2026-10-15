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
	sdk "github.com/wneessen/go-mail"
	"mime"
	stdmail "net/mail"
	"slices"
	"strings"
)

func token(value string, limit int) bool {
	if len(value) > limit {
		return false
	}
	for _, char := range value {
		if char < 33 || char > 126 || strings.ContainsRune("()<>@,;:\\\"/[]?=", char) {
			return false
		}
	}
	return true
}
func encodingValid(value string) bool {
	switch value {
	case "", "base64", "quoted-printable", "7bit", "8bit":
		return true
	}
	return false
}
func messageID(value string) bool {
	if len(value) < 3 || len(value) > 254 {
		return false
	}
	for _, char := range value {
		if char < 32 || char >= 127 || strings.ContainsRune("<>()", char) {
			return false
		}
	}
	parsed, err := stdmail.ParseAddress(value)
	return err == nil && parsed.Name == "" && strings.Contains(parsed.Address, "@")
}
func fileOptions(encoding, description, id string) sdk.FileOption {
	return func(file *sdk.File) {
		if encoding != "" {
			file.Enc = sdk.Encoding(encoding)
		}
		if description != "" {
			sdk.WithFileDescription(mime.QEncoding.Encode("UTF-8", description))(file)
		}
		if id != "" {
			sdk.WithFileContentID("<" + id + ">")(file)
		}
	}
}

func validateFileFields(fields []Header) (int, error) {
	if len(fields) > 32 {
		return 0, failure(ErrLimit, "file-fields")
	}
	total := 0
	names := map[string]bool{}
	for _, field := range fields {
		name := strings.ToLower(field.Name)
		if !headerName(field.Name) || name == "content-disposition" || name == "content-id" || name == "content-description" || names[name] || len(field.Values) > 32 {
			return 0, failure(ErrInput, "file-fields")
		}
		names[name] = true
		total += len(field.Name)
		for _, value := range field.Values {
			if !line(value, 998) {
				return 0, failure(ErrInput, "file-fields")
			}
			total += len(value)
		}
	}
	return total, nil
}
func cloneFields(fields []Header) []Header {
	fields = slices.Clone(fields)
	for index := range fields {
		fields[index].Values = slices.Clone(fields[index].Values)
	}
	return fields
}
func extraFileFields(fields []Header) sdk.FileOption {
	return func(file *sdk.File) {
		for _, field := range fields {
			values := slices.Clone(field.Values)
			if !field.Preformatted {
				for index := range values {
					values[index] = mime.QEncoding.Encode("UTF-8", values[index])
				}
			}
			file.Header[field.Name] = values
		}
	}
}

// ContentCopy returns detached sensitive input, not source or signing authority.
// Imported messages expose the retained parsed content; EMLCopy is the frozen wire.
func (message Message) ContentCopy() Content {
	if message.content == nil {
		return Content{}
	}
	cloned, _ := NewMessage(*message.content)
	return *cloned.content
}

// EMLCopy returns detached prepared MIME, or nil if not explicitly prepared.
func (message Message) EMLCopy() []byte { return bytes.Clone(message.wire) }
