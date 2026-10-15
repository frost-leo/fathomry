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
	"io"
	"mime"
	"mime/multipart"
	stdmail "net/mail"
	"net/textproto"
	"sort"
	"strings"
)

// go-mail v0.8.1 leaves its PGP multipart unclosed and emits a duplicate
// semicolon for signed PGP. Use its bounded part encoders inside a joined
// standard multipart envelope instead. Payload cryptography remains caller-owned.
func composePGP(ctx context.Context, message Message, limits settings) ([]byte, error) {
	input := message.ContentCopy()
	parts := input.Parts
	mode := input.PGP
	input.PGP, input.Parts = 0, nil
	base, err := NewMessage(input)
	if err != nil {
		return nil, err
	}
	wire, err := compose(ctx, base, limits)
	if err != nil {
		return nil, err
	}
	parsed, err := stdmail.ReadMessage(bytes.NewReader(wire))
	if err != nil {
		return nil, err
	}
	output := &boundedWriter{ctx: ctx, limit: limits.MaxMIMEBytes}
	writer := multipart.NewWriter(output)
	if input.Boundary != "" {
		if err = writer.SetBoundary(input.Boundary); err != nil {
			return nil, err
		}
	}
	kind, protocol := "multipart/encrypted", "application/pgp-encrypted"
	if mode == 2 {
		kind, protocol = "multipart/signed", "application/pgp-signature"
	}
	params := map[string]string{"boundary": writer.Boundary(), "protocol": protocol}
	if mode == 2 {
		params["micalg"] = input.PGPHash
	}
	if mode == 1 && parts[0].ContentType != "application/pgp-encrypted" || mode == 2 && parts[1].ContentType != "application/pgp-signature" {
		return nil, failure(ErrInput, "pgp-parts")
	}
	var keys []string
	for key := range parsed.Header {
		if !strings.HasPrefix(strings.ToLower(key), "content-") {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	for _, key := range keys {
		for _, value := range parsed.Header[key] {
			if _, err = io.WriteString(output, key+": "+value+"\r\n"); err != nil {
				return nil, err
			}
		}
	}
	if _, err = io.WriteString(output, "Content-Type: "+mime.FormatMediaType(kind, params)+"\r\n\r\n"); err != nil {
		return nil, err
	}
	for _, part := range parts {
		bodyInput := Content{Parts: []Part{part}, Charset: input.Charset, Encoding: input.Encoding}
		bodyMessage, err := NewMessage(bodyInput)
		if err != nil {
			return nil, err
		}
		wire, err := compose(ctx, bodyMessage, limits)
		if err != nil {
			return nil, err
		}
		entity, err := stdmail.ReadMessage(bytes.NewReader(wire))
		if err != nil {
			return nil, err
		}
		headers := textproto.MIMEHeader{}
		for key, values := range entity.Header {
			if strings.HasPrefix(strings.ToLower(key), "content-") {
				headers[key] = values
			}
		}
		body, err := io.ReadAll(entity.Body)
		if err != nil {
			return nil, err
		}
		if boundaryCollision(body, writer.Boundary()) {
			return nil, failure(ErrInput, "boundary-collision")
		}
		destination, err := writer.CreatePart(headers)
		if err != nil {
			return nil, err
		}
		if _, err = destination.Write(body); err != nil {
			return nil, err
		}
	}
	if err = writer.Close(); err != nil {
		return nil, err
	}
	if err := checkMIME(output.buffer.Bytes()); err != nil {
		return nil, err
	}
	return output.buffer.Bytes(), nil
}
