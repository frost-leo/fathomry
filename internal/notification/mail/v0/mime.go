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
	"mime/quotedprintable"
	stdmail "net/mail"
	"net/textproto"
	"sort"
	"strings"
)

// normalizeMIME corrects the selected serializer's 7bit fallback before any
// signature is calculated. Native-generated nested boundaries stay independent;
// an explicit caller boundary applies only to the outer multipart entity.
func normalizeMIME(ctx context.Context, wire []byte, limit int, boundary string) ([]byte, error) {
	parsed, err := stdmail.ReadMessage(bytes.NewReader(wire))
	if err != nil {
		return nil, failure(ErrInput, "mime", err)
	}
	count := 0
	var entity func(textproto.MIMEHeader, io.Reader, int) (textproto.MIMEHeader, []byte, error)
	entity = func(headers textproto.MIMEHeader, reader io.Reader, depth int) (textproto.MIMEHeader, []byte, error) {
		count++
		if count > 128 || depth > 8 {
			return nil, nil, failure(ErrLimit, "mime-structure")
		}
		if ctx.Err() != nil {
			return nil, nil, ctx.Err()
		}
		media, params, err := mime.ParseMediaType(headers.Get("Content-Type"))
		if err != nil {
			return nil, nil, failure(ErrInput, "mime-type", err)
		}
		writer := &boundedWriter{ctx: ctx, limit: limit}
		if strings.HasPrefix(media, "multipart/") {
			children := multipart.NewReader(reader, params["boundary"])
			output := multipart.NewWriter(writer)
			selected := params["boundary"]
			if depth == 0 && boundary != "" {
				selected = boundary
			}
			if err = output.SetBoundary(selected); err != nil {
				return nil, nil, err
			}
			params["boundary"] = selected
			headers.Set("Content-Type", mime.FormatMediaType(media, params))
			for {
				child, err := children.NextRawPart()
				if err == io.EOF {
					break
				}
				if err != nil {
					return nil, nil, err
				}
				childHeader, body, err := entity(child.Header, child, depth+1)
				if err != nil {
					return nil, nil, err
				}
				if boundaryCollision(body, selected) {
					return nil, nil, failure(ErrInput, "boundary-collision")
				}
				destination, err := output.CreatePart(childHeader)
				if err != nil {
					return nil, nil, err
				}
				if _, err = destination.Write(body); err != nil {
					return nil, nil, err
				}
			}
			if err = output.Close(); err != nil {
				return nil, nil, err
			}
		} else {
			if strings.EqualFold(headers.Get("Content-Transfer-Encoding"), "7bit") {
				reader = quotedprintable.NewReader(reader)
			}
			if _, err = io.Copy(writer, reader); err != nil {
				return nil, nil, err
			}
			if strings.EqualFold(headers.Get("Content-Transfer-Encoding"), "7bit") {
				for _, char := range writer.buffer.Bytes() {
					if char == 0 || char > 127 {
						return nil, nil, failure(ErrInput, "7bit")
					}
				}
			}
			if encoding := strings.ToLower(headers.Get("Content-Transfer-Encoding")); encoding == "7bit" || encoding == "8bit" {
				data := writer.buffer.Bytes()
				for index, char := range data {
					if char == 0 || char == '\n' && (index == 0 || data[index-1] != '\r') || char == '\r' && (index+1 == len(data) || data[index+1] != '\n') {
						return nil, nil, failure(ErrInput, "mime-line-ending")
					}
				}
			}
		}
		return headers, writer.buffer.Bytes(), nil
	}
	headers, body, err := entity(textproto.MIMEHeader(parsed.Header), parsed.Body, 0)
	if err != nil {
		return nil, failure(ErrInput, "mime", err)
	}
	output := &boundedWriter{ctx: ctx, limit: limit}
	if err = writeHeaders(output, headers); err != nil {
		return nil, err
	}
	if _, err = output.Write(body); err != nil {
		return nil, err
	}
	if err = checkMIME(output.buffer.Bytes()); err != nil {
		return nil, err
	}
	return output.buffer.Bytes(), nil
}

func boundaryCollision(body []byte, boundary string) bool {
	return bytes.Contains(body, []byte("\r\n--"+boundary)) || bytes.HasPrefix(body, []byte("--"+boundary))
}

func writeHeaders(writer io.Writer, headers textproto.MIMEHeader) error {
	keys := make([]string, 0, len(headers))
	for key := range headers {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		for _, value := range headers[key] {
			// Preserve already encoded words; fold only at existing whitespace.
			line := key + ":"
			for _, word := range strings.Split(value, " ") {
				if len(line)+1+len(word) > 76 && len(line) > len(key)+1 {
					if _, err := io.WriteString(writer, line+"\r\n"); err != nil {
						return err
					}
					line = " " + word
				} else {
					line += " " + word
				}
			}
			if _, err := io.WriteString(writer, line+"\r\n"); err != nil {
				return err
			}
		}
	}
	_, err := io.WriteString(writer, "\r\n")
	return err
}
