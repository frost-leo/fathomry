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
	stdmail "net/mail"
	"testing"
)

func FuzzBoundedEML(f *testing.F) {
	for _, input := range []string{
		"From: a@example.invalid\r\nContent-Type: text/plain\r\nContent-Transfer-Encoding: 7bit\r\n\r\n=41\r\n",
		"Content-Type: multipart/mixed; boundary=b\r\n\r\n--b\r\nContent-Type: text/plain\r\nContent-Disposition: attachment; filename=a\r\n\r\nx\r\n--b--\r\n",
		"Content-Type: multipart/mixed; boundary=b\r\n\r\n--b\r\nContent-Type: text/plain\r\nContent-Disposition: attachment; filename*=utf-8''caf%C3%A9.txt\r\n\r\nx\r\n--b--\r\n",
	} {
		f.Add([]byte(input))
	}
	f.Fuzz(func(t *testing.T, input []byte) {
		if len(input) > 8192 {
			return
		}
		parsed, err := stdmail.ReadMessage(bytes.NewReader(input))
		if err != nil {
			return
		}
		content, err := importContent(context.Background(), parsed, 8192)
		if err != nil {
			return
		}
		message, err := NewMessage(content)
		if err != nil {
			return
		}
		limits := defaults(OptionsV1{MaxMessageBytes: 8192, MaxMIMEBytes: 16384})
		wire, err := compose(context.Background(), message, limits)
		if err != nil {
			return
		}
		output, err := stdmail.ReadMessage(bytes.NewReader(wire))
		if err != nil {
			t.Fatal(err)
		}
		if output.Header.Get("Bcc") != "" || output.Header.Get("Resent-Bcc") != "" {
			t.Fatal("private recipient header escaped")
		}
		if _, err = io.ReadAll(output.Body); err != nil {
			t.Fatal(err)
		}
		if len(wire) > limits.MaxMIMEBytes {
			t.Fatal("output limit exceeded")
		}
	})
}
