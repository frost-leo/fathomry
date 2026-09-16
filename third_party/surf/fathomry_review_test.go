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
package surf

import (
	"context"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/enetx/surf/pkg/connectproxy"
)

type fathomryJoinedEOF struct{ cause error }

func (reader fathomryJoinedEOF) Read([]byte) (int, error) {
	return 0, errors.Join(io.EOF, reader.cause)
}
func (reader fathomryJoinedEOF) Close() error { return nil }

func TestFathomryJoinedEOFCausesAreNeverCompletion(t *testing.T) {
	cause := errors.New("independent native read failure")
	framed := &fathomryFraming{ReadCloser: fathomryJoinedEOF{cause}, expected: 1}
	_, err := framed.Read(make([]byte, 1))
	if !errors.Is(err, cause) || !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatal("framing mismatch erased another read cause", err)
	}
	reader := &decodedReadCloser{decoder: fathomryJoinedEOF{cause}, source: io.NopCloser(&fathomryEmpty{})}
	_, err = reader.Read(make([]byte, 1))
	if !errors.Is(err, cause) {
		t.Fatal("decoder EOF erased its independent failure")
	}
}

type fathomryEmpty struct{}

func (*fathomryEmpty) Read([]byte) (int, error) { return 0, io.EOF }

func TestFathomryConfiguredResolverFailureCannotFallBack(t *testing.T) {
	for _, scheme := range []string{"http", "socks4", "socks5"} {
		t.Run(scheme, func(t *testing.T) {
			dialer, err := connectproxy.NewDialer(scheme + "://127.0.0.1:1")
			if err != nil {
				t.Fatal(err)
			}
			var dials atomic.Int32
			dialer.DialSocket = func(context.Context, string, string) (net.Conn, error) {
				dials.Add(1)
				return nil, errors.New("unexpected proxy connection")
			}
			dialer.SetResolver(&net.Resolver{PreferGo: true, Dial: func(context.Context, string, string) (net.Conn, error) {
				return nil, errors.New("configured resolver refused")
			}})
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_, err = dialer.DialContext(ctx, "tcp", "unresolved.invalid:443")
			var dns *net.DNSError
			if !errors.As(err, &dns) || dials.Load() != 0 {
				t.Fatal("configured DNS failure fell back to another route/resolver", err)
			}
		})
	}
}
