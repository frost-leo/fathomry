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

package proxy

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	http "github.com/nukilabs/http"
	nativeh2 "github.com/nukilabs/http/http2"
	nativetls "github.com/nukilabs/utls"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
)

func TestCONNECTHeaderCeilingPreservesBufferedTunnel(t *testing.T) {
	header := "HTTP/1.1 200 Connection Established\r\nX-Padding: " + strings.Repeat("h", 5000) + "\r\n\r\n"
	payload := strings.Repeat("tunnel-", 2048)
	for _, limit := range []int64{int64(len(header)), int64(len(header) - 1)} {
		t.Run(strconv.FormatInt(limit, 10), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			client, peer := net.Pipe()
			defer client.Close()
			defer peer.Close()
			_ = peer.SetDeadline(time.Now().Add(3 * time.Second))
			done := make(chan error, 1)
			go func() {
				_, err := http.ReadRequest(bufio.NewReader(peer))
				if err == nil {
					_, err = io.WriteString(peer, header[:4500])
				}
				if err == nil {
					_, err = io.WriteString(peer, header[4500:]+payload)
				}
				done <- err
			}()
			dialer := &Dialer{maxHeader: limit}
			request := &http.Request{Method: "CONNECT", URL: &url.URL{Host: "origin.invalid:80"}, Host: "origin.invalid:80", Header: make(http.Header)}
			tunnel, err := dialer.connectHttp1(ctx, request, client)
			if limit < int64(len(header)) {
				if !errors.Is(err, ErrProxyHeaderLimit) || tunnel != nil {
					t.Fatal("oversized header accepted", err)
				}
				<-done
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer tunnel.Close()
			if tunnel.(*bufferedTunnel).reader.Buffered() == 0 {
				t.Fatal("fixture did not prefetch tunnel bytes")
			}
			data := make([]byte, len(payload))
			if _, err := io.ReadFull(tunnel, data); err != nil || string(data) != payload {
				t.Fatal("header parser consumed or charged tunnel payload", err)
			}
			if err := <-done; err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestH2CONNECTHeaderCeilingIsAdvertisedAndEnforced(t *testing.T) {
	certificate := httptest.NewTLSServer(nil)
	defer certificate.Close()
	for _, limit := range []int64{1024, 128 << 10} {
		t.Run(strconv.FormatInt(limit, 10), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			listener, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: certificate.TLS.Certificates, NextProtos: []string{"h2"}})
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			advertised, done := make(chan uint32, 1), make(chan error, 1)
			go func() {
				conn, err := listener.Accept()
				if err != nil {
					done <- err
					return
				}
				defer conn.Close()
				_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
				preface := make([]byte, len(http2.ClientPreface))
				if _, err = io.ReadFull(conn, preface); err != nil || string(preface) != http2.ClientPreface {
					done <- errors.Join(err, errors.New("missing H2 preface"))
					return
				}
				framer := http2.NewFramer(conn, conn)
				if err = framer.WriteSettings(); err != nil {
					done <- err
					return
				}
				for {
					frame, err := framer.ReadFrame()
					if err != nil {
						done <- err
						return
					}
					switch frame := frame.(type) {
					case *http2.SettingsFrame:
						if value, ok := frame.Value(http2.SettingMaxHeaderListSize); ok {
							advertised <- value
						}
					case *http2.HeadersFrame:
						var encoded bytes.Buffer
						encoder := hpack.NewEncoder(&encoded)
						if err = encoder.WriteField(hpack.HeaderField{Name: ":status", Value: "200"}); err != nil {
							done <- err
							return
						}
						if err = encoder.WriteField(hpack.HeaderField{Name: "x-large", Value: strings.Repeat("p", 64<<10)}); err != nil {
							done <- err
							return
						}
						block := encoded.Bytes()
						size := min(len(block), 16384)
						err = framer.WriteHeaders(http2.HeadersFrameParam{StreamID: frame.StreamID, BlockFragment: block[:size], EndHeaders: size == len(block)})
						for block = block[size:]; err == nil && len(block) != 0; {
							size = min(len(block), 16384)
							err = framer.WriteContinuation(frame.StreamID, size == len(block), block[:size])
							block = block[size:]
						}
						if err != nil {
							done <- err
							return
						}
						_, _ = io.Copy(io.Discard, conn)
						done <- nil
						return
					}
				}
			}()
			address, err := url.Parse("https://" + listener.Addr().String())
			if err != nil {
				t.Fatal(err)
			}
			native, err := NewWithDialer(address, time.Second, &nativetls.Config{InsecureSkipVerify: true}, Direct(nil, time.Second), nil, limit)
			if err != nil {
				t.Fatal(err)
			}
			dialer := native.(*Dialer)
			defer dialer.Close()
			tunnel, err := dialer.DialContext(ctx, "tcp", "origin.invalid:80")
			if limit == 1024 {
				if !errors.Is(err, nativeh2.ConnectionError(nativeh2.ErrCodeProtocol)) {
					t.Error("native decoder accepted oversized header", err)
				}
			} else if err != nil {
				t.Error("same peer failed with sufficient budget", err)
			}
			if tunnel != nil {
				_ = tunnel.Close()
			}
			if err := dialer.Close(); err != nil {
				t.Error(err)
			}
			if err := <-done; err != nil && !(limit == 1024 &&
				(errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE) || errors.Is(err, net.ErrClosed))) {
				t.Error(err)
			}
			select {
			case actual := <-advertised:
				if int64(actual) != limit {
					t.Fatalf("advertised %d instead of %d", actual, limit)
				}
			default:
				t.Fatal("receive ceiling was not advertised")
			}
		})
	}
}
