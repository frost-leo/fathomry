// fathomry
// Copyright (C) 2026  Frost Leo
// SPDX-License-Identifier: GPL-3.0-or-later
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

package http2_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	nativehttp "github.com/nukilabs/http"
	nativeh2 "github.com/nukilabs/http/http2"
	peerh2 "golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
)

func TestFathomryDecoderBudgetSelection(t *testing.T) {
	for _, test := range []struct {
		name     string
		settings []nativeh2.Setting
		want     uint32
	}{
		{"generated", nil, 65536},
		{"empty-custom", []nativeh2.Setting{}, 4096},
		{"omitted-custom", []nativeh2.Setting{{ID: nativeh2.SettingInitialWindowSize, Val: 65535}}, 4096},
		{"zero-custom", []nativeh2.Setting{{ID: nativeh2.SettingHeaderTableSize, Val: 0}}, 0},
		{"positive-custom", []nativeh2.Setting{{ID: nativeh2.SettingHeaderTableSize, Val: 8192}}, 8192},
	} {
		t.Run(test.name, func(t *testing.T) {
			transport := &nativeh2.Transport{MaxDecoderHeaderTableSize: 65536, Settings: test.settings}
			if got := transport.FathomryMaxDecoderHeaderTableSize(); got != test.want {
				t.Fatalf("resolved receive table = %d, want %d", got, test.want)
			}
		})
	}
}

func TestFathomrySelectedHTTP2WireHPACKCapacity(t *testing.T) {
	for _, test := range []struct {
		name    string
		table   uint32
		dynamic bool
		reject  bool
	}{
		{"zero-static", 0, false, false},
		{"positive-dynamic", 4096, true, false},
		{"zero-dynamic-reference", 0, true, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			local, err := net.Dial("tcp", listener.Addr().String())
			if err != nil {
				t.Fatal(err)
			}
			defer local.Close()
			remote, err := listener.Accept()
			if err != nil {
				t.Fatal(err)
			}
			defer remote.Close()
			_ = remote.SetDeadline(time.Now().Add(3 * time.Second))
			observed := make(chan uint32, 1)
			peerDone := make(chan error, 1)
			go func() {
				defer remote.Close()
				preface := make([]byte, len(peerh2.ClientPreface))
				if _, err := io.ReadFull(remote, preface); err != nil || string(preface) != peerh2.ClientPreface {
					peerDone <- errors.Join(err, errors.New("invalid client preface"))
					return
				}
				framer := peerh2.NewFramer(remote, remote)
				var stream, table uint32
				table = 4096
				for stream == 0 {
					frame, err := framer.ReadFrame()
					if err != nil {
						peerDone <- err
						return
					}
					switch frame := frame.(type) {
					case *peerh2.SettingsFrame:
						if !frame.IsAck() {
							_ = frame.ForeachSetting(func(setting peerh2.Setting) error {
								if setting.ID == peerh2.SettingHeaderTableSize {
									table = setting.Val
								}
								return nil
							})
						}
					case *peerh2.HeadersFrame:
						stream = frame.StreamID
					}
				}
				observed <- table
				if err := framer.WriteSettings(); err != nil {
					peerDone <- err
					return
				}
				var block bytes.Buffer
				encoder := hpack.NewEncoder(&block)
				_ = encoder.WriteField(hpack.HeaderField{Name: ":status", Value: "200"})
				field := hpack.HeaderField{Name: "x-dynamic", Value: "retained", Sensitive: !test.dynamic}
				_ = encoder.WriteField(field)
				_ = encoder.WriteField(field)
				peerDone <- framer.WriteHeaders(peerh2.HeadersFrameParam{StreamID: stream, EndHeaders: true, EndStream: true, BlockFragment: block.Bytes()})
			}()
			transport := &nativeh2.Transport{AllowHTTP: true, MaxDecoderHeaderTableSize: test.table,
				Settings: []nativeh2.Setting{{ID: nativeh2.SettingEnablePush, Val: 0}, {ID: nativeh2.SettingHeaderTableSize, Val: test.table}}}
			connection, err := transport.NewClientConn(local)
			if err != nil {
				t.Fatal(err)
			}
			defer connection.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
			defer cancel()
			request, _ := nativehttp.NewRequestWithContext(ctx, "GET", "http://hpack.invalid/", nil)
			response, err := connection.RoundTrip(request)
			if response != nil {
				defer response.Body.Close()
			}
			if peerErr := <-peerDone; peerErr != nil {
				t.Fatal("independent peer", peerErr)
			}
			if table := <-observed; table != test.table {
				t.Fatal("advertised table", table, test.table)
			}
			if test.reject {
				var compression nativeh2.ConnectionError
				if !errors.As(err, &compression) || compression != nativeh2.ConnectionError(nativeh2.ErrCodeCompression) {
					t.Fatal("accepted dynamic reference beyond advertised table", response, err)
				}
			} else if err != nil || response == nil || response.StatusCode != 200 || len(response.Header.Values("X-Dynamic")) != 2 {
				t.Fatal("rejected valid header control", response, err)
			}
		})
	}
}
