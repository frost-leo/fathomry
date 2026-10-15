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

package consumer

import (
	"encoding/binary"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// Independent protobuf fixture: no Lark SDK frame encoder/decoder is used.
type wireFrame struct {
	method  uint64
	headers map[string]string
	payload []byte
}

func wireBytes(field uint64, value []byte) []byte {
	result := binary.AppendUvarint(nil, field<<3|2)
	result = binary.AppendUvarint(result, uint64(len(value)))
	return append(result, value...)
}
func encodeFrame(frame wireFrame) []byte {
	result := []byte{8, 0, 16, 0, 24, 42, 32, byte(frame.method)}
	for key, value := range frame.headers {
		header := append(wireBytes(1, []byte(key)), wireBytes(2, []byte(value))...)
		result = append(result, wireBytes(5, header)...)
	}
	return append(result, wireBytes(8, frame.payload)...)
}
func decodeFields(data []byte) (map[uint64][][]byte, map[uint64]uint64, error) {
	fields, numbers := map[uint64][][]byte{}, map[uint64]uint64{}
	for len(data) > 0 {
		key, size := binary.Uvarint(data)
		if size <= 0 {
			return nil, nil, io.ErrUnexpectedEOF
		}
		data = data[size:]
		number, size := binary.Uvarint(data)
		if size <= 0 {
			return nil, nil, io.ErrUnexpectedEOF
		}
		data = data[size:]
		if key&7 == 0 {
			numbers[key>>3] = number
			continue
		}
		if key&7 != 2 || number > uint64(len(data)) {
			return nil, nil, io.ErrUnexpectedEOF
		}
		fields[key>>3] = append(fields[key>>3], data[:number])
		data = data[number:]
	}
	return fields, numbers, nil
}
func decodeFrame(data []byte) (wireFrame, error) {
	fields, numbers, err := decodeFields(data)
	if err != nil {
		return wireFrame{}, err
	}
	result := wireFrame{method: numbers[4], headers: map[string]string{}}
	for _, raw := range fields[5] {
		nested, _, err := decodeFields(raw)
		if err != nil || len(nested[1]) != 1 || len(nested[2]) != 1 {
			return wireFrame{}, io.ErrUnexpectedEOF
		}
		result.headers[string(nested[1][0])] = string(nested[2][0])
	}
	if len(fields[8]) == 1 {
		result.payload = fields[8][0]
	}
	return result, nil
}

type wsPeer struct {
	server    *httptest.Server
	connected chan *wsConn
	acks      chan wireFrame
	mu        sync.Mutex
	sockets   []*wsConn
	workers   sync.WaitGroup
}
type wsConn struct {
	conn *websocket.Conn
	mu   sync.Mutex
}

func (conn *wsConn) send(frame wireFrame) error {
	conn.mu.Lock()
	defer conn.mu.Unlock()
	_ = conn.conn.SetWriteDeadline(time.Now().Add(time.Second))
	return conn.conn.WriteMessage(websocket.BinaryMessage, encodeFrame(frame))
}
func newWSPeer(t testing.TB) *wsPeer {
	t.Helper()
	peer := &wsPeer{connected: make(chan *wsConn, 8), acks: make(chan wireFrame, 64)}
	peer.server = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/callback/ws/endpoint" {
			var value struct {
				AppID     string `json:"AppID"`
				AppSecret string `json:"AppSecret"`
			}
			if json.NewDecoder(r.Body).Decode(&value) != nil || value.AppID != "app_fixture" || value.AppSecret != "secret-fixture" {
				t.Error("bootstrap authority changed")
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]any{"URL": strings.Replace(peer.server.URL, "https:", "wss:", 1) + "/ws?service_id=42&ticket=synthetic", "ClientConfig": map[string]int{"ReconnectCount": -1, "ReconnectInterval": 0}}})
			return
		}
		if r.URL.Path != "/ws" {
			w.Header().Set("Content-Type", "application/json")
			if strings.HasSuffix(r.URL.Path, "/internal") {
				_, _ = io.WriteString(w, `{"code":0,"tenant_access_token":"fixture-token","expire":7200}`)
			} else {
				_, _ = io.WriteString(w, `{"code":0,"data":{"message_id":"om_fixture"}}`)
			}
			return
		}
		conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		socket := &wsConn{conn: conn}
		peer.mu.Lock()
		peer.sockets = append(peer.sockets, socket)
		peer.workers.Add(1)
		peer.mu.Unlock()
		defer peer.workers.Done()
		defer conn.Close()
		peer.connected <- socket
		for {
			_, data, err := conn.ReadMessage()
			if err != nil {
				return
			}
			frame, err := decodeFrame(data)
			if err != nil {
				t.Error(err)
				return
			}
			if frame.method == 0 {
				if err := socket.send(wireFrame{headers: map[string]string{"type": "pong"}}); err != nil {
					return
				}
			} else {
				peer.acks <- frame
			}
		}
	}))
	t.Cleanup(func() {
		peer.server.CloseClientConnections()
		peer.mu.Lock()
		for _, socket := range peer.sockets {
			_ = socket.conn.Close()
		}
		peer.mu.Unlock()
		peer.server.Close()
		peer.workers.Wait()
	})
	return peer
}
func (peer *wsPeer) settings() Settings {
	result := fixtureSettings(peer.server)
	result.Profile = "application"
	result.TenantToken = ""
	result.TokenExpiresUnix = 0
	result.AppSecret = "secret-fixture"
	result.WebSocket = &WebSocketSettings{AllowedHosts: []string{strings.TrimPrefix(peer.server.URL, "https://")}, MaxPending: 4, MaxAssemblies: 2, MaxFragments: 4,
		MaxFrameBytes: 16 << 10, MaxMessageBytes: 16 << 10, MaxFragmentBytes: 32 << 10, AckTimeout: 300 * time.Millisecond, FragmentTimeout: 100 * time.Millisecond,
		WriteTimeout: 100 * time.Millisecond, PingInterval: time.Second, PongTimeout: time.Second, ReconnectMin: time.Millisecond, ReconnectMax: 5 * time.Millisecond, MaxConnectAttempts: 3}
	return result
}
func nextConnection(t testing.TB, peer *wsPeer) *wsConn {
	t.Helper()
	select {
	case value := <-peer.connected:
		return value
	case <-time.After(2 * time.Second):
		t.Fatal("WSS not connected")
		return nil
	}
}
func sendEvent(t testing.TB, conn *wsConn, id string) {
	t.Helper()
	if err := conn.send(wireFrame{method: 1, headers: map[string]string{"type": "event", "message_id": id, "sum": "1", "seq": "0"}, payload: callbackBody("im.chat.updated_v1")}); err != nil {
		t.Fatal(err)
	}
}
func peerACK(t testing.TB, peer *wsPeer) (wireFrame, int) {
	t.Helper()
	select {
	case frame := <-peer.acks:
		var value struct {
			Code int `json:"code"`
		}
		if json.Unmarshal(frame.payload, &value) != nil {
			t.Fatal("invalid ACK JSON")
		}
		return frame, value.Code
	case <-time.After(2 * time.Second):
		t.Fatal("no ACK")
		return wireFrame{}, 0
	}
}
func noPositiveACK(t testing.TB, peer *wsPeer) {
	t.Helper()
	for {
		select {
		case frame := <-peer.acks:
			var response struct {
				Code int `json:"code"`
			}
			_ = json.Unmarshal(frame.payload, &response)
			if response.Code == 200 {
				t.Fatal("fabricated positive ACK")
			}
		default:
			return
		}
	}
}
