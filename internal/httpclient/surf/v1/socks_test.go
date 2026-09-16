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
	"bytes"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	stdhttp "net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
)

func socksAddress(reader io.Reader) (string, error) {
	var kind [1]byte
	if _, err := io.ReadFull(reader, kind[:]); err != nil {
		return "", err
	}
	var host string
	switch kind[0] {
	case 1:
		data := make([]byte, 4)
		if _, err := io.ReadFull(reader, data); err != nil {
			return "", err
		}
		host = net.IP(data).String()
	case 4:
		data := make([]byte, 16)
		if _, err := io.ReadFull(reader, data); err != nil {
			return "", err
		}
		host = net.IP(data).String()
	case 3:
		var length [1]byte
		if _, err := io.ReadFull(reader, length[:]); err != nil {
			return "", err
		}
		data := make([]byte, int(length[0]))
		if _, err := io.ReadFull(reader, data); err != nil {
			return "", err
		}
		host = string(data)
	default:
		return "", fmt.Errorf("unsupported synthetic address")
	}
	var port [2]byte
	if _, err := io.ReadFull(reader, port[:]); err != nil {
		return "", err
	}
	return net.JoinHostPort(host, strconv.Itoa(int(binary.BigEndian.Uint16(port[:])))), nil
}
func socksEncoded(address string) []byte {
	host, port, _ := net.SplitHostPort(address)
	number, _ := strconv.Atoi(port)
	result := append([]byte{1}, net.ParseIP(host).To4()...)
	return append(result, byte(number>>8), byte(number))
}

type socksFixture struct {
	address  string
	tcp, udp atomic.Int32
	controls atomic.Int32
}

func newSOCKS(t *testing.T, target string) *socksFixture {
	t.Helper()
	tcp, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	udp, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	fixture := &socksFixture{address: tcp.Addr().String()}
	var mu sync.Mutex
	var connections []net.Conn
	var group sync.WaitGroup
	group.Go(func() {
		for {
			conn, err := tcp.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			connections = append(connections, conn)
			mu.Unlock()
			group.Go(func() {
				defer conn.Close()
				var greeting [2]byte
				if _, err := io.ReadFull(conn, greeting[:]); err != nil {
					return
				}
				methods := make([]byte, int(greeting[1]))
				if _, err := io.ReadFull(conn, methods); err != nil {
					return
				}
				if greeting[0] != 5 {
					return
				}
				_, _ = conn.Write([]byte{5, 2})
				var auth [2]byte
				if _, err := io.ReadFull(conn, auth[:]); err != nil {
					return
				}
				user := make([]byte, int(auth[1]))
				if _, err := io.ReadFull(conn, user); err != nil {
					return
				}
				var count [1]byte
				if _, err := io.ReadFull(conn, count[:]); err != nil {
					return
				}
				password := make([]byte, int(count[0]))
				if _, err := io.ReadFull(conn, password); err != nil {
					return
				}
				if auth[0] != 1 || string(user) != "owner" || string(password) != "synthetic" {
					t.Error("SOCKS credentials changed")
					_, _ = conn.Write([]byte{1, 1})
					return
				}
				_, _ = conn.Write([]byte{1, 0})
				var head [3]byte
				if _, err := io.ReadFull(conn, head[:]); err != nil {
					return
				}
				address, err := socksAddress(conn)
				if err != nil {
					return
				}
				switch head[1] {
				case 1:
					fixture.tcp.Add(1)
					if address != target {
						t.Error("SOCKS target changed")
						return
					}
					remote, err := net.DialTimeout("tcp", target, time.Second)
					if err != nil {
						return
					}
					mu.Lock()
					connections = append(connections, remote)
					mu.Unlock()
					defer remote.Close()
					_, _ = conn.Write(append([]byte{5, 0, 0}, socksEncoded(tcp.Addr().String())...))
					up := make(chan struct{})
					go func() { _, _ = io.Copy(remote, conn); _ = remote.Close(); close(up) }()
					_, _ = io.Copy(conn, remote)
					_ = conn.Close()
					_ = remote.Close()
					<-up
				case 3:
					fixture.udp.Add(1)
					fixture.controls.Add(1)
					defer fixture.controls.Add(-1)
					_, _ = conn.Write(append([]byte{5, 0, 0}, socksEncoded(udp.LocalAddr().String())...))
					_, _ = io.Copy(io.Discard, conn)
				}
			})
		}
	})
	group.Go(func() {
		buffer := make([]byte, 64<<10)
		var client net.Addr
		for {
			count, from, err := udp.ReadFrom(buffer)
			if err != nil {
				return
			}
			if from.String() == target {
				if client != nil {
					response := append([]byte{0, 0, 0}, socksEncoded(target)...)
					response = append(response, buffer[:count]...)
					_, _ = udp.WriteTo(response, client)
				}
				continue
			}
			if count < 4 || !bytes.Equal(buffer[:3], []byte{0, 0, 0}) {
				continue
			}
			reader := bytes.NewReader(buffer[3:count])
			address, err := socksAddress(reader)
			if err != nil || address != target {
				t.Error("SOCKS datagram target changed")
				continue
			}
			client = from
			destination, err := net.ResolveUDPAddr("udp", target)
			if err != nil {
				t.Error(err)
				continue
			}
			_, _ = udp.WriteTo(buffer[count-reader.Len():count], destination)
		}
	})
	t.Cleanup(func() {
		_ = tcp.Close()
		_ = udp.Close()
		mu.Lock()
		owned := append([]net.Conn(nil), connections...)
		mu.Unlock()
		for _, conn := range owned {
			_ = conn.Close()
		}
		done := make(chan struct{})
		go func() { group.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("SOCKS fixture did not stop")
		}
	})
	return fixture
}
func TestSOCKSNativeTCPAndHTTP3Release(t *testing.T) {
	for _, mode := range []ProtocolMode{HTTP1Only, PreferHTTP3} {
		t.Run(string(mode), func(t *testing.T) {
			var calls atomic.Int32
			peer := newPeers(t, func(w stdhttp.ResponseWriter, r *stdhttp.Request) { calls.Add(1); _, _ = io.WriteString(w, "relay") }, mode == PreferHTTP3)
			proxy := newSOCKS(t, peer.tcp.Listener.Addr().String())
			options := OptionsV1{Name: "socks", Mode: mode, Native: NativeOptionsV1{TLSConfig: &tls.Config{RootCAs: peer.roots}}}
			fix := newFixture(t, options, 2)
			route := "socks5://owner:synthetic@" + proxy.address
			for _, id := range []string{"one", "two"} {
				receipt, err := fix.client.Do(testContext(t), testContext(t), fault.Correlation{Call: id}, request(t, "GET", peer.tcp.URL, ""), RequestOptionsV1{ProxyURL: &route})
				if err != nil {
					t.Fatal("native SOCKS request", err)
				}
				value := settle(t, fix, receipt)
				if !value.Outcome.Value.Complete() || string(value.Outcome.Value.DataCopy()) != "relay" {
					t.Fatal("relay result differs")
				}
				if mode == PreferHTTP3 && value.Outcome.Value.Metadata().Protocol() != "HTTP/3.0" {
					t.Fatal("H3 SOCKS used fallback")
				}
			}
			if calls.Load() != 2 {
				t.Fatal("unexpected peer calls")
			}
			if mode == HTTP1Only && proxy.tcp.Load() != 1 || mode == PreferHTTP3 && proxy.udp.Load() != 1 {
				t.Fatal("relay path or pooling changed")
			}
			if err := fix.assembly.Close(testContext(t)); err != nil {
				t.Fatal(err)
			}
			deadline := time.Now().Add(time.Second)
			for proxy.controls.Load() != 0 && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			if proxy.controls.Load() != 0 {
				t.Fatal("SOCKS TCP association survived native release")
			}
		})
	}
}
