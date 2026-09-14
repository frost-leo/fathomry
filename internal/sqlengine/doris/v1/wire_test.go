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

package doris

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/pem"
	"errors"
	"io"
	"math/big"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// This peer exercises the actual driver; it is not a Doris service oracle.
type sqlPeer struct {
	listener       net.Listener
	tls            *tls.Config
	roots          string
	mu             sync.Mutex
	sockets        map[net.Conn]bool
	workers        sync.WaitGroup
	queries        atomic.Int64
	allowUnbounded atomic.Bool
	denyGreeting   atomic.Bool
	switchAuth     atomic.Bool
	emptyPassword  atomic.Bool
	extensions     atomic.Uint32
	handshakes     atomic.Int64
	entered        chan struct{}
	once           sync.Once
}

func testCertificate(t testing.TB) (tls.Certificate, string) {
	t.Helper()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	cert := &x509.Certificate{SerialNumber: big.NewInt(42), DNSNames: []string{"doris.test"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, cert, cert, public, private)
	if err != nil {
		t.Fatal(err)
	}
	key, err := x509.MarshalPKCS8PrivateKey(private)
	if err != nil {
		t.Fatal(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	pair, err := tls.X509KeyPair(certPEM, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: key}))
	if err != nil {
		t.Fatal(err)
	}
	return pair, string(certPEM)
}
func readPacket(conn net.Conn) ([]byte, byte, error) {
	var header [4]byte
	if _, err := io.ReadFull(conn, header[:]); err != nil {
		return nil, 0, err
	}
	size := int(header[0]) | int(header[1])<<8 | int(header[2])<<16
	if size > 2<<20 {
		return nil, 0, errors.New("peer packet bound")
	}
	body := make([]byte, size)
	_, err := io.ReadFull(conn, body)
	return body, header[3], err
}
func sendPacket(conn net.Conn, sequence byte, body []byte) error {
	frame := []byte{byte(len(body)), byte(len(body) >> 8), byte(len(body) >> 16), sequence}
	frame = append(frame, body...)
	_, err := conn.Write(frame)
	return err
}
func encodeCell(value string) []byte {
	if len(value) < 251 {
		return append([]byte{byte(len(value))}, value...)
	}
	return append([]byte{0xfc, byte(len(value)), byte(len(value) >> 8)}, value...)
}
func peerGreeting(secure bool) []byte {
	flags := uint32(0x0138828c)
	if secure {
		flags |= 1 << 11
	}
	body := append([]byte{10}, []byte("5.7.99-doris-protocol-peer\x00")...)
	body = append(body, 1, 0, 0, 0)
	body = append(body, []byte("12345678")...)
	body = append(body, 0, byte(flags), byte(flags>>8), 33, 2, 0, byte(flags>>16), byte(flags>>24), 21)
	body = append(body, make([]byte, 10)...)
	return append(body, []byte("abcdefghijkl\x00mysql_native_password\x00")...)
}
func newSQLPeer(t testing.TB, secure bool) *sqlPeer {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	peer := &sqlPeer{listener: listener, sockets: make(map[net.Conn]bool), entered: make(chan struct{})}
	if secure {
		cert, roots := testCertificate(t)
		peer.roots = roots
		peer.tls = &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{cert}}
	}
	peer.workers.Go(func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			peer.mu.Lock()
			peer.sockets[conn] = true
			peer.mu.Unlock()
			peer.workers.Go(func() {
				defer func() { _ = conn.Close(); peer.mu.Lock(); delete(peer.sockets, conn); peer.mu.Unlock() }()
				peer.serve(conn)
			})
		}
	})
	t.Cleanup(func() {
		_ = listener.Close()
		peer.mu.Lock()
		for conn := range peer.sockets {
			_ = conn.Close()
		}
		peer.mu.Unlock()
		peer.workers.Wait()
	})
	return peer
}
func (p *sqlPeer) options() OptionsV1 {
	o := OptionsV1{Name: "doris", SQLAddress: p.listener.Addr().String(), Database: "gh42", User: "synthetic", Password: "credential-canary",
		Plaintext: p.tls == nil, Timeout: 2 * time.Second}
	if p.tls != nil {
		o.RootCAPEM = p.roots
		o.SQLServerName = "doris.test"
	}
	if p.emptyPassword.Load() {
		o.Password = ""
	}
	return o
}
func (p *sqlPeer) serve(raw net.Conn) {
	_ = raw.SetDeadline(time.Now().Add(5 * time.Second))
	conn := raw
	if p.denyGreeting.Load() {
		_ = sendPacket(conn, 0, []byte{0xff, 0x10, 0x04, '#', '0', '8', '0', '0', '4'})
		return
	}
	greeting := peerGreeting(p.tls != nil)
	capabilities := bytes.IndexByte(greeting[1:], 0) + 15
	binary.LittleEndian.PutUint32(greeting[capabilities+14:], p.extensions.Load())
	if sendPacket(conn, 0, greeting) != nil {
		return
	}
	auth, sequence, err := readPacket(conn)
	if err != nil {
		return
	}
	if p.tls != nil {
		if len(auth) != 32 || sequence != 1 || binary.LittleEndian.Uint32(auth)&(1<<11) == 0 {
			return
		}
		secure := tls.Server(raw, p.tls)
		if secure.Handshake() != nil {
			return
		}
		conn = secure
		auth, sequence, err = readPacket(conn)
		if err != nil || sequence != 2 {
			return
		}
		p.handshakes.Add(1)
	}
	if len(auth) < 32 {
		return
	}
	flags := binary.LittleEndian.Uint32(auth)
	if !p.allowUnbounded.Load() && flags&(1<<7|1<<16|1<<17|1<<24|1<<5) != 0 {
		return
	}
	if p.switchAuth.Load() {
		if sendPacket(conn, sequence+1, append([]byte{0xfe}, []byte("mysql_native_password\x0012345678abcdefghijkl\x00")...)) != nil {
			return
		}
		auth, sequence, err = readPacket(conn)
		expected := 20
		if p.emptyPassword.Load() {
			expected = 0
		}
		if err != nil || len(auth) != expected {
			return
		}
	}
	if sendPacket(conn, sequence+1, []byte{0, 0, 0, 2, 0, 0, 0}) != nil {
		return
	}
	command, _, err := readPacket(conn)
	if err != nil || len(command) == 0 || command[0] != 3 {
		return
	}
	p.queries.Add(1)
	query := string(command[1:])
	switch query {
	case "SELECT stalled":
		p.once.Do(func() { close(p.entered) })
		var tail [1]byte
		_, _ = conn.Read(tail[:])
		return
	case "SELECT infile":
		_ = sendPacket(conn, 1, append([]byte{0xfb}, []byte("/private/fixture")...))
		return
	case "SELECT huge":
		_, _ = conn.Write([]byte{255, 255, 127, 1})
		return
	case "SELECT wide":
		_ = sendPacket(conn, 1, []byte{65})
		return
	case "SELECT malformed-column":
		_ = sendPacket(conn, 1, []byte{1})
		_ = sendPacket(conn, 2, []byte{0xfc})
		return
	case "SELECT column-error":
		_ = sendPacket(conn, 1, []byte{1})
		_ = sendPacket(conn, 2, []byte{0xff, 0x10, 0x04, '#', 'H', 'Y', '0', '0', '0'})
		return
	case "SELECT more":
		_ = sendPacket(conn, 1, []byte{0, 0, 0, 8, 0, 0, 0})
		return
	case "INSERT lost":
		return
	case "INSERT overflow":
		_ = sendPacket(conn, 1, []byte{0, 0xfe, 255, 255, 255, 255, 255, 255, 255, 255, 0, 2, 0, 0, 0})
		return
	}
	if !strings.HasPrefix(query, "SELECT") {
		_ = sendPacket(conn, 1, []byte{0, 2, 0, 2, 0, 0, 0})
		return
	}
	cells := [][]byte{[]byte("row-canary")}
	kinds := []byte{0xfd}
	if query == "SELECT floats" {
		cells = [][]byte{[]byte("0.1"), []byte("0.1")}
		kinds = []byte{4, 5}
	}
	if query == "SELECT exact" {
		cells = [][]byte{nil, {}, []byte("18446744073709551615"), []byte("12345678901234567890.00100"),
			[]byte("2026-09-14 01:02:03.123456"), {0, 1, 255}, []byte(`{"nested":[null,18446744073709551615]}`)}
		kinds = []byte{0xfd, 0xfd, 8, 0xf6, 0x0c, 0xfc, 0xfd}
	}
	if query == "SELECT large" {
		cells = [][]byte{[]byte(strings.Repeat("x", 2048))}
	}
	_ = sendPacket(conn, 1, []byte{byte(len(cells))})
	seq := byte(2)
	for index := range cells {
		column := []byte{}
		for _, value := range []string{"def", "gh42", "fixture", "fixture", "value", "value"} {
			column = append(column, encodeCell(value)...)
		}
		flags := uint16(0)
		if kinds[index] == 8 {
			flags = 32
		}
		column = append(column, 12, 33, 0, 64, 0, 0, 0, kinds[index], byte(flags), byte(flags>>8), 5, 0, 0)
		if sendPacket(conn, seq, column) != nil {
			return
		}
		seq++
	}
	if sendPacket(conn, seq, []byte{0xfe, 0, 0, 2, 0}) != nil {
		return
	}
	seq++
	if query == "SELECT empty" {
		_ = sendPacket(conn, seq, []byte{0xfe, 0, 0, 2, 0})
		return
	}
	if query == "SELECT false-eof" {
		_ = sendPacket(conn, seq, []byte{0xfe, 0, 0, 0, 0, 0, 0, 0, 0})
		return
	}
	if query == "SELECT malformed-row" {
		_ = sendPacket(conn, seq, []byte{20, 'a'})
		return
	}
	row := []byte{}
	for _, cell := range cells {
		if cell == nil {
			row = append(row, 0xfb)
		} else {
			row = append(row, encodeCell(string(cell))...)
		}
	}
	if sendPacket(conn, seq, row) != nil {
		return
	}
	seq++
	if query == "SELECT partial" {
		_ = sendPacket(conn, seq, append([]byte{0xff, 0x35, 0x04, '#', 'H', 'Y', '0', '0', '0'}, []byte("native-error-canary")...))
		return
	}
	if query == "SELECT over" {
		if sendPacket(conn, seq, row) != nil {
			return
		}
		seq++
	}
	_ = sendPacket(conn, seq, []byte{0xfe, 0, 0, 2, 0})
}
