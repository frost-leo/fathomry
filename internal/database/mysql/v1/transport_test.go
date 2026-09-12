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

package mysql

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/pem"
	"errors"
	"io"
	"math/big"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
)

// A bounded native-protocol peer, not MySQL or an InnoDB/durability oracle.
type protocolPeer struct {
	listener   net.Listener
	trust      string
	tls        *tls.Config
	key        *rsa.PrivateKey
	fullAuth   bool
	nativeAuth bool
	mu         sync.Mutex
	sockets    map[net.Conn]struct{}
	workers    sync.WaitGroup
	stopped    bool
	entered    chan struct{}
	queries    atomic.Int64
	commits    atomic.Int64
	rollbacks  atomic.Int64
	secure     atomic.Int64
	fileCalls  atomic.Int64
	upload     string
	dropCommit atomic.Bool
}

func packetRead(conn net.Conn) ([]byte, byte, error) {
	var h [4]byte
	if _, err := io.ReadFull(conn, h[:]); err != nil {
		return nil, 0, err
	}
	size := int(h[0]) | int(h[1])<<8 | int(h[2])<<16
	if size > 2<<20 {
		return nil, 0, errors.New("fixture packet limit")
	}
	body := make([]byte, size)
	_, err := io.ReadFull(conn, body)
	return body, h[3], err
}
func packetSend(conn net.Conn, seq byte, body []byte) error {
	packet := make([]byte, 4+len(body))
	packet[0], packet[1], packet[2], packet[3] = byte(len(body)), byte(len(body)>>8), byte(len(body)>>16), seq
	copy(packet[4:], body)
	_, err := conn.Write(packet)
	return err
}
func encoded(value string) []byte {
	if len(value) < 251 {
		return append([]byte{byte(len(value))}, value...)
	}
	if len(value) <= 65535 {
		return append([]byte{0xfc, byte(len(value)), byte(len(value) >> 8)}, value...)
	}
	return append([]byte{0xfd, byte(len(value)), byte(len(value) >> 8), byte(len(value) >> 16)}, value...)
}
func greeting(secure bool) []byte {
	flags := uint32(1 | 4 | 8 | 128 | 512 | 8192 | 32768 | 1<<17 | 1<<19 | 1<<20 | 1<<21 | 1<<24)
	if secure {
		flags |= 1 << 11
	}
	data := append([]byte{10}, []byte("8.4.11-protocol-peer\x00")...)
	data = append(data, 1, 0, 0, 0)
	data = append(data, []byte("12345678")...)
	data = append(data, 0, byte(flags), byte(flags>>8), 45, 2, 0, byte(flags>>16), byte(flags>>24), 21)
	data = append(data, make([]byte, 10)...)
	return append(data, []byte("abcdefghijkl\x00caching_sha2_password\x00")...)
}
func newPeer(t testing.TB, secure, fullAuth bool) *protocolPeer {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &protocolPeer{listener: listener, sockets: make(map[net.Conn]struct{}), entered: make(chan struct{}, 8), fullAuth: fullAuth}
	if secure {
		public, private, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		template := &x509.Certificate{SerialNumber: big.NewInt(25), DNSNames: []string{"mysql.fixture.invalid"}, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour),
			IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
		der, err := x509.CreateCertificate(rand.Reader, template, template, public, private)
		if err != nil {
			t.Fatal(err)
		}
		p.trust = string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
		p.tls = &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: private}}}
	}
	if fullAuth {
		p.key, err = rsa.GenerateKey(rand.Reader, 2048)
		if err != nil {
			t.Fatal(err)
		}
	}
	p.workers.Go(func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			p.mu.Lock()
			if p.stopped {
				p.mu.Unlock()
				_ = conn.Close()
				return
			}
			p.sockets[conn] = struct{}{}
			p.mu.Unlock()
			p.workers.Go(func() {
				defer conn.Close()
				defer func() { p.mu.Lock(); delete(p.sockets, conn); p.mu.Unlock() }()
				p.serve(conn)
			})
		}
	})
	t.Cleanup(func() {
		p.mu.Lock()
		p.stopped = true
		_ = listener.Close()
		for conn := range p.sockets {
			_ = conn.Close()
		}
		p.mu.Unlock()
		p.workers.Wait()
	})
	return p
}
func (p *protocolPeer) options() OptionsV1 {
	_, port, _ := net.SplitHostPort(p.listener.Addr().String())
	number, _ := strconv.Atoi(port)
	s := OptionsV1{Name: "fixture", Address: "127.0.0.1", Port: uint16(number), Database: "fixture", User: "fixture", Password: "credential-canary", MaxConnections: 1, Timeout: 2 * time.Second, CloseTimeout: time.Second}
	if p.tls == nil {
		s.Plaintext = true
	} else {
		s.RootCAPEM = p.trust
		s.ServerName = "mysql.fixture.invalid"
	}
	return s
}
func (p *protocolPeer) authenticate(conn net.Conn) (net.Conn, bool) {
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	if packetSend(conn, 0, greeting(p.tls != nil)) != nil {
		return conn, false
	}
	auth, seq, err := packetRead(conn)
	if err != nil || seq != 1 {
		return conn, false
	}
	if len(auth) == 32 {
		if p.tls == nil {
			return conn, false
		}
		secure := tls.Server(conn, p.tls)
		if secure.HandshakeContext(context.Background()) != nil {
			return conn, false
		}
		conn = secure
		p.secure.Add(1)
		auth, seq, err = packetRead(conn)
		if err != nil || seq != 2 || len(auth) < 32 || binary.LittleEndian.Uint32(auth)&(1<<11) == 0 {
			return conn, false
		}
	}
	if p.nativeAuth {
		request := append([]byte{0xfe}, []byte("mysql_native_password\x00abcdefghijkl12345678\x00")...)
		if packetSend(conn, seq+1, request) != nil {
			return conn, false
		}
		response, next, err := packetRead(conn)
		if err != nil || next != seq+2 || len(response) != sha1.Size {
			return conn, false
		}
		seq = next
	}
	if p.fullAuth {
		seq++
		if packetSend(conn, seq, []byte{1, 4}) != nil {
			return conn, false
		}
		request, requestSeq, err := packetRead(conn)
		if _, encrypted := conn.(*tls.Conn); encrypted {
			if err != nil || requestSeq != seq+1 || string(request) != "credential-canary\x00" {
				return conn, false
			}
			return conn, packetSend(conn, requestSeq+1, []byte{0, 0, 0, 2, 0, 0, 0}) == nil
		}
		if err != nil || len(request) != 1 || request[0] != 2 || requestSeq != seq+1 {
			return conn, false
		}
		der, err := x509.MarshalPKIXPublicKey(&p.key.PublicKey)
		if err != nil {
			return conn, false
		}
		seq = requestSeq + 1
		if packetSend(conn, seq, append([]byte{1}, pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der})...)) != nil {
			return conn, false
		}
		encrypted, encryptedSeq, err := packetRead(conn)
		if err != nil || encryptedSeq != seq+1 {
			return conn, false
		}
		decrypted, err := rsa.DecryptOAEP(sha1.New(), rand.Reader, p.key, encrypted, nil)
		if err != nil {
			return conn, false
		}
		seed := []byte("12345678abcdefghijkl")
		for index := range decrypted {
			decrypted[index] ^= seed[index%len(seed)]
		}
		if string(decrypted) != "credential-canary\x00" {
			return conn, false
		}
		seq = encryptedSeq
	}
	return conn, packetSend(conn, seq+1, []byte{0, 0, 0, 2, 0, 0, 0}) == nil
}
func columnPacket(name string, kind byte, flags uint16) []byte {
	var data []byte
	for _, v := range []string{"def", "fixture", "", "", name, name} {
		data = append(data, encoded(v)...)
	}
	return append(data, 12, 45, 0, 255, 255, 0, 0, kind, byte(flags), byte(flags>>8), 0, 0, 0)
}

type preparedQuery struct {
	query      string
	parameters int
}

func (p *protocolPeer) serve(socket net.Conn) {
	conn, ok := p.authenticate(socket)
	if !ok {
		return
	}
	statements := make(map[uint32]preparedQuery)
	var id uint32
	inTx := false
	for {
		body, _, err := packetRead(conn)
		if err != nil || len(body) == 0 {
			return
		}
		status := byte(2)
		if inTx {
			status = 3
		}
		switch body[0] {
		case 1:
			return
		case 14:
			if packetSend(conn, 1, []byte{0, 0, 0, status, 0, 0, 0}) != nil {
				return
			}
		case 3:
			query := string(body[1:])
			switch {
			case strings.HasPrefix(query, "SET "):
			case strings.HasPrefix(query, "START TRANSACTION"):
				inTx = true
				status = 3
			case query == "COMMIT":
				p.commits.Add(1)
				inTx = false
				status = 2
				if p.dropCommit.Load() {
					return
				}
			case query == "ROLLBACK":
				p.rollbacks.Add(1)
				inTx = false
				status = 2
			case query == "SELECT huge_header":
				_, _ = conn.Write([]byte{0, 0, 128, 1})
				_, _, _ = packetRead(conn)
				return
			case strings.HasPrefix(query, "SELECT") || strings.HasPrefix(query, "SHOW"):
				p.queries.Add(1)
				if !p.textResult(conn, query, status) {
					return
				}
				if query == "SELECT partial" {
					inTx = false
				}
				continue
			case strings.HasPrefix(query, "CREATE "), strings.HasPrefix(query, "SAVEPOINT "), strings.HasPrefix(query, "ROLLBACK TO "), strings.HasPrefix(query, "RELEASE SAVEPOINT "):
				p.queries.Add(1)
			default:
				return
			}
			if packetSend(conn, 1, []byte{0, 0, 0, status, 0, 0, 0}) != nil {
				return
			}
		case 0x16:
			query := string(body[1:])
			id++
			params := strings.Count(query, "?")
			statements[id] = preparedQuery{query, params}
			columns := 0
			if strings.HasPrefix(query, "SELECT") {
				columns = 1
			}
			result := []byte{0, byte(id), byte(id >> 8), byte(id >> 16), byte(id >> 24), byte(columns), 0, byte(params), 0, 0, 0, 0}
			if packetSend(conn, 1, result) != nil {
				return
			}
			seq := byte(2)
			for range params {
				if packetSend(conn, seq, columnPacket("argument", 253, 0)) != nil {
					return
				}
				seq++
			}
			if params > 0 {
				if packetSend(conn, seq, []byte{0xfe, 0, 0, status, 0}) != nil {
					return
				}
				seq++
			}
			for range columns {
				column := columnPacket("value", 253, 0)
				if query == "SELECT malformed_metadata" {
					column = []byte{0, 0, 0, 0, 0, 0}
				}
				if packetSend(conn, seq, column) != nil {
					return
				}
				seq++
			}
			if columns > 0 {
				if packetSend(conn, seq, []byte{0xfe, 0, 0, status, 0}) != nil {
					return
				}
			}
		case 0x19:
			if len(body) < 5 {
				return
			}
			delete(statements, binary.LittleEndian.Uint32(body[1:]))
		case 0x17:
			if len(body) < 10 {
				return
			}
			selected, found := statements[binary.LittleEndian.Uint32(body[1:])]
			if !found {
				return
			}
			p.queries.Add(1)
			if selected.query == "SELECT wait" {
				p.entered <- struct{}{}
				_, _, _ = packetRead(conn)
				return
			}
			if selected.query == "SELECT infile" {
				if packetSend(conn, 1, append([]byte{0xfb}, p.upload...)) != nil {
					return
				}
				for {
					part, _, err := packetRead(conn)
					if err != nil {
						return
					}
					if len(part) > 0 {
						p.fileCalls.Add(1)
					} else {
						break
					}
				}
				return
			}
			if !strings.HasPrefix(selected.query, "SELECT") {
				if packetSend(conn, 1, []byte{0, 1, 7, status, 0, 0, 0}) != nil {
					return
				}
				continue
			}
			if selected.query == "SELECT huge_header" {
				_, _ = conn.Write([]byte{0, 0, 128, 1})
				_, _, _ = packetRead(conn)
				return
			}
			count := 1
			if selected.query == "SELECT cells" {
				count = 3
			}
			if selected.query == "SELECT wide" {
				count = 65
			}
			if packetSend(conn, 1, []byte{byte(count)}) != nil {
				return
			}
			seq := byte(2)
			kind := byte(253)
			flags := uint16(0)
			if selected.query == "SELECT unsigned" {
				kind = 8
				flags = 32
			}
			if selected.query == "SELECT decimal" {
				kind = 246
			}
			if selected.query == "SELECT zero_date" {
				kind = 12
			}
			if selected.query == "SELECT json" {
				kind = 245
			}
			for range count {
				if packetSend(conn, seq, columnPacket("value", kind, flags)) != nil {
					return
				}
				seq++
			}
			if packetSend(conn, seq, []byte{0xfe, 0, 0, status, 0}) != nil {
				return
			}
			seq++
			if selected.query != "SELECT empty" {
				data := append([]byte{0, 0}, encoded("value-canary")...)
				switch selected.query {
				case "SELECT cells":
					data = append([]byte{0, 4, 0}, encoded("value-canary")...)
				case "SELECT unsigned":
					data = append([]byte{0, 0}, []byte{255, 255, 255, 255, 255, 255, 255, 255}...)
				case "SELECT decimal":
					data = append([]byte{0, 0}, encoded("12345678901234567890.00100")...)
				case "SELECT zero_date":
					data = []byte{0, 0, 0}
				case "SELECT json":
					data = append([]byte{0, 0}, encoded("{\"exact\":18446744073709551615}")...)
				case "SELECT large":
					data = append([]byte{0, 0}, encoded(strings.Repeat("x", 4096))...)
				case "SELECT ?":
					if selected.parameters != 1 || len(body) < 14 {
						return
					}
					if body[10]&1 != 0 {
						data = []byte{0, 4}
					} else {
						data = append([]byte{0, 0}, body[14:]...)
					}
				}
				if packetSend(conn, seq, data) != nil {
					return
				}
				seq++
				if selected.query == "SELECT over" {
					if packetSend(conn, seq, data) != nil {
						return
					}
					seq++
				}
				if selected.query == "SELECT partial" {
					inTx = false
					_ = packetSend(conn, seq, append([]byte{255, 0xbd, 4, '#', '4', '0', '0', '0', '1'}, []byte("native-error-canary")...))
					continue
				}
			}
			if packetSend(conn, seq, []byte{0xfe, 0, 0, status, 0}) != nil {
				return
			}
		default:
			return
		}
	}
}

func (p *protocolPeer) textResult(conn net.Conn, query string, status byte) bool {
	if query == "SELECT wait" {
		p.entered <- struct{}{}
		_, _, _ = packetRead(conn)
		return false
	}
	if query == "SELECT infile" {
		if packetSend(conn, 1, append([]byte{0xfb}, p.upload...)) != nil {
			return false
		}
		for {
			body, seq, err := packetRead(conn)
			if err != nil {
				return false
			}
			if len(body) == 0 {
				return packetSend(conn, seq+1, []byte{0, 0, 0, status, 0, 0, 0}) == nil
			}
			p.fileCalls.Add(1)
		}
	}
	count := 1
	if query == "SELECT cells" {
		count = 3
	}
	if query == "SELECT wide" {
		count = 65
	}
	kind := byte(253)
	flags := uint16(0)
	switch query {
	case "SELECT unsigned":
		kind = 8
		flags = 32
	case "SELECT decimal":
		kind = 246
	case "SELECT zero_date":
		kind = 12
	case "SELECT json":
		kind = 245
	}
	if packetSend(conn, 1, []byte{byte(count)}) != nil {
		return false
	}
	seq := byte(2)
	for range count {
		if packetSend(conn, seq, columnPacket("value", kind, flags)) != nil {
			return false
		}
		seq++
	}
	if packetSend(conn, seq, []byte{0xfe, 0, 0, status, 0}) != nil {
		return false
	}
	seq++
	if query != "SELECT empty" {
		data := encoded("value-canary")
		switch query {
		case "SELECT cells":
			data = append([]byte{0xfb, 0}, encoded("value-canary")...)
		case "SELECT unsigned":
			data = encoded("18446744073709551615")
		case "SELECT decimal":
			data = encoded("12345678901234567890.00100")
		case "SELECT zero_date":
			data = encoded("0000-00-00 00:00:00")
		case "SELECT json":
			data = encoded("{\"exact\":18446744073709551615}")
		case "SELECT large", "SELECT drain_error":
			data = encoded(strings.Repeat("x", 4096))
		case "SELECT duplicate":
			return packetSend(conn, seq, append([]byte{255, 0x26, 4, '#', '2', '3', '0', '0', '0'}, []byte("duplicate-canary")...)) == nil
		}
		if packetSend(conn, seq, data) != nil {
			return false
		}
		seq++
		if query == "SELECT over" {
			if packetSend(conn, seq, data) != nil {
				return false
			}
			seq++
		}
		if query == "SELECT partial" || query == "SELECT drain_error" {
			return packetSend(conn, seq, append([]byte{255, 0xbd, 4, '#', '4', '0', '0', '0', '1'}, []byte("native-error-canary")...)) == nil
		}
	}
	return packetSend(conn, seq, []byte{0xfe, 0, 0, status, 0}) == nil
}

type boundFixture struct {
	assembly *resource.Assembly
	selected resource.Selection[Source]
	db       *Database
	inbox    *invocation.Inbox[Result]
}

func bindFixture(t testing.TB, options OptionsV1, count int) boundFixture {
	t.Helper()
	selected, err := Select(options)
	if err != nil {
		t.Fatal(err)
	}
	selected = resource.WithLimits(selected, LimitsV1(options))
	assembly, err := resource.Assemble(context.Background(), context.Background(), options.Name, selected)
	if err != nil {
		t.Fatal(err)
	}
	inbox, err := invocation.NewInbox[Result](count, int64(count)*defaults(options).evidenceReservation())
	if err != nil {
		t.Fatal(err)
	}
	db, err := Bind(assembly, selected, inbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
		defer cancel()
		if err := assembly.Close(ctx); err != nil {
			t.Error("fixture pool cleanup failed", err)
		}
		if inbox.Usage().Outstanding != 0 {
			t.Error("fixture evidence abandoned")
		}
	})
	return boundFixture{assembly, selected, db, inbox}
}
func observe(t testing.TB, receipt *invocation.Receipt[Result], err error) invocation.Result[Result] {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	result, err := receipt.WaitReleased(ctx)
	if err != nil {
		t.Fatal("call remained owned", err)
	}
	if !result.Final || !result.Released {
		t.Fatal("missing final local evidence")
	}
	return result
}
func drain(t testing.TB, inbox *invocation.Inbox[Result], count int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	for range count {
		record, err := inbox.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = record.Receipt().WaitReleased(ctx); err != nil {
			t.Fatal(err)
		}
		if err = record.Release(); err != nil {
			t.Fatal(err)
		}
	}
}
func correlation(id string) fault.Correlation { return fault.Correlation{Call: id} }
