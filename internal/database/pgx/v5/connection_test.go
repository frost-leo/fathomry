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

package pgx

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
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
	"github.com/jackc/pgx/v5/pgproto3"
)

// protocolPeer executes client protocol paths only. It is NOT PostgreSQL and
// cannot establish database atomicity, server authorization or durability.
type protocolPeer struct {
	listener     net.Listener
	tls          *tls.Config
	roots        string
	mu           sync.Mutex
	sockets      map[net.Conn]struct{}
	cancel       map[uint32]context.CancelFunc
	workers      sync.WaitGroup
	stop         chan struct{}
	entered      chan struct{}
	queries      atomic.Int64
	connects     atomic.Int64
	commits      atomic.Int64
	rollbacks    atomic.Int64
	dropCommit   atomic.Bool
	blockStartup atomic.Bool
}

func newProtocolPeer(t testing.TB, secure bool) *protocolPeer {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal("loopback peer could not listen")
	}
	peer := &protocolPeer{listener: listener, sockets: make(map[net.Conn]struct{}), cancel: make(map[uint32]context.CancelFunc), stop: make(chan struct{}), entered: make(chan struct{}, 32)}
	if secure {
		public, private, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		template := &x509.Certificate{SerialNumber: big.NewInt(1), DNSNames: []string{"fixture.invalid"}, NotBefore: time.Now().Add(-time.Hour),
			NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
			ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
		der, err := x509.CreateCertificate(rand.Reader, template, template, public, private)
		if err != nil {
			t.Fatal(err)
		}
		peer.roots = string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
		peer.tls = &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: private}}, MinVersion: tls.VersionTLS12}
	}
	peer.workers.Go(func() {
		for {
			socket, err := listener.Accept()
			if err != nil {
				return
			}
			peer.mu.Lock()
			select {
			case <-peer.stop:
				peer.mu.Unlock()
				socket.Close()
				return
			default:
			}
			peer.sockets[socket] = struct{}{}
			peer.mu.Unlock()
			peer.workers.Go(func() {
				defer socket.Close()
				defer func() { peer.mu.Lock(); delete(peer.sockets, socket); peer.mu.Unlock() }()
				peer.serve(socket)
			})
		}
	})
	t.Cleanup(func() {
		close(peer.stop)
		listener.Close()
		peer.mu.Lock()
		for socket := range peer.sockets {
			socket.Close()
		}
		for _, cancel := range peer.cancel {
			cancel()
		}
		peer.mu.Unlock()
		peer.workers.Wait()
	})
	return peer
}
func (peer *protocolPeer) options() OptionsV1 {
	input := testOptions()
	_, port, _ := net.SplitHostPort(peer.listener.Addr().String())
	number, _ := strconv.Atoi(port)
	input.Port = uint16(number)
	input.Timeout = time.Second
	input.CloseTimeout = 100 * time.Millisecond
	if peer.tls != nil {
		input.Plaintext = false
		input.RootCAPEM = peer.roots
		input.ServerName = "fixture.invalid"
	}
	return input
}
func authenticatePeer(backend *pgproto3.Backend) bool {
	_ = backend.SetAuthType(pgproto3.AuthTypeSASL)
	backend.Send(&pgproto3.AuthenticationSASL{AuthMechanisms: []string{"SCRAM-SHA-256"}})
	if backend.Flush() != nil {
		return false
	}
	message, err := backend.Receive()
	first, ok := message.(*pgproto3.SASLInitialResponse)
	if err != nil || !ok || first.AuthMechanism != "SCRAM-SHA-256" {
		return false
	}
	_, bare, ok := strings.Cut(string(first.Data), ",,")
	if !ok {
		return false
	}
	_, nonce, ok := strings.Cut(bare, ",r=")
	if !ok {
		return false
	}
	salt := []byte("bounded-protocol-peer-salt")
	serverFirst := "r=" + nonce + "server,s=" + base64.StdEncoding.EncodeToString(salt) + ",i=4096"
	_ = backend.SetAuthType(pgproto3.AuthTypeSASLContinue)
	backend.Send(&pgproto3.AuthenticationSASLContinue{Data: []byte(serverFirst)})
	if backend.Flush() != nil {
		return false
	}
	message, err = backend.Receive()
	final, ok := message.(*pgproto3.SASLResponse)
	if err != nil || !ok {
		return false
	}
	withoutProof, proofText, ok := strings.Cut(string(final.Data), ",p=")
	if !ok {
		return false
	}
	auth := bare + "," + serverFirst + "," + withoutProof
	salted, err := pbkdf2.Key(sha256.New, "credential-canary", salt, 4096, 32)
	if err != nil {
		return false
	}
	mac := func(key []byte, value string) []byte {
		hash := hmac.New(sha256.New, key)
		hash.Write([]byte(value))
		return hash.Sum(nil)
	}
	clientKey := mac(salted, "Client Key")
	stored := sha256.Sum256(clientKey)
	signature := mac(stored[:], auth)
	proof, err := base64.StdEncoding.DecodeString(proofText)
	if err != nil || len(proof) != len(clientKey) {
		return false
	}
	for index := range proof {
		proof[index] ^= signature[index]
	}
	if !hmac.Equal(proof, clientKey) {
		backend.Send(&pgproto3.ErrorResponse{Severity: "FATAL", Code: "28P01", Message: "authentication-canary"})
		backend.Flush()
		return false
	}
	backend.Send(&pgproto3.AuthenticationSASLFinal{Data: []byte("v=" + base64.StdEncoding.EncodeToString(mac(mac(salted, "Server Key"), auth)))})
	return backend.Flush() == nil
}
func (peer *protocolPeer) serve(socket net.Conn) {
	backend := pgproto3.NewBackend(socket, socket)
	message, err := backend.ReceiveStartupMessage()
	if err != nil {
		return
	}
	if _, ok := message.(*pgproto3.SSLRequest); ok {
		if peer.tls == nil {
			socket.Write([]byte("N"))
			return
		}
		if _, err := socket.Write([]byte("S")); err != nil {
			return
		}
		socket = tls.Server(socket, peer.tls)
		backend = pgproto3.NewBackend(socket, socket)
		message, err = backend.ReceiveStartupMessage()
		if err != nil {
			return
		}
	}
	if cancel, ok := message.(*pgproto3.CancelRequest); ok {
		peer.mu.Lock()
		if cancelFunc := peer.cancel[cancel.ProcessID]; cancelFunc != nil {
			cancelFunc()
		}
		peer.mu.Unlock()
		return
	}
	if _, ok := message.(*pgproto3.StartupMessage); !ok {
		return
	}
	if peer.blockStartup.Load() {
		select {
		case peer.entered <- struct{}{}:
		default:
		}
		buffer := make([]byte, 1)
		socket.Read(buffer)
		return
	}
	if !authenticatePeer(backend) {
		return
	}
	pid := uint32(peer.connects.Add(1))
	ctx, cancel := context.WithCancel(context.Background())
	peer.mu.Lock()
	peer.cancel[pid] = cancel
	peer.mu.Unlock()
	defer func() { cancel(); peer.mu.Lock(); delete(peer.cancel, pid); peer.mu.Unlock() }()
	status := byte('I')
	backend.Send(&pgproto3.AuthenticationOk{})
	backend.Send(&pgproto3.BackendKeyData{ProcessID: pid, SecretKey: []byte{1, 2, 3, 4}})
	backend.Send(&pgproto3.ParameterStatus{Name: "server_version", Value: "18.6"})
	backend.Send(&pgproto3.ParameterStatus{Name: "client_encoding", Value: "UTF8"})
	backend.Send(&pgproto3.ParameterStatus{Name: "standard_conforming_strings", Value: "on"})
	backend.Send(&pgproto3.ReadyForQuery{TxStatus: status})
	if backend.Flush() != nil {
		return
	}
	var sql string
	var parameters [][]byte
	fields := func() {
		count := 1
		if sql == "SELECT cells" {
			count = 3
		}
		if sql == "SELECT wide_nulls" || sql == "SELECT columns64" {
			count = 64
		}
		if sql == "SELECT columns65" {
			count = 65
		}
		if strings.HasPrefix(sql, "INSERT") || strings.HasPrefix(sql, "UPDATE") || strings.HasPrefix(sql, "DELETE") {
			backend.Send(&pgproto3.NoData{})
			return
		}
		values := make([]pgproto3.FieldDescription, count)
		for index := range values {
			values[index] = pgproto3.FieldDescription{Name: []byte("value"), DataTypeOID: 25, DataTypeSize: -1, TypeModifier: -1}
		}
		backend.Send(&pgproto3.RowDescription{Fields: values})
	}
	execute := func() {
		peer.queries.Add(1)
		switch sql {
		case "SELECT wide_nulls":
			for range 8192 {
				backend.Send(&pgproto3.DataRow{Values: make([][]byte, 64)})
			}
		case "SELECT columns64", "SELECT columns65":
			count := 64
			if sql == "SELECT columns65" {
				count = 65
			}
			backend.Send(&pgproto3.DataRow{Values: make([][]byte, count)})
		case "SELECT cells":
			backend.Send(&pgproto3.DataRow{Values: [][]byte{nil, {}, []byte("value-canary")}})
		case "SELECT empty":
		case "SELECT partial":
			backend.Send(&pgproto3.DataRow{Values: [][]byte{[]byte("first")}})
			backend.Send(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "22012", Message: "native-cause-canary", Detail: "private-detail-canary"})
			if status == 'T' {
				status = 'E'
			}
			return
		case "SELECT late":
			backend.Send(&pgproto3.DataRow{Values: [][]byte{[]byte("first")}})
			backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")})
			backend.Send(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "23514", Message: "late-cause-canary"})
			return
		case "SELECT over":
			for range 3 {
				backend.Send(&pgproto3.DataRow{Values: [][]byte{[]byte("row")}})
			}
			backend.Send(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "22012", Message: "drain-cause-canary"})
			return
		case "SELECT large":
			backend.Send(&pgproto3.DataRow{Values: [][]byte{bytes.Repeat([]byte("x"), 2048)}})
		case "SELECT wait":
			select {
			case peer.entered <- struct{}{}:
			default:
			}
			select {
			case <-ctx.Done():
			case <-peer.stop:
			}
			backend.Send(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "57014", Message: "canceled"})
			return
		case "SELECT $1::text":
			var value []byte
			if len(parameters) > 0 {
				value = parameters[0]
			}
			backend.Send(&pgproto3.DataRow{Values: [][]byte{value}})
		default:
			if !strings.HasPrefix(sql, "INSERT") && !strings.HasPrefix(sql, "UPDATE") && !strings.HasPrefix(sql, "DELETE") {
				backend.Send(&pgproto3.DataRow{Values: [][]byte{[]byte("one")}})
			}
		}
		tag := "SELECT 1"
		if sql == "SELECT empty" {
			tag = "SELECT 0"
		}
		if strings.HasPrefix(sql, "INSERT") {
			tag = "INSERT 0 1"
		}
		backend.Send(&pgproto3.CommandComplete{CommandTag: []byte(tag)})
	}
	for {
		message, err := backend.Receive()
		if err != nil {
			return
		}
		switch message := message.(type) {
		case *pgproto3.Terminate:
			return
		case *pgproto3.Parse:
			sql = message.Query
			backend.Send(&pgproto3.ParseComplete{})
		case *pgproto3.Bind:
			parameters = make([][]byte, len(message.Parameters))
			for index, value := range message.Parameters {
				if value != nil {
					parameters[index] = append([]byte{}, value...)
				}
			}
			backend.Send(&pgproto3.BindComplete{})
		case *pgproto3.Describe:
			fields()
		case *pgproto3.Execute:
			execute()
		case *pgproto3.Sync:
			backend.Send(&pgproto3.ReadyForQuery{TxStatus: status})
		case *pgproto3.Query:
			sql = message.String
			switch {
			case strings.HasPrefix(sql, "begin"):
				status = 'T'
				backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("BEGIN")})
			case sql == "commit":
				peer.commits.Add(1)
				if peer.dropCommit.Load() {
					return
				}
				tag := "COMMIT"
				if status == 'E' {
					tag = "ROLLBACK"
				}
				status = 'I'
				backend.Send(&pgproto3.CommandComplete{CommandTag: []byte(tag)})
			case sql == "rollback":
				peer.rollbacks.Add(1)
				status = 'I'
				backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("ROLLBACK")})
			default:
				fields()
				execute()
			}
			backend.Send(&pgproto3.ReadyForQuery{TxStatus: status})
		default:
			return
		}
		if backend.Flush() != nil {
			return
		}
	}
}

type boundFixture struct {
	assembly *resource.Assembly
	selected resource.Selection[Source]
	database *Database
	inbox    *invocation.Inbox[Result]
}

func bindFixture(t testing.TB, options OptionsV1, capacity int) boundFixture {
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
	inbox, err := invocation.NewInbox[Result](capacity, int64(capacity)*defaults(options).evidenceReservation())
	if err != nil {
		t.Fatal(err)
	}
	database, err := Bind(assembly, selected, inbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := assembly.Close(context.Background()); err != nil {
			t.Error("fixture ownership remains", err)
		}
		if inbox.Usage().Outstanding != 0 {
			t.Error("fixture evidence was abandoned")
		}
	})
	return boundFixture{assembly, selected, database, inbox}
}
func operationResult(t testing.TB, receipt *invocation.Receipt[Result], err error) invocation.Result[Result] {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
	result, ok := receipt.Result()
	if !ok || !result.Final || !result.Released {
		t.Fatal("finite result is not released")
	}
	return result
}
func drain(t testing.TB, inbox *invocation.Inbox[Result], count int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for range count {
		record, err := inbox.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := record.Receipt().WaitReleased(ctx); err != nil {
			t.Fatal(err)
		}
		if err := record.Release(); err != nil {
			t.Fatal(err)
		}
	}
}
func correlation(id string) fault.Correlation { return fault.Correlation{Call: id} }
