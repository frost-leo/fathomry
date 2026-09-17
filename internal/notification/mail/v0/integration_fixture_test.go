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

package mail

import (
	"bufio"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"io"
	"math/big"
	"net"
	"net/textproto"
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

type peerOptions struct {
	wrongHostname   bool
	startTLS        bool
	noStartTLS      bool
	auth            string
	rejectAuth      bool
	rejectRecipient int
	rejectMessage   int
	dataReply       string
	dropAck         bool
	resetReply      string
	quitReply       string
	greeting        string
	stall           string
}
type capturedMail struct {
	sender     string
	recipients []string
	data       []byte
}
type smtpPeer struct {
	address, roots string
	mu             sync.Mutex
	captured       []capturedMail
	commands       []string
	hellos         []string
	connections    atomic.Int32
	active         atomic.Int32
	maximum        atomic.Int32
	reached        chan string
}

func certificate(t testing.TB) (tls.Certificate, string) {
	return certificateFor(t, "localhost")
}
func certificateFor(t testing.TB, name string) (tls.Certificate, string) {
	t.Helper()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: name},
		DNSNames:  []string{name},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	if name == "localhost" {
		template.IPAddresses = []net.IP{net.ParseIP("127.0.0.1")}
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, public, private)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: private}, string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
}
func newPeer(t testing.TB, options peerOptions) *smtpPeer {
	t.Helper()
	cert, roots := certificate(t)
	if options.wrongHostname {
		cert, roots = certificateFor(t, "wrong.fixture.test")
	}
	config := &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	peer := &smtpPeer{address: listener.Addr().String(), roots: roots, reached: make(chan string, 256)}
	var workers sync.WaitGroup
	var mu sync.Mutex
	connections := map[net.Conn]bool{}
	workers.Add(1)
	go func() {
		defer workers.Done()
		for {
			raw, err := listener.Accept()
			if err != nil {
				return
			}
			peer.connections.Add(1)
			active := peer.active.Add(1)
			for previous := peer.maximum.Load(); active > previous && !peer.maximum.CompareAndSwap(previous, active); previous = peer.maximum.Load() {
			}
			mu.Lock()
			connections[raw] = true
			mu.Unlock()
			workers.Add(1)
			go func() {
				defer workers.Done()
				defer peer.active.Add(-1)
				defer raw.Close()
				defer func() { mu.Lock(); delete(connections, raw); mu.Unlock() }()
				_ = raw.SetDeadline(time.Now().Add(10 * time.Second))
				var wire net.Conn = raw
				if !options.startTLS {
					wire = tls.Server(raw, config)
				}
				reader := textproto.NewReader(bufio.NewReader(wire))
				reply := func(value string) bool { _, err := io.WriteString(wire, value+"\r\n"); return err == nil }
				greeting := options.greeting
				if greeting == "" {
					greeting = "220 fixture ESMTP"
				}
				if !reply(greeting) {
					return
				}
				var sender string
				var recipients []string
				messageNumber := 0
				for {
					line, err := reader.ReadLine()
					if err != nil {
						return
					}
					command, argument, _ := strings.Cut(line, " ")
					peer.mu.Lock()
					peer.commands = append(peer.commands, command)
					if command == "EHLO" {
						peer.hellos = append(peer.hellos, argument)
					}
					peer.mu.Unlock()
					select {
					case peer.reached <- command:
					default:
					}
					if options.stall == command {
						_, _ = io.Copy(io.Discard, wire)
						return
					}
					switch command {
					case "EHLO":
						capabilities := "250-fixture\r\n"
						if options.startTLS && !options.noStartTLS {
							capabilities += "250-STARTTLS\r\n"
						}
						if options.auth != "" {
							capabilities += "250-AUTH " + strings.ToUpper(options.auth) + "\r\n"
						}
						if !reply(capabilities + "250 SIZE 33554432") {
							return
						}
					case "HELO":
						if !reply("250 fixture") {
							return
						}
					case "STARTTLS":
						if !reply("220 ready") {
							return
						}
						wire = tls.Server(raw, config)
						reader = textproto.NewReader(bufio.NewReader(wire))
					case "AUTH":
						mechanism, payload, _ := strings.Cut(argument, " ")
						valid := false
						switch mechanism {
						case "PLAIN":
							decoded, _ := base64.StdEncoding.DecodeString(payload)
							valid = string(decoded) == "\x00fixture-user\x00fixture-secret"
						case "XOAUTH2":
							decoded, _ := base64.StdEncoding.DecodeString(payload)
							valid = string(decoded) == "user=fixture-user\x01auth=Bearer fixture-secret\x01\x01"
						case "LOGIN":
							if !reply("334 VXNlcm5hbWU6") {
								return
							}
							username, err := reader.ReadLine()
							if err != nil || !reply("334 UGFzc3dvcmQ6") {
								return
							}
							password, err := reader.ReadLine()
							if err != nil {
								return
							}
							valid = username == base64.StdEncoding.EncodeToString([]byte("fixture-user")) && password == base64.StdEncoding.EncodeToString([]byte("fixture-secret"))
						}
						if options.rejectAuth || !valid {
							if !reply("535 5.7.8 fixture rejection") {
								return
							}
						} else if !reply("235 2.7.0 authenticated") {
							return
						}
					case "MAIL":
						messageNumber++
						sender = strings.TrimPrefix(strings.Split(argument, " ")[0], "FROM:")
						recipients = nil
						if !reply("250 2.1.0 sender") {
							return
						}
					case "RCPT":
						recipients = append(recipients, strings.TrimPrefix(argument, "TO:"))
						if len(recipients) == options.rejectRecipient || messageNumber == options.rejectMessage {
							if !reply("550 5.1.1 fixture private rejection") {
								return
							}
						} else if !reply("250 2.1.5 recipient") {
							return
						}
					case "DATA":
						if !reply("354 send content") {
							return
						}
						data, err := io.ReadAll(io.LimitReader(reader.DotReader(), 33<<20))
						if err != nil {
							return
						}
						peer.mu.Lock()
						peer.captured = append(peer.captured, capturedMail{sender: sender, recipients: append([]string(nil), recipients...), data: data})
						peer.mu.Unlock()
						select {
						case peer.reached <- "BODY":
						default:
						}
						if options.dropAck {
							return
						}
						response := options.dataReply
						if response == "" {
							response = "250 2.0.0 accepted"
						}
						if !reply(response) {
							return
						}
					case "RSET":
						response := options.resetReply
						if response == "" {
							response = "250 reset"
						}
						if !reply(response) {
							return
						}
					case "QUIT":
						response := options.quitReply
						if response == "" {
							response = "221 bye"
						}
						reply(response)
						if strings.HasPrefix(response, "221 ") {
							return
						}
					default:
						if !reply("500 unsupported") {
							return
						}
					}
				}
			}()
		}
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		mu.Lock()
		for conn := range connections {
			_ = conn.Close()
		}
		mu.Unlock()
		workers.Wait()
	})
	return peer
}
func (peer *smtpPeer) snapshot() ([]capturedMail, []string) {
	peer.mu.Lock()
	defer peer.mu.Unlock()
	return append([]capturedMail(nil), peer.captured...), append([]string(nil), peer.commands...)
}
func testOptions(peer *smtpPeer) OptionsV1 {
	host, port, _ := net.SplitHostPort(peer.address)
	number, _ := strconv.Atoi(port)
	return OptionsV1{Name: "mail", Host: host, Port: number, Hello: "fixture.test", TLSMode: "implicit", RootCAPEM: peer.roots,
		Auth: "none", Timeout: 2 * time.Second, CloseTimeout: time.Second, MaxActive: 1, MaxMessages: 4}
}

type bound struct {
	client    *Client
	assembly  *resource.Assembly
	selected  resource.Selection[Source]
	inbox     *invocation.Inbox[Result]
	closeWant error
}

func bindTest(t testing.TB, options OptionsV1, capacity int) *bound {
	t.Helper()
	selected, err := Select(options)
	if err != nil {
		t.Fatal(err)
	}
	selected = resource.WithLimits(selected, LimitsV1(options))
	assembly, err := resource.Assemble(context.Background(), context.Background(), "mail-test", selected)
	if err != nil {
		t.Fatal(err)
	}
	inbox, err := invocation.NewInbox[Result](capacity, defaults(options).evidenceReservation()*int64(capacity))
	if err != nil {
		t.Fatal(err)
	}
	client, err := Bind(assembly, selected, inbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	result := &bound{client: client, assembly: assembly, selected: selected, inbox: inbox}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		err := assembly.Close(ctx)
		if result.closeWant == nil && err != nil || result.closeWant != nil && !errors.Is(err, result.closeWant) {
			t.Errorf("unexpected cleanup classification: %v", err)
		}
	})
	return result
}
func plainMessage(t testing.TB, id string) Message {
	t.Helper()
	message, err := NewMessage(Content{ID: id + "@fixture.test", From: "Sender <sender@fixture.test>",
		To: []string{"to@fixture.test"}, Subject: "Fixture notification", Text: "Synthetic data only."})
	if err != nil {
		t.Fatal(err)
	}
	return message
}
func resolved(t testing.TB, receipt *invocation.Receipt[Result], err error) invocation.Result[Result] {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	result, err := receipt.WaitFinal(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return result
}
func sendTest(t testing.TB, client *Client, id string, messages ...Message) invocation.Result[Result] {
	t.Helper()
	receipt, err := client.SendBatch(context.Background(), fault.Correlation{Call: id}, messages)
	return resolved(t, receipt, err)
}
func drain(t testing.TB, inbox *invocation.Inbox[Result]) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for inbox.Usage().Outstanding > 0 {
		record, err := inbox.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = record.Receipt().WaitFinal(ctx); err != nil {
			t.Fatal(err)
		}
		if err = record.Release(); err != nil {
			t.Fatal(err)
		}
	}
}
