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

package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	stdmail "net/mail"
	"net/textproto"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	mail "github.com/frost-leo/fathomry/adapters/notification/mail/v1"
	"go.yaml.in/yaml/v3"
)

type welcomePeerOptions struct {
	login, dsn, dropACK, resetFailure, quitFailure bool
	rejectMessage                                  int
}
type welcomeCapture struct {
	wire       []byte
	sender     string
	recipients []string
}
type welcomeSMTP struct {
	listener net.Listener
	settings mail.Settings
	mu       sync.Mutex
	sockets  map[net.Conn]bool
	commands []string
	messages []welcomeCapture
	entries  int
	workers  sync.WaitGroup
}

func newWelcomePeer(t testing.TB, options welcomePeerOptions) *welcomeSMTP {
	t.Helper()
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, template, template, key.Public(), key)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}})
	if err != nil {
		t.Fatal(err)
	}
	host, port, _ := net.SplitHostPort(listener.Addr().String())
	number, _ := strconv.Atoi(port)
	settings := localSettings()
	settings.Host, settings.Port = host, number
	settings.RootCAPEM = string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
	if options.login {
		settings.Auth, settings.Username, settings.Password = "login", "welcome-user", "welcome-secret"
	}
	peer := &welcomeSMTP{listener: listener, settings: settings, sockets: map[net.Conn]bool{}}
	peer.workers.Add(1)
	go func() {
		defer peer.workers.Done()
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			peer.mu.Lock()
			peer.sockets[conn] = true
			peer.mu.Unlock()
			peer.workers.Add(1)
			go peer.serve(conn, options)
		}
	}()
	t.Cleanup(func() {
		listener.Close()
		peer.mu.Lock()
		for conn := range peer.sockets {
			conn.Close()
		}
		peer.mu.Unlock()
		peer.workers.Wait()
	})
	return peer
}
func (peer *welcomeSMTP) snapshot() ([]welcomeCapture, []string, int) {
	peer.mu.Lock()
	defer peer.mu.Unlock()
	return append([]welcomeCapture(nil), peer.messages...), append([]string(nil), peer.commands...), peer.entries
}
func (peer *welcomeSMTP) serve(conn net.Conn, options welcomePeerOptions) {
	defer peer.workers.Done()
	defer conn.Close()
	defer func() { peer.mu.Lock(); delete(peer.sockets, conn); peer.mu.Unlock() }()
	conn.SetDeadline(time.Now().Add(8 * time.Second))
	reply := func(line string) bool { _, err := io.WriteString(conn, line+"\r\n"); return err == nil }
	reader := textproto.NewReader(bufio.NewReader(conn))
	if !reply("220 welcome peer") {
		return
	}
	var current welcomeCapture
	messageNumber := 0
	for {
		line, err := reader.ReadLine()
		if err != nil {
			return
		}
		command, argument, _ := strings.Cut(line, " ")
		peer.mu.Lock()
		peer.commands = append(peer.commands, line)
		peer.mu.Unlock()
		switch command {
		case "EHLO":
			extensions := "250-welcome\r\n250-SMTPUTF8\r\n"
			if options.login {
				extensions += "250-AUTH LOGIN\r\n"
			}
			if options.dsn {
				extensions += "250-DSN\r\n"
			}
			if !reply(extensions + "250 8BITMIME") {
				return
			}
		case "AUTH":
			method, user, _ := strings.Cut(argument, " ")
			if method != "LOGIN" || !options.login {
				reply("504 unsupported")
				return
			}
			if user == "" {
				if !reply("334 VXNlcm5hbWU6") {
					return
				}
				user, err = reader.ReadLine()
				if err != nil {
					return
				}
			}
			if user != base64.StdEncoding.EncodeToString([]byte("welcome-user")) {
				reply("535 invalid fixture user")
				return
			}
			if !reply("334 UGFzc3dvcmQ6") {
				return
			}
			password, err := reader.ReadLine()
			if err != nil || password != base64.StdEncoding.EncodeToString([]byte("welcome-secret")) {
				reply("535 invalid fixture password")
				return
			}
			if !reply("235 authenticated") {
				return
			}
		case "NOOP", "VRFY":
			if !reply("250 ready") {
				return
			}
		case "MAIL":
			peer.mu.Lock()
			peer.entries++
			messageNumber = peer.entries
			peer.mu.Unlock()
			current = welcomeCapture{sender: argument}
			if !reply("250 sender") {
				return
			}
		case "RCPT":
			current.recipients = append(current.recipients, argument)
			if messageNumber == options.rejectMessage {
				if !reply("550 5.1.1 rejected") {
					return
				}
				continue
			}
			if !reply("250 recipient") {
				return
			}
		case "DATA":
			if !reply("354 proceed") {
				return
			}
			data, err := io.ReadAll(io.LimitReader(reader.DotReader(), 16<<20))
			if err != nil {
				return
			}
			current.wire = bytes.ReplaceAll(data, []byte("\n"), []byte("\r\n"))
			peer.mu.Lock()
			peer.messages = append(peer.messages, current)
			peer.mu.Unlock()
			if options.dropACK {
				return
			}
			if !reply("250 2.0.0 welcome-queue") {
				return
			}
		case "RSET":
			if options.resetFailure {
				if !reply("550 reset rejected") {
					return
				}
				continue
			}
			if !reply("250 reset") {
				return
			}
		case "QUIT":
			if options.quitFailure {
				reply("550 quit rejected")
			} else {
				reply("221 bye")
			}
			return
		default:
			reply("500 unsupported")
			return
		}
	}
}

func TestWelcomeCommandCompositeTLSAndAttemptGuard(t *testing.T) {
	for _, mode := range []string{"accepted", "lost-ACK", "reset-failure", "quit-failure"} {
		t.Run(mode, func(t *testing.T) {
			peer := newWelcomePeer(t, welcomePeerOptions{login: true, dropACK: mode == "lost-ACK", resetFailure: mode == "reset-failure", quitFailure: mode == "quit-failure"})
			directory := t.TempDir()
			config := filepath.Join(directory, "private.yaml")
			attempt := filepath.Join(directory, "attempt.jsonl")
			private := smtpSettings{Enabled: true, Host: peer.settings.Host, Port: peer.settings.Port, TLSMode: "implicit", RootCAPEM: peer.settings.RootCAPEM, Auth: "login", Hello: peer.settings.Hello, Username: peer.settings.Username, Password: peer.settings.Password, From: "sender@fixture.test", To: "recipient@fixture.test"}
			data, err := yaml.Marshal(map[string]smtpSettings{"fathomry_mail_gh63": private})
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(config, data, 0600); err != nil {
				t.Fatal(err)
			}
			args := []string{"-send", "-config", config, "-attempt", attempt}
			var output bytes.Buffer
			err = run(context.Background(), args, &output)
			if (err != nil) != (mode != "accepted") {
				t.Fatal("unexpected command outcome", err)
			}
			if strings.Contains(output.String(), private.Password) || strings.Contains(output.String(), private.From) {
				t.Fatal("private fields leaked")
			}
			var summary submission
			if json.Unmarshal(output.Bytes(), &summary) != nil {
				t.Fatal("invalid summary")
			}
			if !summary.Prepared || !summary.Observed || !summary.SourceReleased || !summary.RuntimeClosed || summary.EvidenceOutstanding != 0 || summary.EvidenceRecords != 4 || summary.Attempts != 1 || !summary.AttemptsExact {
				t.Fatal("incomplete command/evidence lifecycle", output.String())
			}
			if mode == "lost-ACK" {
				if summary.Effect != "unknown" || !summary.PrimaryFailure {
					t.Fatal("lost ACK disposition")
				}
			} else if summary.Effect != "accepted" || summary.Code != 250 || summary.EnhancedCode != "2.0.0" || summary.PrimaryFailure {
				t.Fatal("relay acceptance was lost")
			}
			if summary.CleanupFailure != (mode == "reset-failure" || mode == "quit-failure") {
				t.Fatal("cleanup distinction lost")
			}
			messages, _, entries := peer.snapshot()
			if len(messages) != 1 || entries != 1 {
				t.Fatal("duplicate or missing actual message")
			}
			root, parts := parseWelcome(t, messages[0].wire)
			if root.Header.Get("Message-ID") != "<"+summary.MessageID+">" || root.Header.Get("X-Fathomry-Welcome") != "public-api" {
				t.Fatal("captured identity/transform mismatch")
			}
			plain, html, files := 0, 0, 0
			for _, part := range parts {
				if part.filename != "" {
					files++
					continue
				}
				if part.kind == "text/plain" && strings.Contains(string(part.data), "Welcome to Fathomry") {
					plain++
				}
				if part.kind == "text/html" && strings.Contains(string(part.data), "Welcome to<br>Fathomry.") {
					html++
				}
			}
			if plain != 1 || html != 1 || files != 5 || len(messages[0].wire) != summary.MIMEBytes {
				t.Fatal("welcome composite was not actually transmitted")
			}
			guard, err := os.ReadFile(attempt)
			if err != nil {
				t.Fatal(err)
			}
			decoder := json.NewDecoder(bytes.NewReader(guard))
			for _, event := range []string{"reserved", "finished"} {
				var record struct {
					Event   string
					Outcome submission
				}
				if decoder.Decode(&record) != nil || record.Event != event {
					t.Fatal("attempt event missing")
				}
				if event == "finished" && (record.Outcome.EvidenceOutstanding != 0 || !record.Outcome.SourceReleased || !record.Outcome.RuntimeClosed) {
					t.Fatal("attempt persisted before cleanup")
				}
			}
			if err := run(context.Background(), args, &bytes.Buffer{}); err == nil {
				t.Fatal("attempt allowed reentry")
			}
			_, _, after := peer.snapshot()
			if after != entries {
				t.Fatal("attempt retry re-entered SMTP")
			}
			unchanged, _ := os.ReadFile(config)
			if !bytes.Equal(unchanged, data) {
				t.Fatal("configuration modified")
			}
		})
	}
}

func TestWelcomeDSNMDNAndBCCEnvelope(t *testing.T) {
	peer := newWelcomePeer(t, welcomePeerOptions{dsn: true})
	settings := peer.settings
	settings.DSNReturn, settings.DSNNotify = "HDRS", []string{"SUCCESS", "FAILURE"}
	session, summary := testMailSession(t, settings)
	content := reportWelcome(t, "dsn")
	content.EnvelopeFrom = "bounce@fixture.test"
	content.Cc = []string{"copy@fixture.test"}
	content.Bcc = []string{"hidden@fixture.test"}
	content.MDN = []string{"receipts@fixture.test"}
	message := preparedWelcome(t, session, content, nil)
	if err := session.send(context.Background(), message); err != nil {
		t.Fatal(err)
	}
	messages, commands, _ := peer.snapshot()
	if len(messages) != 1 || !strings.Contains(messages[0].sender, "FROM:<bounce@fixture.test>") || !strings.Contains(messages[0].sender, "RET=HDRS") || len(messages[0].recipients) != 3 {
		t.Fatal("envelope/DSN mapping")
	}
	for _, recipient := range messages[0].recipients {
		if !strings.Contains(recipient, "NOTIFY=SUCCESS,FAILURE") {
			t.Fatal("DSN notify selection lost")
		}
	}
	root, parts := parseWelcome(t, messages[0].wire)
	if root.Header.Get("Bcc") != "" || bytes.Contains(messages[0].wire, []byte("hidden@fixture.test")) || root.Header.Get("Disposition-Notification-To") != "<receipts@fixture.test>" {
		t.Fatal("BCC privacy or MDN header lost")
	}
	images := 0
	for _, part := range parts {
		if part.cid != "" {
			images++
		}
	}
	if images != 4 {
		t.Fatal("welcome report CID resources missing on SMTP")
	}
	if !strings.Contains(strings.Join(commands, "\n"), "NOOP") {
		t.Fatal("default session readiness omitted")
	}
	if err := session.close(); err != nil || summary.EvidenceOutstanding != 0 {
		t.Fatal("source cleanup", err)
	}
}

func TestWelcomeBatchFailFastAndContinuation(t *testing.T) {
	for _, continued := range []bool{false, true} {
		t.Run(strconv.FormatBool(continued), func(t *testing.T) {
			peer := newWelcomePeer(t, welcomePeerOptions{rejectMessage: 2})
			session, _ := testMailSession(t, peer.settings)
			var messages []mail.Message
			for _, id := range []string{"first", "rejected", "third"} {
				messages = append(messages, preparedWelcome(t, session, basicWelcome(t, id), nil))
			}
			result, operationErr := session.owner.Client().SendBatch(context.Background(), messages, mail.BatchOptions{ContinueOnError: continued})
			result, err := session.accept("send", result, operationErr)
			if err == nil {
				t.Fatal("recipient rejection lost")
			}
			deliveries := result.Messages()
			if len(deliveries) != 3 || deliveries[0].Effect() != mail.Accepted || deliveries[1].Effect() != mail.NotAccepted {
				t.Fatal("batch positions/effects lost")
			}
			captured, _, entries := peer.snapshot()
			if continued {
				if deliveries[2].Effect() != mail.Accepted || len(captured) != 2 || entries != 3 {
					t.Fatal("continuation resent/stopped a position")
				}
			} else if deliveries[2].Effect() != mail.NotAttempted || len(captured) != 1 || entries != 2 {
				t.Fatal("fail-fast suffix changed")
			}
			if !result.Attempts().Exact || result.Attempts().Observed != uint64(entries) {
				t.Fatal("batch attempt facts changed")
			}
			for _, capture := range captured {
				root, _ := stdmail.ReadMessage(bytes.NewReader(capture.wire))
				if root.Header.Get("Message-ID") == "<rejected@fixture.test>" {
					t.Fatal("rejected position reached DATA")
				}
			}
		})
	}
}
