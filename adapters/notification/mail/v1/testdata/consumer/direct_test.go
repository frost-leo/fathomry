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
	"bufio"
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	stdmail "net/mail"
	"net/textproto"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	mail "github.com/frost-leo/fathomry/adapters/notification/mail/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
)

type capture struct {
	sender     string
	recipients []string
	mime       []byte
}
type peer struct {
	listener net.Listener
	config   mail.Settings
	mu       sync.Mutex
	sockets  map[net.Conn]bool
	messages []capture
	commands []string
	wg       sync.WaitGroup
	hold     chan struct{}
	reached  chan struct{}
}

func newPeer(t testing.TB, name string) *peer {
	t.Helper()
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	certificate := &x509.Certificate{SerialNumber: big.NewInt(1), IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, certificate, certificate, key.Public(), key)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	listener := tls.NewListener(raw, &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}})
	host, port, _ := net.SplitHostPort(raw.Addr().String())
	number, _ := strconv.Atoi(port)
	value := &peer{listener: listener, sockets: map[net.Conn]bool{}, reached: make(chan struct{}, 1), config: mail.Settings{Name: name, Host: host, Port: number, Hello: "consumer.invalid", TLSMode: "implicit", Auth: "none", RootCAPEM: string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})), MaxActive: 1, MaxMessages: 4, MaxMessageBytes: 64 << 10, MaxMIMEBytes: 128 << 10, Timeout: 3 * time.Second, CloseTimeout: time.Second}}
	value.wg.Add(1)
	go func() {
		defer value.wg.Done()
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			value.mu.Lock()
			value.sockets[conn] = true
			value.mu.Unlock()
			value.wg.Add(1)
			go value.serve(conn)
		}
	}()
	t.Cleanup(func() {
		listener.Close()
		value.mu.Lock()
		for conn := range value.sockets {
			conn.Close()
		}
		value.mu.Unlock()
		value.wg.Wait()
	})
	return value
}
func (value *peer) serve(conn net.Conn) {
	defer value.wg.Done()
	defer conn.Close()
	defer func() { value.mu.Lock(); delete(value.sockets, conn); value.mu.Unlock() }()
	conn.SetDeadline(time.Now().Add(12 * time.Second))
	reply := func(line string) bool { _, err := io.WriteString(conn, line+"\r\n"); return err == nil }
	if !reply("220 isolated consumer peer") {
		return
	}
	reader := textproto.NewReader(bufio.NewReader(conn))
	var current capture
	for {
		line, err := reader.ReadLine()
		if err != nil {
			return
		}
		command, argument, _ := strings.Cut(line, " ")
		value.mu.Lock()
		value.commands = append(value.commands, line)
		value.mu.Unlock()
		switch command {
		case "EHLO":
			if !reply("250-isolated\r\n250-DSN\r\n250-SMTPUTF8\r\n250 8BITMIME") {
				return
			}
		case "NOOP", "RSET", "VRFY":
			if !reply("250 controlled") {
				return
			}
		case "MAIL":
			current = capture{sender: argument}
			if !reply("250 sender") {
				return
			}
		case "RCPT":
			current.recipients = append(current.recipients, argument)
			if !reply("250 recipient") {
				return
			}
		case "DATA":
			if !reply("354 data") {
				return
			}
			data, err := io.ReadAll(io.LimitReader(reader.DotReader(), 256<<10))
			if err != nil {
				return
			}
			current.mime = data
			value.mu.Lock()
			value.messages = append(value.messages, current)
			hold := value.hold
			value.hold = nil
			value.mu.Unlock()
			if hold != nil {
				value.reached <- struct{}{}
				<-hold
			}
			if !reply("250 2.0.0 relay acceptance") {
				return
			}
		case "QUIT":
			reply("221 bye")
			return
		default:
			if !reply("500 unsupported") {
				return
			}
		}
	}
}
func (value *peer) snapshot() ([]capture, []string) {
	value.mu.Lock()
	defer value.mu.Unlock()
	return append([]capture(nil), value.messages...), append([]string(nil), value.commands...)
}
func makeMessage(t testing.TB, id string) mail.Message {
	t.Helper()
	message, err := mail.NewMessage(mail.Content{ID: id + "@example.invalid", From: "o'hara@example.invalid", EnvelopeFrom: "bounce@example.invalid", To: []string{"to@example.invalid"}, Bcc: []string{"hidden@example.invalid"}, Subject: "", Text: "synthetic consumer", Headers: map[string]string{"Organization": "independent"}})
	if err != nil {
		t.Fatal(err)
	}
	return message
}
func waitContext(t testing.TB) (context.Context, context.CancelFunc) {
	t.Helper()
	return context.WithTimeout(context.Background(), 8*time.Second)
}
func take(t testing.TB, ctx context.Context, inbox *adapters.Inbox[mail.Result]) (adapters.Delivery[mail.Result], adapters.Snapshot[mail.Result]) {
	t.Helper()
	delivery, err := inbox.NextReleased(ctx)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := delivery.Receipt()
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := receipt.WaitReleased(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return delivery, snapshot
}
func TestDirectWireEvidenceAndSaturation(t *testing.T) {
	ctx, cancel := waitContext(t)
	defer cancel()
	peer := newPeer(t, "direct")
	prepared, err := mail.Prepare(peer.config, mail.NativeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	policy, err := prepared.Policy()
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := adapters.New(ctx, policy.Runtime)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close(context.Background())
	inbox, err := adapters.NewInbox[mail.Result](policy.Evidence)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := prepared.Open(ctx, mail.Dependencies{Runtime: runtime, Evidence: inbox})
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close(context.Background())
	client, err := owner.Client().WithID("opaque-consumer")
	if err != nil {
		t.Fatal(err)
	}
	message := makeMessage(t, "direct")
	first, err := client.Send(ctx, message)
	if err != nil {
		t.Fatal(err)
	}
	if !first.HasData() || first.Messages()[0].Effect() != mail.Accepted || first.Messages()[0].Code() != 250 || first.Messages()[0].ServerResponse() != "2.0.0 relay acceptance" || first.Attribution().ID != "opaque-consumer" || first.Attribution().Source.Generation != 0 {
		t.Fatal("public result distinction")
	}
	if strings.Contains(fmt.Sprintf("%+v %#v", first, first.Messages()[0]), "relay acceptance") {
		t.Fatal("sensitive successful reply leaked through formatting")
	}
	delivery, snapshot := take(t, ctx, inbox)
	value, ok := snapshot.ValueCopy()
	if !ok || value.Attribution().Sequence != first.Attribution().Sequence {
		t.Fatal("not exact independent evidence")
	}
	if err = delivery.Retry(); err != nil {
		t.Fatal(err)
	}
	redelivery, repeated := take(t, ctx, inbox)
	if repeated.Info().Sequence != snapshot.Info().Sequence {
		t.Fatal("redelivery changed operation")
	}
	if err = redelivery.Ack(); err != nil {
		t.Fatal(err)
	}
	messages, _ := peer.snapshot()
	if len(messages) != 1 {
		t.Fatal("evidence retry resent email")
	}
	if !strings.Contains(messages[0].sender, "bounce@example.invalid") || len(messages[0].recipients) != 2 || !strings.Contains(messages[0].recipients[1], "hidden@example.invalid") {
		t.Fatal("envelope mapping")
	}
	parsed, err := stdmail.ReadMessage(bytes.NewReader(messages[0].mime))
	if err != nil || parsed.Header.Get("Bcc") != "" || parsed.Header.Get("Organization") != "independent" {
		t.Fatal("MIME fidelity/privacy")
	}
	// Two finite records fit beside the live source, then evidence refuses dispatch.
	for i := 0; i < policy.Evidence.Capacity-1; i++ {
		if _, err = client.Send(ctx, message); err != nil {
			t.Fatal(err)
		}
	}
	before, _ := peer.snapshot()
	if _, err = client.Send(ctx, message); !errors.Is(err, adapters.ErrEvidence) {
		t.Fatal("saturated evidence did not refuse", err)
	}
	after, _ := peer.snapshot()
	if len(before) != len(after) {
		t.Fatal("saturation sent mail")
	}
	for i := 0; i < policy.Evidence.Capacity-1; i++ {
		entry, _ := take(t, ctx, inbox)
		if err = entry.Ack(); err != nil {
			t.Fatal(err)
		}
	}
	if err = owner.Close(ctx); err != nil {
		t.Fatal(err)
	}
	entry, source := take(t, ctx, inbox)
	if source.Info().Operation != "notification.mail.open" {
		t.Fatal("source record missing")
	}
	if err = entry.Ack(); err != nil {
		t.Fatal(err)
	}
	if !owner.ShutdownComplete() {
		t.Fatal("source not joined")
	}
}

func TestPreparationPublicFieldsAndPrivacy(t *testing.T) {
	ctx, cancel := waitContext(t)
	defer cancel()
	settings := mail.Settings{Name: "offline", Host: "localhost", Port: 465, Hello: "consumer.invalid", TLSMode: "implicit", Auth: "none", MaxActive: 1, MaxMessageBytes: 64 << 10, MaxMIMEBytes: 128 << 10}
	policy, err := mail.Recommend(settings)
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := adapters.New(ctx, policy.Runtime)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close(context.Background())
	inbox, err := adapters.NewInbox[mail.Result](policy.Evidence)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := mail.Open(ctx, settings, mail.Dependencies{Runtime: runtime, Evidence: inbox})
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close(context.Background())
	message, err := mail.NewMessage(mail.Content{From: "sender@example.invalid", Subject: "=?windows-1252?Q?caf=E9?=", SubjectPreformatted: true, To: []string{"to@example.invalid"}, MDN: []string{"read@example.invalid"}, ReplyToList: []string{"reply@example.invalid"}, Parts: []mail.Part{{ContentType: "text/plain", Charset: "ISO-8859-1", Encoding: "base64", Description: "description", Data: []byte{0xe9}}}, Fields: []mail.Header{{Name: "References", Values: []string{"<old@example.invalid>"}, Preformatted: true}}})
	if err != nil {
		t.Fatal(err)
	}
	result, err := owner.Client().PrepareMessage(ctx, message, mail.Preparation{})
	if err != nil {
		t.Fatal(err)
	}
	prepared, ok := result.PreparedMessage()
	if !ok {
		t.Fatal("no public MIME")
	}
	parsed, err := stdmail.ReadMessage(bytes.NewReader(prepared.EMLCopy()))
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Header.Get("Reply-To") != "<reply@example.invalid>" || parsed.Header.Get("Disposition-Notification-To") == "" || parsed.Header.Get("References") != "<old@example.invalid>" {
		t.Fatalf("header mapping: %v", parsed.Header)
	}
	if parsed.Header.Get("Subject") != "=?windows-1252?Q?caf=E9?=" || !prepared.ContentCopy().SubjectPreformatted {
		t.Fatal("preformatted Subject mapping lost")
	}
	body, err := io.ReadAll(parsed.Body)
	if err != nil || !bytes.Contains(body, []byte("6Q==")) {
		t.Fatal("binary part bytes changed")
	}
	copied := prepared.EMLCopy()
	copied[0] = '!'
	if bytes.Equal(copied, prepared.EMLCopy()) {
		t.Fatal("wire alias")
	}
	if strings.Contains(fmt.Sprintf("%+v", prepared), "sender") || strings.Contains(fmt.Sprintf("%#v", settings), "consumer") {
		t.Fatal("unsafe normal formatting")
	}
	entry, _ := take(t, ctx, inbox)
	entry.Ack()
	var exported bytes.Buffer
	exportedResult, err := owner.Client().Export(ctx, prepared, &exported)
	if err != nil || exportedResult.BytesWritten() != int64(exported.Len()) || !bytes.Equal(exported.Bytes(), prepared.EMLCopy()) {
		t.Fatal("export mapping", err)
	}
	entry, _ = take(t, ctx, inbox)
	entry.Ack()
}
