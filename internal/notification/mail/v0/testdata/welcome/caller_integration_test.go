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

package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"io"
	"math/big"
	"mime/quotedprintable"
	"net"
	stdmail "net/mail"
	"net/textproto"
	"strconv"
	"strings"
	"testing"
	"time"

	mail "github.com/frost-leo/fathomry/internal/notification/mail/v0"
)

func TestSubmitThroughPublicFacade(t *testing.T) {
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	certificate := &x509.Certificate{SerialNumber: big.NewInt(1), NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, certificate, certificate, public, private)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	accepted := make(chan net.Conn, 1)
	captured := make(chan []byte, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		accepted <- conn
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
		secure := tls.Server(conn, &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: private}}})
		protocol := textproto.NewConn(secure)
		if protocol.PrintfLine("220 caller fixture") != nil {
			return
		}
		for {
			line, err := protocol.ReadLine()
			if err != nil {
				return
			}
			switch {
			case strings.HasPrefix(line, "EHLO"), strings.HasPrefix(line, "MAIL"), strings.HasPrefix(line, "RCPT"), line == "RSET":
				if protocol.PrintfLine("250 ok") != nil {
					return
				}
			case line == "DATA":
				if protocol.PrintfLine("354 proceed") != nil {
					return
				}
				data, err := io.ReadAll(io.LimitReader(protocol.DotReader(), 1<<20))
				if err != nil {
					return
				}
				captured <- data
				if protocol.PrintfLine("250 2.0.0 accepted") != nil {
					return
				}
			case line == "QUIT":
				_ = protocol.PrintfLine("221 bye")
				return
			default:
				_ = protocol.PrintfLine("500 unsupported")
			}
		}
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		select {
		case conn := <-accepted:
			_ = conn.Close()
		default:
		}
		<-done
	})
	host, port, _ := net.SplitHostPort(listener.Addr().String())
	number, _ := strconv.Atoi(port)
	options := mail.OptionsV1{Name: "caller-fixture", Host: host, Port: number, Hello: "caller.fixture.test", TLSMode: "implicit", Auth: "none",
		RootCAPEM: string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})), MaxActive: 1, MaxMessages: 1, MaxRecipients: 1, Timeout: time.Second}
	message, err := mail.NewMessage(mail.Content{ID: "caller@fixture.test", From: "from@fixture.test", To: []string{"to@fixture.test"},
		Subject: "Public facade", Text: "Caller-controlled body."})
	if err != nil {
		t.Fatal(err)
	}
	summary, err := submit(context.Background(), options, message)
	if err != nil || !summary.Observed || summary.Effect != "accepted" || summary.Code != 250 || summary.PrimaryFailure || summary.CleanupFailure {
		t.Fatal("public caller submission failed")
	}
	select {
	case data := <-captured:
		parsed, err := stdmail.ReadMessage(bufio.NewReader(bytes.NewReader(data)))
		if err != nil || parsed.Header.Get("Message-ID") != "<caller@fixture.test>" {
			t.Fatal("independent message identity changed")
		}
		body, err := io.ReadAll(quotedprintable.NewReader(parsed.Body))
		if err != nil || strings.TrimSpace(string(body)) != "Caller-controlled body." {
			t.Fatal("independent body capture changed")
		}
	case <-time.After(time.Second):
		t.Fatal("accepted message was not observed by the receiver")
	}
}
