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

package redis

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"io"
	"math/big"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	sdk "github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/maintnotifications"
)

func TestFrameValidation(t *testing.T) {
	for _, wire := range []string{"+OK\r\n", "$0\r\n\r\n", "$-1\r\n", "%1\r\n+k\r\n*2\r\n:0\r\n_\r\n", "#t\r\n", "=5\r\ntxt:x\r\n", ">2\r\n+invalidate\r\n_\r\n", "(123456789012345678901234567890\r\n"} {
		got, err := readFrame(bufio.NewReader(strings.NewReader(wire)), 1024, 16)
		if err != nil || string(got) != wire {
			t.Fatal("valid RESP frame rejected")
		}
	}
	for _, wire := range []string{"(-\r\n", "$99999999999\r\n", "*999999999\r\n", "%9223372036854775807\r\n", "$-2\r\n", "$2\r\nabxx", "#x\r\n", "_x\r\n", "=3\r\nabc\r\n", strings.Repeat("*1\r\n", 40) + ":1\r\n"} {
		if _, err := readFrame(bufio.NewReader(strings.NewReader(wire)), 1024, 64); err == nil {
			t.Fatal("malformed/oversized RESP accepted")
		}
	}
}

func FuzzFrame(f *testing.F) {
	for _, seed := range []string{"+OK\r\n", "$-1\r\n", "*2\r\n:1\r\n:2\r\n", "%9223372036854775807\r\n",
		"|0\r\n:1\r\n", "*1\r\n|0\r\n:1\r\n", "|1\r\n+meta\r\n+value\r\n|0\r\n:1\r\n", "|1\r\n|0\r\n+k\r\n+v\r\n:1\r\n"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, input string) {
		if len(input) > 8192 {
			return
		}
		output, err := readFrame(bufio.NewReader(strings.NewReader(input)), 4096, 128)
		if len(output) > 4096 {
			t.Fatal("frame bound exceeded")
		}
		if err == nil && (len(output) == 0 || len(output) > len(input)) {
			t.Fatal("invalid accepted frame")
		}
	})
}

func TestVerifiedTLSAndMutualTLS(t *testing.T) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "redis.test"}, DNSNames: []string{"redis.test"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), BasicConstraintsValid: true, IsCA: true,
		KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	certificate, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(certPEM)
	listener, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{certificate}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: roots})
	if err != nil {
		t.Fatal(err)
	}
	address := servePeer(t, listener, func([]string) string { return "+PONG\r\n" })
	options := testOptions(address)
	options.Plaintext = false
	options.RootCAPEM = string(certPEM)
	options.ServerName = "redis.test"
	options.MaintenanceMode = "enabled"
	options.CertificatePEM = string(certPEM)
	options.PrivateKeyPEM = string(keyPEM)
	client, _, inbox, _ := bindTest(t, options, 2)
	if endpoint := client.owner.native.(*sdk.Client).Options().MaintNotificationsConfig.EndpointType; endpoint != maintnotifications.EndpointTypeInternalFQDN {
		t.Fatal("TLS maintenance identity was downgraded to an IP endpoint")
	}
	got := executeTest(t, client, "tls", "PING")
	if got.Err() != nil {
		t.Fatal(got.Err())
	}
	drain(t, inbox)
	options.ServerName = "wrong.test"
	rejected, _, evidence, _ := bindTest(t, options, 2)
	got = executeTest(t, rejected, "bad-name", "PING")
	var validation *tls.CertificateVerificationError
	if !errors.As(got.Err(), &validation) {
		t.Fatal("untrusted peer name was not rejected with original TLS cause")
	}
	drain(t, evidence)
	if rejected.Stats().Sockets != 0 {
		t.Fatal("failed TLS initialization retained a socket")
	}
}

func TestAttributesBoundOneLogicalReply(t *testing.T) {
	for _, test := range []struct {
		name     string
		wire     string
		elements int
	}{
		{"leading", "|0\r\n:1\r\n", 2},
		{"nested", "*1\r\n|0\r\n:1\r\n", 3},
		{"chained", "|1\r\n+meta\r\n+value\r\n|0\r\n:1\r\n", 5},
	} {
		t.Run(test.name, func(t *testing.T) {
			const next = ":2\r\n"
			reader := bufio.NewReader(strings.NewReader(test.wire + next))
			got, err := readFrame(reader, len(test.wire), test.elements)
			if err != nil || string(got) != test.wire {
				t.Fatal("attribute detached from its decorated reply at the exact bounds")
			}
			got, err = readFrame(reader, len(next), 1)
			if err != nil || string(got) != next {
				t.Fatal("logical frame consumed bytes from the next reply")
			}
			if _, err := readFrame(bufio.NewReader(strings.NewReader(test.wire)), len(test.wire)-1, test.elements); !errors.Is(err, ErrLimit) {
				t.Fatal("attributes and decorated reply escaped the combined byte bound")
			}
			if _, err := readFrame(bufio.NewReader(strings.NewReader(test.wire)), len(test.wire), test.elements-1); !errors.Is(err, ErrLimit) {
				t.Fatal("attributes and decorated reply escaped the combined element bound")
			}
		})
	}
	for _, wire := range []string{strings.Repeat("*1\r\n|0\r\n", 40) + ":1\r\n", strings.Repeat("|0\r\n", 40) + ":1\r\n"} {
		if _, err := readFrame(bufio.NewReader(strings.NewReader(wire)), 4096, 256); !errors.Is(err, ErrLimit) {
			t.Fatal("native recursion escaped frame limits")
		}
	}
	for _, wire := range []string{"|1\r\n|0\r\n+k\r\n+v\r\n:1\r\n", "|1\r\n+k\r\n*1\r\n|0\r\n+v\r\n:1\r\n"} {
		if _, err := readFrame(bufio.NewReader(strings.NewReader(wire)), 1024, 32); !errors.Is(err, ErrProtocol) {
			t.Fatal("ambiguous nested metadata admitted")
		}
	}
	for _, wire := range []string{"|0\r\n", "|1\r\n+k\r\n+v\r\n", "*1\r\n|0\r\n"} {
		if _, err := readFrame(bufio.NewReader(strings.NewReader(wire)), 1024, 32); !errors.Is(err, io.EOF) {
			t.Fatal("attributes without a decorated value accepted as a complete reply")
		}
	}
}
func TestConsumedFrameDoesNotRemainInIdleSocket(t *testing.T) {
	owner := newTransport(defaults(testOptions("127.0.0.1:1")))
	defer owner.stop()
	payload := strings.Repeat("x", 16384)
	wire := "$16384\r\n" + payload + "\r\n"
	conn := &wireConn{owner: owner, reader: bufio.NewReader(strings.NewReader(wire))}
	output := make([]byte, len(wire))
	count, err := conn.Read(output)
	if err != nil || count != len(wire) || conn.pending != nil {
		t.Fatal("consumed frame still retained by idle connection")
	}
}

func TestNativeAttributeDecoderAgreement(t *testing.T) {
	integer := Value{kind: Integer, integer: 1}
	for _, test := range []struct {
		name, frame, rawRESP3 string
		value                 Value
	}{
		{"bare", ":1\r\n", ":1\r\n", integer},
		{"leading", "|0\r\n:1\r\n", ":1\r\n", integer},
		{"nested-array", "*1\r\n|0\r\n:1\r\n", "*1\r\n|0\r\n:1\r\n", Value{kind: Array, elements: []Value{integer}}},
		{"chained", "|1\r\n+metadata\r\n+value\r\n|0\r\n:1\r\n", ":1\r\n", integer},
		{"nested-map", "%1\r\n+k\r\n|0\r\n:1\r\n", "%1\r\n+k\r\n|0\r\n:1\r\n", Value{kind: Map, pairs: []Pair{{key: Value{kind: Text, text: "k"}, value: integer}}}},
		{"leading-and-nested", "|0\r\n*1\r\n|0\r\n:1\r\n", "*1\r\n|0\r\n:1\r\n", Value{kind: Array, elements: []Value{integer}}},
	} {
		for _, protocol := range []int{2, 3} {
			t.Run(test.name+"/resp"+strconv.Itoa(protocol), func(t *testing.T) {
				address := peer(t, func(args []string) string {
					if strings.EqualFold(args[0], "PING") {
						return "+NEXT\r\n"
					}
					return test.frame
				})
				options := testOptions(address)
				// Protocol 2 disables the RESP3 push pre-read in this synthetic
				// peer, isolating RawCmd's parser, not certifying RESP2 attributes.
				options.Protocol = protocol
				client, _, inbox, _ := bindTest(t, options, 4)
				result := executeTest(t, client, "decoded", "GET", "owned")
				if result.Err() != nil {
					t.Fatal("native generic decoder rejected accepted logical frame")
				}
				replies := result.Outcome.Value.Commands()
				if len(replies) != 1 || replies[0].State() != Replied || !reflect.DeepEqual(replies[0].Value(), test.value) {
					t.Fatal("native generic decoder changed the decorated value")
				}
				drain(t, inbox)
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				raw := sdk.NewRawCmd(ctx, "GET", "owned")
				if err := client.owner.native.Process(ctx, raw); err != nil {
					t.Fatal("native raw decoder rejected accepted logical frame")
				}
				want := test.frame
				if protocol == 3 {
					// PeekReplyType discards leading attributes before RawCmd reads;
					// ReadRawReply still preserves attributes inside aggregates.
					want = test.rawRESP3
				}
				if actual, err := raw.Result(); err != nil || string(actual) != want {
					t.Fatal("native raw decoder disagrees with the selected pre-read path")
				}
				pipe := client.owner.native.Pipeline()
				raw = sdk.NewRawCmd(ctx, "GET", "owned")
				next := sdk.NewRawCmd(ctx, "PING")
				for _, command := range []*sdk.RawCmd{raw, next} {
					if err := pipe.Process(ctx, command); err != nil {
						t.Fatal("native oracle pipeline setup failed")
					}
				}
				if _, err := pipe.Exec(ctx); err != nil {
					t.Fatal("native oracle pipeline failed")
				}
				if string(raw.Val()) != want || string(next.Val()) != "+NEXT\r\n" {
					t.Fatal("native raw decoder crossed a logical reply boundary")
				}
			})
		}
	}
}
