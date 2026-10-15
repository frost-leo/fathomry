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

package mail

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/md5"
	"crypto/pbkdf2"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"net"
	"net/textproto"
	"strings"
	"sync/atomic"
	"testing"
	"unicode/utf16"

	"github.com/frost-leo/fathomry/internal/fault"
	smtp "github.com/wneessen/go-mail/smtp"
	"golang.org/x/crypto/md4"
)

func TestAllNativeAuthenticationFamiliesOnTLS(t *testing.T) {
	for _, mechanism := range []string{"cram-md5", "ntlm", "scram-sha-1", "scram-sha-256", "scram-sha-1-plus", "scram-sha-256-plus"} {
		for _, version := range []uint16{tls.VersionTLS12, tls.VersionTLS13} {
			t.Run(fmt.Sprintf("%s-%x", mechanism, version), func(t *testing.T) {
				var verified atomic.Bool
				peer := newPeer(t, peerOptions{auth: mechanism, authExchange: func(wire net.Conn, reader *textproto.Reader, argument string) bool {
					send := func(data []byte) bool {
						_, err := fmt.Fprintf(wire, "334 %s\r\n", base64.StdEncoding.EncodeToString(data))
						return err == nil
					}
					read := func() []byte {
						line, err := reader.ReadLine()
						if err != nil {
							return nil
						}
						data, err := base64.StdEncoding.DecodeString(line)
						if err != nil {
							return nil
						}
						return data
					}
					name, initial, _ := strings.Cut(argument, " ")
					if name != strings.ToUpper(mechanism) {
						return false
					}
					var valid bool
					switch mechanism {
					case "cram-md5":
						challenge := []byte("<fixture@example.invalid>")
						if !send(challenge) {
							return false
						}
						mac := hmac.New(md5.New, []byte("fixture-secret"))
						mac.Write(challenge)
						valid = string(read()) == "fixture-user "+hex.EncodeToString(mac.Sum(nil))
					case "ntlm":
						initialBytes, err := base64.StdEncoding.DecodeString(initial)
						if err != nil || len(initialBytes) < 12 || string(initialBytes[:8]) != "NTLMSSP\x00" || binary.LittleEndian.Uint32(initialBytes[8:12]) != 1 {
							return false
						}
						challenge := make([]byte, 52)
						copy(challenge, []byte("NTLMSSP\x00"))
						binary.LittleEndian.PutUint32(challenge[8:12], 2)
						binary.LittleEndian.PutUint32(challenge[16:20], 48)
						binary.LittleEndian.PutUint32(challenge[20:24], 0x00888201)
						copy(challenge[24:32], []byte("12345678"))
						binary.LittleEndian.PutUint16(challenge[40:42], 4)
						binary.LittleEndian.PutUint16(challenge[42:44], 4)
						binary.LittleEndian.PutUint32(challenge[44:48], 48)
						if !send(challenge) {
							return false
						}
						response := read()
						if len(response) < 64 || string(response[:8]) != "NTLMSSP\x00" || binary.LittleEndian.Uint32(response[8:12]) != 3 {
							return false
						}
						length := int(binary.LittleEndian.Uint16(response[20:22]))
						offset := int(binary.LittleEndian.Uint32(response[24:28]))
						if length < 16 || offset > len(response)-length {
							return false
						}
						ntResponse := response[offset : offset+length]
						utf := func(value string) []byte {
							words := utf16.Encode([]rune(value))
							data := make([]byte, 2*len(words))
							for i, word := range words {
								binary.LittleEndian.PutUint16(data[2*i:], word)
							}
							return data
						}
						digest := md4.New()
						digest.Write(utf("fixture-secret"))
						mac := hmac.New(md5.New, digest.Sum(nil))
						mac.Write(utf("FIXTURE-USERFIXTURE"))
						key := mac.Sum(nil)
						proof := hmac.New(md5.New, key)
						proof.Write(challenge[24:32])
						proof.Write(ntResponse[16:])
						valid = hmac.Equal(proof.Sum(nil), ntResponse[:16])
					default:
						var algorithm func() hash.Hash = sha1.New
						if strings.Contains(mechanism, "256") {
							algorithm = sha256.New
						}
						if !send(nil) {
							return false
						}
						first := string(read())
						prefix, bare, found := strings.Cut(first, ",,")
						if !found {
							return false
						}
						_, nonce, found := strings.Cut(bare, ",r=")
						if !found || nonce == "" {
							return false
						}
						salt := []byte("independent-salt")
						serverFirst := "r=" + nonce + "SERVER,s=" + base64.StdEncoding.EncodeToString(salt) + ",i=4096"
						if !send([]byte(serverFirst)) {
							return false
						}
						final := string(read())
						withoutProof, proofText, found := strings.Cut(final, ",p=")
						if !found {
							return false
						}
						channelField, _, _ := strings.Cut(withoutProof, ",")
						channel, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(channelField, "c="))
						if err != nil {
							return false
						}
						expectedChannel := []byte("n,,")
						if strings.HasSuffix(mechanism, "plus") {
							state := wire.(*tls.Conn).ConnectionState()
							binding := state.TLSUnique
							kind := "tls-unique"
							if state.Version >= tls.VersionTLS13 || binding == nil {
								kind = "tls-exporter"
								binding, err = state.ExportKeyingMaterial("EXPORTER-Channel-Binding", []byte{}, 32)
								if err != nil {
									return false
								}
							}
							if prefix != "p="+kind {
								return false
							}
							expectedChannel = append([]byte(prefix+",,"), binding...)
						}
						if !bytes.Equal(channel, expectedChannel) {
							return false
						}
						authMessage := bare + "," + serverFirst + "," + withoutProof
						salted, err := pbkdf2.Key(algorithm, "fixture-secret", salt, 4096, algorithm().Size())
						if err != nil {
							return false
						}
						mac := func(key, data []byte) []byte {
							digest := hmac.New(algorithm, key)
							digest.Write(data)
							return digest.Sum(nil)
						}
						clientKey := mac(salted, []byte("Client Key"))
						stored := algorithm()
						stored.Write(clientKey)
						signature := mac(stored.Sum(nil), []byte(authMessage))
						expected := make([]byte, len(signature))
						for i := range signature {
							expected[i] = clientKey[i] ^ signature[i]
						}
						actual, err := base64.StdEncoding.DecodeString(proofText)
						if err != nil || !hmac.Equal(actual, expected) {
							return false
						}
						serverKey := mac(salted, []byte("Server Key"))
						serverProof := mac(serverKey, []byte(authMessage))
						if !send([]byte("v=" + base64.StdEncoding.EncodeToString(serverProof))) {
							return false
						}
						valid = len(read()) == 0
					}
					verified.Store(valid)
					return valid
				}})
				value := testOptions(peer)
				value.Auth = mechanism
				value.Username = "fixture-user"
				value.Password = "fixture-secret"
				value.AuthDomain = "FIXTURE"
				config, _ := defaults(value).tls()
				config.MinVersion, config.MaxVersion = version, version
				value.Native.TLS = config
				bound := bindTest(t, value, 1)
				out := sendTest(t, bound.client, "auth", plainMessage(t, "auth"))
				if out.Err() != nil || !verified.Load() {
					t.Fatalf("independent auth failed: %v verified=%v", out.Err(), verified.Load())
				}
				if out.Outcome.Value.SessionInfo().TLSVersion != version {
					t.Fatal("wrong negotiated TLS")
				}
				drain(t, bound.inbox)
			})
		}
	}
}

type forbiddenAuth struct{ calls int }

func (value *forbiddenAuth) Start(*smtp.ServerInfo) (string, []byte, error) {
	return "SCRAM-SHA-256", nil, nil
}
func (value *forbiddenAuth) Next([]byte, bool) ([]byte, error) { value.calls++; return nil, nil }
func TestSCRAMRejectsBeforeExpensiveNativeWork(t *testing.T) {
	for _, challenge := range []string{"r=nonceX,s=c2FsdA==,i=100001", "r=nonceX,s=c2FsdA==,i=0", "r=nonceX,s=!,i=4096", "r=nonce,s=c2FsdA==,i=4096", "r=nonceX,s=c2FsdA==,i=4096,i=2", strings.Repeat("x", 2049)} {
		native := &forbiddenAuth{}
		guard := &boundedAuth{ctx: context.Background(), native: native, limit: 2048, iterations: 100000, mechanism: "SCRAM-SHA-256", nonce: "nonce", scramStep: 1}
		if _, err := guard.Next([]byte(challenge), true); err == nil || native.calls != 0 {
			t.Fatal("unbounded native authentication entered")
		}
	}
	native := &forbiddenAuth{}
	guard := &boundedAuth{ctx: context.Background(), native: native, limit: 2048, iterations: 100000, mechanism: "SCRAM-SHA-256", nonce: "nonce", scramStep: 1}
	if _, err := guard.Next([]byte("r=nonceX,s=c2FsdA==,i=4096"), true); err != nil || native.calls != 1 {
		t.Fatal("positive challenge control", err)
	}
}
func TestOfflineHooksAndExplicitAuthSelection(t *testing.T) {
	for _, mode := range []string{"auto", "preferred", "custom"} {
		t.Run(mode, func(t *testing.T) {
			peer := newPeer(t, peerOptions{auth: "plain"})
			value := testOptions(peer)
			value.Auth = mode
			var called atomic.Int32
			if mode == "custom" {
				value.Native.Auth = func(ctx context.Context, state tls.ConnectionState) (smtp.Auth, error) {
					called.Add(1)
					if ctx.Err() != nil || !state.HandshakeComplete {
						return nil, errors.New("invalid factory lifetime")
					}
					return smtp.PlainAuth("", "fixture-user", "fixture-secret", value.Host, false), nil
				}
			} else {
				value.Username = "fixture-user"
				value.Password = "fixture-secret"
			}
			if mode == "preferred" {
				value.AuthPreferences = []string{"scram-sha-256", "plain"}
			}
			prepared, err := PrepareV1(value)
			if err != nil {
				t.Fatal(err)
			}
			if called.Load() != 0 || prepared.Metadata().WorkBytes < prepared.Metadata().EvidenceBytes {
				t.Fatal("preparation invoked hook or omitted bytes")
			}
			bound := bindTest(t, value, 1)
			out := sendTest(t, bound.client, "selection", plainMessage(t, "selection"))
			if out.Err() != nil {
				t.Fatal(out.Err())
			}
			if mode == "custom" && called.Load() != 1 {
				t.Fatal("factory count")
			}
			drain(t, bound.inbox)
		})
	}
	peer := newPeer(t, peerOptions{auth: "plain"})
	value := testOptions(peer)
	value.Auth = "preferred"
	value.Username = "fixture-user"
	value.Password = "fixture-secret"
	value.AuthPreferences = []string{"scram-sha-256"}
	bound := bindTest(t, value, 1)
	out := sendTest(t, bound.client, "no-fallback", plainMessage(t, "no-fallback"))
	if !errors.Is(out.Err(), ErrUnsupported) {
		t.Fatal("silent downgrade")
	}
	drain(t, bound.inbox)
}
func TestDSNAndSessionInspection(t *testing.T) {
	peer := newPeer(t, peerOptions{extensions: []string{"DSN", "SMTPUTF8", "8BITMIME"}})
	value := testOptions(peer)
	value.DSNReturn = "HDRS"
	value.DSNNotify = []string{"FAILURE", "DELAY"}
	bound := bindTest(t, value, 2)
	receipt, err := bound.client.Session(context.Background(), fault.Correlation{Call: "session"}, SessionRequest{Extensions: []string{"DSN", "MISSING"}, Noop: true, Reset: true, Verify: "user@example.invalid"})
	result := resolved(t, receipt, err)
	if result.Err() != nil {
		t.Fatal(result.Err())
	}
	extensions := result.Outcome.Value.SessionInfo().Extensions
	if len(extensions) != 2 || !extensions[0].Supported || extensions[1].Supported {
		t.Fatal("extensions not observed")
	}
	if out := sendTest(t, bound.client, "dsn", plainMessage(t, "dsn")); out.Err() != nil {
		t.Fatal(out.Err())
	}
	captured, _ := peer.snapshot()
	if len(captured) != 1 || !strings.HasSuffix(captured[0].recipients[0], " NOTIFY=FAILURE,DELAY") {
		t.Fatal("DSN RCPT request absent")
	}
	drain(t, bound.inbox)
}
