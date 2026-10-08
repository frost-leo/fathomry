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

package httpcloak

import (
	"crypto/ecdh"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"errors"
	"io"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	dnswire "github.com/miekg/dns"
	officialquic "github.com/quic-go/quic-go"
	officialh3 "github.com/quic-go/quic-go/http3"
	"github.com/sardanioss/httpcloak/transport"
)

func protocolCertificate(t *testing.T, names ...string) (tls.Certificate, *x509.CertPool) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	certificate := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "local protocol fixture"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), DNSNames: names, IPAddresses: []net.IP{net.IPv4(127, 0, 0, 1)}, KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, IsCA: true, BasicConstraintsValid: true}
	der, err := x509.CreateCertificate(rand.Reader, certificate, certificate, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(parsed)
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: parsed}, roots
}

func protocolH3Peer(t *testing.T, config *tls.Config, handler http.Handler) string {
	t.Helper()
	return protocolH3PeerConfig(t, config, nil, handler)
}

func protocolH3PeerConfig(t *testing.T, config *tls.Config, quicConfig *officialquic.Config, handler http.Handler) string {
	t.Helper()
	packet, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := &officialh3.Server{TLSConfig: config, QUICConfig: quicConfig, Handler: handler, EnableDatagrams: true, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	done := make(chan struct{})
	go func() { defer close(done); _ = server.Serve(packet) }()
	t.Cleanup(func() {
		_ = server.Close()
		_ = packet.Close()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("H3 protocol peer retained serve loop")
		}
	})
	return "https://" + packet.LocalAddr().String()
}

func protocolECHKey(t *testing.T) (tls.EncryptedClientHelloKey, []byte) {
	t.Helper()
	key, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	contents := []byte{7, 0, 0x20}
	contents = binary.BigEndian.AppendUint16(contents, uint16(len(key.PublicKey().Bytes())))
	contents = append(contents, key.PublicKey().Bytes()...)
	contents = append(contents, 0, 4, 0, 1, 0, 1, 0)
	const publicName = "public.invalid"
	contents = append(contents, byte(len(publicName)))
	contents = append(contents, publicName...)
	contents = append(contents, 0, 0)
	config := []byte{0xfe, 0x0d}
	config = binary.BigEndian.AppendUint16(config, uint16(len(contents)))
	config = append(config, contents...)
	list := binary.BigEndian.AppendUint16(nil, uint16(len(config)))
	list = append(list, config...)
	return tls.EncryptedClientHelloKey{Config: config, PrivateKey: key.Bytes(), SendAsRetry: true}, list
}

func TestECHProtocolDisabledExplicitDiscoveryAndFallback(t *testing.T) {
	for _, protocol := range []ProtocolMode{HTTP2, HTTP3} {
		modes := []string{"disabled", "explicit", "discovery", "fallback", "unconfigured-target"}
		for _, mode := range modes {
			t.Run(string(protocol)+"/"+mode, func(t *testing.T) {
				certificate, roots := protocolCertificate(t, "target.invalid", "public.invalid")
				key, list := protocolECHKey(t)
				serverConfig := &tls.Config{Certificates: []tls.Certificate{certificate}, MinVersion: tls.VersionTLS13, EncryptedClientHelloKeys: []tls.EncryptedClientHelloKey{key}}
				handler := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
					_, _ = io.WriteString(writer, strconv.FormatBool(request.TLS.ECHAccepted))
				})
				var address string
				if protocol == HTTP3 {
					address = protocolH3Peer(t, serverConfig, handler)
				} else {
					server := httptest.NewUnstartedServer(handler)
					server.EnableHTTP2, server.TLS = true, serverConfig
					server.StartTLS()
					t.Cleanup(server.Close)
					address = server.URL
				}
				parsed, _ := url.Parse(address)
				parsed.Host = net.JoinHostPort("target.invalid", parsed.Port())
				var queries, verified atomic.Int32
				var observed atomic.Bool
				options := OptionsV1{Name: "ech-wire", PresetName: "chrome-148", Protocol: protocol, Native: NativeOptionsV1{Transport: &transport.TransportConfig{ConnectTo: map[string]string{"target.invalid": "127.0.0.1"}}, Verify: &transport.TLSVerify{RootCAs: roots, VerifyConnection: func(state tls.ConnectionState) error { verified.Add(1); observed.Store(state.ECHAccepted); return nil }}}}
				switch mode {
				case "disabled", "discovery", "unconfigured-target":
					options.ResolverAddress = dnsPeer(t, func(writer dnswire.ResponseWriter, request *dnswire.Msg) {
						queries.Add(1)
						wantName := "ech.invalid."
						if mode == "unconfigured-target" {
							wantName = "target.invalid."
						}
						if len(request.Question) != 1 || request.Question[0].Qtype != dnswire.TypeHTTPS || request.Question[0].Name != wantName {
							t.Error("ECH discovery used undeclared name or record authority", request.Question)
						}
						echDNSAnswer(writer, request, list)
					})
					if mode != "unconfigured-target" {
						options.Native.Transport.ECHConfigDomain = "ech.invalid"
					}
					options.DisableECH = mode == "disabled"
				case "explicit":
					options.Native.Transport.ECHConfig = list
				case "fallback":
					_, options.Native.Transport.ECHConfig = protocolECHKey(t)
				}
				fixture := bindFixture(t, options, 1)
				receipt, err := fixture.client.Do(testContext(t), testContext(t), fault.Correlation{Call: "ech-wire"}, request(t, "GET", parsed.String(), nil))
				if err != nil {
					t.Fatal("ECH protocol request", err)
				}
				result := settle(t, fixture, receipt)
				discovered := mode == "discovery" || protocol == HTTP3 && mode == "unconfigured-target"
				expected := mode == "explicit" || discovered
				if !result.Outcome.Value.Complete() || string(result.Outcome.Value.DataCopy()) != strconv.FormatBool(expected) || verified.Load() == 0 || observed.Load() != expected {
					t.Fatal("configured ECH did not match independent handshake observations", mode, string(result.Outcome.Value.DataCopy()), verified.Load(), observed.Load())
				}
				if discovered && queries.Load() != 1 || !discovered && queries.Load() != 0 {
					t.Fatal("unexpected discovery authority/effect", mode, queries.Load())
				}
			})
		}
	}
}

func TestECHNativeProtocolApplicabilityWithoutAmbientResolver(t *testing.T) {
	for _, protocol := range []ProtocolMode{HTTP1, HTTP3} {
		t.Run(string(protocol), func(t *testing.T) {
			address, options := peer(t, protocol, func(writer http.ResponseWriter, input *http.Request) {
				_, _ = io.WriteString(writer, strconv.FormatBool(input.TLS.ECHAccepted))
			})
			options.DisableECH = false
			fixture := bindFixture(t, options, 1)
			receipt, err := fixture.client.Do(testContext(t), testContext(t), fault.Correlation{Call: "literal"}, request(t, "GET", address, nil))
			if err != nil {
				t.Fatal("literal target required unapproved resolver authority", err)
			}
			if result := settle(t, fixture, receipt); !result.Outcome.Value.Complete() || string(result.Outcome.Value.DataCopy()) != "false" {
				t.Fatal("literal target invented ECH acceptance")
			}
			if protocol == HTTP1 {
				for _, config := range []*transport.TransportConfig{{ECHConfig: []byte{0}}, {ECHConfigDomain: "ech.invalid"}} {
					options.Native.Transport = config
					if _, err := PrepareV1(options); !errors.Is(err, ErrUnsupported) {
						t.Fatal("H1 accepted an ineffective configured ECH option", err)
					}
				}
			}
		})
	}
}
