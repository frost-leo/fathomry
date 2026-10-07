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

package tlsclient

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	stdtls "crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"fmt"
	"io"
	"log"
	"maps"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	fhttp "github.com/bogdanfinn/fhttp"
	sdk "github.com/bogdanfinn/tls-client"
	"github.com/bogdanfinn/tls-client/profiles"
	utls "github.com/bogdanfinn/utls"
	"github.com/frost-leo/fathomry/adapters/v1"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
)

func wireOwner(t *testing.T, settings Settings, native NativeOptions) (*Owner, Dependencies) {
	t.Helper()
	prepared, err := Prepare(settings, native)
	if err != nil {
		t.Fatal(err)
	}
	dependencies := testMechanisms(t, prepared)
	owner, err := prepared.Open(testContext(t), dependencies)
	if owner != nil {
		t.Cleanup(func() {
			if err := owner.Close(testContext(t)); err != nil {
				t.Error("public source cleanup", err)
				return
			}
			for {
				status, err := dependencies.Evidence.Inspect()
				if err != nil {
					t.Error(err)
					return
				}
				if status.Outstanding == 0 {
					break
				}
				delivery, err := dependencies.Evidence.NextReleased(testContext(t))
				if err != nil {
					t.Error(err)
					return
				}
				receipt, err := delivery.Receipt()
				if err != nil {
					t.Error(err)
					return
				}
				snapshot, err := receipt.WaitReleased(testContext(t))
				if err != nil || snapshot.Err() != nil || !strings.HasSuffix(snapshot.Info().Operation, ".open") {
					t.Error("unhandled source/operation evidence during cleanup", err, snapshot.Err())
				}
				if err := delivery.Ack(); err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	if err != nil || owner == nil {
		t.Fatal("public source construction", err)
	}
	return owner, dependencies
}

func wireReceive(t *testing.T, dependencies Dependencies, receipt *adapters.Receipt[Result]) (Result, error) {
	t.Helper()
	if receipt == nil {
		return Result{}, errors.New("missing public receipt")
	}
	direct, err := receipt.WaitReleased(testContext(t))
	if err != nil {
		t.Fatal(err)
	}
	delivery, err := dependencies.Evidence.NextReleased(testContext(t))
	if err != nil {
		t.Fatal(err)
	}
	independentReceipt, err := delivery.Receipt()
	if err != nil {
		t.Fatal(err)
	}
	independent, err := independentReceipt.WaitReleased(testContext(t))
	if err != nil || independent.Info() != direct.Info() {
		t.Fatal("public independent evidence attribution changed", err)
	}
	if err := delivery.Ack(); err != nil {
		t.Fatal(err)
	}
	value, _ := direct.ValueCopy()
	return value, direct.Err()
}

func wireCertificate(t *testing.T, parent *x509.Certificate, parentKey *ecdsa.PrivateKey, name string, client bool) (stdtls.Certificate, *x509.Certificate, *ecdsa.PrivateKey) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: serial, Subject: pkix.Name{CommonName: name},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true}
	if parent == nil {
		template.IsCA = true
		template.KeyUsage |= x509.KeyUsageCertSign
		parent, parentKey = template, key
	} else if client {
		template.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	} else {
		template.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
		template.IPAddresses = []net.IP{net.IPv4(127, 0, 0, 1)}
	}
	der, err := x509.CreateCertificate(rand.Reader, template, parent, &key.PublicKey, parentKey)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return stdtls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: parsed}, parsed, key
}

func TestPublicMTLSActualPeerIdentityAndRefusal(t *testing.T) {
	_, authority, signing := wireCertificate(t, nil, nil, "fixture-client-authority", false)
	valid, _, _ := wireCertificate(t, authority, signing, "fixture-client", true)
	_, foreign, foreignKey := wireCertificate(t, nil, nil, "untrusted-client-authority", false)
	wrong, _, _ := wireCertificate(t, foreign, foreignKey, "wrong-client", true)
	for _, variant := range []string{"valid", "missing", "wrong-issuer", "untrusted-server"} {
		t.Run(variant, func(t *testing.T) {
			var requests atomic.Int64
			peer := httptest.NewUnstartedServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				requests.Add(1)
				if request.TLS == nil || len(request.TLS.VerifiedChains) == 0 || len(request.TLS.PeerCertificates) == 0 || request.TLS.PeerCertificates[0].Subject.CommonName != "fixture-client" {
					t.Error("mTLS peer did not verify the intended client identity")
				}
				_, _ = io.WriteString(writer, "verified-client")
			}))
			clientRoots := x509.NewCertPool()
			clientRoots.AddCert(authority)
			peer.TLS = &stdtls.Config{ClientAuth: stdtls.RequireAndVerifyClientCert, ClientCAs: clientRoots, NextProtos: []string{"http/1.1"}}
			peer.Config.ErrorLog = log.New(io.Discard, "", 0)
			peer.StartTLS()
			defer peer.Close()
			serverRoots := x509.NewCertPool()
			if variant != "untrusted-server" {
				serverRoots.AddCert(peer.Certificate())
			}
			transport := &sdk.TransportOptions{RootCAs: serverRoots}
			if variant != "missing" {
				selected := valid
				if variant == "wrong-issuer" {
					selected = wrong
				}
				transport.Certificates = []utls.Certificate{{Certificate: selected.Certificate, PrivateKey: selected.PrivateKey}}
			}
			native := testNative()
			native.Transport = transport
			owner, dependencies := wireOwner(t, Settings{Name: "mtls-" + variant}, native)
			request, _ := fhttp.NewRequest("GET", peer.URL, nil)
			receipt, direct := owner.Client().Do(testContext(t), testContext(t), request)
			value, terminal := wireReceive(t, dependencies, receipt)
			if variant == "valid" {
				if direct != nil || terminal != nil || !value.Complete() || string(value.DataCopy()) != "verified-client" || requests.Load() != 1 {
					t.Fatal("valid mTLS control failed", direct, terminal)
				}
			} else if direct == nil && terminal == nil || value.Complete() || requests.Load() != 0 {
				t.Fatal("untrusted or absent identity reached application HTTP", direct, terminal)
			}
		})
	}
}

type wireKeyLog struct {
	mu      sync.Mutex
	labels  map[string]int
	entered chan struct{}
	resume  chan struct{}
	once    sync.Once
}

func (writer *wireKeyLog) Write(input []byte) (int, error) {
	if writer.entered != nil {
		writer.once.Do(func() { close(writer.entered) })
		<-writer.resume
	}
	writer.mu.Lock()
	defer writer.mu.Unlock()
	for _, line := range strings.Split(string(input), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 3 {
			writer.labels[strings.Clone(fields[0])]++
		}
	}
	return len(input), nil
}

func (writer *wireKeyLog) count() int {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	return len(writer.labels)
}

func TestPublicKeyLogActualTLSAndCancellationOwnership(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprintf("enabled-%t", enabled), func(t *testing.T) {
			peer := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(writer, "tls") }))
			defer peer.Close()
			roots := x509.NewCertPool()
			roots.AddCert(peer.Certificate())
			log := &wireKeyLog{labels: make(map[string]int)}
			native := testNative()
			native.Transport = &sdk.TransportOptions{RootCAs: roots}
			if enabled {
				native.Transport.KeyLogWriter = log
			}
			owner, dependencies := wireOwner(t, Settings{Name: "keylog"}, native)
			request, _ := fhttp.NewRequest("GET", peer.URL, nil)
			receipt, err := owner.Client().Do(testContext(t), testContext(t), request)
			value, terminal := wireReceive(t, dependencies, receipt)
			if err != nil || terminal != nil || !value.Complete() || enabled && log.count() < 2 || !enabled && log.count() != 0 {
				t.Fatal("actual TLS key-log effect differs", err, terminal, log.count())
			}
		})
	}
	t.Run("blocked-writer", func(t *testing.T) {
		var requests atomic.Int64
		peer := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) { requests.Add(1) }))
		defer peer.Close()
		roots := x509.NewCertPool()
		roots.AddCert(peer.Certificate())
		writer := &wireKeyLog{labels: make(map[string]int), entered: make(chan struct{}), resume: make(chan struct{})}
		resume := sync.OnceFunc(func() { close(writer.resume) })
		defer resume()
		native := testNative()
		native.Transport = &sdk.TransportOptions{RootCAs: roots, KeyLogWriter: writer}
		owner, dependencies := wireOwner(t, Settings{Name: "blocked-keylog"}, native)
		request, _ := fhttp.NewRequest("GET", peer.URL, nil)
		ctx, cancel := context.WithCancel(testContext(t))
		defer cancel()
		done := make(chan *adapters.Receipt[Result], 1)
		go func() { receipt, _ := owner.Client().Do(ctx, context.Background(), request); done <- receipt }()
		select {
		case <-writer.entered:
		case <-testContext(t).Done():
			t.Fatal("native TLS key-log callback did not run")
		}
		cancel()
		var receipt *adapters.Receipt[Result]
		select {
		case receipt = <-done:
		case <-testContext(t).Done():
			t.Fatal("canceled public TLS waiter did not return")
		}
		short, stop := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer stop()
		if _, err := receipt.WaitReleased(short); !errors.Is(err, context.DeadlineExceeded) || owner.ShutdownComplete() {
			t.Fatal("blocked native writer lost ownership", err)
		}
		resume()
		value, terminal := wireReceive(t, dependencies, receipt)
		if !errors.Is(terminal, context.Canceled) || value.Complete() || requests.Load() != 0 {
			t.Fatal("canceled TLS writer produced a complete HTTP result", terminal)
		}
	})
}

func TestPublicNativeSessionResumptionAndDisabledControl(t *testing.T) {
	for _, disabled := range []bool{false, true} {
		t.Run(fmt.Sprintf("disabled-%t", disabled), func(t *testing.T) {
			type handshake struct {
				resumed bool
				remote  string
			}
			observed := make(chan handshake, 2)
			peer := httptest.NewUnstartedServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				observed <- handshake{request.TLS.DidResume, request.RemoteAddr}
				_, _ = io.WriteString(writer, "resumption")
			}))
			peer.TLS = &stdtls.Config{MinVersion: stdtls.VersionTLS13, MaxVersion: stdtls.VersionTLS13, NextProtos: []string{"http/1.1"}}
			peer.StartTLS()
			defer peer.Close()
			roots := x509.NewCertPool()
			roots.AddCert(peer.Certificate())
			profile := profiles.Chrome_144_PSK
			native := NativeOptions{Profile: &profile, Transport: &sdk.TransportOptions{RootCAs: roots, DisableKeepAlives: true}}
			owner, dependencies := wireOwner(t, Settings{Name: "session-ticket", DisableSessionTickets: pointer(disabled)}, native)
			for range 2 {
				request, _ := fhttp.NewRequest("GET", peer.URL, nil)
				receipt, err := owner.Client().Do(testContext(t), testContext(t), request)
				value, terminal := wireReceive(t, dependencies, receipt)
				if err != nil || terminal != nil || !value.Complete() || value.Metadata().Protocol() != "HTTP/1.1" {
					t.Fatal("fresh TLS connection request failed", err, terminal)
				}
			}
			first, second := <-observed, <-observed
			if first.resumed || first.remote == second.remote || second.resumed == disabled {
				t.Fatalf("peer did not observe intended fresh/resumed handshakes: first=%t second=%t distinct-connections=%t disabled=%t", first.resumed, second.resumed, first.remote != second.remote, disabled)
			}
		})
	}
}

type wireCapture struct {
	fields   []string
	settings []uint16
}

type wirePeer struct {
	address string
	roots   *x509.CertPool
	seen    chan wireCapture
	count   atomic.Int64
}

func newWirePeer(t *testing.T, h2 bool) *wirePeer {
	t.Helper()
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	peer := &wirePeer{address: "http://" + listener.Addr().String(), seen: make(chan wireCapture, 4)}
	if h2 {
		_, root, signing := wireCertificate(t, nil, nil, "wire-authority", false)
		certificate, _, _ := wireCertificate(t, root, signing, "wire-peer", false)
		peer.roots = x509.NewCertPool()
		peer.roots.AddCert(root)
		listener = stdtls.NewListener(listener, &stdtls.Config{Certificates: []stdtls.Certificate{certificate}, NextProtos: []string{"h2"}})
		peer.address = "https://" + listener.Addr().String()
	}
	var connections sync.Map
	var workers sync.WaitGroup
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			connection, err := listener.Accept()
			if err != nil {
				return
			}
			connections.Store(connection, true)
			workers.Add(1)
			go func() {
				defer workers.Done()
				defer connections.Delete(connection)
				defer connection.Close()
				if h2 {
					peer.h2(connection)
				} else {
					peer.h1(connection)
				}
			}()
		}
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		<-done
		connections.Range(func(key, _ any) bool { _ = key.(net.Conn).Close(); return true })
		finished := make(chan struct{})
		go func() { workers.Wait(); close(finished) }()
		select {
		case <-finished:
		case <-testContext(t).Done():
			t.Error("independent wire parser did not terminate")
		}
	})
	return peer
}

func (peer *wirePeer) h1(connection net.Conn) {
	reader := bufio.NewReader(connection)
	if _, err := reader.ReadString('\n'); err != nil {
		return
	}
	peer.count.Add(1)
	var capture wireCapture
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return
		}
		if line == "\r\n" {
			break
		}
		name, _, _ := strings.Cut(line, ":")
		capture.fields = append(capture.fields, strings.ToLower(name))
	}
	peer.seen <- capture
	_, _ = io.WriteString(connection, "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
}

func (peer *wirePeer) h2(connection net.Conn) {
	preface := make([]byte, len(http2.ClientPreface))
	if _, err := io.ReadFull(connection, preface); err != nil || string(preface) != http2.ClientPreface {
		return
	}
	framer := http2.NewFramer(connection, connection)
	if err := framer.WriteSettings(); err != nil {
		return
	}
	decoder := hpack.NewDecoder(4096, nil)
	var capture wireCapture
	for {
		frame, err := framer.ReadFrame()
		if err != nil {
			return
		}
		switch frame := frame.(type) {
		case *http2.SettingsFrame:
			if !frame.IsAck() {
				_ = frame.ForeachSetting(func(setting http2.Setting) error {
					capture.settings = append(capture.settings, uint16(setting.ID))
					return nil
				})
				if framer.WriteSettingsAck() != nil {
					return
				}
			}
		case *http2.HeadersFrame:
			peer.count.Add(1)
			stream := frame.StreamID
			block := append([]byte(nil), frame.HeaderBlockFragment()...)
			ended := frame.HeadersEnded()
			for !ended {
				fragment, err := framer.ReadFrame()
				if err != nil {
					return
				}
				next, ok := fragment.(*http2.ContinuationFrame)
				if !ok || next.StreamID != stream {
					return
				}
				block = append(block, next.HeaderBlockFragment()...)
				ended = next.HeadersEnded()
			}
			fields, err := decoder.DecodeFull(block)
			if err != nil {
				return
			}
			for _, field := range fields {
				capture.fields = append(capture.fields, field.Name)
			}
			peer.seen <- capture
			var response bytes.Buffer
			encoder := hpack.NewEncoder(&response)
			_ = encoder.WriteField(hpack.HeaderField{Name: ":status", Value: "200"})
			_ = encoder.WriteField(hpack.HeaderField{Name: "content-length", Value: "2"})
			if framer.WriteHeaders(http2.HeadersFrameParam{StreamID: stream, BlockFragment: response.Bytes(), EndHeaders: true}) != nil || framer.WriteData(stream, true, []byte("ok")) != nil {
				return
			}
		}
	}
}

func TestPublicNativeHeaderAndSettingsWireOrder(t *testing.T) {
	for _, h2 := range []bool{false, true} {
		for _, ordered := range []bool{false, true} {
			t.Run(fmt.Sprintf("h2-%t/ordered-%t", h2, ordered), func(t *testing.T) {
				peer := newWirePeer(t, h2)
				native := testNative()
				base := *native.Profile
				settingsOrder := slices.Clone(base.GetSettingsOrder())
				if ordered {
					slices.Reverse(settingsOrder)
				}
				profile := profiles.NewClientProfile(base.GetClientHelloId(), maps.Clone(base.GetSettings()), settingsOrder, base.GetPseudoHeaderOrder(), base.GetConnectionFlow(), base.GetPriorities(), base.GetHeaderPriority(),
					base.GetStreamID(), base.GetAllowHTTP(), base.GetHttp3Settings(), base.GetHttp3SettingsOrder(), base.GetHttp3PriorityParam(), base.GetHttp3PseudoHeaderOrder(), base.GetHttp3SendGreaseFrames())
				native.Profile = &profile
				native.Transport = &sdk.TransportOptions{RootCAs: peer.roots}
				owner, dependencies := wireOwner(t, Settings{Name: "native-wire-order"}, native)
				request, _ := fhttp.NewRequest("GET", peer.address+"/wire", nil)
				request.Header = fhttp.Header{"x-z": {"z"}, "x-a": {"a"}, "x-m": {"m"}}
				wantHeaders := []string{"x-a", "x-m", "x-z"}
				wantPseudo := base.GetPseudoHeaderOrder()
				if ordered {
					wantHeaders = []string{"x-m", "x-z", "x-a"}
					wantPseudo = []string{":path", ":scheme", ":authority", ":method"}
					request.Header[fhttp.HeaderOrderKey] = wantHeaders
					request.Header[fhttp.PHeaderOrderKey] = wantPseudo
				}
				receipt, err := owner.Client().Do(testContext(t), testContext(t), request)
				value, terminal := wireReceive(t, dependencies, receipt)
				if err != nil || terminal != nil || !value.Complete() || string(value.DataCopy()) != "ok" {
					t.Fatal("wire peer response", err, terminal)
				}
				var captured wireCapture
				select {
				case captured = <-peer.seen:
				case <-testContext(t).Done():
					t.Fatal("wire peer received no complete header block")
				}
				var fields, pseudos []string
				for _, field := range captured.fields {
					if strings.HasPrefix(field, "x-") {
						fields = append(fields, field)
					}
					if strings.HasPrefix(field, ":") {
						pseudos = append(pseudos, field)
					}
					if strings.Contains(field, "header-order") {
						t.Fatal("native order metadata leaked as a wire header")
					}
				}
				if !reflect.DeepEqual(fields, wantHeaders) {
					t.Fatalf("actual header order differs: got=%v want=%v", fields, wantHeaders)
				}
				if h2 {
					wantSettings := make([]uint16, len(settingsOrder))
					for index, setting := range settingsOrder {
						wantSettings[index] = uint16(setting)
					}
					if !reflect.DeepEqual(pseudos, wantPseudo) || !reflect.DeepEqual(captured.settings, wantSettings) {
						t.Fatalf("actual H2 profile order differs: pseudo=%v settings=%v", pseudos, captured.settings)
					}
				}
				before := peer.count.Load()
				bad, _ := fhttp.NewRequest("GET", peer.address+"/rejected", nil)
				bad.Header = fhttp.Header{"X-Invalid\r\nInjected": {"value"}}
				receipt, rejected := owner.Client().Do(testContext(t), testContext(t), bad)
				value, terminal = wireReceive(t, dependencies, receipt)
				if rejected == nil && terminal == nil || value.Complete() || peer.count.Load() != before {
					t.Fatal("invalid header bypassed pre-dispatch validation", rejected, terminal)
				}
			})
		}
	}
}
