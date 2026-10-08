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

package surf

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	stdhttp "net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/enetx/g"
	http "github.com/enetx/http"
	sdk "github.com/enetx/surf"
	"github.com/enetx/surf/profiles"
	"github.com/enetx/surf/profiles/chrome"
	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/quic-go/quic-go/http3"
	utls "github.com/refraction-networking/utls"
	"golang.org/x/net/dns/dnsmessage"
)

func reviewPublicSettled(t *testing.T, dependencies Dependencies, receipt *adapters.Receipt[Result]) (Result, error) {
	t.Helper()
	if receipt == nil {
		t.Fatal("accepted operation has no receipt")
	}
	direct, err := receipt.WaitReleased(testContext(t))
	if err != nil {
		t.Fatal(err)
	}
	matched := false
	for range 2 {
		delivery, err := dependencies.Evidence.NextReleased(testContext(t))
		if err != nil {
			t.Fatal(err)
		}
		independentReceipt, err := delivery.Receipt()
		if err != nil {
			t.Fatal(err)
		}
		independent, err := independentReceipt.WaitReleased(testContext(t))
		if err != nil {
			t.Fatal(err)
		}
		if err := delivery.Ack(); err != nil {
			t.Fatal(err)
		}
		if independent.Info() == direct.Info() {
			if independent.Primary() != direct.Primary() || independent.Cleanup() != direct.Cleanup() {
				t.Fatal("independent public evidence differs")
			}
			matched = true
			break
		}
	}
	if !matched {
		t.Fatal("operation receipt absent from bounded independent evidence")
	}
	value, present := direct.ValueCopy()
	if !present {
		t.Fatal("released operation has no projected result")
	}
	return value, direct.Err()
}

func TestPublicOSOverrideAndRefusal(t *testing.T) {
	for _, os := range []profiles.OSKey{profiles.Windows, profiles.MacOS, profiles.Linux, profiles.Android, profiles.IOS} {
		t.Run(fmt.Sprint(os), func(t *testing.T) {
			var builds, requests atomic.Int64
			peer := httptest.NewServer(stdhttp.HandlerFunc(func(writer stdhttp.ResponseWriter, request *stdhttp.Request) {
				requests.Add(1)
				if request.UserAgent() != string(chrome.UserAgent[os]) || request.Header.Get("Sec-Ch-Ua-Platform") != string(chrome.Platform[os]) || request.Header.Get("Sec-Ch-Ua-Mobile") != string(os.Mobile()) {
					t.Errorf("OS %d did not select native header values: UA=%q platform=%q mobile=%q", os, request.UserAgent(), request.Header.Get("Sec-Ch-Ua-Platform"), request.Header.Get("Sec-Ch-Ua-Mobile"))
				}
				_, _ = io.WriteString(writer, "os-control")
			}))
			t.Cleanup(peer.Close)
			profile := chrome.Desktop
			profile.HelloSpec, profile.HelloID, profile.ShuffleExtensions = nil, utls.ClientHelloID{}, false
			build := profile.BuildHeaders
			profile.BuildHeaders = func(selected profiles.OSKey) *g.MapOrd[g.String, g.String] {
				builds.Add(1)
				if selected != os {
					t.Error("OS changed before native BuildHeaders")
				}
				return build(selected)
			}
			prepared, err := Prepare(Settings{Name: "public-os", Mode: pointer(HTTP1Only)}, NativeOptions{Profile: &profile, OS: os})
			if err != nil {
				t.Fatal(err)
			}
			dependencies := testMechanisms(t, prepared)
			owner, err := prepared.Open(testContext(t), dependencies)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := owner.Close(testContext(t)); err != nil {
					t.Error(err)
				}
			})
			if builds.Load() != 0 || requests.Load() != 0 {
				t.Fatal("offline OS preparation called native profile")
			}
			input, _ := http.NewRequest("GET", peer.URL, nil)
			receipt, err := owner.Client().Do(testContext(t), testContext(t), input)
			if err != nil {
				t.Fatal(err)
			}
			value, final := reviewPublicSettled(t, dependencies, receipt)
			if final != nil || !value.Complete() || string(value.DataCopy()) != "os-control" || builds.Load() != 1 || requests.Load() != 1 {
				t.Fatal("OS positive control failed", final, builds.Load(), requests.Load())
			}
		})
	}
	for _, os := range []profiles.OSKey{-1, 5} {
		profile := chrome.Desktop
		var calls atomic.Int64
		profile.BuildHeaders = func(profiles.OSKey) *g.MapOrd[g.String, g.String] {
			calls.Add(1)
			return nil
		}
		if _, err := Prepare(Settings{Name: "refused-os"}, NativeOptions{Profile: &profile, OS: os}); !errors.Is(err, ErrInput) || calls.Load() != 0 {
			t.Fatal("invalid profile OS not rejected offline", os, err)
		}
	}
}

func reviewPublicResolver(t *testing.T, selection string, answerGate ...<-chan struct{}) (*net.Resolver, *atomic.Int64, error) {
	t.Helper()
	var calls atomic.Int64
	var pending sync.WaitGroup
	t.Cleanup(pending.Wait)
	cause := errors.New("fixture DNS callback failure")
	resolver := &net.Resolver{PreferGo: true, StrictErrors: true,
		Dial: func(ctx context.Context, _, _ string) (net.Conn, error) {
			calls.Add(1)
			if selection == "panic" {
				panic(cause)
			}
			if selection == "dial-refused" {
				return nil, cause
			}
			client, server := net.Pipe()
			_ = server.SetDeadline(time.Now().Add(2 * time.Second))
			pending.Go(func() {
				defer server.Close()
				var prefix [2]byte
				if _, err := io.ReadFull(server, prefix[:]); err != nil {
					t.Error("DNS query length", err)
					return
				}
				length := binary.BigEndian.Uint16(prefix[:])
				if length == 0 || length > 2048 {
					t.Error("DNS query exceeds fixture bound", length)
					return
				}
				wire := make([]byte, int(length))
				if _, err := io.ReadFull(server, wire); err != nil {
					t.Error("DNS query bytes", err)
					return
				}
				var query dnsmessage.Message
				if err := query.Unpack(wire); err != nil || len(query.Questions) != 1 {
					t.Error("DNS query parse", err)
					return
				}
				question := query.Questions[0]
				if question.Name.String() != "review-resolver.invalid." {
					t.Error("unexpected resolver name", question.Name.String())
					return
				}
				answer := dnsmessage.Message{Header: dnsmessage.Header{ID: query.ID, Response: true, Authoritative: true, RecursionDesired: true, RecursionAvailable: true}, Questions: query.Questions}
				if selection == "nxdomain" {
					answer.RCode = dnsmessage.RCodeNameError
				} else if question.Type == dnsmessage.TypeA {
					answer.Answers = []dnsmessage.Resource{{Header: dnsmessage.ResourceHeader{Name: question.Name, Type: dnsmessage.TypeA, Class: dnsmessage.ClassINET}, Body: &dnsmessage.AResource{A: [4]byte{127, 0, 0, 1}}}}
				}
				wire, err := answer.Pack()
				if err != nil {
					t.Error(err)
					return
				}
				if len(answerGate) > 0 {
					select {
					case <-answerGate[0]:
					case <-ctx.Done():
						return
					}
				}
				binary.BigEndian.PutUint16(prefix[:], uint16(len(wire)))
				if _, err := server.Write(append(prefix[:], wire...)); err != nil && ctx.Err() == nil {
					t.Error("DNS answer write", err)
				}
			})
			return client, nil
		}}
	return resolver, &calls, cause
}

func TestPublicCustomResolverSuccessAndRefusal(t *testing.T) {
	for _, mode := range []ProtocolMode{HTTP1Only, PreferHTTP3} {
		for _, selection := range []string{"success", "nxdomain", "dial-refused", "panic"} {
			t.Run(string(mode)+"/"+selection, func(t *testing.T) {
				var requests atomic.Int64
				handler := stdhttp.HandlerFunc(func(writer stdhttp.ResponseWriter, request *stdhttp.Request) {
					requests.Add(1)
					_, _ = io.WriteString(writer, request.Proto)
				})
				peer := httptest.NewTLSServer(handler)
				t.Cleanup(peer.Close)
				if mode == PreferHTTP3 {
					packet, err := net.ListenPacket("udp", peer.Listener.Addr().String())
					if err != nil {
						t.Fatal(err)
					}
					server := &http3.Server{TLSConfig: &tls.Config{Certificates: peer.TLS.Certificates}, Handler: handler, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
					stopped := make(chan error, 1)
					go func() { stopped <- server.Serve(packet) }()
					t.Cleanup(func() {
						_ = server.Close()
						_ = packet.Close()
						select {
						case <-stopped:
						case <-time.After(2 * time.Second):
							t.Error("independent H3 resolver peer did not stop")
						}
					})
				}
				pool := x509.NewCertPool()
				pool.AddCert(peer.Certificate())
				resolver, dnsCalls, callbackCause := reviewPublicResolver(t, selection)
				prepared, err := Prepare(Settings{Name: "public-resolver", Mode: pointer(mode)}, NativeOptions{Resolver: resolver, TLSConfig: &tls.Config{RootCAs: pool, ServerName: peer.Certificate().DNSNames[0]}})
				if err != nil {
					t.Fatal(err)
				}
				dependencies := testMechanisms(t, prepared)
				owner, err := prepared.Open(testContext(t), dependencies)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					err := owner.Close(testContext(t))
					if selection == "panic" {
						if !errors.Is(err, callbackCause) || !errors.Is(err, sdk.ErrFathomryCallback) {
							t.Error("Resolver panic not preserved through owner shutdown", err)
						}
					} else if err != nil {
						t.Error(err)
					}
				})
				if dnsCalls.Load() != 0 || requests.Load() != 0 {
					t.Fatal("offline resolver selection contacted a peer")
				}
				endpoint, _ := url.Parse(peer.URL)
				endpoint.Host = net.JoinHostPort("review-resolver.invalid", endpoint.Port())
				input, _ := http.NewRequest("GET", endpoint.String(), nil)
				receipt, err := owner.Client().Do(testContext(t), testContext(t), input)
				value, final := reviewPublicSettled(t, dependencies, receipt)
				if dnsCalls.Load() == 0 {
					t.Fatal("configured Resolver was bypassed")
				}
				if selection == "success" {
					want := "HTTP/1.1"
					if mode == PreferHTTP3 {
						want = "HTTP/3.0"
					}
					if err != nil || final != nil || !value.Complete() || requests.Load() != 1 || string(value.DataCopy()) != want || value.Metadata().Protocol() != want {
						t.Fatal("custom DNS success did not reach exact protocol peer", err, final, value.Metadata().Protocol(), requests.Load())
					}
				} else {
					var directDNS, finalDNS *net.DNSError
					if requests.Load() != 0 || value.Complete() || selection != "panic" && (!errors.As(err, &directDNS) || !errors.As(final, &finalDNS)) {
						t.Fatal("configured DNS refusal escaped or lost independent DNS error", err, final, requests.Load())
					}
					if selection == "panic" && (!errors.Is(err, ErrCallback) || !errors.Is(final, ErrCallback) || !errors.Is(err, callbackCause) || !errors.Is(final, callbackCause)) {
						t.Fatal("managed Resolver panic lost its original error-valued cause", err, final)
					}
				}
			})
		}
	}
}
