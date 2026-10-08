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

package httpcloak_test

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	p "github.com/frost-leo/fathomry/adapters/httpclient/httpcloak/v1"
	shared "github.com/frost-leo/fathomry/adapters/httpclient/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	officialh3 "github.com/quic-go/quic-go/http3"
	nativehttp "github.com/sardanioss/http"
	"github.com/sardanioss/httpcloak/fingerprint"
	"github.com/sardanioss/httpcloak/transport"
)

func pointer[T any](value T) *T { return &value }
func testContext(t testing.TB) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}
func testSettings(name string, mode p.ProtocolMode) p.Settings {
	return p.Settings{Name: name, PresetName: "chrome-148", Protocol: pointer(mode),
		DisableECH: pointer(true), MaxActive: pointer(2), MaxBindings: pointer(2), MaxConnections: pointer(8)}
}
func mechanisms(t testing.TB, policy shared.Policy) p.Dependencies {
	t.Helper()
	runtime, err := adapters.New(context.Background(), policy.Runtime)
	if err != nil {
		t.Fatal(err)
	}
	inbox, err := adapters.NewInbox[p.Result](policy.Evidence)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := runtime.Close(testContext(t)); err != nil {
			t.Error(err)
		}
	})
	return p.Dependencies{Runtime: runtime, Evidence: inbox}
}
func opened(t testing.TB, value p.Settings, native p.NativeOptions) (*p.Owner, p.Dependencies) {
	t.Helper()
	prepared, err := p.Prepare(value, native)
	if err != nil {
		t.Fatal(err)
	}
	policy, err := prepared.Policy()
	if err != nil {
		t.Fatal(err)
	}
	dependencies := mechanisms(t, policy)
	owner, err := prepared.Open(testContext(t), dependencies)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := owner.Close(testContext(t)); err != nil {
			t.Error(err)
		}
		drain(t, dependencies.Evidence)
	})
	return owner, dependencies
}
func drain(t testing.TB, inbox *adapters.Inbox[p.Result]) {
	t.Helper()
	for {
		state, err := inbox.Inspect()
		if err != nil {
			t.Fatal(err)
		}
		if state.Outstanding == 0 {
			return
		}
		delivery, err := inbox.NextReleased(testContext(t))
		if err != nil {
			t.Fatal(err)
		}
		if err := delivery.Ack(); err != nil {
			t.Fatal(err)
		}
	}
}
func observe(t testing.TB, inbox *adapters.Inbox[p.Result], receipt *adapters.Receipt[p.Result]) p.Result {
	t.Helper()
	if receipt == nil {
		t.Fatal("missing accepted receipt")
	}
	direct, err := receipt.WaitReleased(testContext(t))
	if err != nil || direct.Err() != nil {
		t.Fatalf("native completion: %v / %v", err, direct.Err())
	}
	delivery, err := inbox.NextReleased(testContext(t))
	if err != nil {
		t.Fatal(err)
	}
	independentReceipt, err := delivery.Receipt()
	if err != nil {
		t.Fatal(err)
	}
	independent, err := independentReceipt.WaitReleased(testContext(t))
	if err != nil || independent.Info() != direct.Info() {
		t.Fatal("independent evidence differs", err)
	}
	if err := delivery.Ack(); err != nil {
		t.Fatal(err)
	}
	result, present := direct.ValueCopy()
	if !present {
		t.Fatal("response evidence absent")
	}
	return result
}
func input(t testing.TB, ctx context.Context, target string) *nativehttp.Request {
	t.Helper()
	request, err := nativehttp.NewRequestWithContext(ctx, "GET", target, nil)
	if err != nil {
		t.Fatal(err)
	}
	return request
}

func TestPublicFrozenPresetAndNativeSelection(t *testing.T) {
	const name = "fathomry-public-gh120-frozen"
	if fingerprint.GetStrict(name) != nil {
		t.Fatal("registry fixture name already occupied")
	}
	original := fingerprint.GetStrict("chrome-148")
	original.HeaderOrder = []fingerprint.HeaderPair{{Key: "X-Frozen", Value: "before"}}
	fingerprint.Register(name, original)
	t.Cleanup(func() { fingerprint.Unregister(name) })
	settings := testSettings("frozen", p.HTTP1)
	settings.PresetName = name
	prepared, err := p.Prepare(settings, p.NativeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	before, err := prepared.Policy()
	if err != nil {
		t.Fatal(err)
	}
	original.HeaderOrder[0].Value = "after"
	fingerprint.Register(name, original)
	*settings.Protocol = p.HTTP3
	after, err := prepared.Policy()
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("prepared budget changed with caller/registry mutation", err)
	}
	dependencies := mechanisms(t, before)
	for _, native := range []p.NativeOptions{{Preset: original}, {Transport: &transport.TransportConfig{}},
		{Verify: &transport.TLSVerify{}}, {ProxyVerify: &transport.TLSVerify{}},
		{CheckRedirect: func(*nativehttp.Request, []*nativehttp.Request) error { t.Fatal("unused hook entered"); return nil }}} {
		altered := dependencies
		altered.Native = native
		if owner, err := prepared.Open(testContext(t), altered); owner != nil || !errors.Is(err, p.ErrInput) {
			t.Fatal("prepared source accepted a second native selection", err)
		}
	}
	owner, err := prepared.Open(testContext(t), dependencies)
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close(context.Background())
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		_, _ = io.WriteString(w, request.Header.Get("X-Frozen"))
	}))
	defer peer.Close()
	receipt, err := owner.Client().Do(testContext(t), testContext(t), input(t, context.Background(), peer.URL))
	if err != nil {
		t.Fatal(err)
	}
	result := observe(t, dependencies.Evidence, receipt)
	if string(result.DataCopy()) != "before" || result.Metadata().Protocol() != "HTTP/1.1" {
		t.Fatal("construction reread mutable preset or protocol")
	}
	if _, err := json.Marshal(prepared); !errors.Is(err, p.ErrSerialization) {
		t.Fatal("prepared runtime serialized", err)
	}
	if err := owner.Close(testContext(t)); err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies.Evidence)
}

func TestPublicActualProtocolsFiniteAndRetainedStream(t *testing.T) {
	for _, mode := range []p.ProtocolMode{p.HTTP1, p.HTTP2, p.HTTP3} {
		t.Run(string(mode), func(t *testing.T) {
			handler := http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
				if request.Header.Get("X-Input") != "yes" {
					t.Error("request extension lost")
				}
				w.Header().Set("Trailer", "X-Final")
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = io.WriteString(w, "body")
				w.Header().Set("X-Final", "complete")
			})
			server := httptest.NewUnstartedServer(handler)
			server.EnableHTTP2 = mode == p.HTTP2
			server.StartTLS()
			defer server.Close()
			roots := x509.NewCertPool()
			roots.AddCert(server.Certificate())
			target := server.URL
			if mode == p.HTTP3 {
				packet, err := net.ListenPacket("udp", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				peer := &officialh3.Server{TLSConfig: &tls.Config{Certificates: server.TLS.Certificates}, Handler: handler}
				exited := make(chan error, 1)
				go func() { exited <- peer.Serve(packet) }()
				defer func() {
					_ = peer.Close()
					_ = packet.Close()
					select {
					case <-exited:
					case <-time.After(time.Second):
						t.Error("independent H3 peer retained work")
					}
				}()
				target = "https://" + packet.LocalAddr().String()
			}
			owner, dependencies := opened(t, testSettings(string(mode), mode), p.NativeOptions{Verify: &transport.TLSVerify{RootCAs: roots}})
			for _, retained := range []bool{false, true} {
				request := input(t, context.Background(), target)
				request.Header.Set("X-Input", "yes")
				var receipt *adapters.Receipt[p.Result]
				if retained {
					stream, accepted, err := owner.Client().Open(testContext(t), request)
					if err != nil {
						t.Fatal(err)
					}
					data, err := io.ReadAll(stream)
					if err != nil || string(data) != "body" {
						t.Fatal("retained stream", err)
					}
					if err := stream.Close(testContext(t)); err != nil {
						t.Fatal(err)
					}
					receipt = accepted
				} else {
					var err error
					receipt, err = owner.Client().Do(testContext(t), testContext(t), request)
					if err != nil {
						t.Fatal(err)
					}
				}
				result := observe(t, dependencies.Evidence, receipt)
				if !result.Complete() || !result.InputComplete() || result.Metadata().StatusCode() != 503 ||
					result.TrailersCopy().Get("X-Final") != "complete" || result.BytesRead() != 4 ||
					result.WireBytesRead() != 4 || result.Exchanges() != 1 || result.Source().Provider != p.ProviderID {
					t.Fatal("native response/input/status/evidence facts were normalized")
				}
				if mode == p.HTTP1 && result.Metadata().Protocol() != "HTTP/1.1" ||
					mode == p.HTTP2 && result.Metadata().Protocol() != "HTTP/2.0" ||
					mode == p.HTTP3 && result.Metadata().Protocol() != "HTTP/3.0" {
					t.Fatal("wrong actual protocol", result.Metadata().Protocol())
				}
				if !retained && string(result.DataCopy()) != "body" || retained && result.DataCopy() != nil {
					t.Fatal("finite/stream storage distinction lost")
				}
			}
		})
	}
}

type untouchedReader struct{ touches atomic.Int64 }

func (reader *untouchedReader) Read([]byte) (int, error) { reader.touches.Add(1); return 0, io.EOF }
func (reader *untouchedReader) Close() error             { reader.touches.Add(1); return nil }

func TestPublicPreAdmissionBoundsAndNativeRequestContext(t *testing.T) {
	var effects atomic.Int64
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) { effects.Add(1); _, _ = io.WriteString(w, "ok") }))
	defer peer.Close()
	owner, dependencies := opened(t, testSettings("input", p.HTTP1), p.NativeOptions{})
	before, _ := dependencies.Runtime.Inspect()
	reader := &untouchedReader{}
	request := input(t, context.Background(), peer.URL)
	request.Body = reader
	oversized := p.RequestOptions{HeaderOrder: make([]string, 1025)}
	if receipt, err := owner.Client().Do(testContext(t), testContext(t), request, oversized); receipt != nil || err == nil {
		t.Fatal("oversized extension reached public admission")
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if receipt, err := owner.Client().Do(testContext(t), testContext(t), input(t, canceled, peer.URL)); receipt != nil || !errors.Is(err, context.Canceled) {
		t.Fatal("native request context was silently replaced", err)
	}
	after, _ := dependencies.Runtime.Inspect()
	if after.Accepted != before.Accepted || reader.touches.Load() != 0 || effects.Load() != 0 {
		t.Fatal("refusal acquired input or retained work/evidence")
	}
	receipt, err := owner.Client().Do(testContext(t), testContext(t), input(t, context.Background(), peer.URL))
	if err != nil {
		t.Fatal(err)
	}
	if string(observe(t, dependencies.Evidence, receipt).DataCopy()) != "ok" {
		t.Fatal("unchanged live-context control failed")
	}
}

func TestPublicExactHeaderOrderCasingAndDuplicates(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	lines := make(chan []string, 1)
	failed := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			failed <- err
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
		reader := bufio.NewReader(conn)
		var fields []string
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				failed <- err
				return
			}
			fields = append(fields, strings.TrimSuffix(line, "\r\n"))
			if line == "\r\n" {
				break
			}
		}
		lines <- fields
		_, err = io.WriteString(conn, "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
		failed <- err
	}()
	owner, dependencies := opened(t, testSettings("exact", p.HTTP1), p.NativeOptions{})
	request := input(t, context.Background(), "http://"+listener.Addr().String()+"/")
	request.Header.Set("X-Ordinary", "must-not-appear")
	receipt, err := owner.Client().Do(testContext(t), testContext(t), request, p.RequestOptions{ExactHeaders: []fingerprint.HeaderPair{{Key: "x-first", Value: "one"}, {Key: "X-Second", Value: "two"}, {Key: "x-first", Value: "three"}}})
	if err != nil {
		t.Fatal(err)
	}
	if string(observe(t, dependencies.Evidence, receipt).DataCopy()) != "ok" {
		t.Fatal("response lost")
	}
	fields := <-lines
	joined := strings.Join(fields, "\n")
	first, second, last := strings.Index(joined, "x-first: one"), strings.Index(joined, "X-Second: two"), strings.Index(joined, "x-first: three")
	if first < 0 || second <= first || last <= second || strings.Contains(joined, "X-Ordinary") || strings.Contains(strings.ToLower(joined), "user-agent:") {
		t.Fatalf("exact native wire fields changed: %q", fields)
	}
	if err := <-failed; err != nil {
		t.Fatal(err)
	}
}

func TestPublicQueuedRequestContextCancelsBeforeAdmission(t *testing.T) {
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		_, _ = io.WriteString(w, "prefix")
		w.(http.Flusher).Flush()
		<-request.Context().Done()
	}))
	defer peer.Close()
	value := testSettings("queued-context", p.HTTP1)
	value.MaxActive, value.QueuedCalls = pointer(1), pointer(1)
	owner, dependencies := opened(t, value, p.NativeOptions{})
	stream, _, err := owner.Client().Open(testContext(t), input(t, context.Background(), peer.URL))
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close(context.Background())
	before, _ := dependencies.Runtime.Inspect()
	requestCtx, cancelRequest := context.WithCancel(context.Background())
	defer cancelRequest()
	reader := &untouchedReader{}
	request := input(t, requestCtx, peer.URL)
	request.Body = reader
	type reply struct {
		receipt *adapters.Receipt[p.Result]
		err     error
	}
	returned := make(chan reply, 1)
	method := testContext(t)
	go func() { receipt, err := owner.Client().Do(method, method, request); returned <- reply{receipt, err} }()
	for {
		state, err := dependencies.Runtime.Inspect()
		if err != nil {
			t.Fatal(err)
		}
		if state.Queued == 1 {
			break
		}
		if method.Err() != nil {
			t.Fatal("public request never entered queue")
		}
		runtime.Gosched()
	}
	cancelRequest()
	select {
	case result := <-returned:
		if result.receipt != nil || !errors.Is(result.err, context.Canceled) {
			t.Fatal("canceled queued request was admitted", result.err)
		}
	case <-time.After(time.Second):
		t.Fatal("native Request.Context cancellation did not stop public admission wait")
	}
	after, _ := dependencies.Runtime.Inspect()
	if after.Accepted != before.Accepted || after.Queued != 0 || reader.touches.Load() != 0 || method.Err() != nil {
		t.Fatal("queued cancellation acquired body/work or changed the live method context")
	}
}
