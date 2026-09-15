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
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	nativehttp "github.com/bogdanfinn/fhttp"
	sdk "github.com/bogdanfinn/tls-client"
	"github.com/bogdanfinn/tls-client/profiles"
	quic "github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
)

type nativePeers struct {
	tcp   *httptest.Server
	roots *x509.CertPool
	mu    sync.Mutex
	conns []*quic.Conn
}

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}
func newNativePeers(t *testing.T, tcpHandler, h3Handler http.HandlerFunc) *nativePeers {
	t.Helper()
	peers := &nativePeers{roots: x509.NewCertPool()}
	peers.tcp = httptest.NewUnstartedServer(tcpHandler)
	peers.tcp.EnableHTTP2 = true
	peers.tcp.StartTLS()
	t.Cleanup(peers.tcp.Close)
	peers.roots.AddCert(peers.tcp.Certificate())
	packet, err := net.ListenPacket("udp", peers.tcp.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	server := &http3.Server{
		TLSConfig: &tls.Config{Certificates: peers.tcp.TLS.Certificates},
		Handler:   h3Handler,
		Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
		ConnContext: func(ctx context.Context, conn *quic.Conn) context.Context {
			peers.mu.Lock()
			peers.conns = append(peers.conns, conn)
			peers.mu.Unlock()
			return ctx
		},
	}
	finished := make(chan error, 1)
	go func() { finished <- server.Serve(packet) }()
	t.Cleanup(func() {
		_ = server.Close()
		_ = packet.Close()
		select {
		case <-finished:
		case <-time.After(3 * time.Second):
			t.Error("local H3 peer did not stop")
		}
	})
	return peers
}
func (peers *nativePeers) client(t *testing.T, options ...sdk.HttpClientOption) sdk.HttpClient {
	t.Helper()
	input := []sdk.HttpClientOption{
		sdk.WithClientProfile(profiles.Chrome_144),
		sdk.WithTransportOptions(&sdk.TransportOptions{RootCAs: peers.roots}),
		sdk.WithProtocolRacing(),
		sdk.WithTimeoutSeconds(3),
	}
	client, err := sdk.NewHttpClient(sdk.NewNoopLogger(), append(input, options...)...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := sdk.Close(client); err != nil {
			t.Error("native cleanup failed", err)
		}
	})
	return client
}
func (peers *nativePeers) assertQUICClosed(t *testing.T) {
	t.Helper()
	peers.mu.Lock()
	connections := append([]*quic.Conn(nil), peers.conns...)
	peers.mu.Unlock()
	if len(connections) == 0 {
		t.Fatal("test did not establish an H3 connection")
	}
	for _, conn := range connections {
		select {
		case <-conn.Context().Done():
		case <-time.After(2 * time.Second):
			t.Fatal("original H3 peer connection survived client Close")
		}
	}
}

func TestNativeH3WinnerOwnsActualTransport(t *testing.T) {
	var tcpCalls, h3Calls atomic.Int64
	peers := newNativePeers(t,
		func(writer http.ResponseWriter, request *http.Request) {
			tcpCalls.Add(1)
			<-request.Context().Done()
		},
		func(writer http.ResponseWriter, request *http.Request) {
			h3Calls.Add(1)
			_, _ = io.WriteString(writer, "quic")
		},
	)
	client := peers.client(t)
	before := socketIdentities(t)
	for range 2 {
		request, _ := nativehttp.NewRequestWithContext(testContext(t), "GET", peers.tcp.URL, nil)
		response, err := client.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		data, readErr := io.ReadAll(response.Body)
		closeErr := response.Body.Close()
		if readErr != nil || closeErr != nil || response.ProtoMajor != 3 || string(data) != "quic" {
			t.Fatal("H3 response changed", readErr, closeErr)
		}
	}
	during := socketIdentities(t)
	var owned []string
	for identity := range during {
		if !before[identity] {
			owned = append(owned, identity)
		}
	}
	if runtime.GOOS == "linux" && len(owned) == 0 {
		t.Fatal("test did not observe the native client socket")
	}
	if err := sdk.Close(client); err != nil {
		t.Fatal(err)
	}
	peers.assertQUICClosed(t)
	until := time.Now().Add(2 * time.Second)
	for {
		after := socketIdentities(t)
		remaining := false
		for _, identity := range owned {
			remaining = remaining || after[identity]
		}
		if !remaining {
			break
		}
		if time.Now().After(until) {
			t.Fatal("client Close retained an observed native socket")
		}
		time.Sleep(10 * time.Millisecond)
	}
	peers.mu.Lock()
	connections := len(peers.conns)
	peers.mu.Unlock()
	if connections != 1 || h3Calls.Load() != 2 || tcpCalls.Load() > 1 {
		t.Fatal("H3 winner was replaced or the cached winner was not reused", connections, tcpCalls.Load())
	}
}

func socketIdentities(t *testing.T) map[string]bool {
	t.Helper()
	result := make(map[string]bool)
	if runtime.GOOS != "linux" {
		return result
	}
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		target, err := os.Readlink(filepath.Join("/proc/self/fd", entry.Name()))
		if err == nil && strings.HasPrefix(target, "socket:[") {
			result[target] = true
		}
	}
	return result
}

func TestNativeBothRacingLegsKeepReplayAndWinnerLifetime(t *testing.T) {
	for _, winner := range []string{"h2", "h3"} {
		t.Run(winner, func(t *testing.T) {
			reached := make(chan string, 2)
			h2Release, h3Release := make(chan struct{}), make(chan struct{})
			var stopOnce sync.Once
			unblock := func() { stopOnce.Do(func() { close(h2Release); close(h3Release) }) }
			// Separate winner release keeps the loser waiting on its canceled context.
			winRelease := make(chan struct{})
			defer unblock()
			handler := func(protocol string) http.HandlerFunc {
				return func(writer http.ResponseWriter, request *http.Request) {
					data, err := io.ReadAll(request.Body)
					if err != nil || !bytes.Equal(data, []byte("payload")) || request.Header.Get("X-Input") != "independent" {
						t.Error("native leg altered request data", protocol, err)
					}
					reached <- protocol
					if protocol == winner {
						select {
						case <-winRelease:
						case <-request.Context().Done():
							return
						}
					} else {
						select {
						case <-request.Context().Done():
							return
						case <-h2Release:
							return
						case <-h3Release:
							return
						}
					}
					_, _ = io.WriteString(writer, "first")
					writer.(http.Flusher).Flush()
					select {
					case <-h2Release:
					case <-request.Context().Done():
						return
					}
					_, _ = io.WriteString(writer, "second")
				}
			}
			peers := newNativePeers(t, handler("h2"), handler("h3"))
			client := peers.client(t)
			request, _ := nativehttp.NewRequestWithContext(testContext(t), "POST", peers.tcp.URL, strings.NewReader("payload"))
			request.Header.Set("X-Input", "independent")
			type outcome struct {
				response *nativehttp.Response
				err      error
			}
			result := make(chan outcome, 1)
			go func() { response, err := client.Do(request); result <- outcome{response, err} }()
			observed := map[string]bool{}
			for range 2 {
				select {
				case protocol := <-reached:
					observed[protocol] = true
				case <-time.After(3 * time.Second):
					t.Fatal("both real protocol legs did not reach peers")
				}
			}
			if !observed["h2"] || !observed["h3"] {
				t.Fatal("test did not exercise both protocols")
			}
			close(winRelease)
			var received outcome
			select {
			case received = <-result:
			case <-time.After(3 * time.Second):
				t.Fatal("winner not handed off")
			}
			if received.err != nil {
				t.Fatal(received.err)
			}
			prefix := make([]byte, 5)
			if _, err := io.ReadFull(received.response.Body, prefix); err != nil || string(prefix) != "first" {
				t.Fatal("winner failed after headers", err)
			}
			unblock()
			rest, err := io.ReadAll(received.response.Body)
			closeErr := received.response.Body.Close()
			if err != nil || closeErr != nil || string(rest) != "second" {
				t.Fatal("winning stream was canceled with losing leg", err, closeErr)
			}
			if err := sdk.Close(client); err != nil {
				t.Fatal(err)
			}
			peers.assertQUICClosed(t)
		})
	}
}

func TestNativeCallerCancellationAndFullClose(t *testing.T) {
	entered := make(chan struct{}, 1)
	peers := newNativePeers(t,
		func(writer http.ResponseWriter, request *http.Request) { <-request.Context().Done() },
		func(writer http.ResponseWriter, request *http.Request) {
			entered <- struct{}{}
			<-request.Context().Done()
		},
	)
	client := peers.client(t)
	ctx, cancel := context.WithCancel(testContext(t))
	defer cancel()
	request, _ := nativehttp.NewRequestWithContext(ctx, "GET", peers.tcp.URL, nil)
	finished := make(chan error, 1)
	go func() {
		response, err := client.Do(request)
		if response != nil {
			_ = response.Body.Close()
		}
		finished <- err
	}()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("H3 cancellation boundary was not reached")
	}
	cancel()
	select {
	case err := <-finished:
		if !errors.Is(err, context.Canceled) {
			t.Fatal("native race ignored caller cancellation", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("native race did not return after cancellation")
	}
	if err := sdk.Close(client); err != nil {
		t.Fatal(err)
	}
	peers.assertQUICClosed(t)
}

func TestNativeHooksAndCustomDialAreNotRemoved(t *testing.T) {
	peer := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		_, _ = io.WriteString(writer, request.Header.Get("X-Hook"))
	}))
	defer peer.Close()
	roots := x509.NewCertPool()
	roots.AddCert(peer.Certificate())
	var pre, post, dials atomic.Int64
	client, err := sdk.NewHttpClient(sdk.NewNoopLogger(),
		sdk.WithClientProfile(profiles.Chrome_150),
		sdk.WithDisableHttp3(), sdk.WithForceHttp1(),
		sdk.WithTransportOptions(&sdk.TransportOptions{RootCAs: roots}),
		sdk.WithDialContext(func(ctx context.Context, network, address string) (net.Conn, error) {
			dials.Add(1)
			return (&net.Dialer{}).DialContext(ctx, network, address)
		}),
		sdk.WithPreHook(func(request *nativehttp.Request) error {
			pre.Add(1)
			request.Header.Set("X-Hook", "preserved")
			return nil
		}),
		sdk.WithPostHook(func(*sdk.PostResponseContext) error { post.Add(1); return nil }),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer sdk.Close(client)
	request, _ := nativehttp.NewRequestWithContext(testContext(t), "GET", peer.URL, nil)
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if err != nil || string(data) != "preserved" || pre.Load() != 1 || post.Load() != 1 || dials.Load() != 1 {
		t.Fatal("native extension disappeared", err)
	}
	if err := sdk.Close(client); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Do(request); !errors.Is(err, sdk.ErrClientClosed) || pre.Load() != 1 {
		t.Fatal("closed client entered native callbacks", err)
	}
}

func TestNativeCompatibilityUnitControls(t *testing.T) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("test source unavailable")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "../../../../"))
	goBinary := filepath.Join(runtime.GOROOT(), "bin", "go")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	environment := append(os.Environ(), "GOTOOLCHAIN=local", "GOPROXY=off", "GOSUMDB=off", "GOWORK=off", "GOFLAGS=")
	nativeRoot := filepath.Join(root, "third_party/tls-client")
	moduleFile := filepath.Join(t.TempDir(), "native.mod")
	// Exercise the consuming dependency selections, not an independently resolved
	// upstream module that can accidentally depend on a developer's warm cache.
	for _, extension := range []string{"mod", "sum"} {
		data, err := os.ReadFile(filepath.Join(root, "go."+extension))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(strings.TrimSuffix(moduleFile, ".mod")+"."+extension, data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	edit := exec.CommandContext(ctx, goBinary, "-C", nativeRoot, "mod", "edit", "-modfile="+moduleFile,
		"-module=github.com/bogdanfinn/tls-client", "-droprequire=github.com/bogdanfinn/tls-client", "-dropreplace=github.com/bogdanfinn/tls-client")
	edit.Env = environment
	if output, err := edit.CombinedOutput(); err != nil {
		t.Fatalf("prepare consuming native module: %v\n%s", err, output)
	}
	configuration := exec.CommandContext(ctx, goBinary, "env", "CGO_ENABLED")
	configuration.Env = environment
	cgo, err := configuration.Output()
	if err != nil {
		t.Fatal(err)
	}
	arguments := []string{"-C", nativeRoot, "test", "-modfile=" + moduleFile, "-mod=readonly"}
	if strings.TrimSpace(string(cgo)) == "1" {
		arguments = append(arguments, "-race")
	}
	arguments = append(arguments, "-count=1", "-timeout=30s", "-run=^TestFathomry", "./...")
	command := exec.CommandContext(ctx, goBinary, arguments...)
	command.Env = environment
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("native compatibility controls failed: %v\n%s", err, output)
	}
}
