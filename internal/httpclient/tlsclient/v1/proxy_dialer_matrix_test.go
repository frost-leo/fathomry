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
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	sdk "github.com/bogdanfinn/tls-client"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
	"golang.org/x/net/dns/dnsmessage"
)

type controlledDNS struct {
	address string
	queries atomic.Int64
	closes  atomic.Int64
	dials   atomic.Int64
}

type controlledDNSConnection struct {
	net.Conn
	closed *atomic.Int64
	once   sync.Once
}

type controlledDNSPacketConnection struct {
	*controlledDNSConnection
	packet net.PacketConn
}

func (connection *controlledDNSPacketConnection) ReadFrom(output []byte) (int, net.Addr, error) {
	return connection.packet.ReadFrom(output)
}

func (connection *controlledDNSPacketConnection) WriteTo(input []byte, address net.Addr) (int, error) {
	return connection.packet.WriteTo(input, address)
}

func (connection *controlledDNSConnection) Close() error {
	err := connection.Conn.Close()
	if err == nil {
		connection.once.Do(func() { connection.closed.Add(1) })
	}
	return err
}

func controlledNewDNS(t *testing.T) *controlledDNS {
	t.Helper()
	packet, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	dns := &controlledDNS{address: packet.LocalAddr().String()}
	done := make(chan struct{})
	go func() {
		defer close(done)
		buffer := make([]byte, 4096)
		for {
			count, remote, err := packet.ReadFrom(buffer)
			if err != nil {
				return
			}
			var parser dnsmessage.Parser
			header, err := parser.Start(buffer[:count])
			if err != nil {
				continue
			}
			questions, err := parser.AllQuestions()
			if err != nil {
				continue
			}
			dns.queries.Add(1)
			builder := dnsmessage.NewBuilder(nil, dnsmessage.Header{ID: header.ID, Response: true, Authoritative: true, RecursionDesired: true, RecursionAvailable: true})
			builder.EnableCompression()
			if builder.StartQuestions() != nil {
				continue
			}
			for _, question := range questions {
				_ = builder.Question(question)
			}
			_ = builder.StartAnswers()
			for _, question := range questions {
				if question.Name.String() == "controlled-proxy.invalid." && question.Type == dnsmessage.TypeA {
					_ = builder.AResource(dnsmessage.ResourceHeader{Name: question.Name, Class: dnsmessage.ClassINET, TTL: 0}, dnsmessage.AResource{A: [4]byte{127, 0, 0, 1}})
				}
			}
			response, err := builder.Finish()
			if err == nil {
				_, _ = packet.WriteTo(response, remote)
			}
		}
	}()
	t.Cleanup(func() {
		_ = packet.Close()
		select {
		case <-done:
		case <-testContext(t).Done():
			t.Error("fixture DNS peer did not terminate")
		}
	})
	return dns
}

func (dns *controlledDNS) dial(ctx context.Context, network, _ string) (net.Conn, error) {
	connection, err := (&net.Dialer{}).DialContext(ctx, network, dns.address)
	if err != nil {
		return nil, err
	}
	dns.dials.Add(1)
	tracked := &controlledDNSConnection{Conn: connection, closed: &dns.closes}
	if packet, ok := connection.(net.PacketConn); ok {
		return &controlledDNSPacketConnection{controlledDNSConnection: tracked, packet: packet}, nil
	}
	return tracked, nil
}

type controlledProxy struct {
	url   string
	count atomic.Int64
	mu    sync.Mutex
	hosts []string
}

func controlledConnectProxy(t *testing.T, target string) *controlledProxy {
	t.Helper()
	proxy := new(controlledProxy)
	var workers sync.WaitGroup
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != "CONNECT" || request.Host != target {
			http.Error(writer, "fixture rejects wrong CONNECT target", http.StatusBadRequest)
			return
		}
		proxy.count.Add(1)
		remote, _, err := net.SplitHostPort(request.RemoteAddr)
		if err != nil {
			http.Error(writer, "fixture remote address unavailable", http.StatusBadRequest)
			return
		}
		proxy.mu.Lock()
		proxy.hosts = append(proxy.hosts, remote)
		proxy.mu.Unlock()
		upstream, err := (&net.Dialer{}).DialContext(request.Context(), "tcp", target)
		if err != nil {
			http.Error(writer, "fixture origin unavailable", http.StatusBadGateway)
			return
		}
		client, buffered, err := writer.(http.Hijacker).Hijack()
		if err != nil {
			_ = upstream.Close()
			return
		}
		workers.Add(1)
		defer workers.Done()
		defer client.Close()
		defer upstream.Close()
		if _, err := buffered.WriteString("HTTP/1.1 200 Connection Established\r\n\r\n"); err != nil {
			return
		}
		if err := buffered.Flush(); err != nil {
			return
		}
		done := make(chan struct{})
		go func() {
			defer close(done)
			_, _ = io.Copy(upstream, buffered)
			_ = upstream.Close()
			_ = client.Close()
		}()
		_, _ = io.Copy(client, upstream)
		_ = client.Close()
		_ = upstream.Close()
		<-done
	}))
	address, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	address.Host = net.JoinHostPort("controlled-proxy.invalid", address.Port())
	proxy.url = address.String()
	t.Cleanup(func() {
		server.Close()
		done := make(chan struct{})
		go func() { workers.Wait(); close(done) }()
		select {
		case <-done:
		case <-testContext(t).Done():
			t.Error("fixture CONNECT workers retained connections")
		}
	})
	return proxy
}

func controlledFrozenDialerProvider(t *testing.T, options OptionsV1) *providerFixture {
	t.Helper()
	selected, err := Select(options)
	if err != nil {
		t.Fatal(err)
	}
	limits, err := LimitsV1(options)
	if err != nil {
		t.Fatal(err)
	}
	selected = resource.WithLimits(selected, limits)
	options.Native.Dialer.ControlContext = func(context.Context, string, string, syscall.RawConn) error {
		return errors.New("mutated caller dialer must not be consulted")
	}
	options.Native.LocalAddr.IP[3] = 9
	assembly, err := resource.Assemble(testContext(t), testContext(t), "controlled-proxy-frozen", selected)
	if err != nil {
		t.Fatal(err)
	}
	inbox, err := invocation.NewInbox[Result](1, defaults(options).evidenceBytes())
	if err != nil {
		t.Fatal(err)
	}
	client, err := Bind(assembly, selected, inbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	fixture := &providerFixture{client: client, selected: selected, assembly: assembly, inbox: inbox}
	t.Cleanup(func() {
		if err := assembly.Close(testContext(t)); err != nil {
			t.Error("frozen dialer owner cleanup", err)
		}
	})
	return fixture
}

func TestBuiltinProxyDialerResolverLocalBindAndCopy(t *testing.T) {
	for _, driver := range []string{"native", "internal"} {
		t.Run(driver, func(t *testing.T) {
			var originRequests atomic.Int64
			endpoint, options := providerPeer(t, HTTP1Only, func(writer http.ResponseWriter, request *http.Request) {
				originRequests.Add(1)
				_, _ = io.WriteString(writer, "built-in-connect")
			})
			proxy := controlledConnectProxy(t, strings.TrimPrefix(endpoint, "https://"))
			dns := controlledNewDNS(t)
			var controls, descriptors atomic.Int64
			dialer := &net.Dialer{Resolver: &net.Resolver{PreferGo: true, Dial: dns.dial}, ControlContext: func(ctx context.Context, network, address string, raw syscall.RawConn) error {
				controls.Add(1)
				if ctx == nil || !strings.HasPrefix(network, "tcp") || !strings.HasPrefix(address, "127.0.0.1:") {
					return errors.New("dialer control received unexpected route")
				}
				return raw.Control(func(uintptr) { descriptors.Add(1) })
			}}
			local := &net.TCPAddr{IP: net.IP{127, 0, 0, 2}}
			if driver == "native" {
				client, err := sdk.NewHttpClient(sdk.NewNoopLogger(), sdk.WithClientProfile(*options.Native.Profile), sdk.WithTransportOptions(options.Native.Transport),
					sdk.WithForceHttp1(), sdk.WithDisableHttp3(), sdk.WithProxyUrl(proxy.url), sdk.WithDialer(*dialer), sdk.WithLocalAddr(*local), sdk.WithTimeoutSeconds(5))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = sdk.Close(client) })
				response, err := client.Do(providerRequest(t, "GET", endpoint, nil).WithContext(testContext(t)))
				if err != nil {
					t.Fatal(err)
				}
				content, err := io.ReadAll(response.Body)
				if closeErr := response.Body.Close(); err != nil || closeErr != nil || string(content) != "built-in-connect" {
					t.Fatal("native proxy response", err, closeErr)
				}
				if err := sdk.Close(client); err != nil {
					t.Fatal(err)
				}
			} else {
				options.ProxyURL, options.Native.Dialer, options.Native.LocalAddr = proxy.url, dialer, local
				fixture := controlledFrozenDialerProvider(t, options)
				receipt, err := fixture.client.Do(testContext(t), testContext(t), providerID("controlled-proxy-dialer"), providerRequest(t, "GET", endpoint, nil))
				if err != nil {
					t.Fatal(err)
				}
				result := settleProvider(t, fixture, receipt)
				if result.Err() != nil || !result.Outcome.Value.Complete() || string(result.Outcome.Value.DataCopy()) != "built-in-connect" {
					t.Fatal("controlled built-in proxy result", result.Err())
				}
				fixture.client.owner.mu.Lock()
				active := fixture.client.owner.tcp
				fixture.client.owner.mu.Unlock()
				if active < 1 {
					t.Fatal("built-in proxy bypassed source TCP accounting")
				}
				if err := fixture.assembly.Close(testContext(t)); err != nil {
					t.Fatal(err)
				}
				fixture.client.owner.mu.Lock()
				remaining := fixture.client.owner.tcp
				fixture.client.owner.mu.Unlock()
				if remaining != 0 {
					t.Fatal("confirmed source release retained TCP allowance")
				}
			}
			proxy.mu.Lock()
			hosts := append([]string(nil), proxy.hosts...)
			proxy.mu.Unlock()
			if proxy.count.Load() != 1 || originRequests.Load() != 1 || len(hosts) != 1 || hosts[0] != "127.0.0.2" ||
				dns.queries.Load() == 0 || dns.dials.Load() == 0 || controls.Load() == 0 || descriptors.Load() != controls.Load() || dns.closes.Load() != dns.dials.Load() {
				t.Fatalf("native dialer effect missing: proxy=%d origin=%d hosts=%v DNS queries/dials/closes=%d/%d/%d controls/descriptors=%d/%d",
					proxy.count.Load(), originRequests.Load(), hosts, dns.queries.Load(), dns.dials.Load(), dns.closes.Load(), controls.Load(), descriptors.Load())
			}
		})
	}
}

func TestBuiltinDialerH3InapplicabilityRejected(t *testing.T) {
	for _, option := range []string{"dialer", "local"} {
		t.Run(option, func(t *testing.T) {
			options := providerOptions()
			options.Mode = HTTP3Racing
			if option == "dialer" {
				options.Native.Dialer = new(net.Dialer)
			} else {
				options.Native.LocalAddr = &net.TCPAddr{IP: net.IP{127, 0, 0, 2}}
			}
			if _, err := Select(options); !errors.Is(err, ErrUnsupported) {
				t.Fatal("TCP-only control was accepted as effective in H3 racing", err)
			}
		})
	}
}

func TestBuiltinDialerControlCancellationRetainsOwnership(t *testing.T) {
	endpoint, options := providerPeer(t, HTTP1Only, func(writer http.ResponseWriter, request *http.Request) {
		t.Error("canceled socket control reached the origin")
	})
	proxy := controlledConnectProxy(t, strings.TrimPrefix(endpoint, "https://"))
	dns := controlledNewDNS(t)
	entered, resume := make(chan struct{}), make(chan struct{})
	resumeControl := sync.OnceFunc(func() { close(resume) })
	defer resumeControl()
	options.ProxyURL = proxy.url
	options.Native.Dialer = &net.Dialer{Resolver: &net.Resolver{PreferGo: true, Dial: dns.dial}, ControlContext: func(ctx context.Context, _ string, _ string, _ syscall.RawConn) error {
		close(entered)
		<-resume
		return ctx.Err()
	}}
	fixture := bindProvider(t, options, 1)
	ctx, cancel := context.WithCancel(testContext(t))
	defer cancel()
	done := make(chan *invocation.Receipt[Result], 1)
	go func() {
		receipt, _ := fixture.client.Do(ctx, context.Background(), providerID("controlled-control-cancel"), providerRequest(t, "GET", endpoint, nil))
		done <- receipt
	}()
	select {
	case <-entered:
	case <-testContext(t).Done():
		t.Fatal("custom built-in socket control was not invoked")
	}
	cancel()
	var receipt *invocation.Receipt[Result]
	select {
	case receipt = <-done:
	case <-testContext(t).Done():
		t.Fatal("socket-control cancellation waiter did not return")
	}
	if receipt == nil {
		t.Fatal("accepted socket control lost its evidence")
	}
	short, stop := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer stop()
	if _, err := receipt.WaitReleased(short); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("noncooperative socket control released early", err)
	}
	fixture.client.owner.mu.Lock()
	held := fixture.client.owner.tcp
	fixture.client.owner.mu.Unlock()
	if held < 1 || proxy.count.Load() != 0 {
		t.Fatal("pending socket control lost quota or performed CONNECT")
	}
	resumeControl()
	result := settleProvider(t, fixture, receipt)
	if !errors.Is(result.Err(), context.Canceled) || result.Outcome.Value.Complete() || proxy.count.Load() != 0 {
		t.Fatal("canceled control lost outcome or attempted CONNECT", result.Err())
	}
	if err := fixture.assembly.Close(testContext(t)); err != nil {
		t.Fatal(err)
	}
}
