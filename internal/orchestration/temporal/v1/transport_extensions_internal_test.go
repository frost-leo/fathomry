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

package temporal

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"
)

func TestContextDialerLateConnectionRemainsOwned(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var scope transportLifetime
		entered, proceed, dialed, closed := make(chan struct{}), make(chan struct{}), make(chan error, 1), make(chan struct{})
		connection := &reviewBlockingTLSConn{}
		var calls atomic.Int32
		dial := func(context.Context, string) (net.Conn, error) {
			calls.Add(1)
			close(entered)
			<-proceed
			return connection, nil
		}
		go func() { _, err := scope.dial(dial, context.Background(), "fixture"); dialed <- err }()
		<-entered
		go func() { scope.close(func() {}); close(closed) }()
		synctest.Wait()
		select {
		case <-closed:
			t.Fatal("source released an entered context dialer")
		default:
		}
		close(proceed)
		if err := <-dialed; !errors.Is(err, net.ErrClosed) {
			t.Fatal("late connection was accepted", err)
		}
		<-closed
		if connection.closes.Load() != 1 {
			t.Fatal("late returned connection leaked")
		}
		if _, err := scope.dial(dial, context.Background(), "fixture"); !errors.Is(err, net.ErrClosed) || calls.Load() != 1 {
			t.Fatal("released dialer was reentered", err)
		}
	})
}

func TestContextDialerConnectionIOAndCloseAreJoined(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var scope transportLifetime
		native := &reviewBlockingTLSConn{readEntered: make(chan struct{}), readRelease: make(chan struct{})}
		connection, err := scope.dial(func(context.Context, string) (net.Conn, error) { return native, nil }, context.Background(), "fixture")
		if err != nil {
			t.Fatal(err)
		}
		readDone, closeDone := make(chan struct{}), make(chan struct{})
		go func() { _, _ = connection.Read(make([]byte, 1)); close(readDone) }()
		<-native.readEntered
		go func() { scope.close(func() {}); close(closeDone) }()
		synctest.Wait()
		if native.closes.Load() != 1 {
			t.Fatal("source did not close transferred connection")
		}
		select {
		case <-closeDone:
			t.Fatal("source released active connection I/O")
		default:
		}
		close(native.readRelease)
		<-readDone
		<-closeDone
		if _, err := connection.Read(make([]byte, 1)); !errors.Is(err, net.ErrClosed) || native.reads.Load() != 1 {
			t.Fatal("released connection entered native I/O", err)
		}
		if err := connection.Close(); err != nil || native.closes.Load() != 1 {
			t.Fatal("late close reentered native connection", err)
		}
	})
}

func TestContextDialerClosesCanceledAndErrorReturns(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		var scope transportLifetime
		native := &reviewBlockingTLSConn{}
		ctx, cancel := context.WithCancel(context.Background())
		cause := errors.New("dial error with acquired connection")
		if canceled {
			cancel()
			cause = nil
		}
		connection, err := scope.dial(func(context.Context, string) (net.Conn, error) { return native, cause }, ctx, "fixture")
		cancel()
		if connection != nil || err == nil || native.closes.Load() != 1 {
			t.Fatal("failed returned connection leaked", err)
		}
		scope.close(func() {})
	}
}

func TestContextDialerBoundsTransferredConnections(t *testing.T) {
	var scope transportLifetime
	defer scope.close(func() {})
	var calls atomic.Int32
	dial := func(context.Context, string) (net.Conn, error) {
		calls.Add(1)
		return &reviewBlockingTLSConn{}, nil
	}
	var first net.Conn
	for index := range 32 {
		connection, err := scope.dial(dial, context.Background(), "fixture")
		if err != nil {
			t.Fatal("bounded positive connection control", err)
		}
		if index == 0 {
			first = connection
		}
	}
	if _, err := scope.dial(dial, context.Background(), "fixture"); !errors.Is(err, ErrLimit) || calls.Load() != 32 {
		t.Fatal("connection saturation entered native dialer", err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := scope.dial(dial, context.Background(), "fixture"); err != nil || calls.Load() != 33 {
		t.Fatal("closed connection did not release its slot", err)
	}
}

func TestContextDialerBoundsEnteredAttempts(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var scope transportLifetime
		entered, proceed, completed := make(chan struct{}, 32), make(chan struct{}), make(chan error, 32)
		var release sync.Once
		defer release.Do(func() { close(proceed) })
		for range 32 {
			go func() {
				_, err := scope.dial(func(context.Context, string) (net.Conn, error) {
					entered <- struct{}{}
					<-proceed
					return &reviewBlockingTLSConn{}, nil
				}, context.Background(), "fixture")
				completed <- err
			}()
		}
		for range 32 {
			<-entered
		}
		called := false
		_, err := scope.dial(func(context.Context, string) (net.Conn, error) {
			called = true
			return &reviewBlockingTLSConn{}, nil
		}, context.Background(), "fixture")
		if !errors.Is(err, ErrLimit) || called {
			t.Error("attempt saturation entered native callback", err)
		}
		release.Do(func() { close(proceed) })
		for range 32 {
			if err := <-completed; err != nil {
				t.Error("admitted attempt did not complete", err)
			}
		}
		scope.close(func() {})
	})
}

type resolverClientProbe struct {
	updates atomic.Int32
	errors  atomic.Int32
	state   resolver.State
}

func (client *resolverClientProbe) UpdateState(state resolver.State) error {
	client.updates.Add(1)
	client.state = state
	return nil
}
func (client *resolverClientProbe) ReportError(error)      { client.errors.Add(1) }
func (*resolverClientProbe) NewAddress([]resolver.Address) {}
func (*resolverClientProbe) ParseServiceConfig(string) *serviceconfig.ParseResult {
	return &serviceconfig.ParseResult{}
}

type resolverBuilderProbe struct {
	build func(resolver.Target, resolver.ClientConn, resolver.BuildOptions) (resolver.Resolver, error)
}

func (*resolverBuilderProbe) Scheme() string { return "fixture" }
func (builder *resolverBuilderProbe) Build(target resolver.Target, client resolver.ClientConn, options resolver.BuildOptions) (resolver.Resolver, error) {
	return builder.build(target, client, options)
}

type resolverProbe struct {
	resolve func()
	close   func()
}

func (native *resolverProbe) ResolveNow(resolver.ResolveNowOptions) {
	if native.resolve != nil {
		native.resolve()
	}
}
func (native *resolverProbe) Close() {
	if native.close != nil {
		native.close()
	}
}

func TestResolverBuildAndLateResultRemainOwned(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var scope transportLifetime
		entered, proceed, completed, released := make(chan struct{}), make(chan struct{}), make(chan error, 1), make(chan struct{})
		var closed atomic.Int32
		var retained resolver.ClientConn
		native := &resolverBuilderProbe{build: func(_ resolver.Target, client resolver.ClientConn, options resolver.BuildOptions) (resolver.Resolver, error) {
			retained = client
			if !options.DisableServiceConfig {
				t.Error("service config override enabled")
			}
			close(entered)
			<-proceed
			return &resolverProbe{close: func() { closed.Add(1) }}, nil
		}}
		builder := &ownedResolverBuilder{scope: &scope, binding: ResolverBinding{Scheme: "fixture", Builder: native}}
		go func() {
			_, err := builder.Build(resolver.Target{}, &resolverClientProbe{}, resolver.BuildOptions{})
			completed <- err
		}()
		<-entered
		go func() { scope.close(func() {}); close(released) }()
		synctest.Wait()
		select {
		case <-released:
			t.Fatal("source released during resolver Build")
		default:
		}
		close(proceed)
		if err := <-completed; !errors.Is(err, net.ErrClosed) {
			t.Fatal("late resolver accepted", err)
		}
		<-released
		if closed.Load() != 1 {
			t.Fatal("late resolver not closed")
		}
		if err := retained.UpdateState(resolver.State{}); !errors.Is(err, net.ErrClosed) {
			t.Fatal("late resolver callback accepted", err)
		}
	})
}

func TestResolverResolveAndCloseCallbacksRemainOwned(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var scope transportLifetime
		entered, proceed, completed, released := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
		var closed atomic.Int32
		native := &resolverBuilderProbe{build: func(_ resolver.Target, _ resolver.ClientConn, _ resolver.BuildOptions) (resolver.Resolver, error) {
			return &resolverProbe{resolve: func() { close(entered); <-proceed }, close: func() { closed.Add(1) }}, nil
		}}
		builder := &ownedResolverBuilder{scope: &scope, binding: ResolverBinding{Scheme: "fixture", Builder: native}}
		owned, err := builder.Build(resolver.Target{}, &resolverClientProbe{}, resolver.BuildOptions{})
		if err != nil {
			t.Fatal(err)
		}
		go func() { owned.ResolveNow(resolver.ResolveNowOptions{}); close(completed) }()
		<-entered
		go func() { scope.close(func() {}); close(released) }()
		synctest.Wait()
		if closed.Load() != 1 {
			t.Fatal("native resolver was not closed")
		}
		select {
		case <-released:
			t.Fatal("active ResolveNow callback was released")
		default:
		}
		close(proceed)
		<-completed
		<-released
		owned.Close()
		if closed.Load() != 1 {
			t.Fatal("resolver close repeated after release")
		}
	})
}

func TestResolverStateRefusesOverridesAndCopiesContainers(t *testing.T) {
	var scope transportLifetime
	native := &resolverClientProbe{}
	client := &ownedResolverClient{native: native, scope: &scope, authority: "selected"}
	if result := client.ParseServiceConfig("{}"); !errors.Is(result.Err, ErrAuthority) {
		t.Fatal("service-config parse accepted")
	}
	for _, state := range []resolver.State{
		{ServiceConfig: &serviceconfig.ParseResult{}},
		{Addresses: []resolver.Address{{Addr: "127.0.0.1:7233", ServerName: "different"}}},
		{Addresses: make([]resolver.Address, maxResolverAddresses+1)},
	} {
		if err := client.UpdateState(state); err == nil || native.updates.Load() != 0 {
			t.Fatal("resolver override accepted", err)
		}
	}
	state := resolver.State{Addresses: []resolver.Address{{Addr: "127.0.0.1:7233"}},
		Endpoints: []resolver.Endpoint{{Addresses: []resolver.Address{{Addr: "127.0.0.1:7233"}}}}}
	if err := client.UpdateState(state); err != nil {
		t.Fatal(err)
	}
	state.Addresses[0].Addr = "mutated"
	state.Endpoints[0].Addresses[0].Addr = "mutated"
	if native.state.Addresses[0].Addr != "127.0.0.1:7233" || native.state.Endpoints[0].Addresses[0].Addr != "127.0.0.1:7233" {
		t.Fatal("resolver state retained mutable slice containers")
	}
	scope.close(func() {})
	if err := client.UpdateState(resolver.State{}); !errors.Is(err, net.ErrClosed) {
		t.Fatal("released callback accepted", err)
	}
}
