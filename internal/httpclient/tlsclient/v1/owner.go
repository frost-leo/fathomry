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
	"net"
	"net/url"
	"sync"
	"time"

	http "github.com/bogdanfinn/fhttp"
	sdk "github.com/bogdanfinn/tls-client"
	"github.com/frost-leo/fathomry/internal/resource"
	"golang.org/x/net/proxy"
)

type bindingKey struct {
	scheme, authority, proxy string
	headers                  [32]byte
	factory                  bool
}
type binding struct {
	key      bindingKey
	native   sdk.HttpClient
	ready    chan struct{}
	err      error
	users    int
	released bool
}
type owner struct {
	settings    settings
	native      NativeOptionsV1
	callbacks   *activity
	mu          sync.Mutex
	bindings    map[bindingKey]*binding
	held        map[*binding]struct{}
	retiring    int
	tcp, h3     int
	stopping    bool
	cleanup     error
	releaseMu   sync.Mutex
	releaseDone chan struct{}
}

func (own *owner) acquire(h3 bool) (func(), error) {
	own.mu.Lock()
	current, maximum := own.tcp, own.settings.MaxTCPConnections
	if h3 {
		current, maximum = own.h3, own.settings.MaxHTTP3Transports
	}
	if own.stopping || current >= maximum {
		own.mu.Unlock()
		return nil, failure(ErrLimit, "native-capacity", resource.ErrCapacity)
	}
	if h3 {
		own.h3++
	} else {
		own.tcp++
	}
	own.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			own.mu.Lock()
			if h3 {
				own.h3--
			} else {
				own.tcp--
			}
			own.mu.Unlock()
		})
	}, nil
}
func (own *owner) recordCleanup(err error) {
	if err == nil {
		return
	}
	own.mu.Lock()
	defer own.mu.Unlock()
	if own.cleanup == nil {
		own.cleanup = failure(ErrCleanup, "native", err)
	}
}
func (own *owner) nativeClient(key bindingKey, route routeChoice) (sdk.HttpClient, error) {
	options := []sdk.HttpClientOption{
		sdk.WithClientProfile(own.profile()), sdk.WithTransportOptions(own.transportOptions()),
		sdk.WithTimeoutMilliseconds(int(own.settings.Timeout / time.Millisecond)), sdk.WithNotFollowRedirects(),
	}
	switch own.settings.Mode {
	case HTTP1Only:
		options = append(options, sdk.WithForceHttp1(), sdk.WithDisableHttp3())
	case HTTP3Racing:
		options = append(options, sdk.WithProtocolRacing())
	default:
		options = append(options, sdk.WithDisableHttp3())
	}
	if route.proxy != "" {
		address, _ := url.Parse(route.proxy)
		if address.Scheme == "http" || address.Scheme == "https" {
			address.User = nil
		}
		options = append(options, sdk.WithProxyUrl(address.String()))
	}
	options = append(options, sdk.WithConnectHeaders(route.headers.Clone()))
	if own.settings.ServerName != "" {
		options = append(options, sdk.WithServerNameOverwrite(own.settings.ServerName))
	}
	if own.settings.InsecureSkipVerify {
		options = append(options, sdk.WithInsecureSkipVerify())
	}
	if own.settings.RandomTLSExtensionOrder {
		options = append(options, sdk.WithRandomTLSExtensionOrder())
	}
	if own.settings.DisableSessionTickets {
		options = append(options, sdk.WithDisableSessionTickets())
	}
	if own.settings.DisableIPV4 {
		options = append(options, sdk.WithDisableIPV4())
	}
	if own.settings.DisableIPV6 {
		options = append(options, sdk.WithDisableIPV6())
	}
	if own.native.DialContext != nil {
		options = append(options, sdk.WithDialContext(func(ctx context.Context, network, address string) (net.Conn, error) {
			done, err := own.enterCallback(ctx)
			if err != nil {
				return nil, err
			}
			defer done()
			return own.native.DialContext(ctx, network, address)
		}))
	}
	if key.factory {
		factory := own.native.ProxyDialerFactory
		options = append(options, sdk.WithProxyDialerFactory(func(value string, timeout time.Duration, local *net.TCPAddr, headers http.Header, logger sdk.Logger) (proxy.ContextDialer, error) {
			if !own.callbacks.enter() {
				return nil, failure(ErrState, "proxy-factory")
			}
			defer own.callbacks.leave()
			native, err := factory(value, timeout, local, headers.Clone(), logger)
			if native == nil || nilLike(native) {
				return nil, errors.Join(failure(ErrInput, "proxy-dialer"), err)
			}
			if err != nil {
				return nil, err
			}
			return guardedDialer{own, native}, nil
		}))
	}
	if len(own.native.CertificatePins) > 0 {
		copied, _ := copyNative(own.native)
		handler := sdk.BadPinHandlerFunc(nil)
		if own.native.BadPinHandler != nil {
			handler = func(request *http.Request) {
				done, err := own.enterCallback(request.Context())
				if err != nil {
					return
				}
				defer done()
				own.native.BadPinHandler(previewRequest(request))
			}
		}
		options = append(options, sdk.WithCertificatePinning(copied.CertificatePins, handler))
	}
	client, err := sdk.NewHttpClient(sdk.NewNoopLogger(), options...)
	if err != nil {
		return nil, err
	}
	if err := sdk.ConfigureFathomry(client, sdk.FathomryControlV1{AcquireTCP: func() (func(), error) { return own.acquire(false) }, AcquireHTTP3: func() (func(), error) { return own.acquire(true) }, MaxProxyHeaderBytes: own.settings.MaxHeaderBytes}); err != nil {
		own.recordCleanup(sdk.Close(client))
		return nil, err
	}
	return client, nil
}

type guardedDialer struct {
	owner  *owner
	native proxy.ContextDialer
}

func (dialer guardedDialer) Dial(network, address string) (net.Conn, error) {
	return dialer.DialContext(context.Background(), network, address)
}
func (dialer guardedDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	done, err := dialer.owner.enterCallback(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	return dialer.native.DialContext(ctx, network, address)
}
func (own *owner) getBinding(ctx context.Context, address *url.URL, route routeChoice) (*binding, error) {
	key := bindingKey{scheme: address.Scheme, authority: authority(address), proxy: route.proxy, headers: route.digest,
		factory: route.selection == ProxyFromProvider && own.native.ProxyDialerFactory != nil}
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		own.mu.Lock()
		if own.stopping {
			own.mu.Unlock()
			return nil, failure(ErrState, "source-closed")
		}
		if current := own.bindings[key]; current != nil {
			ready := current.ready
			select {
			case <-ready:
				current.users++
				own.mu.Unlock()
				return current, nil
			default:
				own.mu.Unlock()
				select {
				case <-ready:
					if current.err != nil {
						return nil, current.err
					}
				case <-ctx.Done():
					return nil, ctx.Err()
				}
				continue
			}
		}
		if len(own.bindings)+own.retiring < own.settings.MaxBindings {
			created := &binding{key: key, ready: make(chan struct{}), users: 1}
			own.bindings[key] = created
			own.mu.Unlock()
			native, err := own.nativeClient(key, route)
			own.mu.Lock()
			created.native, created.err = native, err
			if err != nil {
				delete(own.bindings, key)
			}
			close(created.ready)
			own.mu.Unlock()
			if err != nil {
				return nil, err
			}
			return created, nil
		}
		var retire *binding
		for _, candidate := range own.bindings {
			select {
			case <-candidate.ready:
				if candidate.users == 0 {
					retire = candidate
				}
			default:
			}
			if retire != nil {
				break
			}
		}
		if retire == nil {
			own.mu.Unlock()
			return nil, failure(ErrLimit, "native-bindings", resource.ErrCapacity)
		}
		delete(own.bindings, retire.key)
		own.retiring++
		own.mu.Unlock()
		own.retire(retire)
	}
}
func (own *owner) returnBinding(current *binding) {
	own.mu.Lock()
	current.users--
	own.mu.Unlock()
}
func (own *owner) retire(current *binding) {
	err := sdk.Close(current.native)
	own.recordCleanup(err)
	complete := sdk.FathomryQuiescent(current.native)
	own.mu.Lock()
	if complete {
		if !current.released {
			own.retiring--
			current.released = true
		}
		delete(own.held, current)
	} else {
		own.held[current] = struct{}{}
	}
	own.mu.Unlock()
}
func (own *owner) release(ctx context.Context) resource.ReleaseResult {
	own.releaseMu.Lock()
	if own.releaseDone == nil {
		done := make(chan struct{})
		own.releaseDone = done
		own.mu.Lock()
		own.stopping = true
		var targets []*binding
		for _, current := range own.bindings {
			targets = append(targets, current)
			own.retiring++
		}
		clear(own.bindings)
		for current := range own.held {
			targets = append(targets, current)
		}
		own.mu.Unlock()
		own.callbacks.stop()
		go func() {
			for _, current := range targets {
				<-current.ready
				if current.native != nil {
					own.retire(current)
				}
			}
			<-own.callbacks.done
			close(done)
		}()
	}
	done := own.releaseDone
	own.releaseMu.Unlock()
	select {
	case <-done:
		own.mu.Lock()
		complete := own.retiring == 0 && own.tcp == 0 && own.h3 == 0
		err := own.cleanup
		own.mu.Unlock()
		if complete {
			return resource.ReleaseResult{Quiescent: true, Released: true, Err: err}
		}
		own.releaseMu.Lock()
		if own.releaseDone == done {
			own.releaseDone = nil
		}
		own.releaseMu.Unlock()
		return resource.ReleaseResult{Err: err, Continue: own.release}
	case <-ctx.Done():
		return resource.ReleaseResult{Err: errors.Join(ctx.Err(), context.Cause(ctx)), Continue: own.release}
	}
}
