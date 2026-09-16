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
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"

	"github.com/enetx/g"
	http "github.com/enetx/http"
	sdk "github.com/enetx/surf"
	"github.com/frost-leo/fathomry/internal/resource"
	utls "github.com/refraction-networking/utls"
)

type routeKey struct {
	proxy   string
	headers [32]byte
}
type routeBinding struct {
	client *sdk.Client
	ready  chan struct{}
	err    error
	retry  bool
}
type owner struct {
	settings  settings
	native    NativeOptionsV1
	work      *activity
	mu        sync.Mutex
	routes    map[routeKey]*routeBinding
	tcp, udp  int
	stopping  bool
	closeOnce sync.Once
	done      chan struct{}
	cleanup   error
}

func routeIdentity(route routeChoice) routeKey {
	keys := make([]string, 0, len(route.headers))
	for key := range route.headers {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var identity strings.Builder
	for _, key := range keys {
		fmt.Fprintf(&identity, "%d:%s:", len(key), key)
		for _, value := range route.headers[key] {
			fmt.Fprintf(&identity, "%d:%s:", len(value), value)
		}
		identity.WriteByte(0)
	}
	return routeKey{proxy: route.proxy, headers: sha256.Sum256([]byte(identity.String()))}
}
func (own *owner) reserve(packet bool) (func(), error) {
	own.mu.Lock()
	if own.stopping || !packet && own.tcp >= own.settings.MaxTCPConnections || packet && own.udp >= own.settings.MaxUDPSockets {
		own.mu.Unlock()
		return nil, failure(ErrLimit, "native-capacity", resource.ErrCapacity)
	}
	if packet {
		own.udp++
	} else {
		own.tcp++
	}
	own.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			own.mu.Lock()
			defer own.mu.Unlock()
			if packet {
				own.udp--
			} else {
				own.tcp--
			}
		})
	}, nil
}
func (own *owner) enter(ctx context.Context) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, errors.Join(err, context.Cause(ctx))
	}
	if !own.work.enter() {
		return nil, failure(ErrState, "native-work")
	}
	op, _ := ctx.Value(operationKey{}).(*operation)
	if op != nil && !op.work.enter() {
		own.work.leave()
		return nil, failure(ErrState, "call-ended")
	}
	return func() {
		if op != nil {
			op.work.leave()
		}
		own.work.leave()
	}, nil
}
func (own *owner) binding(ctx context.Context, route routeChoice) (*routeBinding, error) {
	key := routeIdentity(route)
	for {
		own.mu.Lock()
		if own.stopping {
			own.mu.Unlock()
			return nil, failure(ErrState, "route")
		}
		if existing := own.routes[key]; existing != nil {
			own.mu.Unlock()
			select {
			case <-existing.ready:
				if err := ctx.Err(); err != nil {
					return nil, errors.Join(err, context.Cause(ctx))
				}
				if existing.retry {
					continue
				}
				return existing, existing.err
			case <-ctx.Done():
				return nil, errors.Join(ctx.Err(), context.Cause(ctx))
			}
		}
		if len(own.routes) >= own.settings.MaxRoutes {
			own.mu.Unlock()
			return nil, failure(ErrLimit, "routes", resource.ErrCapacity)
		}
		binding := &routeBinding{ready: make(chan struct{})}
		own.routes[key] = binding
		own.mu.Unlock()
		binding.client, binding.err = own.nativeClient(ctx, route)
		if binding.err != nil && (binding.client == nil || binding.client.FathomryQuiescent()) {
			own.mu.Lock()
			if own.routes[key] == binding {
				delete(own.routes, key)
				binding.retry = ctx.Err() != nil
			}
			own.mu.Unlock()
		}
		close(binding.ready)
		return binding, binding.err
	}
}
func (own *owner) nativeClient(ctx context.Context, route routeChoice) (client *sdk.Client, err error) {
	returned := false
	defer func() {
		if !returned {
			cause, _ := recover().(error)
			err = failure(ErrCallback, "native-construction", cause)
		}
		if err != nil && client != nil {
			if cleanup := client.Close(); cleanup != nil {
				err = &sdk.FathomryFailure{Primary: err, Cleanup: cleanup}
			}
		}
	}()
	native, err := copyNative(own.native)
	if err != nil {
		returned = true
		return nil, err
	}
	guardCallbacks(&native)
	client = sdk.NewClient()
	control := sdk.FathomryControlV1{
		ConstructionContext: ctx,
		BindContext: func(native context.Context) (context.Context, func()) {
			op, _ := native.Value(operationKey{}).(*operation)
			if op == nil {
				return native, func() {}
			}
			work, cancel := context.WithCancelCause(native)
			stop := context.AfterFunc(op.ctx, func() { cancel(context.Cause(op.ctx)) })
			if op.ctx.Err() != nil {
				cancel(context.Cause(op.ctx))
			}
			var deadlineCancel context.CancelFunc = func() {}
			var bound context.Context = work
			if deadline, ok := op.ctx.Deadline(); ok {
				bound, deadlineCancel = context.WithDeadline(work, deadline)
			}
			return bound, func() { stop(); deadlineCancel(); cancel(nil) }
		},
		AcquireTCP: func() (func(), error) { return own.reserve(false) },
		AcquireUDP: func() (func(), error) { return own.reserve(true) },
		Enter:      own.enter, DialContext: native.DialContext, ListenPacket: native.ListenPacket,
		ProxyTLSConfig: native.ProxyTLSConfig, JAConfig: native.JAConfig, HelloSpecFactory: native.HelloSpecFactory,
		MaxRequestBytes: own.settings.MaxRequestBytes, MaxResponseBytes: own.settings.MaxResponseBytes, MaxHeaderBytes: own.settings.MaxNativeHeaderBytes, MaxProxyHeaderBytes: own.settings.MaxHeaderBytes,
		RequestClient: func(ctx context.Context, value http.Client) (http.Client, error) {
			op, _ := ctx.Value(operationKey{}).(*operation)
			if op == nil {
				return value, failure(ErrState, "unmanaged-native-client")
			}
			value.CheckRedirect = op.redirect
			if native.Jar != nil {
				value.Jar = operationJar{op: op, jar: native.Jar}
			}
			return value, nil
		},
	}
	if err := client.ConfigureFathomry(control); err != nil {
		returned = true
		return client, err
	}
	if native.TLSConfig != nil {
		if err := client.FathomrySetTLSConfig(native.TLSConfig); err != nil {
			returned = true
			return client, err
		}
	}
	if native.Resolver != nil {
		client.GetDialer().Resolver = native.Resolver
	}
	transport := client.GetTransport().(*http.Transport)
	transport.MaxConnsPerHost = own.settings.MaxTCPConnections
	transport.MaxIdleConns = own.settings.MaxTCPConnections
	transport.MaxIdleConnsPerHost = own.settings.MaxTCPConnections
	transport.MaxResponseHeaderBytes = own.settings.MaxNativeHeaderBytes
	transport.IdleConnTimeout = own.settings.IdleConnTimeout
	builder := client.Builder().Proxy(g.String(route.proxy)).Timeout(own.settings.Timeout).MaxRedirects(own.settings.MaxRoundTrips)
	if own.settings.DisableCompression {
		builder.DisableCompression()
	}
	if native.TLSConfig == nil {
		builder.SecureTLS()
	}
	switch own.settings.Mode {
	case HTTP1Only:
		builder.ForceHTTP1()
	case HTTP2Only:
		builder.ForceHTTP2()
	case PreferHTTP3:
		builder.ForceHTTP3()
	}
	if native.Profile != nil {
		builder.FathomryApplyVariant(*native.Profile, native.OS)
	} else {
		if native.HelloSpecFactory != nil {
			builder.JA().SetHelloSpec(utls.ClientHelloSpec{})
		}
		if own.settings.Mode == PreferHTTP3 {
			builder.HTTP3Settings().Set()
		}
	}
	if own.settings.NativeRetries > 0 {
		builder.Retry(own.settings.NativeRetries, own.settings.RetryDelay, own.settings.RetryCodes...)
	}
	builder.With(func(request *sdk.Request) error {
		op, _ := request.GetRequest().Context().Value(operationKey{}).(*operation)
		if op == nil {
			return failure(ErrState, "native-request")
		}
		return op.prepare(request)
	})
	builder.With(func(response *sdk.Response) error {
		request := response.GetResponse().Request
		op, _ := request.Context().Value(operationKey{}).(*operation)
		if op == nil {
			return failure(ErrState, "native-response")
		}
		return op.observe(response)
	})
	result := builder.Build()
	if result.IsErr() {
		returned = true
		return client, result.Err()
	}
	if err := ctx.Err(); err != nil {
		returned = true
		return client, errors.Join(err, context.Cause(ctx))
	}
	raw := client.GetClient().Transport
	client.GetClient().Transport = routeTransport{raw: raw, headers: route.headers}
	returned = true
	return client, nil
}

type routeTransport struct {
	raw     http.RoundTripper
	headers http.Header
}

func (transport routeTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	op, _ := request.Context().Value(operationKey{}).(*operation)
	if op == nil {
		return nil, failure(ErrState, "native-bypass")
	}
	return op.roundTrip(transport.raw, request, transport.headers)
}
func (transport routeTransport) Close() error {
	if closer, ok := transport.raw.(io.Closer); ok {
		return closer.Close()
	}
	transport.CloseIdleConnections()
	return nil
}
func (transport routeTransport) CloseIdleConnections() {
	if closer, ok := transport.raw.(interface{ CloseIdleConnections() }); ok {
		closer.CloseIdleConnections()
	}
}
func (own *owner) release(ctx context.Context) resource.ReleaseResult {
	own.closeOnce.Do(func() {
		own.mu.Lock()
		own.stopping = true
		own.done = make(chan struct{})
		bindings := make([]*routeBinding, 0, len(own.routes))
		for _, binding := range own.routes {
			bindings = append(bindings, binding)
		}
		own.mu.Unlock()
		go func() {
			var causes []error
			for _, binding := range bindings {
				<-binding.ready
				if binding.client != nil {
					if err := binding.client.Close(); err != nil {
						causes = append(causes, err)
					}
				}
			}
			<-own.work.stop()
			own.mu.Lock()
			if own.tcp != 0 || own.udp != 0 {
				causes = append(causes, failure(ErrState, "native-release-unconfirmed"))
			}
			own.cleanup = errors.Join(causes...)
			close(own.done)
			own.mu.Unlock()
		}()
	})
	select {
	case <-own.done:
		own.mu.Lock()
		defer own.mu.Unlock()
		confirmed := own.tcp == 0 && own.udp == 0
		result := resource.ReleaseResult{Quiescent: confirmed, Released: confirmed, Err: own.cleanup}
		if !confirmed {
			result.Continue = own.release
		}
		return result
	case <-ctx.Done():
		return resource.ReleaseResult{Err: failure(ErrCleanup, "release-wait", ctx.Err(), context.Cause(ctx)), Continue: own.release}
	}
}
