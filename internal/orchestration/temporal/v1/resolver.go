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
	"net"
	"slices"
	"strings"
	"sync"
	"sync/atomic"

	"google.golang.org/grpc"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/resolver/dns"
	"google.golang.org/grpc/serviceconfig"
)

const maxResolverAddresses = 256

func nativeEndpoint(endpoint string) string {
	if !strings.Contains(endpoint, "://") {
		return "dns:///" + endpoint
	}
	return endpoint
}

func (scope *transportLifetime) dialOptions(options []grpc.DialOption, runtime RuntimeOptions) []grpc.DialOption {
	bindings := []ResolverBinding{{Scheme: "dns", Builder: dns.NewBuilder()}, {Scheme: "passthrough", Builder: passthroughBuilder{}}}
	for _, binding := range runtime.ResolverBuilders {
		replaced := false
		for index := range bindings {
			if bindings[index].Scheme == binding.Scheme {
				bindings[index] = binding
				replaced = true
			}
		}
		if !replaced {
			bindings = append(bindings, binding)
		}
	}
	builders := make([]resolver.Builder, len(bindings))
	for index, binding := range bindings {
		builders[index] = &ownedResolverBuilder{binding: binding, scope: scope}
	}
	options = append(options, grpc.WithResolvers(builders...))
	if runtime.ContextDialer != nil {
		options = append(options, grpc.WithContextDialer(func(ctx context.Context, address string) (net.Conn, error) {
			return scope.dial(runtime.ContextDialer, ctx, address)
		}))
	}
	if runtime.UserAgent != "" {
		options = append(options, grpc.WithUserAgent(runtime.UserAgent))
	}
	return options
}

type ownedResolverBuilder struct {
	binding ResolverBinding
	scope   *transportLifetime
}

func (builder *ownedResolverBuilder) Scheme() string { return builder.binding.Scheme }

func (builder *ownedResolverBuilder) Build(target resolver.Target, client resolver.ClientConn, options resolver.BuildOptions) (resolver.Resolver, error) {
	if !builder.scope.enter(false) {
		return nil, net.ErrClosed
	}
	defer builder.scope.leave()
	if builder.binding.Builder.Scheme() != builder.binding.Scheme {
		return nil, failure(ErrInput, "resolver-scheme")
	}
	options.DisableServiceConfig = true
	if options.DialCreds != nil {
		options.DialCreds = options.DialCreds.Clone()
	}
	callbacks := &ownedResolverClient{native: client, scope: builder.scope, authority: options.Authority}
	native, err := builder.binding.Builder.Build(target, callbacks, options)
	if nilRuntime(native) {
		callbacks.closed.Store(true)
		if err == nil {
			err = failure(ErrConnect, "nil-resolver")
		}
		return nil, err
	}
	owned := &ownedResolver{native: native, scope: builder.scope, callbacks: callbacks}
	builder.scope.mu.Lock()
	closing := builder.scope.closing || builder.scope.released
	if !closing && err == nil {
		if builder.scope.resolvers == nil {
			builder.scope.resolvers = make(map[*ownedResolver]struct{})
		}
		builder.scope.resolvers[owned] = struct{}{}
	}
	builder.scope.mu.Unlock()
	if err != nil || closing {
		owned.Close()
		if err == nil {
			err = net.ErrClosed
		}
		return nil, err
	}
	return owned, nil
}

type ownedResolver struct {
	native    resolver.Resolver
	scope     *transportLifetime
	callbacks *ownedResolverClient
	once      sync.Once
}

func (owned *ownedResolver) ResolveNow(options resolver.ResolveNowOptions) {
	if owned.callbacks.closed.Load() || !owned.scope.enter(false) {
		return
	}
	defer owned.scope.leave()
	owned.native.ResolveNow(options)
}

func (owned *ownedResolver) Close() {
	owned.once.Do(func() {
		owned.callbacks.closed.Store(true)
		if !owned.scope.enter(true) {
			return
		}
		defer owned.scope.leave()
		owned.native.Close()
		owned.scope.mu.Lock()
		delete(owned.scope.resolvers, owned)
		owned.scope.mu.Unlock()
	})
}

type ownedResolverClient struct {
	native    resolver.ClientConn
	scope     *transportLifetime
	authority string
	closed    atomic.Bool
}

func (client *ownedResolverClient) enter() bool {
	return !client.closed.Load() && client.scope.enter(false)
}

func (client *ownedResolverClient) UpdateState(state resolver.State) error {
	if !client.enter() {
		return net.ErrClosed
	}
	defer client.scope.leave()
	copied, err := copyResolverState(state, client.authority)
	if err != nil {
		client.native.ReportError(err)
		return err
	}
	return client.native.UpdateState(copied)
}

func (client *ownedResolverClient) ReportError(err error) {
	if !client.enter() {
		return
	}
	defer client.scope.leave()
	client.native.ReportError(err)
}

func (client *ownedResolverClient) NewAddress(addresses []resolver.Address) {
	_ = client.UpdateState(resolver.State{Addresses: addresses})
}

func (*ownedResolverClient) ParseServiceConfig(string) *serviceconfig.ParseResult {
	return &serviceconfig.ParseResult{Err: failure(ErrAuthority, "resolver-service-config")}
}

func copyResolverState(state resolver.State, authority string) (resolver.State, error) {
	if state.ServiceConfig != nil || state.Attributes != nil {
		return resolver.State{}, failure(ErrAuthority, "resolver-options")
	}
	count := len(state.Addresses)
	if count > maxResolverAddresses || len(state.Endpoints) > maxResolverAddresses {
		return resolver.State{}, failure(ErrLimit, "resolver-addresses")
	}
	validateAddresses := func(addresses []resolver.Address) error {
		for _, address := range addresses {
			if !validText(address.Addr, 512) || address.ServerName != "" && address.ServerName != authority ||
				address.Attributes != nil || address.BalancerAttributes != nil || address.Metadata != nil {
				return failure(ErrAuthority, "resolver-address")
			}
		}
		return nil
	}
	if err := validateAddresses(state.Addresses); err != nil {
		return resolver.State{}, err
	}
	state.Addresses = slices.Clone(state.Addresses)
	state.Endpoints = slices.Clone(state.Endpoints)
	for index, endpoint := range state.Endpoints {
		count += len(endpoint.Addresses)
		if count > maxResolverAddresses || endpoint.Attributes != nil {
			return resolver.State{}, failure(ErrLimit, "resolver-addresses")
		}
		if err := validateAddresses(endpoint.Addresses); err != nil {
			return resolver.State{}, err
		}
		state.Endpoints[index].Addresses = slices.Clone(endpoint.Addresses)
	}
	return state, nil
}

type passthroughBuilder struct{}

func (passthroughBuilder) Scheme() string { return "passthrough" }

func (passthroughBuilder) Build(target resolver.Target, client resolver.ClientConn, _ resolver.BuildOptions) (resolver.Resolver, error) {
	err := client.UpdateState(resolver.State{Addresses: []resolver.Address{{Addr: target.Endpoint()}}})
	if err != nil {
		return nil, err
	}
	return passthroughResolver{}, nil
}

type passthroughResolver struct{}

func (passthroughResolver) ResolveNow(resolver.ResolveNowOptions) {}
func (passthroughResolver) Close()                                {}
