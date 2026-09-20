/*
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

package nacos

import (
	"slices"
	"strings"
	"time"

	"github.com/frost-leo/fathomry/framework/configuration"
	native "github.com/frost-leo/fathomry/internal/configsource/nacos/v2"
)

// Server selects both channels of one authorized cluster member. HTTPS does not
// imply gRPC security: AllowInsecure controls the separate gRPC transport.
type Server struct {
	private
	HTTPURL     string
	GRPCAddress string
}

// Source binds a private group/data ID to a public non-secret label and layer.
// Group defaults to DEFAULT_GROUP. Optional permits only observed remote absence,
// not empty, invalid, denied, unavailable or unsupported content.
type Source struct {
	private
	Name     string
	Group    string
	DataID   string
	Layer    configuration.Layer
	Optional bool
}

// Options declares finite remote acquisition, not settings obtained from Nacos.
// Bootstrap must be available before loading and must not be mutated during New.
// Namespace, AppName, endpoints, credentials and trust use the internal Nacos
// bounds documented in the package reference. No ambient discovery is performed.
type Options struct {
	private
	SchemaVersion uint32
	Namespace     string
	AppName       string
	Servers       []Server
	Sources       []Source
	Username      string
	Password      string
	RootCAPEM     string
	AllowInsecure bool
	// RequestTimeout bounds all acquisitions in one load, including setup/login
	// and failover. Zero is 10 seconds; allowed range is 1 ms to 1 minute.
	// Cleanup joins native work and can exceed this cooperative deadline.
	RequestTimeout time.Duration
	// RetryDelay is the native registration retry delay, not another retry loop.
	// Zero is 100 ms; allowed range is 1 ms to 1 minute.
	RetryDelay time.Duration
	// ConcurrentLoads bounds transient clients owned by this Provider, including
	// pending cleanup. Zero is 4, allowed range 1–16. Excess calls fail immediately.
	ConcurrentLoads int
}

// New validates and freezes declarations without network I/O, goroutines or
// native clients. SchemaVersion defaults to 1. One to three unique sources with
// unique Base/Environment/Local layers are required; layer is not transport kind.
func New(options Options) (*Provider, error) {
	if len(options.Sources) < 1 || len(options.Sources) > configuration.MaxSources ||
		len(options.Servers) < 1 || len(options.Servers) > native.MaxServers ||
		options.ConcurrentLoads < 0 || options.ConcurrentLoads > 16 {
		return nil, problem(configuration.InvalidInput, "", nil)
	}
	bootstrap := native.OptionsV1{
		Name: ProviderID, Namespace: options.Namespace, AppName: options.AppName,
		Username: options.Username, Password: options.Password, RootCAPEM: options.RootCAPEM,
		AllowInsecure: options.AllowInsecure, RequestTimeout: options.RequestTimeout,
		RetryDelay: options.RetryDelay, ConcurrentRequests: 2,
	}
	for _, server := range options.Servers {
		bootstrap.Servers = append(bootstrap.Servers, native.ServerV1{HTTPURL: server.HTTPURL, GRPCAddress: server.GRPCAddress})
	}
	sources := slices.Clone(options.Sources)
	names := make(map[string]bool)
	layers := make(map[configuration.Layer]bool)
	for index, source := range sources {
		if !label(source.Name) || names[source.Name] || layers[source.Layer] ||
			source.Layer < configuration.Base || source.Layer > configuration.Local {
			return nil, problem(configuration.InvalidInput, "", nil)
		}
		names[source.Name], layers[source.Layer] = true, true
		if source.Group == "" {
			source.Group = "DEFAULT_GROUP"
		}
		sources[index] = source
		bootstrap.Keys = append(bootstrap.Keys, native.KeyV1{Group: source.Group, DataID: source.DataID})
	}
	if err := native.ValidateOptions(bootstrap); err != nil {
		return nil, problem(configuration.InvalidInput, "", nil)
	}
	bootstrap.Namespace, bootstrap.AppName = strings.Clone(bootstrap.Namespace), strings.Clone(bootstrap.AppName)
	bootstrap.Username, bootstrap.Password = strings.Clone(bootstrap.Username), strings.Clone(bootstrap.Password)
	bootstrap.RootCAPEM = strings.Clone(bootstrap.RootCAPEM)
	for index := range bootstrap.Servers {
		bootstrap.Servers[index].HTTPURL = strings.Clone(bootstrap.Servers[index].HTTPURL)
		bootstrap.Servers[index].GRPCAddress = strings.Clone(bootstrap.Servers[index].GRPCAddress)
	}
	bootstrap.Keys = nil
	for index := range sources {
		sources[index].Name = strings.Clone(sources[index].Name)
		sources[index].Group = strings.Clone(sources[index].Group)
		sources[index].DataID = strings.Clone(sources[index].DataID)
		bootstrap.Keys = append(bootstrap.Keys, native.KeyV1{Group: sources[index].Group, DataID: sources[index].DataID})
	}
	slices.SortFunc(sources, func(left, right Source) int { return int(left.Layer) - int(right.Layer) })
	if bootstrap.RequestTimeout == 0 {
		bootstrap.RequestTimeout = 10 * time.Second
	}
	format := options.SchemaVersion
	if format == 0 {
		format = 1
	}
	concurrent := options.ConcurrentLoads
	if concurrent == 0 {
		concurrent = 4
	}
	return &Provider{options: bootstrap, sources: sources, format: format, slots: make(chan struct{}, concurrent)}, nil
}

func label(value string) bool {
	if value == "" || len(value) > 64 {
		return false
	}
	for _, char := range value {
		if !(char >= 'a' && char <= 'z' || char >= '0' && char <= '9' || char == '.' || char == '_' || char == '-') {
			return false
		}
	}
	return true
}
