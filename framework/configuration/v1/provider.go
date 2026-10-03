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

package configuration

import (
	"context"
	"path/filepath"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	nacos "github.com/frost-leo/fathomry/adapters/configsource/nacos/v1"
	configsource "github.com/frost-leo/fathomry/adapters/configsource/v1"
	viper "github.com/frost-leo/fathomry/adapters/configsource/viper/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
)

// Provider is an immutable, reusable declaration of one explicitly selected
// implementation. Construction does not acquire clients or start workers.
// Each Load/Watch acquires independent ownership; no global instance is inferred.
type Provider struct {
	private
	state *providerPlan
}
type providerPlan struct {
	name   string
	layers []layer
	open   func(context.Context, *adapters.Runtime) (sourceBinding, error)
}
type sourceBinding struct {
	source  configsource.Source
	close   func(context.Context) error
	records func(context.Context) ([]Record, error)
}

// Values explicitly selects a defaults/captured-values-only Load without I/O.
// It cannot be used with external document slots or Watch.
func Values() Provider {
	return Provider{state: &providerPlan{name: "values", open: func(context.Context, *adapters.Runtime) (sourceBinding, error) { return sourceBinding{}, nil }}}
}

// File binds an original file to its layer policy. Path must be absolute.
// Duplicate literal paths reject; declarations do not resolve filesystem aliases.
// Optional permits positive absence, never an invalid or unreadable document.
type File struct {
	Path     string
	Kind     LayerKind
	Encoding Encoding
	Optional bool
}

// ViperOptions selects 1..3 original files with their layer policy. It neither discovers paths
// nor reads native Viper environment/defaults. Zero Interval uses 1s (10ms..5min);
// zero QueueCapacity uses 16 (1..64). Documents are copied at declaration.
type ViperOptions struct {
	Documents     []File
	Interval      time.Duration
	QueueCapacity int
}

// Viper explicitly declares the Viper-backed local source without performing I/O.
func Viper(options ViperOptions) (Provider, error) {
	if len(options.Documents) == 0 || len(options.Documents) > 3 || options.Interval != 0 && (options.Interval < 10*time.Millisecond || options.Interval > 5*time.Minute) || options.QueueCapacity < 0 || options.QueueCapacity > 64 {
		return Provider{}, fail(ErrDeclaration, "viper")
	}
	paths := make([]string, len(options.Documents))
	layers := make([]layer, len(options.Documents))
	seen := make(map[string]bool, len(options.Documents))
	for index, document := range options.Documents {
		path := document.Path
		if len(path) > 4096 || !filepath.IsAbs(path) || !utf8.ValidString(path) || strings.ContainsRune(path, 0) || seen[path] {
			return Provider{}, fail(ErrDeclaration, "viper_path")
		}
		seen[path] = true
		paths[index] = path
		layers[index] = layer{Kind: document.Kind, Encoding: document.Encoding, Optional: document.Optional}
	}
	if err := validateLayers(layers); err != nil {
		return Provider{}, err
	}
	frozen := viper.WatchSettings{Paths: paths, Interval: options.Interval, QueueCapacity: options.QueueCapacity}
	return Provider{state: &providerPlan{name: "viper", layers: layers, open: func(ctx context.Context, runtime *adapters.Runtime) (sourceBinding, error) {
		inbox, err := adapters.NewInbox[viper.Evidence](adapters.EvidenceOptions{Capacity: 4, MaxBytes: 256 << 10})
		if err != nil {
			return sourceBinding{}, err
		}
		binding := sourceBinding{records: func(ctx context.Context) ([]Record, error) {
			return takeRecords(ctx, inbox, "viper", func(value viper.Evidence) recordFacts {
				return recordFacts{source: SourceEvidence{Documents: value.Documents, Missing: value.Missing, FailedIndex: -1}, sourcePresent: true}
			})
		}}
		client, err := viper.New(viper.Dependencies{Runtime: runtime, Evidence: inbox})
		if err != nil {
			return binding, err
		}
		binding.source, err = client.Source(frozen)
		return binding, err
	}}}, nil
}

// NacosServer contains explicit authorized HTTP and gRPC endpoints.
type NacosServer struct {
	HTTPURL     string `json:"http_url"`
	GRPCAddress string `json:"grpc_address"`
}

// NacosKey selects one document in the configured namespace, not an environment.
type NacosKey struct {
	Group  string `json:"group"`
	DataID string `json:"data_id"`
}

// NacosConnection contains bootstrap data for the supported read-only connection.
// Name is required. Empty Namespace means Nacos's default namespace. Credentials
// occur together; empty RootCAPEM uses system trust. Plaintext requires AllowInsecure.
// Durations use nanoseconds; zero requests/retry/reconcile use 10s/100ms/30s.
type NacosConnection struct {
	Name              string        `json:"name"`
	Namespace         string        `json:"namespace"`
	AppName           string        `json:"app_name"`
	Servers           []NacosServer `json:"servers"`
	Username          string        `json:"username"`
	Password          string        `json:"password"`
	RootCAPEM         string        `json:"root_ca_pem"`
	AllowInsecure     bool          `json:"allow_insecure"`
	RequestTimeout    time.Duration `json:"request_timeout_ns"`
	RetryDelay        time.Duration `json:"retry_delay_ns"`
	ReconcileInterval time.Duration `json:"reconcile_interval_ns"`
}

// NacosDocument binds one explicit remote key to its configuration layer policy.
// Optional accepts only positively missing documents, not acquisition failures.
type NacosDocument struct {
	Key      NacosKey
	Kind     LayerKind
	Encoding Encoding
	Optional bool
}

// NacosOptions selects one connection and 1..3 read-only documents. There is no
// source inference, write authority or dynamic-key API in this scenario.
// ObservationCapacity zero uses 2 complete batches; valid values are 1..16.
type NacosOptions struct {
	Connection          NacosConnection
	Documents           []NacosDocument
	ObservationCapacity int
}

// Nacos explicitly declares the Nacos-backed remote source. It validates and
// copies source data without creating a client or reading process credentials.
func Nacos(options NacosOptions) (Provider, error) {
	if len(options.Documents) == 0 || len(options.Documents) > 3 || len(options.Connection.Servers) > nacos.MaxServers || options.ObservationCapacity < 0 || options.ObservationCapacity > 16 {
		return Provider{}, fail(ErrDeclaration, "nacos")
	}
	connection := options.Connection
	selected := nacos.Settings{Name: connection.Name, Namespace: connection.Namespace, AppName: connection.AppName, Username: connection.Username, Password: connection.Password, RootCAPEM: connection.RootCAPEM, AllowInsecure: connection.AllowInsecure, RequestTimeout: connection.RequestTimeout, RetryDelay: connection.RetryDelay, ReconcileInterval: connection.ReconcileInterval}
	for _, server := range connection.Servers {
		selected.Servers = append(selected.Servers, nacos.Server{HTTPURL: server.HTTPURL, GRPCAddress: server.GRPCAddress})
	}
	layers := make([]layer, len(options.Documents))
	for index, document := range options.Documents {
		selected.Keys = append(selected.Keys, nacos.Key{Group: document.Key.Group, DataID: document.Key.DataID})
		layers[index] = layer{Kind: document.Kind, Encoding: document.Encoding, Optional: document.Optional}
	}
	if err := validateLayers(layers); err != nil {
		return Provider{}, err
	}
	if err := nacos.Validate(selected); err != nil {
		return Provider{}, fail(ErrDeclaration, "nacos", err)
	}
	capacity := options.ObservationCapacity
	return Provider{state: &providerPlan{name: "nacos", layers: layers, open: func(ctx context.Context, runtime *adapters.Runtime) (sourceBinding, error) {
		inbox, err := adapters.NewInbox[nacos.Evidence](adapters.EvidenceOptions{Capacity: 4, MaxBytes: 256 << 10})
		if err != nil {
			return sourceBinding{}, err
		}
		binding := sourceBinding{records: func(ctx context.Context) ([]Record, error) {
			return takeRecords(ctx, inbox, "nacos", func(value nacos.Evidence) recordFacts {
				return recordFacts{source: SourceEvidence{Documents: value.Documents, FailedIndex: value.FailedIndex}, sourcePresent: true}
			})
		}}
		settings := selected
		settings.Servers = slices.Clone(selected.Servers)
		settings.Keys = slices.Clone(selected.Keys)
		owner, err := nacos.Open(ctx, settings, nacos.Dependencies{Runtime: runtime, Evidence: inbox})
		if owner != nil {
			binding.close = owner.Close
		}
		if err != nil {
			return binding, err
		}
		binding.source, err = owner.Client().Source(nacos.ObserveOptions{QueueCapacity: capacity})
		return binding, err
	}}}, nil
}

// NacosBootstrap is the supported deployment document, not application settings.
// Environment labels map to explicit keys; they never select a namespace.
// Connection grants no write or dynamic-key capability. At most 32 environments
// are admitted, and every declared combination must be valid before source I/O.
type NacosBootstrap struct {
	Connection   NacosConnection     `json:"connection"`
	Base         NacosKey            `json:"base"`
	Environments map[string]NacosKey `json:"environments"`
	Override     *NacosKey           `json:"override"`
}

// NacosBootstrapOptions explicitly selects a deployment file and application
// environment. File is relative to the invocation directory or absolute; its
// extension selects .yaml/.yml, .toml or .json. Encoding selects application
// document syntax, independently. Variables are explicit captured overrides on
// the deployment schema, not authority to read credentials from the environment.
type NacosBootstrapOptions struct {
	File        string
	Environment string
	Encoding    Encoding
	Variables   []Variable
}

// PrepareNacos loads and validates one explicit deployment file through the public
// Viper Adapter and the same strict Load path, then returns an inert Nacos provider.
// It joins finite file-read ownership and returns released records on failure.
// No Nacos connection is opened until a subsequent application Load/Watch. The
// selected bootstrap is frozen; watching application documents does not rotate
// its connection or silently switch environment/document identities.
func PrepareNacos(ctx context.Context, options NacosBootstrapOptions) (Provider, []Record, error) {
	if ctx == nil || !environmentLabel(options.Environment) || options.File == "" ||
		len(options.File) > 4096 || !utf8.ValidString(options.File) || strings.ContainsRune(options.File, 0) ||
		options.Encoding != YAML && options.Encoding != TOML && options.Encoding != JSON {
		return Provider{}, nil, fail(ErrDeclaration, "nacos_bootstrap")
	}
	var encoding Encoding
	switch strings.ToLower(filepath.Ext(options.File)) {
	case ".yaml", ".yml":
		encoding = YAML
	case ".toml":
		encoding = TOML
	case ".json":
		encoding = JSON
	default:
		return Provider{}, nil, fail(ErrDeclaration, "bootstrap_encoding")
	}
	path, err := filepath.Abs(options.File)
	if err != nil {
		return Provider{}, nil, fail(ErrDeclaration, "bootstrap_path", err)
	}
	local, err := Viper(ViperOptions{Documents: []File{{Path: path, Kind: Base, Encoding: encoding}}})
	if err != nil {
		return Provider{}, nil, err
	}
	result, err := Load(ctx, Declaration[NacosBootstrap]{
		Schema: Schema[NacosBootstrap]{Version: 1, Validate: func(ctx context.Context, value NacosBootstrap) error {
			if len(value.Environments) == 0 || len(value.Environments) > 32 {
				return fail(ErrDeclaration, "bootstrap_environments")
			}
			for environment := range value.Environments {
				if !environmentLabel(environment) {
					return fail(ErrDeclaration, "bootstrap_environment")
				}
				if err := ctx.Err(); err != nil {
					return fail(ErrWait, "bootstrap", err, context.Cause(ctx))
				}
				if _, err := value.provider(environment, options.Encoding); err != nil {
					return err
				}
			}
			return nil
		}},
		Variables: options.Variables,
	}, Dependencies{Provider: local})
	if err != nil {
		return Provider{}, result.Records, err
	}
	accepted, err := result.State.Capture()
	if err != nil {
		return Provider{}, result.Records, err
	}
	value, err := accepted.ValueCopy()
	if err != nil {
		return Provider{}, result.Records, err
	}
	provider, err := value.provider(options.Environment, options.Encoding)
	return provider, result.Records, err
}
func (value NacosBootstrap) provider(environment string, encoding Encoding) (Provider, error) {
	key, present := value.Environments[environment]
	if !present {
		return Provider{}, fail(ErrDeclaration, "bootstrap_environment")
	}
	documents := []NacosDocument{
		{Key: value.Base, Kind: Base, Encoding: encoding},
		{Key: key, Kind: Environment, Encoding: encoding},
	}
	if value.Override != nil {
		documents = append(documents, NacosDocument{Key: *value.Override, Kind: Override, Encoding: encoding, Optional: true})
	}
	return Nacos(NacosOptions{Connection: value.Connection, Documents: documents})
}
func environmentLabel(value string) bool {
	if len(value) == 0 || len(value) > 64 || value[0] < 'a' || value[0] > 'z' {
		return false
	}
	for _, char := range value {
		if !(char >= 'a' && char <= 'z' || char >= '0' && char <= '9' || char == '_' || char == '-') {
			return false
		}
	}
	return true
}
