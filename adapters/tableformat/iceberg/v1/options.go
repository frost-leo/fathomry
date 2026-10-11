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

package iceberg

import (
	"context"
	"crypto/tls"
	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/apache/iceberg-go/encryption"
	"github.com/apache/iceberg-go/metrics"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/frost-leo/fathomry/adapters/configsource/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	native "github.com/frost-leo/fathomry/internal/tableformat/iceberg/v0"
	"log/slog"
	"net/http"
	"time"
)

const ProviderID = native.ProviderID

// Grants are explicit, independent permissions; no true field implies another.
type Grants struct {
	Read        bool `json:"read"`
	Write       bool `json:"write"`
	Metadata    bool `json:"metadata"`
	Namespace   bool `json:"namespace"`
	Import      bool `json:"import"`
	MultiTable  bool `json:"multi_table"`
	Rewrite     bool `json:"rewrite"`
	Expire      bool `json:"expire"`
	Orphan      bool `json:"orphan"`
	Purge       bool `json:"purge"`
	DeleteFiles bool `json:"delete_files"`
}

// Settings is loadable REST/S3 data. Zero positive limits select native defaults;
// Version zero selects settings format1, not table format. Timeout is ns.
// Explicit serialization can disclose secrets; normal diagnostics are redacted.
type Settings struct {
	Name                string            `json:"name"`
	Version             uint32            `json:"version"`
	CatalogURI          string            `json:"catalog_uri"`
	CatalogPrefix       string            `json:"catalog_prefix"`
	Warehouse           string            `json:"warehouse"`
	Namespace           []string          `json:"namespace"`
	Location            string            `json:"location"`
	Plaintext           bool              `json:"plaintext"`
	RootCAPEM           string            `json:"root_ca_pem"`
	Headers             map[string]string `json:"headers"`
	Properties          map[string]string `json:"properties"`
	BearerToken         string            `json:"bearer_token"`
	OAuthURI            string            `json:"oauth_uri"`
	OAuthCredential     string            `json:"oauth_credential"`
	OAuthScope          string            `json:"oauth_scope"`
	OAuthAudience       string            `json:"oauth_audience"`
	OAuthResource       string            `json:"oauth_resource"`
	SigV4               bool              `json:"sigv4"`
	SigningRegion       string            `json:"signing_region"`
	SigningService      string            `json:"signing_service"`
	StorageEndpoint     string            `json:"storage_endpoint"`
	StorageRegion       string            `json:"storage_region"`
	StorageAccessKey    string            `json:"storage_access_key"`
	StorageSecretKey    string            `json:"storage_secret_key"`
	StorageSessionToken string            `json:"storage_session_token"`
	VendedCredentials   bool              `json:"vended_credentials"`
	Permissions         Grants            `json:"permissions"`
	MaxActive           int               `json:"max_active"`
	MaxOperations       int               `json:"max_operations"`
	MaxHandles          int               `json:"max_handles"`
	MaxRows             int64             `json:"max_rows"`
	MaxBatchBytes       int64             `json:"max_batch_bytes"`
	MaxObjectBytes      int64             `json:"max_object_bytes"`
	MaxIOBytes          int64             `json:"max_io_bytes"`
	MaxFileOps          int64             `json:"max_file_ops"`
	MaxMetadataBytes    int64             `json:"max_metadata_bytes"`
	MaxListEntries      int               `json:"max_list_entries"`
	MaxOpenFiles        int               `json:"max_open_files"`
	Workers             int               `json:"workers"`
	MaxCommitRetries    int               `json:"max_commit_retries"`
	Timeout             time.Duration     `json:"timeout_ns"`
}

// NativeOptions borrows cooperative dependencies until Owner shutdown. Prepare
// freezes TLS containers but invokes no callback. Transport returns owned cleanup.
type NativeOptions struct {
	private
	CatalogCredentials aws.CredentialsProvider
	TLS                *tls.Config
	OAuthTLS           *tls.Config
	Credentials        aws.CredentialsProvider
	Auth               rest.AuthManager
	Reporter           metrics.Reporter
	Logger             *slog.Logger
	Encryption         encryption.EncryptionManager
	Transport          func(*http.Transport) (http.RoundTripper, func() error, error)
}
type Dependencies struct {
	private
	Runtime  *adapters.Runtime
	Evidence *adapters.Inbox[Result]
	Observer *adapters.Observer
	Native   NativeOptions
}

func options(value Settings) native.ManagedSettings {
	return native.ManagedSettings{Name: value.Name,
		Version:             value.Version,
		CatalogURI:          value.CatalogURI,
		CatalogPrefix:       value.CatalogPrefix,
		Warehouse:           value.Warehouse,
		Namespace:           value.Namespace,
		Location:            value.Location,
		Plaintext:           value.Plaintext,
		RootCAPEM:           value.RootCAPEM,
		Headers:             value.Headers,
		Properties:          value.Properties,
		BearerToken:         value.BearerToken,
		OAuthURI:            value.OAuthURI,
		OAuthCredential:     value.OAuthCredential,
		OAuthScope:          value.OAuthScope,
		OAuthAudience:       value.OAuthAudience,
		OAuthResource:       value.OAuthResource,
		SigV4:               value.SigV4,
		SigningRegion:       value.SigningRegion,
		SigningService:      value.SigningService,
		StorageEndpoint:     value.StorageEndpoint,
		StorageRegion:       value.StorageRegion,
		StorageAccessKey:    value.StorageAccessKey,
		StorageSecretKey:    value.StorageSecretKey,
		StorageSessionToken: value.StorageSessionToken,
		VendedCredentials:   value.VendedCredentials,
		Permissions:         native.Grants(value.Permissions),
		MaxActive:           value.MaxActive,
		MaxOperations:       value.MaxOperations,
		MaxHandles:          value.MaxHandles,
		MaxRows:             value.MaxRows,
		MaxBatchBytes:       value.MaxBatchBytes,
		MaxObjectBytes:      value.MaxObjectBytes,
		MaxIOBytes:          value.MaxIOBytes,
		MaxFileOps:          value.MaxFileOps,
		MaxMetadataBytes:    value.MaxMetadataBytes,
		MaxListEntries:      value.MaxListEntries,
		MaxOpenFiles:        value.MaxOpenFiles,
		Workers:             value.Workers,
		MaxCommitRetries:    value.MaxCommitRetries,
		Timeout:             value.Timeout}
}
func settingsFor(value native.ManagedSettings) Settings {
	return Settings{Name: value.Name,
		Version:             value.Version,
		CatalogURI:          value.CatalogURI,
		CatalogPrefix:       value.CatalogPrefix,
		Warehouse:           value.Warehouse,
		Namespace:           value.Namespace,
		Location:            value.Location,
		Plaintext:           value.Plaintext,
		RootCAPEM:           value.RootCAPEM,
		Headers:             value.Headers,
		Properties:          value.Properties,
		BearerToken:         value.BearerToken,
		OAuthURI:            value.OAuthURI,
		OAuthCredential:     value.OAuthCredential,
		OAuthScope:          value.OAuthScope,
		OAuthAudience:       value.OAuthAudience,
		OAuthResource:       value.OAuthResource,
		SigV4:               value.SigV4,
		SigningRegion:       value.SigningRegion,
		SigningService:      value.SigningService,
		StorageEndpoint:     value.StorageEndpoint,
		StorageRegion:       value.StorageRegion,
		StorageAccessKey:    value.StorageAccessKey,
		StorageSecretKey:    value.StorageSecretKey,
		StorageSessionToken: value.StorageSessionToken,
		VendedCredentials:   value.VendedCredentials,
		Permissions:         Grants(value.Permissions),
		MaxActive:           value.MaxActive,
		MaxOperations:       value.MaxOperations,
		MaxHandles:          value.MaxHandles,
		MaxRows:             value.MaxRows,
		MaxBatchBytes:       value.MaxBatchBytes,
		MaxObjectBytes:      value.MaxObjectBytes,
		MaxIOBytes:          value.MaxIOBytes,
		MaxFileOps:          value.MaxFileOps,
		MaxMetadataBytes:    value.MaxMetadataBytes,
		MaxListEntries:      value.MaxListEntries,
		MaxOpenFiles:        value.MaxOpenFiles,
		Workers:             value.Workers,
		MaxCommitRetries:    value.MaxCommitRetries,
		Timeout:             value.Timeout}
}
func nativeOptions(n NativeOptions) native.ManagedNativeOptions {
	return native.ManagedNativeOptions{TLS: n.TLS, OAuthTLS: n.OAuthTLS, Credentials: n.Credentials, CatalogCredentials: n.CatalogCredentials, Auth: n.Auth, Reporter: n.Reporter, Logger: n.Logger, Encryption: n.Encryption, Transport: n.Transport}
}
func Validate(value Settings) error { _, err := Prepare(value, NativeOptions{}); return err }
func Configuration(defaults Settings) (configsource.Schema[Settings], error) {
	prepared, err := Prepare(defaults, NativeOptions{})
	if err != nil {
		return configsource.Schema[Settings]{}, err
	}
	return configsource.Schema[Settings]{Version: 1, Defaults: prepared.Settings(), Validate: func(_ context.Context, value Settings) error {
		_, err := native.PrepareManagedResolved(options(value), native.ManagedNativeOptions{})
		return translate(err, "validate")
	}}, nil
}

type Prepared struct {
	private
	native native.ManagedPreparation
}

func Prepare(settings Settings, options NativeOptions) (Prepared, error) {
	p, err := native.PrepareManaged(optionsFor(settings), nativeOptions(options))
	if err != nil {
		return Prepared{}, translate(err, "prepare")
	}
	result := Prepared{native: p}
	policy := result.Policy()
	if policy.Runtime.MaxWorkBytes > 1<<40 || policy.Evidence.MaxBytes > 1<<40 {
		return Prepared{}, fail(ErrLimit, "policy")
	}
	return result, nil
}
func optionsFor(settings Settings) native.ManagedSettings { return options(settings) }
func (p Prepared) Settings() Settings                     { return settingsFor(p.native.Settings()) }
