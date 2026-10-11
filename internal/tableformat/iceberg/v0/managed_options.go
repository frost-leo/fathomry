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
	"crypto/x509"
	"log/slog"
	"maps"
	"net/http"
	"net/url"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/apache/iceberg-go/encryption"
	"github.com/apache/iceberg-go/metrics"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/frost-leo/fathomry/internal/resource"
)

// Grants separates metadata effects from physical file deletion. None is implied
// by another grant. Quiescence for orphan deletion is an additional per-call fact.
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

// ManagedSettings is the native-semantic REST/S3 profile. Zero numerical limits
// select documented defaults; negative limits are invalid. Durations are ns.
// Credentials are explicit data, never loaded from environment or a registry.
type ManagedSettings struct {
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

// ManagedNativeOptions contains borrowed cooperative dependencies. Prepare copies
// TLS/container data without invoking callbacks. The caller keeps dependencies
// alive until physical source shutdown; Reporter is not closed by the provider.
// Transport must return a source-local transport and a join/cleanup function.
type ManagedNativeOptions struct {
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

// ManagedPreparation freezes source data and the exact effective reservation.
// The SDK still reads EnvConfig at import; this profile never uses it for source
// discovery and supplies explicit per-family native worker/retry ceilings.
type ManagedPreparation struct {
	private
	settings ManagedSettings
	native   ManagedNativeOptions
}

type ManagedBudget struct {
	Active                                int
	WorkBytes, EvidenceBytes, SourceBytes int64
}

func (p ManagedPreparation) Budget() ManagedBudget {
	s := p.settings
	metricsBytes := int64(0)
	if managedRESTMetrics(s.Properties) {
		metricsBytes = 132 * 4 * s.MaxMetadataBytes
	}
	return ManagedBudget{Active: s.MaxActive,
		WorkBytes:     4*s.MaxIOBytes + 4*s.MaxBatchBytes + (2+int64(s.MaxHandles))*s.MaxMetadataBytes + int64(s.MaxOpenFiles)*(5<<20) + 256<<10,
		EvidenceBytes: s.MaxIOBytes + int64(s.MaxOperations+s.MaxHandles)*4096 + (s.MaxFileOps+int64(s.MaxHandles))*(32<<10) + 64<<10,
		SourceBytes:   metricsBytes + 2*s.MaxMetadataBytes + int64(s.MaxActive)*(128<<10) + 256<<10}
}

func managedRESTMetrics(properties map[string]string) bool {
	value, ok := properties["rest-metrics-reporting-enabled"]
	if !ok {
		value = properties["rest.metrics-reporting-enabled"]
	}
	enabled, _ := strconv.ParseBool(value)
	return enabled
}

func (p ManagedPreparation) Settings() ManagedSettings { return cloneManagedSettings(p.settings) }
func (p ManagedPreparation) Limits() resource.Limits {
	b := p.Budget()
	return resource.Limits{Active: b.Active, Bytes: int64(b.Active) * b.WorkBytes, MaxLeases: 4*b.Active + 4}
}

func cloneManagedSettings(s ManagedSettings) ManagedSettings {
	s.Namespace = slices.Clone(s.Namespace)
	s.Headers = maps.Clone(s.Headers)
	s.Properties = maps.Clone(s.Properties)
	return s
}

// PrepareManaged is offline: validation, effective defaults and container copies
// only. It does not call credential, auth, transport, encryption or report hooks.
func PrepareManaged(s ManagedSettings, n ManagedNativeOptions) (ManagedPreparation, error) {
	s = cloneManagedSettings(s)
	if s.Version == 0 {
		s.Version = 1
	}
	if s.MaxActive == 0 {
		s.MaxActive = 2
	}
	if s.MaxOperations == 0 {
		s.MaxOperations = 128
	}
	if s.MaxHandles == 0 {
		s.MaxHandles = 64
	}
	if s.MaxRows == 0 {
		s.MaxRows = 1 << 20
	}
	if s.MaxBatchBytes == 0 {
		s.MaxBatchBytes = 16 << 20
	}
	if s.MaxObjectBytes == 0 {
		s.MaxObjectBytes = 256 << 20
	}
	if s.MaxIOBytes == 0 {
		s.MaxIOBytes = 1 << 30
	}
	if s.MaxFileOps == 0 {
		s.MaxFileOps = 4096
	}
	if s.MaxMetadataBytes == 0 {
		s.MaxMetadataBytes = 8 << 20
	}
	if s.MaxListEntries == 0 {
		s.MaxListEntries = 4096
	}
	if s.MaxOpenFiles == 0 {
		s.MaxOpenFiles = 16
	}
	if s.Workers == 0 {
		s.Workers = 2
	}
	if s.Timeout == 0 {
		s.Timeout = time.Minute
	}
	if s.SigningService == "" {
		s.SigningService = "execute-api"
	}
	if s.OAuthCredential != "" {
		if s.OAuthURI == "" {
			s.OAuthURI = strings.TrimSuffix(s.CatalogURI, "/") + "/v1/oauth/tokens"
		}
		if s.OAuthScope == "" {
			s.OAuthScope = "catalog"
		}
	}
	if err := validateManagedSettings(s, n); err != nil {
		return ManagedPreparation{}, err
	}
	n.TLS = cloneManagedTLS(n.TLS)
	n.OAuthTLS = cloneManagedTLS(n.OAuthTLS)
	return ManagedPreparation{settings: s, native: n}, nil
}

func cloneManagedTLS(config *tls.Config) *tls.Config {
	if config == nil {
		return nil
	}
	config = config.Clone()
	if config.RootCAs != nil {
		config.RootCAs = config.RootCAs.Clone()
	}
	if config.ClientCAs != nil {
		config.ClientCAs = config.ClientCAs.Clone()
	}
	config.EncryptedClientHelloConfigList = slices.Clone(config.EncryptedClientHelloConfigList)
	config.Certificates = slices.Clone(config.Certificates)
	for i := range config.Certificates {
		config.Certificates[i].Leaf = nil
		config.Certificates[i].SupportedSignatureAlgorithms = slices.Clone(config.Certificates[i].SupportedSignatureAlgorithms)
		config.Certificates[i].Certificate = slices.Clone(config.Certificates[i].Certificate)
		for j := range config.Certificates[i].Certificate {
			config.Certificates[i].Certificate[j] = slices.Clone(config.Certificates[i].Certificate[j])
		}
		config.Certificates[i].OCSPStaple = slices.Clone(config.Certificates[i].OCSPStaple)
		config.Certificates[i].SignedCertificateTimestamps = slices.Clone(config.Certificates[i].SignedCertificateTimestamps)
		for j := range config.Certificates[i].SignedCertificateTimestamps {
			config.Certificates[i].SignedCertificateTimestamps[j] = slices.Clone(config.Certificates[i].SignedCertificateTimestamps[j])
		}
	}
	config.NextProtos = slices.Clone(config.NextProtos)
	config.CipherSuites = slices.Clone(config.CipherSuites)
	config.CurvePreferences = slices.Clone(config.CurvePreferences)
	return config
}

// PrepareManagedResolved rejects explicit zero limits after configuration layers.
func PrepareManagedResolved(s ManagedSettings, n ManagedNativeOptions) (ManagedPreparation, error) {
	if err := validateManagedSettings(s, n); err != nil {
		return ManagedPreparation{}, err
	}
	return PrepareManaged(s, n)
}

func typedNil(value any) bool {
	if value == nil {
		return false
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	}
	return false
}

func managedEndpoint(raw string, plaintext bool) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Hostname() == "" || len(raw) > 2048 || u.User != nil || u.Opaque != "" || u.Fragment != "" || u.RawQuery != "" || u.ForceQuery || u.RawPath != "" ||
		u.String() != raw || strings.Contains(u.Path, "//") || strings.Contains(u.Path, "..") || strings.HasSuffix(u.Path, "/") ||
		(u.Scheme != "https" && !(plaintext && u.Scheme == "http")) {
		return nil, failure(ErrInput, "endpoint")
	}
	return u, nil
}

func managedRoot(raw string) bool {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "s3" || u.Host == "" || u.Port() != "" || u.User != nil || u.Opaque != "" || u.RawQuery != "" || u.Fragment != "" || u.RawPath != "" || u.ForceQuery || u.String() != raw ||
		len(raw) > 2048 || !strings.HasSuffix(u.Path, "/") || u.Path == "/" || strings.Contains(u.Path, "//") {
		return false
	}
	for _, part := range strings.Split(strings.Trim(u.Path, "/"), "/") {
		if part == "." || part == ".." || strings.ContainsAny(part, "\\\x00\r\n") {
			return false
		}
	}
	return true
}

func managedText(s string, maximum int, empty bool) bool {
	return len(s) <= maximum && (empty || s != "") && utf8.ValidString(s) && !strings.ContainsFunc(s, func(r rune) bool { return r < 32 || r == 127 })
}

func validateManagedSettings(s ManagedSettings, n ManagedNativeOptions) error {
	if s.Version != 1 || !nameOK(s.Name) || !managedText(s.Warehouse, 1024, false) || !managedText(s.CatalogPrefix, 256, true) || strings.ContainsAny(s.CatalogPrefix, "/\\?#%") || !managedRoot(s.Location) {
		return failure(ErrInput, "settings")
	}
	if _, err := managedEndpoint(s.CatalogURI, s.Plaintext); err != nil {
		return err
	}
	u, err := managedEndpoint(s.StorageEndpoint, s.Plaintext)
	if err != nil {
		return err
	}
	if u.Path != "" {
		return failure(ErrInput, "storage-endpoint")
	}
	if len(s.Namespace) > 16 || catalog.ValidateNamespaceIdentifier(s.Namespace) != nil {
		return failure(ErrInput, "namespace")
	}
	for _, part := range s.Namespace {
		if !managedText(part, 256, false) {
			return failure(ErrInput, "namespace")
		}
	}
	if s.MaxActive < 1 || s.MaxActive > 32 || s.MaxOperations < 1 || s.MaxOperations > 4096 || s.MaxHandles < 1 || s.MaxHandles > 4096 || s.MaxRows < 1 || s.MaxRows > 1<<32 ||
		s.MaxBatchBytes < 1024 || s.MaxBatchBytes > 1<<30 || s.MaxObjectBytes < 1024 || s.MaxObjectBytes > 1<<40 || s.MaxIOBytes < s.MaxObjectBytes || s.MaxIOBytes > 1<<42 ||
		s.MaxMetadataBytes < 1024 || s.MaxMetadataBytes > 64<<20 || s.MaxFileOps < 1 || s.MaxFileOps > 1<<20 || s.MaxListEntries < 1 || s.MaxListEntries > 1<<20 ||
		s.MaxOpenFiles < 1 || s.MaxOpenFiles > 128 || s.Workers < 1 || s.Workers > 32 || s.MaxCommitRetries < 0 || s.MaxCommitRetries > 16 || s.Timeout < time.Millisecond || s.Timeout > time.Hour {
		return failure(ErrInput, "limits")
	}
	if typedNil(n.Auth) || typedNil(n.Credentials) || typedNil(n.CatalogCredentials) || typedNil(n.Reporter) || typedNil(n.Encryption) {
		return failure(ErrInput, "native-options")
	}
	if n.Credentials == nil && !s.VendedCredentials && (s.StorageAccessKey == "" || s.StorageSecretKey == "") || (s.StorageAccessKey == "") != (s.StorageSecretKey == "") {
		return failure(ErrInput, "storage-credentials")
	}
	if n.Credentials != nil && (s.StorageAccessKey != "" || s.StorageSecretKey != "" || s.StorageSessionToken != "") {
		return failure(ErrInput, "credential-conflict")
	}
	for _, value := range []string{s.BearerToken, s.OAuthCredential, s.StorageAccessKey, s.StorageSecretKey, s.StorageSessionToken} {
		if !managedText(value, 8192, true) {
			return failure(ErrInput, "credential")
		}
	}
	if s.StorageRegion == "" || !managedText(s.StorageRegion, 128, false) {
		return failure(ErrInput, "storage-region")
	}
	if s.SigV4 && (s.SigningRegion == "" || !managedText(s.SigningService, 128, false)) {
		return failure(ErrInput, "signing")
	}
	if n.CatalogCredentials != nil && !s.SigV4 || s.SigV4 && n.CatalogCredentials == nil && n.Credentials == nil && s.StorageAccessKey == "" {
		return failure(ErrInput, "signing-credentials")
	}
	methods := 0
	for _, present := range []bool{s.BearerToken != "", s.OAuthCredential != "", n.Auth != nil, s.SigV4} {
		if present {
			methods++
		}
	}
	if methods > 1 {
		return failure(ErrInput, "authentication-conflict")
	}
	if s.OAuthCredential != "" {
		if _, err := managedEndpoint(s.OAuthURI, s.Plaintext); err != nil {
			return err
		}
	} else if s.OAuthURI != "" || s.OAuthScope != "" || s.OAuthAudience != "" || s.OAuthResource != "" {
		return failure(ErrInput, "oauth")
	}
	if len(s.Headers) > 64 || len(s.Properties) > 256 {
		return failure(ErrInput, "property-count")
	}
	for key, value := range s.Headers {
		if !managedText(key, 128, false) || !managedText(value, 8192, true) || strings.ContainsAny(key, " ()<>@,;:\\[]?={}/\"") {
			return failure(ErrInput, "header")
		}
		switch strings.ToLower(key) {
		case "host", "authorization", "proxy-authorization", "content-length", "transfer-encoding", "connection", "x-iceberg-access-delegation":
			return failure(ErrAuthority, "header")
		}
	}
	for key, value := range s.Properties {
		if !managedText(key, 256, false) || !managedText(value, 8192, true) || managedAuthorityProperty(key) {
			return failure(ErrAuthority, "property")
		}
		if key == "metrics-reporter-impl" && value != "" && value != "nop" {
			return failure(ErrAuthority, "reporter-registry")
		}
	}
	if len(s.RootCAPEM) > 256<<10 {
		return failure(ErrInput, "trust")
	}
	if s.RootCAPEM != "" {
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM([]byte(s.RootCAPEM)) {
			return failure(ErrInput, "trust")
		}
	}
	if n.TLS != nil && (n.TLS.InsecureSkipVerify || n.TLS.MinVersion != 0 && n.TLS.MinVersion < tls.VersionTLS12 || s.RootCAPEM != "") {
		return failure(ErrInput, "tls")
	}
	if n.OAuthTLS != nil && (s.OAuthCredential == "" || n.OAuthTLS.InsecureSkipVerify || n.OAuthTLS.MinVersion != 0 && n.OAuthTLS.MinVersion < tls.VersionTLS12) {
		return failure(ErrInput, "oauth-tls")
	}
	return nil
}

func managedAuthorityProperty(key string) bool {
	switch key {
	case "uri", "prefix", "warehouse", "token", "credential", "oauth2-server-uri", "rest.authorization-url", "scope", "audience", "resource", "tls-skip-verify", "rest.sigv4-enabled", "rest.signing-region", "rest.signing-name":
		return true
	}
	return strings.HasPrefix(key, "header.") || strings.HasPrefix(key, "s3.") || strings.HasPrefix(key, "client.")
}

// Select retains no native work until resource.Assemble admits construction.
func (p ManagedPreparation) Select() (resource.Selection[ManagedSource], error) {
	if err := validateManagedSettings(p.settings, p.native); err != nil {
		return resource.Selection[ManagedSource]{}, err
	}
	prepared, err := resource.Prepare(resource.Schema[ManagedSettings]{Format: 1, Defaults: p.settings, Validate: func(s ManagedSettings) error { return validateManagedSettings(s, p.native) }}, resource.Input{Identity: resource.Identity{Provider: ProviderID, Name: p.settings.Name}, Format: 1})
	if err != nil {
		return resource.Selection[ManagedSource]{}, err
	}
	return resource.Select(prepared, func(ctx context.Context, s ManagedSettings) (resource.Resource[ManagedSource], error) {
		return newManagedSource(ctx, ManagedPreparation{settings: s, native: p.native})
	}), nil
}
