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
	"bytes"
	"crypto/x509"
	"encoding/pem"
	"net/netip"
	"net/url"
	"strings"
	"time"

	"github.com/frost-leo/fathomry/internal/resource"
)

// OptionsV1 is borrowed during Select. Version zero selects options format 1,
// independently of SDK major 0 and the admitted Iceberg table format 2.
// Namespace and Location confine table operations to one explicit namespace and
// S3 subtree. Plaintext explicitly opts into trusted-network HTTP on both peers.
// Static storage credentials are used. Server-vended credentials are never used
// and no credential refresher runs, even if the Catalog sends them unsolicited.
type OptionsV1 struct {
	private
	Name                string
	Version             uint32
	CatalogURI          string
	CatalogPrefix       string
	Warehouse           string
	Namespace           string
	Location            string
	Plaintext           bool
	RootCAPEM           string
	BearerToken         string
	StorageEndpoint     string
	StorageRegion       string
	StorageAccessKey    string
	StorageSecretKey    string
	StorageSessionToken string
	Writes              bool
	MaxActive           int
	MaxRows             int
	MaxBatchBytes       int
	MaxObjectBytes      int
	MaxFileOps          int
	MaxIOBytes          int64
	MaxMetadataBytes    int
	Timeout             time.Duration
}

type settings struct {
	CatalogURI          string        `json:"catalog_uri"`
	CatalogPrefix       string        `json:"catalog_prefix"`
	Warehouse           string        `json:"warehouse"`
	Namespace           string        `json:"namespace"`
	Location            string        `json:"location"`
	Plaintext           bool          `json:"plaintext"`
	RootCAPEM           string        `json:"root_ca_pem"`
	BearerToken         string        `json:"bearer_token"`
	StorageEndpoint     string        `json:"storage_endpoint"`
	StorageRegion       string        `json:"storage_region"`
	StorageAccessKey    string        `json:"storage_access_key"`
	StorageSecretKey    string        `json:"storage_secret_key"`
	StorageSessionToken string        `json:"storage_session_token"`
	Writes              bool          `json:"writes"`
	MaxActive           int           `json:"max_active"`
	MaxRows             int           `json:"max_rows"`
	MaxBatchBytes       int           `json:"max_batch_bytes"`
	MaxObjectBytes      int           `json:"max_object_bytes"`
	MaxFileOps          int           `json:"max_file_ops"`
	MaxIOBytes          int64         `json:"max_io_bytes"`
	MaxMetadataBytes    int           `json:"max_metadata_bytes"`
	Timeout             time.Duration `json:"timeout_ns"`
}

func defaults(o OptionsV1) settings {
	s := settings{CatalogURI: o.CatalogURI, CatalogPrefix: o.CatalogPrefix, Warehouse: o.Warehouse,
		Namespace: o.Namespace, Location: o.Location, Plaintext: o.Plaintext, RootCAPEM: o.RootCAPEM, BearerToken: o.BearerToken,
		StorageEndpoint: o.StorageEndpoint, StorageRegion: o.StorageRegion, StorageAccessKey: o.StorageAccessKey,
		StorageSecretKey: o.StorageSecretKey, StorageSessionToken: o.StorageSessionToken, Writes: o.Writes,
		MaxActive: o.MaxActive, MaxRows: o.MaxRows, MaxBatchBytes: o.MaxBatchBytes, MaxObjectBytes: o.MaxObjectBytes,
		MaxFileOps: o.MaxFileOps, MaxIOBytes: o.MaxIOBytes, MaxMetadataBytes: o.MaxMetadataBytes, Timeout: o.Timeout}
	if s.MaxActive == 0 {
		s.MaxActive = 2
	}
	if s.MaxRows == 0 {
		s.MaxRows = 65536
	}
	if s.MaxBatchBytes == 0 {
		s.MaxBatchBytes = 8 << 20
	}
	if s.MaxObjectBytes == 0 {
		s.MaxObjectBytes = 16 << 20
	}
	if s.MaxFileOps == 0 {
		s.MaxFileOps = 128
	}
	if s.MaxIOBytes == 0 {
		s.MaxIOBytes = 128 << 20
	}
	if s.MaxMetadataBytes == 0 {
		s.MaxMetadataBytes = 1 << 20
	}
	if s.Timeout == 0 {
		s.Timeout = time.Minute
	}
	return s
}
func nameOK(name string) bool {
	if len(name) < 1 || len(name) > 64 {
		return false
	}
	for _, char := range name {
		if !(char >= 'a' && char <= 'z' || char >= '0' && char <= '9' || char == '_') {
			return false
		}
	}
	return true
}
func endpointOK(text string, plaintext bool) bool {
	uri, err := url.Parse(text)
	if err != nil || uri.User != nil || uri.RawQuery != "" || uri.Fragment != "" || uri.Opaque != "" ||
		uri.RawPath != "" || uri.ForceQuery || uri.String() != text ||
		(uri.Scheme != "http" && uri.Scheme != "https") || (uri.Scheme == "http") != plaintext {
		return false
	}
	address, err := netip.ParseAddrPort(uri.Host)
	return err == nil && address.Port() != 0 && !address.Addr().IsUnspecified() && address.Addr().Zone() == "" &&
		address.String() == uri.Host && !strings.Contains(uri.Path, "..") && !strings.HasSuffix(uri.Path, "/")
}
func validate(s settings) error {
	if len(s.CatalogURI) > 1024 || len(s.StorageEndpoint) > 1024 || len(s.Location) > 960 ||
		!plainSecret(s.CatalogPrefix, 128, true) || !plainSecret(s.Warehouse, 256, false) || !plainSecret(s.BearerToken, 8192, true) {
		return failure(ErrInput, "source-bounds")
	}
	if !endpointOK(s.CatalogURI, s.Plaintext) || !nameOK(s.Namespace) ||
		len(s.Warehouse) == 0 || len(s.Warehouse) > 256 || len(s.CatalogPrefix) > 128 ||
		strings.ContainsAny(s.CatalogPrefix, "/\\?#%\r\n") || strings.ContainsAny(s.Warehouse, "\r\n") ||
		len(s.BearerToken) > 8192 || strings.ContainsAny(s.BearerToken, "\r\n") {
		return failure(ErrInput, "source")
	}
	uri, err := url.Parse(s.Location)
	if err != nil || uri.Scheme != "s3" || uri.User != nil || uri.Host == "" || uri.Port() != "" ||
		uri.RawQuery != "" || uri.Fragment != "" || uri.RawPath != "" || uri.Opaque != "" || uri.ForceQuery || uri.String() != s.Location ||
		!strings.HasSuffix(uri.Path, "/") || uri.Path == "/" || strings.Contains(uri.Path, "//") {
		return failure(ErrInput, "location")
	}
	for _, part := range strings.Split(strings.Trim(uri.Path, "/"), "/") {
		if !nameOK(part) && !safePathPart(part) {
			return failure(ErrInput, "location")
		}
	}
	if s.MaxActive < 1 || s.MaxActive > 4 || s.MaxRows < 1 || s.MaxRows > 1<<20 ||
		s.MaxBatchBytes < 1024 || s.MaxBatchBytes > 16<<20 || s.MaxObjectBytes < 1024 || s.MaxObjectBytes > 16<<20 ||
		s.MaxFileOps < 1 || s.MaxFileOps > 1024 || s.MaxIOBytes < int64(s.MaxObjectBytes) || s.MaxIOBytes > 1<<30 ||
		s.MaxMetadataBytes < 1024 || s.MaxMetadataBytes > 8<<20 ||
		s.Timeout < time.Millisecond || s.Timeout > 5*time.Minute {
		return failure(ErrInput, "limits")
	}
	if !endpointOK(s.StorageEndpoint, s.Plaintext) {
		return failure(ErrInput, "storage-endpoint")
	}
	storage, _ := url.Parse(s.StorageEndpoint)
	if storage.Path != "" {
		return failure(ErrInput, "storage-endpoint")
	}
	bucket, _ := s.storageAddress()
	if len(bucket) < 3 || len(bucket) > 63 || strings.Contains(bucket, "..") || strings.HasSuffix(bucket, "--x-s3") {
		return failure(ErrInput, "bucket")
	}
	for _, char := range bucket {
		if !(char >= 'a' && char <= 'z' || char >= '0' && char <= '9' || char == '-' || char == '.') {
			return failure(ErrInput, "bucket")
		}
	}
	if bucket[0] == '-' || bucket[0] == '.' || bucket[len(bucket)-1] == '-' || bucket[len(bucket)-1] == '.' {
		return failure(ErrInput, "bucket")
	}
	if _, err := netip.ParseAddr(bucket); err == nil {
		return failure(ErrUnsupported, "bucket")
	}
	if !safePathPart(s.StorageRegion) || len(s.StorageRegion) > 64 || !plainSecret(s.StorageAccessKey, 128, false) || strings.ContainsAny(s.StorageAccessKey, "/ ") || !plainSecret(s.StorageSecretKey, 256, false) || !plainSecret(s.StorageSessionToken, 8192, true) {
		return failure(ErrInput, "storage-credentials")
	}
	if s.Plaintext {
		if s.RootCAPEM != "" {
			return failure(ErrInput, "trust")
		}
		return nil
	}
	if len(s.RootCAPEM) == 0 || len(s.RootCAPEM) > 64<<10 {
		return failure(ErrInput, "trust")
	}
	remaining := bytes.TrimSpace([]byte(s.RootCAPEM))
	if len(remaining) == 0 {
		return failure(ErrInput, "trust")
	}
	for len(remaining) > 0 {
		if !bytes.HasPrefix(remaining, []byte("-----BEGIN CERTIFICATE-----")) {
			return failure(ErrInput, "trust")
		}
		block, rest := pem.Decode(remaining)
		if block == nil || block.Type != "CERTIFICATE" || len(block.Headers) != 0 {
			return failure(ErrInput, "trust")
		}
		if _, err := x509.ParseCertificate(block.Bytes); err != nil {
			return failure(ErrInput, "trust", err)
		}
		remaining = bytes.TrimSpace(rest)
	}
	return nil
}

func plainSecret(value string, maximum int, empty bool) bool {
	if len(value) > maximum || !empty && len(value) == 0 {
		return false
	}
	for _, char := range value {
		if char < 32 || char > 126 {
			return false
		}
	}
	return true
}
func safePathPart(part string) bool {
	if part == "" || part == "." || part == ".." {
		return false
	}
	for _, char := range part {
		if !(char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' || char == '-' || char == '_') {
			return false
		}
	}
	return true
}
func (s settings) storageAddress() (string, string) {
	uri, _ := url.Parse(s.Location)
	bucket, prefix := "", ""
	if uri != nil {
		bucket, prefix = uri.Host, strings.TrimPrefix(uri.Path, "/")
	}
	return bucket, prefix
}
func (s settings) reservation() int64 {
	return 4*s.MaxIOBytes + int64(8*s.MaxBatchBytes+8*s.MaxMetadataBytes+65536)
}
func (s settings) evidenceBytes() int64 {
	return int64(s.MaxBatchBytes + s.MaxMetadataBytes + s.MaxFileOps*(256<<10) + 65536)
}

// LimitsV1 describes local admission and declared byte envelopes, not process RSS.
// Overlays changing limits require composition to attach the matching effective limits.
func LimitsV1(options OptionsV1) resource.Limits {
	s := defaults(options)
	return resource.Limits{Active: s.MaxActive, Bytes: int64(s.MaxActive) * s.reservation(), MaxLeases: 1}
}
