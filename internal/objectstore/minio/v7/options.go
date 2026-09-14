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

package minio

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/frost-leo/fathomry/internal/compatibility"
	"github.com/frost-leo/fathomry/internal/resource"
	"github.com/minio/minio-go/v7/pkg/s3utils"
)

// OptionsV1 is borrowed during Select only; do not mutate it concurrently.
// Format zero selects 1. Layers use the settings' snake_case field names and
// nanosecond durations. Secrets participate in resource's private preparation;
// Profile never reports endpoint, bucket, prefix, keys, credentials or roots.
type OptionsV1 struct {
	private
	Name    string
	Version uint32
	// Endpoint is an http(s) URL with a literal IP and explicit port, no path.
	// HTTP requires Plaintext. HTTPS requires the complete RootCAPEM trust set.
	Endpoint  string
	Plaintext bool
	RootCAPEM string
	Region    string
	Bucket    string
	Prefix    string
	AccessKey string
	SecretKey string
	// SessionToken is static; expiry/rotation requires a new source. No refresh.
	SessionToken string
	// Writes, Versions and Tags are explicit capability grants, not IAM evidence.
	Writes   bool
	Versions bool
	Tags     bool
	// MaxActive defaults to 4 (1–16); QueuedCalls defaults to 0 (0–64).
	MaxActive   int
	QueuedCalls int
	// MaxTransferBytes defaults to 64 MiB (1–PartBytes*MaxParts).
	// MaxReadBytes defaults to 8 MiB (1–16 MiB), and cannot exceed transfer size.
	MaxTransferBytes int64
	MaxReadBytes     int
	// PartBytes defaults to 5 MiB (5–16 MiB); MaxParts to 64 (1–1000).
	// One part buffer per upload; parallel automatic SDK multipart is unavailable.
	PartBytes int
	MaxParts  int
	// MaxEntries defaults to 128 (1–1000), independently of the XML byte bound.
	MaxEntries int
	// MaxResponseBytes defaults to 1 MiB (1 KiB–8 MiB), aggregate control-body
	// bytes per call. Payload bodies instead use the transfer/read limit.
	MaxResponseBytes int64
	// MaxRequests defaults to 128 (1–2048). It bounds intercepted SDK HTTP
	// exchanges, including listing pages and cleanup, not TCP retransmissions.
	MaxRequests int
	// Timeout defaults to 30 s (1 ms–5 min), separately admission and work.
	// CleanupTimeout defaults to 5 s (1 ms–30 s) within the explicitly supplied
	// cleanup context. No detached or automatic cleanup retry occurs.
	Timeout        time.Duration
	CleanupTimeout time.Duration
}
type settings struct {
	Endpoint         string        `json:"endpoint"`
	Plaintext        bool          `json:"plaintext"`
	RootCAPEM        string        `json:"root_ca_pem"`
	Region           string        `json:"region"`
	Bucket           string        `json:"bucket"`
	Prefix           string        `json:"prefix"`
	AccessKey        string        `json:"access_key"`
	SecretKey        string        `json:"secret_key"`
	SessionToken     string        `json:"session_token"`
	Writes           bool          `json:"writes"`
	Versions         bool          `json:"versions"`
	Tags             bool          `json:"tags"`
	MaxActive        int           `json:"max_active"`
	QueuedCalls      int           `json:"queued_calls"`
	MaxTransferBytes int64         `json:"max_transfer_bytes"`
	MaxReadBytes     int           `json:"max_read_bytes"`
	PartBytes        int           `json:"part_bytes"`
	MaxParts         int           `json:"max_parts"`
	MaxEntries       int           `json:"max_entries"`
	MaxResponseBytes int64         `json:"max_response_bytes"`
	MaxRequests      int           `json:"max_requests"`
	Timeout          time.Duration `json:"timeout_ns"`
	CleanupTimeout   time.Duration `json:"cleanup_timeout_ns"`
}

func defaults(input OptionsV1) settings {
	value := settings{
		Endpoint: input.Endpoint, Plaintext: input.Plaintext, RootCAPEM: input.RootCAPEM,
		Region: input.Region, Bucket: input.Bucket, Prefix: input.Prefix, AccessKey: input.AccessKey,
		SecretKey: input.SecretKey, SessionToken: input.SessionToken, Writes: input.Writes,
		Versions: input.Versions, Tags: input.Tags, MaxActive: input.MaxActive, QueuedCalls: input.QueuedCalls,
		MaxTransferBytes: input.MaxTransferBytes, MaxReadBytes: input.MaxReadBytes,
		PartBytes: input.PartBytes, MaxParts: input.MaxParts, MaxEntries: input.MaxEntries,
		MaxResponseBytes: input.MaxResponseBytes, MaxRequests: input.MaxRequests,
		Timeout: input.Timeout, CleanupTimeout: input.CleanupTimeout}
	if value.MaxActive == 0 {
		value.MaxActive = 4
	}
	if value.MaxTransferBytes == 0 {
		value.MaxTransferBytes = 64 << 20
	}
	if value.MaxReadBytes == 0 {
		value.MaxReadBytes = 8 << 20
	}
	if value.PartBytes == 0 {
		value.PartBytes = 5 << 20
	}
	if value.MaxParts == 0 {
		value.MaxParts = 64
	}
	if value.MaxEntries == 0 {
		value.MaxEntries = 128
	}
	if value.MaxResponseBytes == 0 {
		value.MaxResponseBytes = 1 << 20
	}
	if value.MaxRequests == 0 {
		value.MaxRequests = 128
	}
	if value.Timeout == 0 {
		value.Timeout = 30 * time.Second
	}
	if value.CleanupTimeout == 0 {
		value.CleanupTimeout = 5 * time.Second
	}
	return value
}
func validText(value string, maximum int, empty bool) bool {
	if len(value) > maximum || !empty && value == "" || !utf8.ValidString(value) {
		return false
	}
	for _, char := range value {
		if char < 32 || char == 127 {
			return false
		}
	}
	return true
}
func validPath(value string, empty bool) bool {
	if !validText(value, 1024, empty) || strings.Contains(value, "\\") {
		return false
	}
	for _, part := range strings.Split(value, "/") {
		if part == "." || part == ".." {
			return false
		}
	}
	return true
}
func validate(value settings) error {
	endpoint, err := url.Parse(value.Endpoint)
	if err != nil || endpoint == nil || endpoint.User != nil || endpoint.RawQuery != "" || endpoint.ForceQuery || endpoint.Fragment != "" || endpoint.String() != value.Endpoint ||
		endpoint.Path != "" || endpoint.Opaque != "" || endpoint.Scheme != "http" && endpoint.Scheme != "https" {
		return failure(ErrInput, "endpoint")
	}
	address, err := netip.ParseAddrPort(endpoint.Host)
	if err != nil || address.Port() == 0 || address.Addr().IsUnspecified() || address.Addr().Zone() != "" || address.String() != endpoint.Host {
		return failure(ErrUnsupported, "endpoint")
	}
	if endpoint.Scheme == "http" != value.Plaintext {
		return failure(ErrInput, "transport")
	}
	if strings.HasSuffix(value.Bucket, "--x-s3") {
		return failure(ErrUnsupported, "directory-bucket")
	}
	if !validText(value.Region, 64, false) || strings.ContainsAny(value.Region, " /") ||
		s3utils.CheckValidBucketNameStrict(value.Bucket) != nil || !validPath(value.Prefix, true) ||
		!validText(value.AccessKey, 128, false) || !validText(value.SecretKey, 256, false) || !validText(value.SessionToken, 8192, true) {
		return failure(ErrInput, "source")
	}
	if value.MaxActive < 1 || value.MaxActive > 16 || value.QueuedCalls < 0 || value.QueuedCalls > 64 ||
		value.PartBytes < 5<<20 || value.PartBytes > 16<<20 || value.MaxParts < 1 || value.MaxParts > 1000 ||
		value.MaxTransferBytes < 1 || value.MaxTransferBytes > int64(value.PartBytes)*int64(value.MaxParts) ||
		value.MaxReadBytes < 1 || value.MaxReadBytes > 16<<20 || int64(value.MaxReadBytes) > value.MaxTransferBytes ||
		value.MaxEntries < 1 || value.MaxEntries > 1000 || value.MaxResponseBytes < 1024 || value.MaxResponseBytes > 8<<20 ||
		value.MaxRequests < 1 || value.MaxRequests > 2048 || value.Timeout < time.Millisecond || value.Timeout > 5*time.Minute ||
		value.CleanupTimeout < time.Millisecond || value.CleanupTimeout > 30*time.Second {
		return failure(ErrInput, "limits")
	}
	_, err = tlsConfig(value)
	return err
}
func tlsConfig(value settings) (*tls.Config, error) {
	if value.Plaintext {
		if value.RootCAPEM != "" {
			return nil, failure(ErrInput, "tls")
		}
		return nil, nil
	}
	if len(value.RootCAPEM) == 0 || len(value.RootCAPEM) > 64<<10 {
		return nil, failure(ErrInput, "tls")
	}
	roots := x509.NewCertPool()
	remaining := bytes.TrimSpace([]byte(value.RootCAPEM))
	if len(remaining) == 0 {
		return nil, failure(ErrInput, "tls")
	}
	for len(remaining) > 0 {
		const begin, end = "-----BEGIN CERTIFICATE-----", "-----END CERTIFICATE-----"
		endIndex := bytes.Index(remaining, []byte(end))
		if !bytes.HasPrefix(remaining, []byte(begin)) || endIndex < 0 {
			return nil, failure(ErrInput, "tls")
		}
		block, rest := pem.Decode(remaining[:endIndex+len(end)])
		if block == nil || block.Type != "CERTIFICATE" || len(block.Headers) != 0 || len(bytes.TrimSpace(rest)) != 0 {
			return nil, failure(ErrInput, "tls")
		}
		certificate, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return nil, failure(ErrInput, "tls", err)
		}
		roots.AddCert(certificate)
		remaining = bytes.TrimSpace(remaining[endIndex+len(end):])
	}
	return &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}, nil
}
func (value settings) reservation() int64 {
	return int64(2*value.PartBytes+2*value.MaxReadBytes+value.MaxEntries*4096+value.MaxRequests*1024+65536) + 8*value.MaxResponseBytes
}
func (value settings) evidenceReservation() int64 {
	return int64(value.MaxReadBytes+value.MaxEntries*4096+value.MaxRequests*1024+65536) + 4*value.MaxResponseBytes
}

// LimitsV1 recommends defaulted resource limits. Attach resource.WithLimits;
// overlay changes require composition to use the corresponding effective limits.
func LimitsV1(options OptionsV1) resource.Limits {
	value := defaults(options)
	return resource.Limits{Active: value.MaxActive, Queued: value.QueuedCalls,
		Bytes: int64(value.MaxActive) * value.reservation(), QueuedBytes: int64(value.QueuedCalls) * value.reservation(), MaxLeases: 1}
}

// Profile describes effective local options, not tested service compatibility.
func (client *Client) Profile() compatibility.Profile {
	if client == nil || client.owner == nil {
		return compatibility.Profile{}
	}
	value := client.owner.settings
	opts := []compatibility.Option{{Name: "addressing", Value: "path-literal-ip"}, {Name: "credentials", Value: "static-v4"},
		{Name: "sdk-retries", Value: "disabled"}, {Name: "multipart", Value: "serial-core"}, {Name: "plaintext", Value: strconv.FormatBool(value.Plaintext)},
		{Name: "writes", Value: strconv.FormatBool(value.Writes)}, {Name: "versions", Value: strconv.FormatBool(value.Versions)},
		{Name: "tags", Value: strconv.FormatBool(value.Tags)}}
	for _, pair := range []struct {
		name  string
		value int64
	}{
		{"max-active", int64(value.MaxActive)}, {"queued-calls", int64(value.QueuedCalls)}, {"max-transfer-bytes", value.MaxTransferBytes},
		{"max-read-bytes", int64(value.MaxReadBytes)}, {"part-bytes", int64(value.PartBytes)}, {"max-parts", int64(value.MaxParts)},
		{"max-entries", int64(value.MaxEntries)}, {"max-response-bytes", value.MaxResponseBytes}, {"max-requests", int64(value.MaxRequests)},
		{"timeout-ns", int64(value.Timeout)}, {"cleanup-timeout-ns", int64(value.CleanupTimeout)}} {
		opts = append(opts, compatibility.Option{Name: pair.name, Value: strconv.FormatInt(pair.value, 10)})
	}
	return compatibility.Profile{ImplementationModule: compatibility.FrameworkModule, SDKMode: "minio-bounded-http-core",
		Protocol: compatibility.Fact{Kind: compatibility.Declared, Value: "s3-v4"},
		Native:   compatibility.Fact{Kind: compatibility.NotApplicable}, Options: opts}
}
