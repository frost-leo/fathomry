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

package franz

import (
	"crypto/tls"
	"crypto/x509"
	"net"
	"net/netip"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/frost-leo/fathomry/internal/compatibility"
	"github.com/frost-leo/fathomry/internal/resource"
)

// OptionsV1 is borrowed only during Select. Do not mutate it concurrently with
// Select. No native options, hooks, TLS/SASL objects, DNS or environment discovery
// are accepted. All durations are nanoseconds in configuration layers.
// Version zero selects format 1; other versions reject. Source revisions are independent.
type OptionsV1 struct {
	private
	Name    string
	Version uint32
	// Brokers is both the bootstrap set and complete permitted dial endpoint set:
	// 1–16 distinct literal IP:port pairs. Metadata cannot expand this authority.
	Brokers []string
	// ClusterID is required. Topics (1–32 names) are the only permitted topics.
	// Their observed incarnation IDs are frozen at assembly readiness.
	ClusterID string
	Topics    []string
	// Plaintext explicitly authorizes unencrypted transport. Otherwise RootCAPEM
	// (1–64 KiB) is the complete trust set; peer names are verified from the IP.
	Plaintext bool
	RootCAPEM string
	// TransactionalID optionally enables a separate, serialized transactional
	// producer. It must be exclusively assigned by composition (1–128 bytes).
	TransactionalID string
	// OffsetGroup enables explicit checkpoints for manually assigned reads, not
	// consumer-group membership or rebalance ownership. Empty disables it.
	// Composition exclusively owns this group name (1–128 bytes). Checkpoints
	// require Kafka OffsetCommit/OffsetFetch v10; no name-only downgrade occurs.
	OffsetGroup string
	// MaxActive defaults to 4 (1–16), QueuedCalls to 0 (0–64, reject overload).
	MaxActive   int
	QueuedCalls int
	// Count and byte bounds are independent. Defaults: 256 records (1–4096),
	// 1 MiB/record (1 KiB–4 MiB), 4 MiB/call (record bound–16 MiB).
	MaxRecords     int
	MaxRecordBytes int
	MaxBatchBytes  int
	// MaxWireBytes defaults to 8 MiB (batch bound+1024–32 MiB).
	// MaxDecodedBatchBytes defaults to 4 MiB (record bound+512–16 MiB).
	// Neither wire nor decoded limits are RSS limits.
	MaxWireBytes         int
	MaxDecodedBatchBytes int
	// MaxDecodedRecords defaults to 4096 (1–65536). It bounds all decoded
	// records, including control/aborted data, independently of output page size.
	// It must cover MaxActive*MaxRecords, because native batching may coalesce
	// concurrently submitted records. Decoded bytes must cover record bytes+512.
	MaxDecodedRecords int
	// Timeout defaults to 10 s (1 s–1 min), separately for admission and work.
	// CleanupTimeout defaults to 5 s (1 ms–30 s). Native in-flight delivery
	// cancellation is cooperative, not a hard local-use deadline.
	Timeout        time.Duration
	CleanupTimeout time.Duration
	// Retries selects the SDK's ordinary retry limit, range 0–10. Uncertain
	// idempotent delivery can exceed it until producer fencing; background
	// metadata/PID/coordinator recovery is not an exact attempt count or quota.
	Retries int
	// Linger defaults to zero, range 0–1 s; Compression defaults to "none".
	// Only "none" and "gzip" are supported on production AND consumption.
	Linger      time.Duration
	Compression string
}

type settings struct {
	Brokers              []string      `json:"brokers"`
	ClusterID            string        `json:"cluster_id"`
	Topics               []string      `json:"topics"`
	Plaintext            bool          `json:"plaintext"`
	RootCAPEM            string        `json:"root_ca_pem"`
	TransactionalID      string        `json:"transactional_id"`
	OffsetGroup          string        `json:"offset_group"`
	MaxActive            int           `json:"max_active"`
	QueuedCalls          int           `json:"queued_calls"`
	MaxRecords           int           `json:"max_records"`
	MaxRecordBytes       int           `json:"max_record_bytes"`
	MaxBatchBytes        int           `json:"max_batch_bytes"`
	MaxWireBytes         int           `json:"max_wire_bytes"`
	MaxDecodedBatchBytes int           `json:"max_decoded_batch_bytes"`
	MaxDecodedRecords    int           `json:"max_decoded_records"`
	Timeout              time.Duration `json:"timeout_ns"`
	CleanupTimeout       time.Duration `json:"cleanup_timeout_ns"`
	Retries              int           `json:"retries"`
	Linger               time.Duration `json:"linger_ns"`
	Compression          string        `json:"compression"`
}

func defaults(input OptionsV1) settings {
	value := settings{Brokers: input.Brokers, ClusterID: input.ClusterID, Topics: input.Topics,
		Plaintext: input.Plaintext, RootCAPEM: input.RootCAPEM, TransactionalID: input.TransactionalID,
		OffsetGroup: input.OffsetGroup,
		MaxActive:   input.MaxActive, QueuedCalls: input.QueuedCalls, MaxRecords: input.MaxRecords,
		MaxRecordBytes: input.MaxRecordBytes, MaxBatchBytes: input.MaxBatchBytes, MaxWireBytes: input.MaxWireBytes,
		MaxDecodedBatchBytes: input.MaxDecodedBatchBytes, Timeout: input.Timeout, CleanupTimeout: input.CleanupTimeout,
		MaxDecodedRecords: input.MaxDecodedRecords,
		Retries:           input.Retries, Linger: input.Linger, Compression: input.Compression}
	if value.MaxActive == 0 {
		value.MaxActive = 4
	}
	if value.MaxRecords == 0 {
		value.MaxRecords = 256
	}
	if value.MaxRecordBytes == 0 {
		value.MaxRecordBytes = 1 << 20
	}
	if value.MaxBatchBytes == 0 {
		value.MaxBatchBytes = 4 << 20
	}
	if value.MaxWireBytes == 0 {
		value.MaxWireBytes = 8 << 20
	}
	if value.MaxDecodedBatchBytes == 0 {
		value.MaxDecodedBatchBytes = 4 << 20
	}
	if value.MaxDecodedRecords == 0 {
		value.MaxDecodedRecords = 4096
	}
	if value.Timeout == 0 {
		value.Timeout = 10 * time.Second
	}
	if value.CleanupTimeout == 0 {
		value.CleanupTimeout = 5 * time.Second
	}
	if value.Compression == "" {
		value.Compression = "none"
	}
	return value
}
func validText(value string, limit int) bool {
	return value != "" && len(value) <= limit && utf8.ValidString(value) && !strings.ContainsRune(value, 0)
}
func validTopic(value string) bool {
	if len(value) == 0 || len(value) > 249 || value == "." || value == ".." {
		return false
	}
	for _, char := range value {
		if !(char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' || char == '.' || char == '_' || char == '-') {
			return false
		}
	}
	return true
}
func endpoint(value string) (netip.AddrPort, error) {
	parsed, err := netip.ParseAddrPort(value)
	if err != nil || parsed.Port() == 0 || parsed.Addr().IsUnspecified() || parsed.Addr().Zone() != "" || parsed.String() != value {
		return netip.AddrPort{}, failure(ErrInput, "endpoint")
	}
	return parsed, nil
}
func validate(value settings) error {
	if len(value.Brokers) < 1 || len(value.Brokers) > 16 || len(value.Topics) < 1 || len(value.Topics) > 32 ||
		!validText(value.ClusterID, 128) || value.TransactionalID != "" && !validText(value.TransactionalID, 128) ||
		value.OffsetGroup != "" && !validText(value.OffsetGroup, 128) ||
		value.MaxActive < 1 || value.MaxActive > 16 || value.QueuedCalls < 0 || value.QueuedCalls > 64 ||
		value.MaxRecords < 1 || value.MaxRecords > 4096 || value.MaxRecordBytes < 1024 || value.MaxRecordBytes > 4<<20 ||
		value.MaxBatchBytes < value.MaxRecordBytes || value.MaxBatchBytes > 16<<20 ||
		value.MaxWireBytes < value.MaxBatchBytes+1024 || value.MaxWireBytes > 32<<20 ||
		value.MaxDecodedBatchBytes < value.MaxRecordBytes+512 || value.MaxDecodedBatchBytes > 16<<20 ||
		value.MaxDecodedRecords < value.MaxActive*value.MaxRecords || value.MaxDecodedRecords > 65536 ||
		value.Timeout < time.Second || value.Timeout > time.Minute || value.Timeout%time.Millisecond != 0 ||
		value.CleanupTimeout < time.Millisecond || value.CleanupTimeout > 30*time.Second ||
		value.Retries < 0 || value.Retries > 10 || value.Linger < 0 || value.Linger > time.Second ||
		value.Compression != "none" && value.Compression != "gzip" {
		return failure(ErrInput, "options")
	}
	seen := make(map[string]bool)
	for _, address := range value.Brokers {
		_, err := endpoint(address)
		if err != nil || seen[address] {
			return failure(ErrInput, "brokers", err)
		}
		seen[address] = true
	}
	seen = make(map[string]bool)
	for _, topic := range value.Topics {
		if !validTopic(topic) || seen[topic] {
			return failure(ErrInput, "topics")
		}
		seen[topic] = true
	}
	_, err := tlsConfig(value)
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
	if !roots.AppendCertsFromPEM([]byte(value.RootCAPEM)) {
		return nil, failure(ErrInput, "tls")
	}
	return &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}, nil
}
func (value settings) allowed(address string) bool {
	for _, permitted := range value.Brokers {
		if permitted == address {
			return true
		}
	}
	return false
}
func brokerAddress(host string, port int32) string {
	return net.JoinHostPort(host, strconv.Itoa(int(port)))
}
func (value settings) reservation() int64 {
	return int64(2*value.MaxBatchBytes + 2*value.MaxWireBytes + 2*value.MaxDecodedBatchBytes + value.MaxRecords*4096 + value.MaxDecodedRecords*8192 + 65536)
}
func (value settings) evidenceReservation() int64 {
	return int64(value.MaxBatchBytes + value.MaxRecords*4096 + 65536)
}
func (value settings) limits() resource.Limits {
	return resource.Limits{Active: value.MaxActive, Queued: value.QueuedCalls,
		Bytes: int64(value.MaxActive) * value.reservation(), QueuedBytes: int64(value.QueuedCalls) * value.reservation(), MaxLeases: 2}
}

// LimitsV1 recommends limits for defaulted options. Composition must attach
// resource.WithLimits; when overlays change bounds, use the resolved policy.
func LimitsV1(options OptionsV1) resource.Limits { return defaults(options).limits() }

// Profile reports effective, non-secret settings, not service qualification.
func (client *Client) Profile() compatibility.Profile {
	if client == nil || client.owner == nil {
		return compatibility.Profile{}
	}
	value := client.owner.settings
	declared := func(value string) compatibility.Fact {
		return compatibility.Fact{Kind: compatibility.Declared, Value: value}
	}
	options := []compatibility.Option{
		{Name: "acks", Value: "all-isr"}, {Name: "idempotent", Value: "true"}, {Name: "isolation", Value: "read-committed"},
		{Name: "partitioner", Value: "manual"}, {Name: "auto-create", Value: "false"}, {Name: "client-metrics", Value: "false"},
		{Name: "compression", Value: value.Compression}, {Name: "plaintext", Value: strconv.FormatBool(value.Plaintext)},
		{Name: "transactions", Value: strconv.FormatBool(value.TransactionalID != "")},
		{Name: "manual-offsets", Value: strconv.FormatBool(value.OffsetGroup != "")},
		{Name: "delivery-timeout-owner", Value: "invocation-and-producer-fence"},
	}
	for _, pair := range []struct {
		name  string
		value int64
	}{
		{"max-active", int64(value.MaxActive)}, {"queued-calls", int64(value.QueuedCalls)},
		{"max-records", int64(value.MaxRecords)}, {"max-record-bytes", int64(value.MaxRecordBytes)},
		{"max-batch-bytes", int64(value.MaxBatchBytes)}, {"max-wire-bytes", int64(value.MaxWireBytes)},
		{"max-decoded-batch-bytes", int64(value.MaxDecodedBatchBytes)},
		{"max-decoded-records", int64(value.MaxDecodedRecords)},
		{"timeout-ns", int64(value.Timeout)}, {"cleanup-timeout-ns", int64(value.CleanupTimeout)},
		{"retries", int64(value.Retries)}, {"linger-ns", int64(value.Linger)},
	} {
		options = append(options, compatibility.Option{Name: pair.name, Value: strconv.FormatInt(pair.value, 10)})
	}
	return compatibility.Profile{ImplementationModule: compatibility.FrameworkModule, SDKMode: "franz-bounded-produce-exact-read",
		Protocol: declared("kafka-topic-ids"), Native: compatibility.Fact{Kind: compatibility.NotApplicable}, Options: options}
}
