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
	"context"
	"crypto/tls"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/kversion"
)

// Source is an opaque non-owning assembly capability. Bind grants operations.
type Source struct {
	private
	owner *connection
}
type connection struct {
	settings    settings
	native      *kgo.Client
	writer      *managedClient
	transaction *managedClient
	clients     []*managedClient
	topics      map[string]Topic
	txGate      chan struct{}
	poisoned    atomic.Bool
	closeOnce   sync.Once
	closed      chan struct{}
}

// Select validates and freezes configuration with resource's existing overlay
// rules. Composition attaches resource.WithLimits before assembly. Readiness
// reads metadata; it never creates topics or joins groups.
func Select(options OptionsV1, layers ...resource.Layer) (resource.Selection[Source], error) {
	format := options.Version
	if format == 0 {
		format = 1
	}
	prepared, err := resource.Prepare(resource.Schema[settings]{Format: 1, Defaults: defaults(options), Validate: validate},
		resource.Input{Identity: resource.Identity{Provider: ProviderID, Name: options.Name}, Format: format, Layers: layers})
	if err != nil {
		return resource.Selection[Source]{}, err
	}
	selected := resource.Select(prepared, func(ctx context.Context, value settings) (resource.Resource[Source], error) {
		owner := &connection{settings: value, txGate: make(chan struct{}, 1), closed: make(chan struct{})}
		result := resource.Resource[Source]{Capability: Source{owner: owner}, Release: owner.close}
		add := func(transaction bool) (*managedClient, error) {
			native, err := owner.newClient(transaction)
			if err != nil {
				return nil, err
			}
			managed := &managedClient{Client: native, closed: make(chan struct{})}
			owner.clients = append(owner.clients, managed)
			result.Acquired = true
			return managed, nil
		}
		control, err := add(false)
		if err != nil {
			return result, err
		}
		owner.native = control.Client
		owner.writer, err = add(false)
		if err != nil {
			return result, err
		}
		if value.TransactionalID != "" {
			owner.transaction, err = add(true)
			if err != nil {
				return result, err
			}
		}
		result.Check = func(ctx context.Context) error {
			work, cancel, err := (invocation.Budget{Limit: value.Timeout}).Context(ctx, invocation.Establish)
			if err != nil {
				return err
			}
			defer cancel()
			metadata, err := owner.metadata(work)
			if err == nil {
				owner.topics = metadata
				err = owner.checkIdentityWith(work, owner.writer.Client)
			}
			if err == nil && owner.transaction != nil {
				err = owner.checkIdentityWith(work, owner.transaction.Client)
			}
			return err
		}
		return result, nil
	})
	return selected, nil
}

// Client is a concurrent, non-owning facade. All bindings/borrowing aliases share
// the source's native clients, admission, transactional fence and lifecycle.
type Client struct {
	private
	owner    *connection
	access   *resource.Access
	inbox    *invocation.Inbox[Result]
	observer *invocation.Observer
}

// Bind joins the exact selected resource, effective limits and independently
// owned inbox. A borrowing alias cannot enlarge the original allowance.
func Bind(assembly *resource.Assembly, selected resource.Selection[Source], inbox *invocation.Inbox[Result], observer *invocation.Observer) (*Client, error) {
	source, _, err := resource.Bind(assembly, selected)
	if err != nil {
		return nil, err
	}
	access, err := resource.AccessFor(assembly, selected)
	if err != nil {
		return nil, err
	}
	if source.owner == nil || inbox == nil {
		return nil, failure(ErrInput, "bind")
	}
	value, limits := source.owner.settings, access.Limits()
	if limits.Active > value.MaxActive || limits.Queued > value.QueuedCalls || limits.Bytes < value.reservation() ||
		limits.MaxLeases < 2 || limits.Queued > 0 && limits.QueuedBytes < value.reservation() {
		return nil, failure(ErrInput, "limits")
	}
	return &Client{owner: source.owner, access: access, inbox: inbox, observer: observer}, nil
}

func (owner *connection) newClient(transaction bool) (*kgo.Client, error) {
	value := owner.settings
	trust, err := tlsConfig(value)
	if err != nil {
		return nil, err
	}
	dial := func(ctx context.Context, network, address string) (net.Conn, error) {
		if !value.allowed(address) {
			return nil, failure(ErrAuthority, "dial")
		}
		dialer := net.Dialer{Timeout: value.Timeout}
		if trust == nil {
			return dialer.DialContext(ctx, network, address)
		}
		secure := tls.Dialer{NetDialer: &dialer, Config: trust}
		return secure.DialContext(ctx, network, address)
	}
	compression := kgo.NoCompression()
	if value.Compression == "gzip" {
		compression = kgo.GzipCompression()
	}
	minimum := new(kversion.Versions)
	minimum.SetMaxKeyVersion(int16(kmsg.Metadata), 10)
	minimum.SetMaxKeyVersion(int16(kmsg.Fetch), 13)
	minimum.SetMaxKeyVersion(int16(kmsg.InitProducerID), 0)
	maximum := kversion.Stable()
	maximum.SetMaxKeyVersion(int16(kmsg.Metadata), 12)
	maximum.SetMaxKeyVersion(int16(kmsg.Fetch), 13)
	maximum.SetMaxKeyVersion(int16(kmsg.ListOffsets), 7)
	if value.OffsetGroup != "" {
		minimum.SetMaxKeyVersion(int16(kmsg.OffsetCommit), 10)
		minimum.SetMaxKeyVersion(int16(kmsg.OffsetFetch), 10)
		maximum.SetMaxKeyVersion(int16(kmsg.OffsetCommit), 10)
		maximum.SetMaxKeyVersion(int16(kmsg.OffsetFetch), 10)
	}
	opts := []kgo.Opt{
		kgo.SeedBrokers(value.Brokers...), kgo.ClientID("fathomry"), kgo.Dialer(dial),
		kgo.MinVersions(minimum), kgo.MaxVersions(maximum),
		kgo.DisableClientMetrics(), kgo.DisableFetchSessions(),
		kgo.RequestRetries(value.Retries), kgo.RecordRetries(value.Retries), kgo.UnknownTopicRetries(value.Retries),
		kgo.RetryTimeout(value.Timeout), kgo.DialTimeout(value.Timeout),
		kgo.RequestTimeoutOverhead(time.Second), kgo.ProduceRequestTimeout(value.Timeout),
		kgo.RecordDeliveryTimeout(0), kgo.ProducerLinger(value.Linger),
		kgo.BrokerMaxReadBytes(int32(value.MaxWireBytes)), kgo.BrokerMaxWriteBytes(int32(value.MaxWireBytes)),
		kgo.FetchMaxBytes(int32(value.MaxWireBytes - 1024)), kgo.FetchMaxPartitionBytes(int32(value.MaxWireBytes - 1024)),
		kgo.ProducerBatchMaxBytes(int32(value.MaxRecordBytes + 512)),
		kgo.MaxBufferedRecords(value.MaxRecords * value.MaxActive), kgo.MaxBufferedBytes(value.MaxBatchBytes * value.MaxActive),
		kgo.RequiredAcks(kgo.AllISRAcks()), kgo.RecordPartitioner(kgo.ManualPartitioner()), kgo.StopProducerOnDataLossDetected(),
		kgo.ProducerBatchCompression(compression),
	}
	if transaction {
		opts = append(opts, kgo.TransactionalID(value.TransactionalID), kgo.TransactionTimeout(value.Timeout))
	}
	native, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, failure(ErrConnect, "construct", err)
	}
	return native, nil
}
func (owner *connection) close(ctx context.Context) resource.ReleaseResult {
	owner.closeOnce.Do(func() {
		go func() {
			for _, native := range owner.clients {
				native.stop(nil)
			}
			for _, native := range owner.clients {
				<-native.closed
			}
			close(owner.closed)
		}()
	})
	select {
	case <-owner.closed:
		return resource.ReleaseResult{Quiescent: true, Released: true}
	case <-ctx.Done():
		return resource.ReleaseResult{Err: failure(ErrCleanup, "close", ctx.Err(), context.Cause(ctx)), Continue: owner.close}
	}
}

// managedClient owns shutdown independently of admitted calls. Fencing closes
// native I/O but does not release their leases, discard callbacks or prove abort.
type managedClient struct {
	*kgo.Client
	mu        sync.Mutex
	fenced    error
	closeOnce sync.Once
	closed    chan struct{}
}

func (managed *managedClient) failure() error {
	managed.mu.Lock()
	defer managed.mu.Unlock()
	return managed.fenced
}

func (managed *managedClient) stop(cause error) {
	managed.mu.Lock()
	if managed.fenced == nil && cause != nil {
		managed.fenced = cause
	}
	managed.mu.Unlock()
	managed.closeOnce.Do(func() {
		go func() { managed.Client.Close(); close(managed.closed) }()
	})
}

func (managed *managedClient) Close() {
	managed.stop(nil)
	<-managed.closed
}

// watch is installed only after a transaction acquires its exclusive gate, or
// immediately before ordinary native submission. Its owner must join stopWatch.
// Context expiry grants a bounded grace period for late native completion, then
// permanently fences this shared producer rather than silently weakening dedupe.
func (managed *managedClient) watch(ctx context.Context, grace time.Duration) func() error {
	done, joined := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(joined)
		select {
		case <-done:
			return
		case <-ctx.Done():
		}
		timer := time.NewTimer(grace)
		defer timer.Stop()
		select {
		case <-done:
		case <-timer.C:
			managed.stop(failure(ErrState, "producer-fenced", ctx.Err(), context.Cause(ctx)))
		}
	}()
	return func() error { close(done); <-joined; return managed.failure() }
}
func (client *Client) begin(ctx context.Context, correlation fault.Correlation, name string, shape invocation.Shape) (*invocation.Call[Result], error) {
	if client == nil || client.owner == nil || ctx == nil {
		return nil, failure(ErrInput, name)
	}
	value := client.owner.settings
	return invocation.Begin(ctx, client.access, invocation.Request{Name: name, Correlation: correlation, Shape: shape,
		Bytes: value.reservation(), EvidenceBytes: value.evidenceReservation(), Admission: invocation.Budget{Limit: value.Timeout}},
		client.inbox, client.observer)
}

func (client *Client) beginWithin(ctx context.Context, correlation fault.Correlation, name string, parent *invocation.Call[Result]) (*invocation.Call[Result], error) {
	metadata, _ := parent.Receipt().Result()
	if correlation.Parent == "" {
		correlation.Parent = metadata.Context.Correlation.Call
	}
	return invocation.BeginNested(ctx, parent.Scope(), invocation.Request{Name: name, Correlation: correlation,
		Shape: invocation.Finite, EvidenceBytes: client.owner.settings.evidenceReservation(),
		Admission: invocation.Budget{Limit: client.owner.settings.Timeout}}, client.inbox, client.observer)
}

// EvidenceBytes is the declared retained-envelope reservation for each call.
// It excludes caller-retained copies and arbitrary native error graphs, not RSS.
func (client *Client) EvidenceBytes() int64 {
	if client == nil || client.owner == nil {
		return 0
	}
	return client.owner.settings.evidenceReservation()
}

// nativeContext strips values (including native opt-ins) from SDK control
// contexts while retaining deadlines and cancellation. Native failures are
// paired with the original caller context to preserve its cancellation cause.
type nativeContext struct{ context.Context }

func (ctx nativeContext) Value(key any) any { return nil }
