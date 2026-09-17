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

package redis

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
	sdk "github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/logging"
	"github.com/redis/go-redis/v9/maintnotifications"
)

// Source is an opaque assembly capability without operations or close authority.
type Source struct {
	private
	owner *owner
}
type owner struct {
	settings  settings
	native    sdk.UniversalClient
	transport *transport
	automatic *sdk.AutoPipeliner
	closeOnce sync.Once
	closed    chan struct{}
	closeErr  error
	password  *Password
}

// Password is an explicitly shared composition-owned credential snapshot.
// Replace performs no I/O. New connections read the current password with the
// source's frozen username; existing authenticated connections are not reauthed.
// Do not hand this rotation authority to business consumers.
// Copies share the same snapshot and synchronization.
type Password struct {
	private
	*passwordState
}
type passwordState struct {
	mu    sync.RWMutex
	value string
}

func NewPassword(value string) (*Password, error) {
	if len(value) > 4096 {
		return nil, failure(ErrLimit, "credentials")
	}
	return &Password{passwordState: &passwordState{value: value}}, nil
}
func (password *Password) Replace(value string) error {
	if password == nil || password.passwordState == nil || len(value) > 4096 {
		return failure(ErrInput, "credentials")
	}
	password.mu.Lock()
	password.value = value
	password.mu.Unlock()
	return nil
}
func (password *Password) read() string {
	password.mu.RLock()
	defer password.mu.RUnlock()
	return password.value
}

// Select prepares frozen options and strict resource overlays without I/O.
func Select(options OptionsV1, layers ...resource.Layer) (resource.Selection[Source], error) {
	return selectSource(options, nil, layers...)
}

// SelectWithPassword uses a local rotating password snapshot, never an arbitrary
// callback or an unaccounted authentication HTTP client. CSC rejects this native
// combination rather than silently disabling the requested cache.
func SelectWithPassword(options OptionsV1, password *Password, layers ...resource.Layer) (resource.Selection[Source], error) {
	if password == nil || password.passwordState == nil {
		return resource.Selection[Source]{}, failure(ErrInput, "credentials")
	}
	return selectSource(options, password, layers...)
}
func selectSource(options OptionsV1, password *Password, layers ...resource.Layer) (resource.Selection[Source], error) {
	version := options.Version
	if version == 0 {
		version = 1
	}
	prepared, err := resource.Prepare(resource.Schema[settings]{Format: 1, Defaults: defaults(options), Validate: func(value settings) error {
		if password != nil && (value.ExperimentalCache || value.Password != "") {
			return failure(ErrUnsupported, "credentials")
		}
		if password == nil && emptyNamedCredential(value.Username, value.Password) || emptyNamedCredential(value.SentinelUsername, value.SentinelPassword) {
			return failure(ErrUnsupported, "credentials")
		}
		return validate(value)
	}}, resource.Input{Identity: resource.Identity{Provider: ProviderID, Name: options.Name}, Format: version, Layers: layers})
	if err != nil {
		return resource.Selection[Source]{}, err
	}
	return resource.Select(prepared, func(ctx context.Context, value settings) (resource.Resource[Source], error) {
		if ctx.Err() != nil {
			return resource.Resource[Source]{}, nativeFailure(ctx, ctx.Err())
		}
		owned := &owner{settings: value, transport: newTransport(value), closed: make(chan struct{}), password: password}
		owned.native = owned.construct(password)
		owned.native.AddHook(batchEvidenceHook{})
		result := resource.Resource[Source]{Acquired: true, Capability: Source{owner: owned}, Release: owned.close}
		if value.ExperimentalAutoPipeline {
			var autoErr error
			owned.automatic, autoErr = owned.native.AsyncAutoPipelineWithOptions(&sdk.AutoPipelineOptions{
				MaxBatchSize: value.MaxActive, MaxBatchBytes: value.MaxRequestBytes, MaxConcurrentBatches: 1, NumShards: 1,
				MaxFlushDelay: time.Millisecond,
			})
			if autoErr != nil {
				return result, failure(ErrUnsupported, "automatic", autoErr)
			}
		}
		return result, nil
	}), nil
}
func (owned *owner) construct(password *Password) sdk.UniversalClient {
	value := owned.settings
	tlsConfig, _ := value.tls()
	options := &sdk.UniversalOptions{
		TLSConfig: tlsConfig,
		Addrs:     append([]string(nil), value.Addrs...), DB: value.DB, Protocol: value.Protocol,
		Username: value.Username, Password: value.Password, SentinelUsername: value.SentinelUsername, SentinelPassword: value.SentinelPassword,
		Dialer: owned.transport.dial, MaxRetries: -1, DialerRetries: 1, MaxRedirects: value.MaxRedirects,
		DialTimeout: value.Timeout, ReadTimeout: value.Timeout, WriteTimeout: value.Timeout, PoolTimeout: value.Timeout,
		ContextTimeoutEnabled: true, PoolSize: min(value.MaxActive, value.poolCapacity()), MaxActiveConns: value.poolCapacity(),
		MaxConcurrentDials: 1, MaxIdleConns: value.poolCapacity(), ConnMaxIdleTime: nativeIdleTime(value.MaxIdleTime), ConnMaxLifetime: value.MaxLifetime,
		ReadBufferSize: 4096, WriteBufferSize: 4096, DisableIdentity: true,
		ReadOnly: value.ReadOnly, MasterName: value.MasterName, IsClusterMode: value.mode() == "cluster",
		MaintNotificationsConfig: maintenance(value),
	}
	if value.MaxRedirects == 0 {
		options.MaxRedirects = -1
	}
	if password != nil {
		options.CredentialsProviderContext = func(ctx context.Context) (string, string, error) {
			if ctx.Err() != nil {
				return "", "", ctx.Err()
			}
			current := password.read()
			if emptyNamedCredential(value.Username, current) {
				return "", "", failure(ErrUnsupported, "credentials")
			}
			return value.Username, current, nil
		}
	}
	if value.ExperimentalCache {
		options.ClientSideCacheConfig = &sdk.ClientSideCacheConfig{MaxEntries: value.CacheEntries, MaxMemoryBytes: value.CacheBytes, MaxStaleness: value.CacheMaxStaleness}
	}
	if value.Mode == "universal" && !(value.mode() == "sentinel" && value.ReadOnly) {
		return sdk.NewUniversalClient(options)
	}
	switch value.mode() {
	case "standalone":
		return sdk.NewClient(options.Simple())
	case "sentinel":
		failover := options.Failover()
		failover.ReplicaOnly = value.ReadOnly
		return sdk.NewFailoverClient(failover)
	case "cluster":
		return sdk.NewClusterClient(options.Cluster())
	default:
		ready := make(chan struct{})
		var ring *sdk.Ring
		ring = sdk.NewRing(&sdk.RingOptions{
			TLSConfig: tlsConfig,
			Addrs:     value.Shards, DB: value.DB, Protocol: value.Protocol, Username: value.Username, Password: value.Password,
			CredentialsProviderContext: options.CredentialsProviderContext,
			Dialer:                     owned.transport.dial, MaxRetries: -1, DialerRetries: 1, DialTimeout: value.Timeout,
			ReadTimeout: value.Timeout, WriteTimeout: value.Timeout, ContextTimeoutEnabled: true,
			PoolSize: min(value.MaxActive, value.poolCapacity()), PoolTimeout: value.Timeout, MaxActiveConns: value.poolCapacity(), MaxIdleConns: value.poolCapacity(),
			ConnMaxIdleTime: nativeIdleTime(value.MaxIdleTime), ConnMaxLifetime: value.MaxLifetime, ReadBufferSize: 4096, WriteBufferSize: 4096,
			HeartbeatFrequency: time.Second, DisableIdentity: true,
			HeartbeatFn: func(ctx context.Context, node *sdk.Client) bool {
				<-ready
				up := false
				for _, current := range ring.GetShardClients() {
					if current == node {
						up = true
						break
					}
				}
				return ringHeartbeat(ctx, node, up)
			},
			NewClient: func(options *sdk.Options) *sdk.Client {
				options.MaxConcurrentDials = 1
				options.MaintNotificationsConfig = maintenance(value)
				return sdk.NewClient(options)
			},
		})
		close(ready)
		return ring
	}
}

func ringHeartbeat(ctx context.Context, client *sdk.Client, previouslyUp bool) bool {
	err := client.Ping(ctx).Err()
	if errors.Is(err, errSocketCapacity) || errors.Is(err, sdk.ErrPoolTimeout) || errors.Is(err, sdk.ErrPoolExhausted) {
		return previouslyUp
	}
	return err == nil
}
func (owned *owner) close(ctx context.Context) resource.ReleaseResult {
	owned.closeOnce.Do(func() {
		// No admitted users remain. Sealing also revokes auxiliary topology dials.
		go func() {
			owned.transport.seal()
			owned.closeErr = owned.native.Close()
			close(owned.closed)
		}()
	})
	select {
	case <-owned.closed:
		if owned.transport.drained(ctx) {
			return resource.ReleaseResult{Quiescent: true, Released: true, Err: owned.closeErr}
		}
	case <-ctx.Done():
	}
	return resource.ReleaseResult{Err: failure(ErrCleanup, "close", ctx.Err(), context.Cause(ctx)), Continue: owned.close}
}

func maintenance(value settings) *maintnotifications.Config {
	return &maintnotifications.Config{Mode: maintnotifications.Mode(value.MaintenanceMode),
		RelaxedTimeout: value.Timeout, HandoffTimeout: value.Timeout, MaxWorkers: value.MaxActive,
		HandoffQueueSize: value.MaxConnections, MaxHandoffRetries: 1}
}

func nativeIdleTime(value time.Duration) time.Duration {
	if value == 0 {
		return -1
	}
	return value
}

func emptyNamedCredential(username, password string) bool {
	return username != "" && username != "default" && password == ""
}

// Client is a concurrent non-owning facade. Copies share admission, pools and
// source identity, including across borrowing aliases.
type Client struct {
	private
	owner    *owner
	access   *resource.Access
	inbox    *invocation.Inbox[Result]
	observer *invocation.Observer
}

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
	if limits.Active > value.MaxActive || limits.Bytes < value.reservation() || limits.MaxLeases < 2 ||
		limits.Queued > 128 || limits.Queued > 0 && limits.QueuedBytes < value.reservation() {
		return nil, failure(ErrInput, "limits")
	}
	return &Client{owner: source.owner, access: access, inbox: inbox, observer: observer}, nil
}
func (client *Client) begin(ctx context.Context, id fault.Correlation, name string, shape invocation.Shape, parent *invocation.Scope) (*invocation.Call[Result], error) {
	if client == nil || client.owner == nil || ctx == nil {
		return nil, failure(ErrInput, "call")
	}
	value := client.owner.settings
	request := invocation.Request{Name: name, Correlation: id, Shape: shape, Bytes: value.reservation(),
		EvidenceBytes: value.evidenceReservation(), Admission: invocation.Budget{Limit: value.Timeout}}
	if parent != nil {
		request.Bytes = 0
		return invocation.BeginNested(ctx, *parent, request, client.inbox, client.observer)
	}
	return invocation.Begin(ctx, client.access, request, client.inbox, client.observer)
}
func (client *Client) work(ctx context.Context) (context.Context, context.CancelFunc, error) {
	return (invocation.Budget{Limit: client.owner.settings.Timeout}).Context(ctx, invocation.Execute)
}

// Stats is a payload-free native pool snapshot. Counters concern local pools,
// not server effects, global quota or process memory.
type Stats struct {
	Hits, Misses, Timeouts uint32
	Total, Idle, Stale     uint32
	Sockets, PendingDials  int
	CacheHits, CacheMisses uint64
	CacheEntries           int
	CacheBytes             int64
}

func (client *Client) Stats() Stats {
	if client == nil || client.owner == nil {
		return Stats{}
	}
	native := client.owner.native.PoolStats()
	result := Stats{Hits: native.Hits, Misses: native.Misses, Timeouts: native.Timeouts, Total: native.TotalConns, Idle: native.IdleConns, Stale: native.StaleConns}
	transport := client.owner.transport
	transport.mu.Lock()
	result.Sockets = len(transport.sockets)
	result.PendingDials = transport.pending
	transport.mu.Unlock()
	if standalone, ok := client.owner.native.(*sdk.Client); ok {
		cache := standalone.CSCStats()
		result.CacheHits, result.CacheMisses, result.CacheEntries, result.CacheBytes = cache.Hits, cache.Misses, cache.Entries, cache.MemoryUsageBytes
	}
	return result
}

func (client *Client) node(ctx context.Context, key string) (*sdk.Client, error) {
	switch native := client.owner.native.(type) {
	case *sdk.Client:
		return native, nil
	case *sdk.ClusterClient:
		return native.MasterForKey(ctx, key)
	case *sdk.Ring:
		return native.GetShardClientForKey(key)
	default:
		return nil, failure(ErrUnsupported, "node")
	}
}
func resultError(receipt *invocation.Receipt[Result], err error) error {
	if err != nil {
		return err
	}
	result, ok := receipt.Result()
	if !ok {
		return failure(ErrState, "result")
	}
	return result.Err()
}

// DisableNativeLogging is a composition-only, process-wide bootstrap action.
// Call it before ANY go-redis clients or goroutines are started. The upstream
// logger is global and not concurrency-safe to replace; source selection never
// silently changes it. Do not re-enable native logging while Providers run:
// native diagnostics can contain endpoints, commands and server error text.
// Client diagnostics instead use the payload-free invocation.Observer and Stats.
// This function does not configure any other SDK's logging or an exporter.
func DisableNativeLogging() { logging.Disable() }
