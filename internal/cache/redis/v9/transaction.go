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

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	sdk "github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/maintnotifications"
)

// Session is a borrowed, node-pinned connection. It is valid only during its
// Dedicated/Watch callback. Concurrent/reentrant commands are rejected, not
// queued behind an operation holding the same reservation. No raw handle escapes.
// Copies share the same gate and lifetime; they do not create another authority.
type Session struct {
	private
	*sessionState
}
type sessionState struct {
	client   *Client
	native   *sdk.Conn
	scope    invocation.Scope
	lifetime context.Context
	mu       sync.Mutex
	ended    bool
}

// Dedicated runs a node-local callback under one shared reservation. routeKey
// selects a primary/shard in Cluster/Ring, not a new quota. Cluster transaction
// validation belongs to the selected server; commands are never split into
// several independently atomic transactions. Ring commands in this callback
// explicitly address that one shard, without per-command rehashing.
// The callback's connection has a private native pool that is closed on exit;
// connection-local state is never returned to the source's reusable pool.
func (client *Client) Dedicated(ctx, cleanupCtx context.Context, id fault.Correlation, routeKey string, run func(context.Context, *Session) error) (*invocation.Receipt[Result], error) {
	return client.session(ctx, cleanupCtx, id, routeKey, nil, run)
}

// Watch pins one connection, issues WATCH and runs the callback once. No callback
// retry is performed. Session.Transaction invokes MULTI/EXEC; redis.TxFailedErr
// remains inspectable. Returning without EXEC triggers explicit UNWATCH cleanup.
// Callback return errors and cleanup errors are retained independently.
func (client *Client) Watch(ctx, cleanupCtx context.Context, id fault.Correlation, keys []string, run func(context.Context, *Session) error) (*invocation.Receipt[Result], error) {
	if len(keys) == 0 {
		return nil, failure(ErrInput, "watch")
	}
	return client.session(ctx, cleanupCtx, id, keys[0], keys, run)
}
func (client *Client) session(ctx, cleanupCtx context.Context, id fault.Correlation, routeKey string, keys []string, run func(context.Context, *Session) error) (receipt *invocation.Receipt[Result], err error) {
	if client == nil || client.owner == nil || !client.owner.settings.AllowSessions {
		return nil, failure(ErrAuthority, "session")
	}
	value := client.owner.settings
	if cleanupCtx == nil || run == nil || len(routeKey) > value.MaxRequestBytes || len(keys) > value.MaxArgs {
		return nil, failure(ErrInput, "session")
	}
	size := 0
	for _, key := range keys {
		if len(key) > value.MaxRequestBytes-size-32 {
			return nil, failure(ErrLimit, "watch")
		}
		size += len(key) + 32
	}
	lifetime, cancel, err := (invocation.Budget{}).Context(ctx, invocation.Lifetime)
	if err != nil {
		return nil, err
	}
	defer cancel()
	call, err := client.begin(ctx, id, "session", invocation.Session, nil)
	if err != nil {
		return nil, err
	}
	receipt = call.Receipt()
	var native *sdk.Conn
	var dedicated *sdk.Client
	var session *Session
	var primary error
	returned := false
	defer func() {
		_ = recover()
		if !returned {
			primary = failure(ErrState, "callback")
		}
		if session != nil {
			session.mu.Lock()
			session.ended = true
			defer session.mu.Unlock()
		}
		var cleanup error
		if native != nil {
			clean, stop, budgetErr := (invocation.Budget{Limit: value.CloseTimeout}).Context(cleanupCtx, invocation.Cleanup)
			if budgetErr != nil {
				cleanup = budgetErr
			} else {
				if len(keys) > 0 {
					cleanup = native.Process(clean, sdk.NewCmd(clean, "unwatch"))
				}
				stop()
			}
			cleanup = errors.Join(cleanup, native.Close(), dedicated.Close())
		}
		call.Complete(invocation.Outcome[Result]{Present: native != nil, Primary: primary, Cleanup: cleanup})
	}()
	work, stop, workErr := client.work(lifetime)
	if workErr != nil {
		primary = workErr
		returned = true
		return receipt, nil
	}
	node, nodeErr := client.node(work, routeKey)
	if nodeErr != nil {
		stop()
		primary = nativeFailure(work, nodeErr)
		returned = true
		return receipt, nil
	}
	if ring, ok := client.owner.native.(*sdk.Ring); ok {
		for _, key := range keys {
			target, targetErr := ring.GetShardClientForKey(key)
			if targetErr != nil || target != node {
				stop()
				primary = failure(ErrUnsupported, "ring-watch", targetErr)
				returned = true
				return receipt, nil
			}
		}
	}
	options := sdk.Options{Addr: node.Options().Addr, Dialer: node.Options().Dialer,
		Protocol: value.Protocol, DB: value.DB, Username: value.Username, Password: value.Password,
		PoolSize: 1, MaxActiveConns: 1, MaxIdleConns: 1, MaxConcurrentDials: 1,
		MaxRetries: -1, DialerRetries: 1, DialTimeout: value.Timeout, ReadTimeout: value.Timeout,
		WriteTimeout: value.Timeout, PoolTimeout: value.Timeout, ContextTimeoutEnabled: true,
		ReadBufferSize: 4096, WriteBufferSize: 4096, DisableIdentity: true,
		MaintNotificationsConfig: &maintnotifications.Config{Mode: maintnotifications.ModeDisabled}}
	if password := client.owner.password; password != nil {
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
	dedicated = sdk.NewClient(&options)
	native = dedicated.Conn()
	if len(keys) > 0 {
		args := []any{"watch"}
		for _, key := range keys {
			args = append(args, key)
		}
		_, _ = call.Attempt()
		primary = nativeFailure(work, native.Process(work, sdk.NewCmd(work, args...)))
	}
	stop()
	if primary == nil {
		session = &Session{sessionState: &sessionState{client: client, native: native, scope: call.Scope(), lifetime: lifetime}}
		primary = run(lifetime, session)
	}
	returned = true
	return receipt, nil
}
func (session *Session) enter(ctx context.Context) (context.Context, func(), error) {
	if session == nil || session.sessionState == nil || ctx == nil || !session.mu.TryLock() {
		return nil, nil, failure(ErrState, "session")
	}
	if session.ended || session.lifetime.Err() != nil {
		session.mu.Unlock()
		return nil, nil, failure(ErrState, "session")
	}
	work, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(session.lifetime, cancel)
	if session.lifetime.Err() != nil {
		cancel()
	}
	return work, func() { stop(); cancel(); session.mu.Unlock() }, nil
}
func (session *Session) Execute(ctx context.Context, id fault.Correlation, command Command) (*invocation.Receipt[Result], error) {
	work, end, err := session.enter(ctx)
	if err != nil {
		return nil, err
	}
	defer end()
	return session.client.execute(work, id, []Command{command}, false, &session.scope, session.native, false)
}
func (session *Session) Pipeline(ctx context.Context, id fault.Correlation, commands ...Command) (*invocation.Receipt[Result], error) {
	work, end, err := session.enter(ctx)
	if err != nil {
		return nil, err
	}
	defer end()
	return session.client.execute(work, id, commands, true, &session.scope, session.native, false)
}
func (session *Session) Transaction(ctx context.Context, id fault.Correlation, commands ...Command) (*invocation.Receipt[Result], error) {
	work, end, err := session.enter(ctx)
	if err != nil {
		return nil, err
	}
	defer end()
	return session.client.execute(work, id, commands, true, &session.scope, session.native, true)
}

// Transaction executes one node-local MULTI/EXEC without a user callback.
// routeKey is mandatory routing input even when the command set has no keys.
// It returns the session lifecycle receipt. Per-command results are separately
// delivered under id.Call+".exec" with the session as Parent; use Dedicated and
// Session.Transaction when the caller also needs direct access to that receipt.
func (client *Client) Transaction(ctx, cleanupCtx context.Context, id fault.Correlation, routeKey string, commands ...Command) (*invocation.Receipt[Result], error) {
	if len(id.Call) > 120 {
		return nil, failure(ErrInput, "correlation")
	}
	return client.Dedicated(ctx, cleanupCtx, id, routeKey, func(ctx context.Context, session *Session) error {
		receipt, err := session.Transaction(ctx, fault.Correlation{Call: id.Call + ".exec", Parent: id.Call, Owner: id.Owner}, commands...)
		return resultError(receipt, err)
	})
}
