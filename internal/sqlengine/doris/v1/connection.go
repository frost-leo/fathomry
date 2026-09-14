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

package doris

import (
	"context"
	"database/sql/driver"
	"net"
	"net/http"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
	sdk "github.com/go-sql-driver/mysql"
)

// Source is an opaque non-owning composition token.
type Source struct {
	private
	owner *owner
}
type owner struct{ settings settings }

// Native hooks in context values must not expose transport handles or execute
// caller callbacks. Keep original contexts at the shared evidence boundary.
type nativeContext struct{ context.Context }

func (nativeContext) Value(any) any { return nil }

// Select freezes typed options and strict overlays without connecting or probing.
// Every assembly constructs independent ownership. No global transport is used.
func Select(options OptionsV1, layers ...resource.Layer) (resource.Selection[Source], error) {
	prepared, err := resource.Prepare(resource.Schema[settings]{Format: 1, Defaults: defaults(options), Validate: validate},
		resource.Input{Identity: resource.Identity{Provider: ProviderID, Name: options.Name}, Format: 1, Layers: layers})
	if err != nil {
		return resource.Selection[Source]{}, err
	}
	return resource.Select(prepared, func(ctx context.Context, s settings) (resource.Resource[Source], error) {
		return resource.Resource[Source]{Acquired: true, Capability: Source{owner: &owner{settings: s}},
			Release: func(context.Context) resource.ReleaseResult {
				return resource.ReleaseResult{Quiescent: true, Released: true}
			}}, nil
	}), nil
}

// Client is a concurrent non-owning facade. Copies and borrowed bindings share
// source admission; inboxes remain independently owned by composition.
type Client struct {
	private
	owner    *owner
	access   *resource.Access
	inbox    *invocation.Inbox[Result]
	observer *invocation.Observer
}

// Bind requires matching admission bounds and an independent mandatory inbox.
func Bind(assembly *resource.Assembly, selection resource.Selection[Source], inbox *invocation.Inbox[Result], observer *invocation.Observer) (*Client, error) {
	source, _, err := resource.Bind(assembly, selection)
	if err != nil {
		return nil, err
	}
	access, err := resource.AccessFor(assembly, selection)
	if err != nil {
		return nil, err
	}
	if source.owner == nil || inbox == nil {
		return nil, failure(ErrInput, "bind")
	}
	s, limits := source.owner.settings, access.Limits()
	if limits.Active > s.Active || limits.Queued > s.Queued || limits.Bytes < s.reservation() ||
		limits.Queued > 0 && limits.QueuedBytes < s.reservation() {
		return nil, failure(ErrInput, "limits")
	}
	return &Client{owner: source.owner, access: access, inbox: inbox, observer: observer}, nil
}
func (c *Client) begin(ctx context.Context, id fault.Correlation, name string) (*invocation.Call[Result], context.Context, context.CancelFunc, error) {
	if c == nil || c.owner == nil || ctx == nil {
		return nil, nil, nil, failure(ErrInput, name)
	}
	s := c.owner.settings
	call, err := invocation.Begin(ctx, c.access, invocation.Request{Name: name, Correlation: id, Shape: invocation.Finite,
		Bytes: s.reservation(), EvidenceBytes: s.evidenceReservation(), Admission: invocation.Budget{Limit: s.Timeout}}, c.inbox, c.observer)
	if err != nil {
		return nil, nil, nil, err
	}
	work, cancel, err := (invocation.Budget{Limit: s.Timeout}).Context(ctx, invocation.Execute)
	if err != nil {
		call.Complete(invocation.Outcome[Result]{Primary: err})
		return call, nil, nil, nil
	}
	return call, work, cancel, nil
}

func (s settings) transport() (*http.Transport, error) {
	trust, err := s.trust()
	if err != nil {
		return nil, err
	}
	return &http.Transport{Proxy: nil, DialContext: s.dialer().DialContext, TLSClientConfig: trust,
		DisableKeepAlives: true, DisableCompression: true, ForceAttemptHTTP2: false,
		TLSHandshakeTimeout: s.Timeout, ResponseHeaderTimeout: s.Timeout, ExpectContinueTimeout: s.Timeout,
		MaxResponseHeaderBytes: 32 << 10, MaxConnsPerHost: 1}, nil
}

func (c *Client) executeSQL(ctx context.Context, call *invocation.Call[Result], query string, read bool, data *resultData, s settings) (primary, cleanup error) {
	if err := ctx.Err(); err != nil {
		return failure(ErrSQL, "connect", err, context.Cause(ctx)), nil
	}
	_, _ = call.Attempt()
	raw, err := s.dialer().DialContext(nativeContext{ctx}, "tcp", s.SQLAddress)
	if err != nil {
		return failure(ErrTransport, "connect", err, context.Cause(ctx)), nil
	}
	deadline, _ := ctx.Deadline()
	w := &wire{raw: raw, transport: raw, settings: s, ctx: nativeContext{ctx}, deadline: deadline}
	_ = w.SetDeadline(deadline)
	done := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { defer close(done); _ = w.Close() })
	var connection driver.Conn
	var rows driver.Rows
	defer func() {
		if !stop() {
			<-done
		}
		if connection != nil {
			cleanup = joined(ErrCleanup, "sql", connection.Close())
		}
		_ = w.Close()
		if rows != nil {
			cleanup = joined(ErrCleanup, "rows", cleanup, rows.Close())
		}
		wireErr, closeErr := w.evidence()
		cleanup = joined(ErrCleanup, "socket", cleanup, closeErr)
		if primary != nil {
			primary = joined(ErrSQL, "execute", primary, wireErr, ctx.Err(), context.Cause(ctx))
		}
	}()
	cfg := sdk.NewConfig()
	cfg.User, cfg.Passwd, cfg.DBName = s.User, s.Password, s.Database
	cfg.Net, cfg.Addr = "tcp", s.SQLAddress
	cfg.Logger = &sdk.NopLogger{}
	cfg.Collation = "utf8mb4_general_ci"
	cfg.AllowNativePasswords = true
	cfg.CheckConnLiveness = false
	cfg.MaxAllowedPacket = MaxSQLBytes + 1024
	cfg.ReadTimeout, cfg.WriteTimeout = s.Timeout, s.Timeout
	cfg.DialFunc = func(context.Context, string, string) (net.Conn, error) { return w, nil }
	connector, err := sdk.NewConnector(cfg)
	if err != nil {
		return failure(ErrSQL, "config", err), nil
	}
	connection, err = connector.Connect(nativeContext{ctx})
	data.serverVersion = w.version
	if err != nil {
		return failure(ErrSQL, "connect", err), nil
	}
	if err = ctx.Err(); err != nil {
		return failure(ErrSQL, "execute", err), nil
	}
	_, _ = call.Attempt()
	data.dispatched = true
	if read {
		rows, err = connection.(driver.QueryerContext).QueryContext(nativeContext{ctx}, query, nil)
		if err == nil {
			err = consume(rows, s, data)
		}
	} else {
		var result driver.Result
		result, err = connection.(driver.ExecerContext).ExecContext(nativeContext{ctx}, query, nil)
		if err == nil {
			data.acknowledged = true
			data.affected, err = result.RowsAffected()
			if err == nil && data.affected < 0 {
				err = failure(ErrProtocol, "affected-rows")
			}
			data.affectedKnown = err == nil
		}
	}
	data.complete = err == nil
	if err != nil {
		return failure(ErrSQL, "execute", err), nil
	}
	return nil, nil
}
