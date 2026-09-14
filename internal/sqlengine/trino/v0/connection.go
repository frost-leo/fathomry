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

package trino

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"strconv"

	"github.com/frost-leo/fathomry/internal/compatibility"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
)

// Source is an opaque non-owning assembly capability. It exposes no SDK handles.
type Source struct {
	private
	owner *connection
}
type connection struct {
	settings        settings
	serverVersion   string
	readinessCancel string
}

// Select validates and freezes configuration without I/O. Assembly readiness
// executes SELECT version() to terminal completion. This observes coordinator
// execution only, not Catalog privileges, table format or connector write support.
// An incomplete readiness query retains its first cancellation for Assembly's
// separate cleanup authority; an attempted cancellation is never replayed.
func Select(options OptionsV1, layers ...resource.Layer) (resource.Selection[Source], error) {
	version := options.Version
	if version == 0 {
		version = 1
	}
	prepared, err := resource.Prepare(resource.Schema[settings]{Format: 1, Defaults: defaults(options), Validate: validate},
		resource.Input{Identity: resource.Identity{Provider: ProviderID, Name: options.Name}, Format: version, Layers: layers})
	if err != nil {
		return resource.Selection[Source]{}, err
	}
	return resource.Select(prepared, func(ctx context.Context, s settings) (resource.Resource[Source], error) {
		owner := &connection{settings: s}
		result := resource.Resource[Source]{Acquired: true, Capability: Source{owner: owner}, Release: owner.release}
		result.Check = func(ctx context.Context) error {
			work, cancel, err := (invocation.Budget{Limit: s.Timeout}).Context(ctx, invocation.Establish)
			if err != nil {
				return err
			}
			defer cancel()
			exchange, primary := owner.execute(work, Statement{SQL: "SELECT version()"}, true, true, nil)
			owner.readinessCancel = exchange.cancellationTarget()
			exchange.closeLocal()
			data, primary, cleanup := exchange.outcome(primary)
			if err := failed(ErrOperation, "readiness", primary, cleanup); err != nil {
				return err
			}
			var rows [][]string
			if !data.complete || json.Unmarshal(data.json, &rows) != nil || len(rows) != 1 || len(rows[0]) != 1 ||
				!ascii(rows[0][0], 64, false) {
				return failure(ErrProtocol, "readiness")
			}
			owner.serverVersion = rows[0][0]
			return nil
		}
		return result, nil
	}), nil
}

func (owner *connection) release(ctx context.Context) resource.ReleaseResult {
	if owner.readinessCancel == "" {
		return resource.ReleaseResult{Quiescent: true, Released: true}
	}
	exchange := newExchange(owner.settings, false, true, nil)
	exchange.next = owner.readinessCancel
	exchange.finish(ctx)
	err := failed(ErrCleanup, "readiness", exchange.cleanup)
	if !exchange.data.cancelAttempted {
		return resource.ReleaseResult{Quiescent: true, Err: err, Continue: owner.release}
	}
	owner.readinessCancel = ""
	return resource.ReleaseResult{Quiescent: true, Released: true, Err: err}
}

// Client is concurrent and non-owning. All native state is private to one call.
type Client struct {
	private
	owner    *connection
	access   *resource.Access
	inbox    *invocation.Inbox[Result]
	observer *invocation.Observer
}

// Bind attaches operations to the exact source and independent required inbox.
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
	s, limits := source.owner.settings, access.Limits()
	if limits.Active > s.MaxActive || limits.Queued != 0 || limits.Bytes < s.reservation() {
		return nil, failure(ErrInput, "limits")
	}
	return &Client{owner: source.owner, access: access, inbox: inbox, observer: observer}, nil
}

// EvidenceBytes is the reservation per accepted receipt, including error evidence.
func (c *Client) EvidenceBytes() int64 {
	if c == nil || c.owner == nil {
		return 0
	}
	return c.owner.settings.evidenceBytes()
}
func (c *Client) start(ctx, cleanup context.Context, id fault.Correlation, name string, queryOnly bool,
	build func(settings) (Statement, error)) (*invocation.Receipt[Result], error) {
	if c == nil || c.owner == nil || ctx == nil || cleanup == nil {
		return nil, failure(ErrInput, "call")
	}
	s := c.owner.settings
	call, err := invocation.Begin(ctx, c.access, invocation.Request{Name: name, Correlation: id, Shape: invocation.Finite,
		Bytes: s.reservation(), EvidenceBytes: s.evidenceBytes(), Admission: invocation.Budget{Limit: s.Timeout}}, c.inbox, c.observer)
	if err != nil {
		return nil, err
	}
	data := &resultData{json: []byte("[]")}
	statement, primary := build(s)
	read := false
	if primary == nil {
		read, primary = prepare(statement, s, queryOnly)
	}
	var cleanupErr error
	if primary == nil {
		work, cancel, err := (invocation.Budget{Limit: s.Timeout}).Context(ctx, invocation.Execute)
		if err != nil {
			primary = err
		} else {
			data, primary, cleanupErr = c.owner.run(work, cleanup, statement, read, queryOnly, call)
			cancel()
		}
	}
	call.Complete(invocation.Outcome[Result]{Present: true, Value: Result{data: data}, Primary: primary, Cleanup: cleanupErr})
	return call.Receipt(), nil
}
func (owner *connection) run(ctx, cleanup context.Context, statement Statement, read, collect bool,
	call *invocation.Call[Result]) (*resultData, error, error) {
	exchange, err := owner.execute(ctx, statement, read, collect, call)
	exchange.finish(cleanup)
	return exchange.outcome(err)
}

func (owner *connection) execute(ctx context.Context, statement Statement, read, collect bool,
	call *invocation.Call[Result]) (*exchange, error) {
	work, cancel := context.WithCancel(ctx)
	exchange := newExchange(owner.settings, collect, read, call)
	conn, err := exchange.open()
	var stmt driver.Stmt
	if err == nil {
		stmt, err = conn.(driver.ConnPrepareContext).PrepareContext(work, statement.SQL)
	}
	if err == nil {
		args := make([]driver.NamedValue, len(statement.Args))
		for i, value := range statement.Args {
			args[i] = driver.NamedValue{Ordinal: i + 1, Value: value}
		}
		_, err = stmt.(driver.StmtExecContext).ExecContext(work, args)
	}
	cancel()
	if stmt != nil {
		exchange.noteCleanup(stmt.Close())
	}
	if conn != nil {
		exchange.noteCleanup(conn.Close())
	}
	if err != nil {
		err = failed(ErrOperation, "native", err, ctx.Err(), context.Cause(ctx))
	}
	return exchange, err
}

func (e *exchange) outcome(err error) (*resultData, error, error) {
	data := e.result()
	if err == nil && !data.success {
		err = failure(ErrProtocol, "missing-terminal")
	}
	data.complete = err == nil && e.primary == nil && data.success
	return data, failed(ErrOperation, "statement", e.primary, err), failed(ErrCleanup, "release", e.cleanup)
}

// Profile returns observed coordinator version and effective non-secret settings.
// It does not infer Catalog implementation, table format, permissions or tested
// support. Use compatibility.Assess with explicitly supplied evidence records.
func (c *Client) Profile() compatibility.Profile {
	if c == nil || c.owner == nil {
		return compatibility.Profile{}
	}
	s := c.owner.settings
	auth := "none"
	if s.Password != "" {
		auth = "basic"
	}
	if s.BearerToken != "" {
		auth = "bearer"
	}
	options := []compatibility.Option{{Name: "result-mode", Value: "direct-json"}, {Name: "sql-session", Value: "fresh-per-statement"},
		{Name: "http-retries", Value: "0"}, {Name: "server-retry-policy", Value: "none"}, {Name: "prepare", Value: "immediate"},
		{Name: "auth", Value: auth}, {Name: "writes", Value: strconv.FormatBool(s.Writes)}, {Name: "maintenance", Value: strconv.FormatBool(s.Maintenance)},
		{Name: "plaintext", Value: strconv.FormatBool(s.Plaintext)}}
	for _, pair := range []struct {
		name  string
		value int64
	}{
		{"max-active", int64(s.MaxActive)}, {"max-sql-bytes", int64(s.MaxSQLBytes)}, {"max-parameters", int64(s.MaxParameters)},
		{"max-rows", int64(s.MaxRows)}, {"max-columns", int64(s.MaxColumns)}, {"max-page-bytes", int64(s.MaxPageBytes)},
		{"max-result-bytes", int64(s.MaxResultBytes)}, {"max-pages", int64(s.MaxPages)}, {"max-wire-bytes", s.MaxWireBytes},
		{"timeout-ns", int64(s.Timeout)}, {"cleanup-timeout-ns", int64(s.CleanupTimeout)}} {
		options = append(options, compatibility.Option{Name: pair.name, Value: strconv.FormatInt(pair.value, 10)})
	}
	version := compatibility.Fact{}
	if identifier(c.owner.serverVersion) {
		version = compatibility.Fact{Kind: compatibility.Observed, Value: c.owner.serverVersion}
	}
	return compatibility.Profile{ImplementationModule: compatibility.FrameworkModule, SDKMode: "trino-direct",
		ServiceVersion: version, Protocol: compatibility.Fact{Kind: compatibility.Declared, Value: "trino-http"},
		Native: compatibility.Fact{Kind: compatibility.NotApplicable}, Options: options}
}
