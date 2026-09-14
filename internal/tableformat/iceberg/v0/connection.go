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
	"errors"
	"net"
	"net/http"
	"strconv"
	"sync"

	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/apache/iceberg-go/config"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/frost-leo/fathomry/internal/compatibility"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
)

// Source is an opaque, non-owning assembly capability. Bind supplies operations.
type Source struct {
	private
	owner *connection
}
type connection struct {
	settings    settings
	catalog     *rest.Catalog
	transport   *http.Transport
	storage     *s3.Client
	mu          sync.Mutex
	stopped     bool
	sockets     map[*socket]struct{}
	dials       sync.WaitGroup
	closeErrors []error
}
type socket struct {
	net.Conn
	owner *connection
	once  sync.Once
	err   error
}

func (s *socket) Close() error {
	s.once.Do(func() {
		s.err = s.Conn.Close()
		s.owner.mu.Lock()
		delete(s.owner.sockets, s)
		if s.err != nil {
			s.owner.closeErrors = append(s.owner.closeErrors, s.err)
		}
		s.owner.mu.Unlock()
	})
	return s.err
}
func (owner *connection) dialPlain(ctx context.Context, network, address string) (net.Conn, error) {
	return owner.dial(ctx, network, address, false)
}
func (owner *connection) dialTLS(ctx context.Context, network, address string) (net.Conn, error) {
	return owner.dial(ctx, network, address, true)
}
func (owner *connection) dial(ctx context.Context, network, address string, secure bool) (net.Conn, error) {
	state, _ := ctx.Value(exchangeKey{}).(*exchange)
	original, _ := ctx.Value(requestContextKey{}).(context.Context)
	if state == nil || original == nil || !state.beginDial() {
		return nil, failure(ErrAuthority, "unowned-dial")
	}
	defer state.dials.Done()
	work, cancel := context.WithCancelCause(ctx)
	stop := context.AfterFunc(original, func() { cancel(context.Cause(original)) })
	defer stop()
	defer cancel(nil)
	if original.Err() != nil {
		cancel(context.Cause(original))
	}
	owner.mu.Lock()
	if owner.stopped {
		owner.mu.Unlock()
		return nil, failure(ErrAuthority, "closed")
	}
	owner.dials.Add(1)
	owner.mu.Unlock()
	defer owner.dials.Done()
	raw, err := (&net.Dialer{Timeout: owner.settings.Timeout}).DialContext(work, network, address)
	if err != nil {
		return nil, err
	}
	s := &socket{Conn: raw, owner: owner}
	owner.mu.Lock()
	owner.sockets[s] = struct{}{}
	stopped := owner.stopped
	owner.mu.Unlock()
	if stopped || work.Err() != nil {
		return nil, errors.Join(failure(ErrAuthority, "closed"), s.Close())
	}
	if secure {
		trust := owner.transport.TLSClientConfig.Clone()
		trust.ServerName, _, err = net.SplitHostPort(address)
		if err != nil {
			return nil, errors.Join(err, s.Close())
		}
		secured := tls.Client(s, trust)
		if err = secured.HandshakeContext(work); err != nil {
			state.note(secured.Close(), true)
			if header, ok := err.(tls.RecordHeaderError); ok {
				header.Conn = nil
				err = header
			}
			return nil, err
		}
		return secured, nil
	}
	return s, nil
}

// Select validates and freezes explicit settings without network I/O. Construction
// owns REST and S3 clients and their transport. Readiness reads Catalog configuration
// and sends a bucket HEAD; it creates no namespace, table or object.
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
		if config.EnvConfig.MaxWorkers != 5 {
			return resource.Resource[Source]{}, failure(ErrUnsupported, "native-workers")
		}
		owner := &connection{settings: s, sockets: make(map[*socket]struct{})}
		result := resource.Resource[Source]{Acquired: true, Capability: Source{owner: owner}, Release: owner.close}
		trust := &tls.Config{MinVersion: tls.VersionTLS12}
		if !s.Plaintext {
			trust.RootCAs = x509.NewCertPool()
			if !trust.RootCAs.AppendCertsFromPEM([]byte(s.RootCAPEM)) {
				return result, failure(ErrInput, "trust")
			}
		}
		owner.transport = &http.Transport{Proxy: nil, TLSClientConfig: trust, DisableKeepAlives: true,
			DisableCompression: true, MaxConnsPerHost: s.MaxActive, MaxResponseHeaderBytes: 32 << 10,
			ResponseHeaderTimeout: s.Timeout, DialContext: owner.dialPlain, DialTLSContext: owner.dialTLS}
		owner.storage = newStorageClient(s, &storageTransport{owner: owner})
		result.Check = func(ctx context.Context) error {
			work, cancel, err := (invocation.Budget{Limit: s.Timeout}).Context(ctx, invocation.Establish)
			if err != nil {
				return err
			}
			defer cancel()
			exchange := newExchange(owner, nil)
			bucket, _ := s.storageAddress()
			if _, err = owner.storage.HeadBucket(storageContext(work, exchange), &s3.HeadBucketInput{Bucket: aws.String(bucket)}); err != nil {
				primary, cleanup := exchange.finish()
				return failureIf(err, primary, cleanup, work.Err(), context.Cause(work))
			}
			owner.catalog, err = rest.NewCatalog(withExchange(work, exchange), "iceberg", s.CatalogURI,
				rest.WithWarehouseLocation(s.Warehouse), rest.WithPrefix(s.CatalogPrefix),
				rest.WithOAuthToken(s.BearerToken), rest.WithCustomTransport(&catalogTransport{owner: owner}),
				rest.WithHeaders(map[string]string{"X-Iceberg-Access-Delegation": ""}))
			primary, cleanup := exchange.finish()
			if err != nil || primary != nil {
				err = failureIf(err, primary, work.Err(), context.Cause(work))
			}
			return failureIf(err, cleanup)
		}
		return result, nil
	}), nil
}
func failureIf(causes ...error) error {
	if errors.Join(causes...) == nil {
		return nil
	}
	return failure(ErrOperation, "native", causes...)
}
func (owner *connection) close(ctx context.Context) resource.ReleaseResult {
	owner.mu.Lock()
	owner.stopped = true
	sockets := make([]*socket, 0, len(owner.sockets))
	for s := range owner.sockets {
		sockets = append(sockets, s)
	}
	owner.mu.Unlock()
	if owner.transport != nil {
		owner.transport.CloseIdleConnections()
	}
	for _, s := range sockets {
		_ = s.Close()
	}
	owner.dials.Wait()
	owner.mu.Lock()
	err := errors.Join(owner.closeErrors...)
	owner.mu.Unlock()
	return resource.ReleaseResult{Quiescent: true, Released: true, Err: err}
}

// Client is concurrent and non-owning. Each operation reloads a table rather than
// sharing a mutable SDK Table, transaction, iterator or Arrow reference.
type Client struct {
	private
	owner    *connection
	access   *resource.Access
	inbox    *invocation.Inbox[Result]
	observer *invocation.Observer
}

// Bind attaches the exact selected resource and its independently owned evidence
// inbox without transferring cleanup authority or creating another allowance.
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
func (client *Client) EvidenceBytes() int64 {
	if client == nil || client.owner == nil {
		return 0
	}
	return client.owner.settings.evidenceBytes()
}
func (client *Client) valid(ctx context.Context, name string, write bool) error {
	if client == nil || client.owner == nil || ctx == nil || !nameOK(name) {
		return failure(ErrInput, "call")
	}
	if write && !client.owner.settings.Writes {
		return failure(ErrAuthority, "writes")
	}
	return nil
}
func (client *Client) start(ctx context.Context, id fault.Correlation, name string, freeze func() error, run func(context.Context, *exchange, *resultData) error) (*invocation.Receipt[Result], error) {
	if config.EnvConfig.MaxWorkers != 5 {
		return nil, failure(ErrUnsupported, "native-workers")
	}
	s := client.owner.settings
	call, err := invocation.Begin(ctx, client.access, invocation.Request{Name: name, Correlation: id, Shape: invocation.Async,
		Bytes: s.reservation(), EvidenceBytes: s.evidenceBytes(), Admission: invocation.Budget{Limit: s.Timeout}}, client.inbox, client.observer)
	if err != nil {
		return nil, err
	}
	if freeze != nil {
		if err = freeze(); err != nil {
			call.Complete(invocation.Outcome[Result]{Primary: err})
			return call.Receipt(), nil
		}
	}
	work, cancel, err := (invocation.Budget{Limit: s.Timeout}).Context(ctx, invocation.Execute)
	if err != nil {
		call.Complete(invocation.Outcome[Result]{Primary: err})
		return call.Receipt(), nil
	}
	exchange := newExchange(client.owner, call)
	go func() {
		defer cancel()
		data := &resultData{}
		err := run(withExchange(work, exchange), exchange, data)
		primary, cleanup := exchange.finish()
		if err != nil || primary != nil {
			primary = failureIf(err, primary, work.Err(), context.Cause(work))
		}
		exchange.mu.Lock()
		data.effect = exchange.effect
		data.files = append([]FileEffect(nil), exchange.files...)
		exchange.mu.Unlock()
		call.Complete(invocation.Outcome[Result]{Present: true, Value: Result{data: data},
			Primary: primary, Cleanup: cleanup})
	}()
	return call.Receipt(), nil
}

// Profile reports effective local settings, not observed service versions or
// tested compatibility. Assess it against explicitly supplied evidence records.
func (client *Client) Profile() compatibility.Profile {
	if client == nil || client.owner == nil {
		return compatibility.Profile{}
	}
	s := client.owner.settings
	options := []compatibility.Option{{Name: "table-format", Value: "2"}, {Name: "fileio", Value: "aws-s3-static-buffered"},
		{Name: "commit-retries", Value: "0"}, {Name: "credential-vending", Value: "not-used"},
		{Name: "plaintext", Value: strconv.FormatBool(s.Plaintext)}, {Name: "writes", Value: strconv.FormatBool(s.Writes)},
		{Name: "native-workers", Value: "5"}, {Name: "scan-concurrency", Value: "1"}, {Name: "file-concurrency-per-call", Value: "1"},
		{Name: "partition-writer", Value: "single-record-clustered"}, {Name: "row-filter", Value: "owned-arrow"},
		{Name: "row-mutation", Value: "bounded-full-rewrite"}}
	for _, pair := range []struct {
		name  string
		value int64
	}{
		{"max-active", int64(s.MaxActive)}, {"max-rows", int64(s.MaxRows)}, {"max-batch-bytes", int64(s.MaxBatchBytes)},
		{"max-object-bytes", int64(s.MaxObjectBytes)}, {"max-file-ops", int64(s.MaxFileOps)},
		{"max-io-bytes", s.MaxIOBytes}, {"max-metadata-bytes", int64(s.MaxMetadataBytes)}, {"timeout-ns", int64(s.Timeout)}} {
		options = append(options, compatibility.Option{Name: pair.name, Value: strconv.FormatInt(pair.value, 10)})
	}
	return compatibility.Profile{ImplementationModule: compatibility.FrameworkModule, SDKMode: "iceberg-rest-s3-buffered",
		Protocol: compatibility.Fact{Kind: compatibility.Declared, Value: "iceberg-rest"}, Native: compatibility.Fact{Kind: compatibility.NotApplicable},
		Options: options}
}
