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
	"io"
	"log/slog"
	"maps"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"

	native "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog/rest"
	nativeio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/metrics"
	"github.com/apache/iceberg-go/table"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go/logging"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
)

// ManagedSource is a non-owning native-semantic construction token.
type ManagedSource struct {
	private
	owner *managedSource
}
type managedSource struct {
	prepared       ManagedPreparation
	ctx            context.Context
	cancel         context.CancelFunc
	catalog        *rest.Catalog
	transport      *http.Transport
	oauthTransport *http.Transport
	base           http.RoundTripper
	storage        *s3.Client
	credentials    aws.CredentialsProvider
	cleanup        func() error
	logger         *slog.Logger
	mu             sync.Mutex
	closed         bool
	sockets        map[*managedSocket]struct{}
	dials          sync.WaitGroup
	closeErrors    []error
	closeOnce      sync.Once
	closeDone      chan struct{}
	closeResult    resource.ReleaseResult
}

type managedSocket struct {
	net.Conn
	owner *managedSource
	once  sync.Once
	err   error
}

func (s *managedSocket) Close() error {
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
func (s *managedSource) dial(ctx context.Context, network, address string) (net.Conn, error) {
	return s.dialOwned(ctx, network, address, false)
}
func (s *managedSource) dialTLS(ctx context.Context, network, address string) (net.Conn, error) {
	return s.dialOwned(ctx, network, address, true)
}
func (s *managedSource) dialOwned(ctx context.Context, network, address string, secure bool) (net.Conn, error) {
	original, _ := ctx.Value(managedRequestContextKey{}).(context.Context)
	if original == nil {
		return nil, failure(ErrAuthority, "dial-context")
	}
	target, _ := original.Value(managedDialTargetKey{}).(string)
	if target == "" || !strings.EqualFold(address, target) {
		return nil, failure(ErrAuthority, "dial-target")
	}
	family, _ := original.Value(managedFamilyKey{}).(*ManagedSession)
	if family != nil {
		family.mu.Lock()
		cleanup, _ := original.Value(managedCleanupContextKey{}).(*ManagedSession)
		if family.closed && cleanup != family {
			family.mu.Unlock()
			return nil, failure(ErrAuthority, "dial-closed")
		}
		family.dials.Add(1)
		family.mu.Unlock()
		defer family.dials.Done()
	}
	work, cancel := context.WithCancelCause(ctx)
	stop := context.AfterFunc(original, func() { cancel(context.Cause(original)) })
	defer stop()
	defer cancel(nil)
	if original.Err() != nil {
		cancel(context.Cause(original))
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, failure(ErrAuthority, "closed")
	}
	s.dials.Add(1)
	s.mu.Unlock()
	defer s.dials.Done()
	raw, err := (&net.Dialer{Timeout: s.prepared.settings.Timeout}).DialContext(work, network, address)
	if err != nil {
		return nil, err
	}
	conn := &managedSocket{Conn: raw, owner: s}
	s.mu.Lock()
	s.sockets[conn] = struct{}{}
	closed := s.closed
	s.mu.Unlock()
	if closed || work.Err() != nil {
		return nil, errors.Join(failure(ErrAuthority, "closed"), work.Err(), context.Cause(work), conn.Close())
	}
	if secure {
		config := s.transport.TLSClientConfig.Clone()
		if original.Value(managedOAuthTLSKey{}) == s && s.oauthTransport != nil {
			config = s.oauthTransport.TLSClientConfig.Clone()
		}
		if config.ServerName == "" {
			config.ServerName, _, err = net.SplitHostPort(address)
			if err != nil {
				return nil, errors.Join(err, conn.Close())
			}
		}
		secured := tls.Client(conn, config)
		if err = secured.HandshakeContext(work); err != nil {
			return nil, errors.Join(err, secured.Close())
		}
		return secured, nil
	}
	return conn, nil
}

func newManagedSource(ctx context.Context, p ManagedPreparation) (resource.Resource[ManagedSource], error) {
	owned, cancel := context.WithCancel(ctx)
	owner := &managedSource{prepared: p, ctx: owned, cancel: cancel, sockets: make(map[*managedSocket]struct{})}
	owner.logger = p.native.Logger
	if owner.logger == nil {
		owner.logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	result := resource.Resource[ManagedSource]{Acquired: true, Capability: ManagedSource{owner: owner}, Release: owner.close}
	trust := &tls.Config{MinVersion: tls.VersionTLS12}
	if p.native.TLS != nil {
		trust = p.native.TLS.Clone()
		if trust.MinVersion == 0 {
			trust.MinVersion = tls.VersionTLS12
		}
	}
	if p.settings.RootCAPEM != "" {
		trust.RootCAs = x509.NewCertPool()
		trust.RootCAs.AppendCertsFromPEM([]byte(p.settings.RootCAPEM))
	}
	owner.transport = &http.Transport{Proxy: nil, TLSClientConfig: trust, DisableCompression: true, MaxConnsPerHost: p.settings.MaxActive*p.settings.Workers + 2,
		MaxIdleConns: 2 * p.settings.MaxActive, MaxIdleConnsPerHost: p.settings.MaxActive, MaxResponseHeaderBytes: 64 << 10, ResponseHeaderTimeout: p.settings.Timeout, DialContext: owner.dial, DialTLSContext: owner.dialTLS}
	if p.native.OAuthTLS != nil {
		owner.oauthTransport = owner.transport.Clone()
		owner.oauthTransport.TLSClientConfig = p.native.OAuthTLS.Clone()
		if owner.oauthTransport.TLSClientConfig.MinVersion == 0 {
			owner.oauthTransport.TLSClientConfig.MinVersion = tls.VersionTLS12
		}
	}
	owner.base = owner.transport
	if p.native.Transport != nil {
		base, cleanup, err := p.native.Transport(owner.transport)
		owner.cleanup = cleanup
		if err != nil {
			return result, err
		}
		if base == nil || typedNil(base) || cleanup == nil {
			return result, failure(ErrInput, "transport-factory")
		}
		owner.base = base
	}
	owner.credentials = p.native.Credentials
	if owner.credentials == nil {
		if p.settings.StorageAccessKey != "" {
			owner.credentials = credentials.NewStaticCredentialsProvider(p.settings.StorageAccessKey, p.settings.StorageSecretKey, p.settings.StorageSessionToken)
		} else {
			owner.credentials = aws.AnonymousCredentials{}
		}
	}
	owner.storage = owner.storageClient(owner.credentials)
	result.Check = func(work context.Context) error {
		setup, cancel := context.WithTimeout(work, p.settings.Timeout)
		defer cancel()
		setup = context.WithValue(setup, managedSetupKey{}, owner)
		headers := maps.Clone(p.settings.Headers)
		if headers == nil {
			headers = map[string]string{}
		}
		headers["X-Iceberg-Access-Delegation"] = ""
		if p.settings.VendedCredentials {
			headers["X-Iceberg-Access-Delegation"] = "vended-credentials"
		}
		opts := []rest.Option{rest.WithWarehouseLocation(p.settings.Warehouse), rest.WithPrefix(p.settings.CatalogPrefix), rest.WithHeaders(headers),
			rest.WithAdditionalProps(maps.Clone(p.settings.Properties)), rest.WithCustomTransport(&managedTransport{source: owner, kind: "catalog"}),
			rest.WithFileIOFactory(owner.files), rest.WithStorageCredentialValidator(owner.validateCredentials),
			rest.WithTableValidator(owner.validateLoadedTable),
			rest.WithScanPlanningContext(owner.planningContext),
			rest.WithLogger(owner.logger)}
		if p.settings.BearerToken != "" {
			opts = append(opts, rest.WithOAuthToken(p.settings.BearerToken))
		}
		if p.native.Auth != nil {
			opts = append(opts, rest.WithAuthManager(&managedAuth{source: owner, delegate: p.native.Auth}))
		}
		if p.settings.OAuthCredential != "" {
			opts = append(opts, rest.WithAuthManager(&managedAuth{source: owner}))
		}
		if p.settings.SigV4 {
			signing := p.native.CatalogCredentials
			if signing == nil {
				signing = owner.credentials
			}
			opts = append(opts, rest.WithAwsConfig(aws.Config{Region: p.settings.SigningRegion, Credentials: signing}), rest.WithSigV4RegionSvc(p.settings.SigningRegion, p.settings.SigningService))
		}
		var err error
		owner.catalog, err = rest.NewCatalog(setup, p.settings.Name, p.settings.CatalogURI, opts...)
		return failureIf(err)
	}
	return result, nil
}

func (s *managedSource) storageClient(provider aws.CredentialsProvider) *s3.Client {
	return s3.New(s3.Options{BaseEndpoint: aws.String(s.prepared.settings.StorageEndpoint), Region: s.prepared.settings.StorageRegion, Credentials: provider,
		HTTPClient:   &http.Client{Transport: &managedTransport{source: s, kind: "storage"}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }},
		UsePathStyle: true, Retryer: aws.NopRetryer{}, Logger: logging.Nop{}, DisableS3ExpressSessionAuth: aws.Bool(true), DisableMultiRegionAccessPoints: true,
		ContinueHeaderThresholdBytes: -1, RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired, ResponseChecksumValidation: aws.ResponseChecksumValidationWhenRequired})
}

func (s *managedSource) close(ctx context.Context) resource.ReleaseResult {
	s.closeOnce.Do(func() {
		s.closeDone = make(chan struct{})
		go func() {
			s.closeResult = s.release()
			close(s.closeDone)
		}()
	})
	return s.waitClosed(ctx)
}

func (s *managedSource) waitClosed(ctx context.Context) resource.ReleaseResult {
	select {
	case <-s.closeDone:
		return s.closeResult
	case <-ctx.Done():
		return resource.ReleaseResult{Continue: s.waitClosed}
	}
}

func (s *managedSource) release() resource.ReleaseResult {
	s.mu.Lock()
	s.closed = true
	s.cancel()
	s.mu.Unlock()
	var err error
	if s.catalog != nil {
		err = s.catalog.CloseAndWait()
	}
	if s.cleanup != nil {
		err = errors.Join(err, s.cleanup())
		s.cleanup = nil
	}
	if s.transport != nil {
		s.transport.CloseIdleConnections()
	}
	if s.oauthTransport != nil {
		s.oauthTransport.CloseIdleConnections()
	}
	s.mu.Lock()
	sockets := make([]*managedSocket, 0, len(s.sockets))
	for socket := range s.sockets {
		sockets = append(sockets, socket)
	}
	s.mu.Unlock()
	for _, socket := range sockets {
		err = errors.Join(err, socket.Close())
	}
	s.dials.Wait()
	s.mu.Lock()
	err = errors.Join(err, errors.Join(s.closeErrors...))
	s.mu.Unlock()
	return resource.ReleaseResult{Quiescent: true, Released: true, Err: failureIf(err)}
}

// ManagedClient executes one admitted callback family. All derived managed
// handles are revoked and joined before its receipt becomes released.
type ManagedClient struct {
	private
	source *managedSource
	access *resource.Access
	inbox  *invocation.Inbox[ManagedResult]
}

func BindManaged(assembly *resource.Assembly, selection resource.Selection[ManagedSource], inbox *invocation.Inbox[ManagedResult]) (*ManagedClient, error) {
	source, _, err := resource.Bind(assembly, selection)
	if err != nil {
		return nil, err
	}
	access, err := resource.AccessFor(assembly, selection)
	if err != nil {
		return nil, err
	}
	if source.owner == nil || inbox == nil {
		return nil, failure(ErrInput, "bind-managed")
	}
	b := source.owner.prepared.Budget()
	if access.Limits().Bytes < int64(b.Active)*b.WorkBytes {
		return nil, failure(ErrInput, "limits")
	}
	return &ManagedClient{source: source.owner, access: access, inbox: inbox}, nil
}

func (c *ManagedClient) Use(ctx context.Context, id fault.Correlation, work func(*ManagedSession) error) (*invocation.Receipt[ManagedResult], error) {
	if c == nil || c.source == nil || ctx == nil || work == nil {
		return nil, failure(ErrInput, "use")
	}
	b := c.source.prepared.Budget()
	s := c.source.prepared.settings
	call, err := invocation.Begin(ctx, c.access, invocation.Request{Name: "native-use", Correlation: id, Shape: invocation.Session, Bytes: b.WorkBytes, EvidenceBytes: b.EvidenceBytes, Admission: invocation.Budget{Limit: s.Timeout}}, c.inbox, nil)
	if err != nil {
		return nil, err
	}
	go func() {
		owned, cancel := context.WithTimeout(ctx, s.Timeout)
		stop := context.AfterFunc(c.source.ctx, cancel)
		defer func() { stop(); cancel() }()
		owned, err = table.WithWorkerLimit(owned, s.Workers)
		if err == nil {
			owned, err = table.WithCommitRetryLimit(owned, s.MaxCommitRetries)
		}
		if err != nil {
			call.Complete(invocation.Outcome[ManagedResult]{Primary: failureIf(err)})
			return
		}
		owned, err = table.WithOperationLogger(owned, c.source.logger)
		if err != nil {
			call.Complete(invocation.Outcome[ManagedResult]{Primary: failureIf(err)})
			return
		}
		family := newManagedSession(owned, cancel, c.source)
		returned := false
		defer func() {
			if !returned {
				recovered := recover()
				cause, _ := recovered.(error)
				err = failure(ErrOperation, "callback-exit", cause)
			}
			err = errors.Join(err, family.errors(), owned.Err(), context.Cause(owned))
			cleanup := family.close()
			result := family.result()
			family.mu.Lock()
			family.files = nil
			family.closers = nil
			family.bases = nil
			family.viewBases = nil
			family.remotePlans = nil
			family.remoteByPath = nil
			family.snapshotSubsets = nil
			family.verifiedTables = nil
			family.data = ManagedResult{}
			family.primary = nil
			family.cleanup = nil
			family.mu.Unlock()
			call.Complete(invocation.Outcome[ManagedResult]{Present: true, Value: result, Primary: failureIf(err), Cleanup: failureIf(cleanup)})
		}()
		err = work(family)
		returned = true
	}()
	return call.Receipt(), nil
}

type managedSetupKey struct{}
type managedFamilyKey struct{}
type managedReportContextKey struct{}

type managedReporter struct {
	source   *managedSource
	reporter metrics.Reporter
}

func (r *managedReporter) Report(ctx context.Context, report metrics.MetricsReport) {
	r.reporter.Report(context.WithValue(ctx, managedReportContextKey{}, r.source), report)
}
func (*managedReporter) Close() error { return nil }

// managedAuth refreshes tokens using the actual admitted request context rather
// than a TokenSource that captured a completed source-construction context.
type managedAuth struct {
	source   *managedSource
	delegate rest.AuthManager
	mu       sync.Mutex
	token    *oauth2.Token
}

func (a *managedAuth) AuthHeader() (string, string, error) {
	return "", "", failure(ErrAuthority, "auth-context")
}
func (a *managedAuth) AuthHeaderWithContext(ctx context.Context) (string, string, error) {
	if ctx.Value(managedReportContextKey{}) == a.source {
		ctx = context.WithValue(context.WithValue(ctx, managedFamilyKey{}, (*ManagedSession)(nil)), managedSetupKey{}, a.source)
	}
	if err := ctx.Err(); err != nil {
		return "", "", errors.Join(err, context.Cause(ctx))
	}
	if a.delegate != nil {
		if aware, ok := a.delegate.(rest.ContextAuthManager); ok {
			return aware.AuthHeaderWithContext(ctx)
		}
		return a.delegate.AuthHeader()
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return "", "", err
	}
	if a.token != nil && a.token.Valid() {
		return "Authorization", a.token.Type() + " " + a.token.AccessToken, nil
	}
	s := a.source.prepared.settings
	id, secret, found := strings.Cut(s.OAuthCredential, ":")
	if !found {
		secret = id
		id = ""
	}
	params := url.Values{}
	if s.OAuthAudience != "" {
		params.Set("audience", s.OAuthAudience)
	}
	if s.OAuthResource != "" {
		params.Set("resource", s.OAuthResource)
	}
	config := clientcredentials.Config{ClientID: id, ClientSecret: secret, TokenURL: s.OAuthURI, EndpointParams: params, AuthStyle: oauth2.AuthStyleInParams}
	config.Scopes = []string{s.OAuthScope}
	client := &http.Client{Transport: &managedTransport{source: a.source, kind: "oauth"}, Timeout: s.Timeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	token, err := config.Token(context.WithValue(ctx, oauth2.HTTPClient, client))
	if err != nil {
		return "", "", err
	}
	if token.AccessToken == "" {
		return "", "", failure(ErrProtocol, "oauth-token")
	}
	a.token = token
	return "Authorization", token.Type() + " " + token.AccessToken, nil
}

func (s *managedSource) files(ctx context.Context, location string, properties native.Properties) (nativeio.IO, error) {
	family, _ := ctx.Value(managedFamilyKey{}).(*ManagedSession)
	if family == nil || family.source != s {
		return nil, failure(ErrAuthority, "fileio-context")
	}
	return family.fileIO(ctx, location, properties)
}

func (s *managedSource) validateCredentials(ctx context.Context, location string, values []rest.StorageCredential) error {
	if len(values) == 0 {
		return nil
	}
	if !s.prepared.settings.VendedCredentials {
		return failure(ErrAuthority, "credential-vending")
	}
	if len(values) > s.prepared.settings.MaxHandles {
		return failure(ErrLimit, "credentials")
	}
	root, _ := url.Parse(s.prepared.settings.Location)
	for _, value := range values {
		prefix, err := canonicalS3URI(value.Prefix)
		if err != nil {
			return err
		}
		if prefix.Host != root.Host || !strings.HasPrefix(strings.TrimSuffix(prefix.Path, "/")+"/", root.Path) {
			return failure(ErrAuthority, "credential-prefix")
		}
		if location != "" && strings.HasPrefix(location, value.Prefix) && location != value.Prefix && !strings.HasSuffix(value.Prefix, "/") && !strings.HasPrefix(location, value.Prefix+"/") {
			return failure(ErrAuthority, "credential-boundary")
		}
		if len(value.Config) > 64 {
			return failure(ErrLimit, "credential-properties")
		}
	}
	return ctx.Err()
}
