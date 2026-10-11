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
	"encoding/json"
	"errors"
	"io"
	"maps"
	"net/url"
	"slices"
	"strings"
	"sync"

	native "github.com/apache/iceberg-go"
	nativeio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/view"
	"github.com/aws/aws-sdk-go-v2/credentials"
)

// ManagedEffect records one admitted method, with commit and reload identities
// kept separate. CommittedSnapshot is populated only for an acknowledged target
// reference change; metadata-only commits are identified by MetadataLocation.
// UUID/location data are deliberate inspection, not safe logging.
type ManagedEffect struct {
	Submissions                                                         []ManagedCommitSubmission
	Ref                                                                 string
	Tables                                                              []ManagedTableEffect
	Operation                                                           string
	Effect                                                              Effect
	Staged, Uploaded, Submitted, Reloaded, Complete                     bool
	TableUUID, MetadataLocation                                         string
	BaseSnapshot, IntendedSnapshot, CommittedSnapshot, ReloadedSnapshot int64
	Err                                                                 error
}

// ManagedCommitSubmission records each actual table-commit request, including
// native conflict retries and partial-progress maintenance batches.
type ManagedCommitSubmission struct {
	Ref, TableUUID, MetadataLocation                  string
	BaseSnapshot, IntendedSnapshot, CommittedSnapshot int64
	HTTPStatus                                        int
	Effect                                            Effect
}

// ManagedFileEffect identifies a file action, not a table commit or proof that all
// object versions are absent. Multipart staging is distinct from object creation.
type ManagedFileEffect struct {
	Operation, URI, UploadID string
	Bytes                    int64
	Effect                   Effect
	Err                      error
}

type ManagedResult struct {
	private
	effects              []ManagedEffect
	files                []ManagedFileEffect
	operations, requests int
	rows, bytes          int64
	released             bool
}

func (r ManagedResult) Effects() []ManagedEffect {
	copy := slices.Clone(r.effects)
	for i := range copy {
		copy[i].Submissions = slices.Clone(copy[i].Submissions)
		copy[i].Tables = slices.Clone(copy[i].Tables)
		for j := range copy[i].Tables {
			copy[i].Tables[j].Identifier = slices.Clone(copy[i].Tables[j].Identifier)
			copy[i].Tables[j].References = slices.Clone(copy[i].Tables[j].References)
			for k := range copy[i].Tables[j].References {
				change := &copy[i].Tables[j].References[k]
				change.Before, change.After = copyReference(change.Before), copyReference(change.After)
			}
		}
	}
	return copy
}
func (r ManagedResult) Files() []ManagedFileEffect { return slices.Clone(r.files) }
func (r ManagedResult) Operations() int            { return r.operations }
func (r ManagedResult) Requests() int              { return r.requests }
func (r ManagedResult) Rows() int64                { return r.rows }
func (r ManagedResult) Bytes() int64               { return r.bytes }
func (r ManagedResult) Released() bool             { return r.released }

// ManagedSession is a serial callback scope. Table, transaction, scan and FileIO
// wrappers created here cannot be used by another scope or after callback return.
// Concurrent or recursive method entry is rejected rather than waiting on itself.
type ManagedSession struct {
	verifiedTables  map[*table.Table]bool
	snapshotSubsets map[string]bool
	outputBytes     int64
	dials           sync.WaitGroup
	private
	source                        *managedSource
	ctx                           context.Context
	cancel                        context.CancelFunc
	gate                          sync.Mutex
	mu                            sync.Mutex
	closed                        bool
	current                       int
	data                          ManagedResult
	primary, cleanup              []error
	files                         []*managedIO
	closers                       []io.Closer
	handles                       int
	fileOps, fileBytes, wireBytes int64
	openFiles                     int
	bufferBytes                   int64
	bases                         map[string]table.Metadata
	viewBases                     map[string]view.Metadata
	creationIntent                table.Metadata
	remotePlans                   []*managedRemotePlan
	remoteByPath                  map[string]*managedRemotePlan
}

func newManagedSession(ctx context.Context, cancel context.CancelFunc, source *managedSource) *ManagedSession {
	f := &ManagedSession{source: source, cancel: cancel, current: -1, bases: make(map[string]table.Metadata), viewBases: make(map[string]view.Metadata), snapshotSubsets: make(map[string]bool), verifiedTables: make(map[*table.Table]bool)}
	f.ctx = context.WithValue(ctx, managedFamilyKey{}, f)
	f.remoteByPath = make(map[string]*managedRemotePlan)
	return f
}

type managedGrant uint8

const (
	grantRead managedGrant = iota
	grantWrite
	grantMetadata
	grantNamespace
	grantImport
	grantMultiTable
	grantRewrite
	grantExpire
	grantOrphan
	grantPurge
	grantDeleteFiles
)

func (f *ManagedSession) allowed(g managedGrant) bool {
	p := f.source.prepared.settings.Permissions
	switch g {
	case grantRead:
		return p.Read
	case grantWrite:
		return p.Write
	case grantMetadata:
		return p.Metadata
	case grantNamespace:
		return p.Namespace
	case grantImport:
		return p.Import
	case grantMultiTable:
		return p.MultiTable
	case grantRewrite:
		return p.Rewrite
	case grantExpire:
		return p.Expire
	case grantOrphan:
		return p.Orphan
	case grantPurge:
		return p.Purge
	case grantDeleteFiles:
		return p.DeleteFiles
	}
	return false
}
func (f *ManagedSession) enter(name string, grant managedGrant) (func(error), error) {
	if f == nil || f.source == nil {
		return nil, failure(ErrInput, "session")
	}
	if !f.gate.TryLock() {
		return nil, failure(ErrAuthority, "session-busy")
	}
	f.mu.Lock()
	if f.closed || f.ctx.Err() != nil {
		f.mu.Unlock()
		f.gate.Unlock()
		return nil, failure(ErrAuthority, "session-closed", f.ctx.Err(), context.Cause(f.ctx))
	}
	if !f.allowed(grant) {
		f.mu.Unlock()
		f.gate.Unlock()
		return nil, failure(ErrAuthority, "grant")
	}
	if f.data.operations >= f.source.prepared.settings.MaxOperations {
		f.mu.Unlock()
		f.gate.Unlock()
		return nil, failure(ErrLimit, "operations")
	}
	f.current = len(f.data.effects)
	index := f.current
	f.data.effects = append(f.data.effects, ManagedEffect{Operation: name})
	f.data.operations++
	f.mu.Unlock()
	return func(err error) {
		f.mu.Lock()
		effect := &f.data.effects[index]
		effect.Complete = err == nil
		effect.Err = failureIf(err)
		if err != nil {
			f.primary = append(f.primary, err)
		}
		f.current = -1
		f.mu.Unlock()
		f.gate.Unlock()
	}, nil
}

func managedCall[T any](f *ManagedSession, name string, grant managedGrant, run func(context.Context) (T, error)) (out T, err error) {
	leave, err := f.enter(name, grant)
	if err != nil {
		return out, err
	}
	returned := false
	defer func() {
		if !returned {
			recovered := recover()
			cause, _ := recovered.(error)
			err = failure(ErrOperation, "native-exit", cause)
			leave(err)
			if recovered != nil {
				panic(recovered)
			}
			return
		}
		leave(err)
		err = failureIf(err)
	}()
	out, err = run(f.ctx)
	if f.ctx.Err() != nil {
		err = errors.Join(err, f.ctx.Err(), context.Cause(f.ctx))
	}
	returned = true
	return out, err
}
func managedDo(f *ManagedSession, name string, grant managedGrant, run func(context.Context) error) error {
	_, err := managedCall(f, name, grant, func(ctx context.Context) (struct{}, error) { return struct{}{}, run(ctx) })
	return err
}

func (f *ManagedSession) handle() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed || f.handles >= f.source.prepared.settings.MaxHandles {
		return failure(ErrLimit, "handles")
	}
	f.handles++
	return nil
}

func (f *ManagedSession) noteCleanup(err error) {
	if err != nil {
		f.mu.Lock()
		f.cleanup = append(f.cleanup, err)
		f.mu.Unlock()
	}
}
func (f *ManagedSession) close() error {
	f.mu.Lock()
	f.closed = true
	f.mu.Unlock()
	f.cancel()
	f.gate.Lock()
	defer f.gate.Unlock()
	for _, closer := range f.closers {
		f.noteCleanup(closer.Close())
	}
	for _, plan := range f.remotePlans {
		f.noteCleanup(plan.close())
	}
	for _, files := range f.files {
		f.noteCleanup(files.Close())
	}
	f.dials.Wait()
	f.mu.Lock()
	defer f.mu.Unlock()
	f.data.released = true
	return errors.Join(f.cleanup...)
}
func (f *ManagedSession) result() ManagedResult {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := f.data
	out.effects = slices.Clone(out.effects)
	out.files = slices.Clone(out.files)
	return out
}
func (f *ManagedSession) errors() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return errors.Join(f.primary...)
}

func (f *ManagedSession) namespace(id table.Identifier) error {
	if len(id) < len(f.source.prepared.settings.Namespace) || len(id) > 16 {
		return failure(ErrAuthority, "namespace")
	}
	if !slices.Equal(id[:len(f.source.prepared.settings.Namespace)], f.source.prepared.settings.Namespace) {
		return failure(ErrAuthority, "namespace")
	}
	for _, part := range id {
		if !managedText(part, 256, false) || strings.Contains(part, "\x1f") {
			return failure(ErrInput, "identifier")
		}
	}
	return nil
}
func (f *ManagedSession) identifier(id table.Identifier) error {
	if len(id) < 2 || !managedText(id[len(id)-1], 256, false) {
		return failure(ErrInput, "identifier")
	}
	return f.namespace(id[:len(id)-1])
}
func (f *ManagedSession) location(raw string) error {
	u, err := canonicalS3URI(raw)
	if err != nil {
		return err
	}
	root, _ := url.Parse(f.source.prepared.settings.Location)
	if u.Host != root.Host || !strings.HasPrefix(u.Path, root.Path) {
		return failure(ErrAuthority, "location")
	}
	return nil
}

func (f *ManagedSession) fileIO(ctx context.Context, location string, properties native.Properties) (nativeio.IO, error) {
	if err := f.location(location); err != nil {
		return nil, err
	}
	s := f.source.prepared.settings
	// Catalog FileIO endpoint/region hints never replace explicitly selected
	// source authority. The custom factory does not call a registry loader.
	client := f.source.storage
	key, secret, token := properties["s3.access-key-id"], properties["s3.secret-access-key"], properties["s3.session-token"]
	if s.VendedCredentials && (key != "" || secret != "" || token != "") {
		if key == "" || secret == "" {
			return nil, failure(ErrProtocol, "vended-credentials")
		}
		client = f.source.storageClient(credentials.NewStaticCredentialsProvider(key, secret, token))
	} else if f.source.prepared.native.Credentials == nil && s.StorageAccessKey == "" {
		return nil, failure(ErrAuthority, "file-credentials")
	}
	f.mu.Lock()
	if f.closed || len(f.files) >= s.MaxHandles {
		f.mu.Unlock()
		return nil, failure(ErrLimit, "fileio-count")
	}
	f.mu.Unlock()
	files, err := newManagedIO(managedIOConfig{Context: ctx, Client: client, Root: s.Location, MaxObjectBytes: s.MaxObjectBytes, MaxIOBytes: s.MaxIOBytes, MaxFileOps: s.MaxFileOps,
		MaxOpenFiles: s.MaxOpenFiles, MaxListEntries: s.MaxListEntries, PartSize: 5 << 20, CleanupTimeout: s.Timeout, AllowWrite: s.Permissions.Write, AllowDelete: s.Permissions.DeleteFiles,
		Check: func(operation, uri string, count int64) error {
			f.mu.Lock()
			defer f.mu.Unlock()
			switch operation {
			case "buffer-release":
				if count < 0 || count > f.bufferBytes {
					return failure(ErrProtocol, "buffer-release")
				}
				f.bufferBytes -= count
				return nil
			case "close-handle":
				if f.openFiles < 1 {
					return failure(ErrProtocol, "file-release")
				}
				f.openFiles--
				return nil
			}
			if f.closed {
				return failure(ErrAuthority, "fileio-closed")
			}
			switch operation {
			case "open-handle":
				if f.openFiles >= s.MaxOpenFiles {
					return failure(ErrLimit, "family-open-files")
				}
				f.openFiles++
				return nil
			case "buffer":
				if count < 0 || count > int64(s.MaxOpenFiles)*(5<<20)-f.bufferBytes {
					return failure(ErrLimit, "file-buffers")
				}
				f.bufferBytes += count
				return nil
			}
			if count < 0 || count > s.MaxIOBytes-f.fileBytes || f.fileOps >= s.MaxFileOps {
				return failure(ErrLimit, "family-file-io")
			}
			f.fileOps++
			f.fileBytes += count
			return nil
		}, Observe: func(event managedIOEvent) {
			f.mu.Lock()
			defer f.mu.Unlock()
			if int64(len(f.data.files)) < 2*s.MaxFileOps {
				f.data.files = append(f.data.files, ManagedFileEffect{Operation: event.Operation, URI: event.URI, UploadID: event.UploadID, Bytes: event.Bytes, Effect: event.Effect, Err: failureIf(event.Err)})
			}
			if f.current >= 0 && event.Effect == Acknowledged && (event.Operation == "put" || event.Operation == "multipart-complete") {
				f.data.effects[f.current].Uploaded = true
			}
		}, Cleanup: f.noteCleanup})
	if err != nil {
		return nil, err
	}
	f.mu.Lock()
	f.files = append(f.files, files)
	f.mu.Unlock()
	return files, nil
}

func (f *ManagedSession) copyMetadata(meta table.Metadata) (table.Metadata, error) {
	if meta == nil {
		return nil, failure(ErrProtocol, "metadata")
	}
	raw, err := json.Marshal(meta)
	if err != nil {
		return nil, err
	}
	if int64(len(raw)) > f.source.prepared.settings.MaxMetadataBytes {
		return nil, failure(ErrLimit, "metadata")
	}
	return table.ParseMetadataBytes(raw)
}

func (f *ManagedSession) validateTable(meta table.Metadata) error {
	if meta == nil || meta.Version() < 1 || meta.Version() > 3 || meta.CurrentSchema() == nil {
		return failure(ErrProtocol, "table-metadata")
	}
	if err := f.location(strings.TrimSuffix(meta.Location(), "/") + "/"); err != nil {
		return err
	}
	if len(meta.Schemas()) > f.source.prepared.settings.MaxListEntries || len(meta.Snapshots()) > f.source.prepared.settings.MaxListEntries {
		return failure(ErrLimit, "metadata-count")
	}
	for _, snapshot := range meta.Snapshots() {
		if snapshot.ManifestList != "" {
			if err := f.location(snapshot.ManifestList); err != nil {
				return err
			}
		}
		for _, path := range snapshot.ManifestLocations {
			if err := f.location(path); err != nil {
				return err
			}
		}
	}
	for previous := range meta.PreviousFiles() {
		if err := f.location(previous.MetadataFile); err != nil {
			return err
		}
	}
	for statistics := range meta.Statistics() {
		if err := f.location(statistics.StatisticsPath); err != nil {
			return err
		}
	}
	for statistics := range meta.PartitionStatistics() {
		if err := f.location(statistics.StatisticsPath); err != nil {
			return err
		}
	}
	properties := meta.Properties()
	if len(properties) > f.source.prepared.settings.MaxListEntries {
		return failure(ErrLimit, "metadata-properties")
	}
	for key, value := range properties {
		if key == "write.metadata.delete-after-commit.enabled" && properties.GetBool(key, false) && !f.allowed(grantDeleteFiles) {
			return failure(ErrAuthority, "automatic-delete")
		}
		if key == "write.data.path" || key == "write.metadata.path" || key == "write.folder-storage.path" {
			if err := f.location(strings.TrimSuffix(value, "/") + "/"); err != nil {
				return err
			}
		}
	}
	return nil
}

func (f *ManagedSession) boundedProperties(props native.Properties) (native.Properties, error) {
	if len(props) > f.source.prepared.settings.MaxListEntries {
		return nil, failure(ErrLimit, "properties")
	}
	raw, err := json.Marshal(props)
	if err != nil {
		return nil, err
	}
	if int64(len(raw)) > f.source.prepared.settings.MaxMetadataBytes {
		return nil, failure(ErrLimit, "properties")
	}
	return maps.Clone(props), nil
}
