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
	"maps"
	"net/http"
	"slices"
	"time"

	"github.com/frost-leo/fathomry/adapters/objectstore/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/internal/invocation"
	native "github.com/frost-leo/fathomry/internal/objectstore/minio/v7"
)

// Address is an exact key and optional version in the frozen bucket. Empty
// VersionID selects current; "null" is a literal version, not an absence sentinel.
type Address struct {
	private
	Key, VersionID string
}

// Transfer separates consumed bytes, final-object effects and cleanup. ETag is
// not a universal digest. Abort ACK never proves final-object absence.
type Transfer struct {
	private
	Effect                                                 objectstore.Effect
	Bytes                                                  int64
	Complete                                               bool
	SHA256                                                 string
	Verified                                               bool
	UploadID                                               string
	PartsAcknowledged                                      int
	CompletionAttempted, AbortAttempted, AbortAcknowledged bool
}

// Object is copied metadata. False/zero/empty native fields do not establish absence.
type Object struct {
	private
	Address              Address
	ETag                 string
	Size                 int64
	ContentType          string
	LastModified         time.Time
	DeleteMarker, Latest bool
	ChecksumMode         string
	metadata             map[string]string
	headers              http.Header
	checksums            map[string]string
}

func (value Object) MetadataCopy() map[string]string { return maps.Clone(value.metadata) }

func (value Object) HeadersCopy() http.Header { return value.headers.Clone() }

func (value Object) ChecksumsCopy() map[string]string { return maps.Clone(value.checksums) }

// Removal retains one entry per original target, including unsubmitted targets.
// Err is an immutable borrowed native/public cause graph, not safe log text.
type Removal struct {
	private
	Address               Address
	Effect                objectstore.Effect
	DeleteMarker          bool
	DeleteMarkerVersionID string
	Err                   error
}

// Result contains immutable public facts projected before native custody ends.
// All mutable accessors return copies. Explicit data/error inspection is sensitive;
// runtime results are neither a durable DTO nor a serialization contract.
type Result struct {
	private
	data *resultData
}

type resultData struct {
	source              objectstore.Info
	attribution         objectstore.Attribution
	attempts            objectstore.Attempts
	present             bool
	complete            bool
	object              Object
	objectPresent       bool
	content             []byte
	transfer            Transfer
	objects             []Object
	removals            []Removal
	uploads             []Upload
	parts               []Part
	nextKey, nextUpload string
	nextPart            int
	tags                map[string]string
	delegation          Delegation
	delegated           bool
}

func (value Result) HasData() bool { return value.data != nil && value.data.present }

func (value Result) Complete() bool { return value.data != nil && value.data.complete }

func (value Result) Source() objectstore.Info {
	if value.data == nil {
		return objectstore.Info{}
	}
	return value.data.source.Clone()
}

func (value Result) Attribution() objectstore.Attribution {
	if value.data == nil {
		return objectstore.Attribution{}
	}
	return value.data.attribution
}

func (value Result) Attempts() objectstore.Attempts {
	if value.data == nil {
		return objectstore.Attempts{}
	}
	return value.data.attempts
}

func (value Result) Object() (Object, bool) {
	if value.data == nil {
		return Object{}, false
	}
	return value.data.object, value.data.objectPresent
}

func (value Result) DataCopy() []byte {
	if value.data == nil {
		return nil
	}
	return slices.Clone(value.data.content)
}

func (value Result) Transfer() Transfer {
	if value.data == nil {
		return Transfer{}
	}
	return value.data.transfer
}

func (value Result) ObjectsCopy() []Object {
	if value.data == nil {
		return nil
	}
	return slices.Clone(value.data.objects)
}

func (value Result) RemovalsCopy() []Removal {
	if value.data == nil {
		return nil
	}
	return slices.Clone(value.data.removals)
}

func (value Result) UploadsCopy() []Upload {
	if value.data == nil {
		return nil
	}
	return slices.Clone(value.data.uploads)
}

func (value Result) PartsCopy() []Part {
	if value.data == nil {
		return nil
	}
	return slices.Clone(value.data.parts)
}

func (value Result) TagsCopy() map[string]string {
	if value.data == nil {
		return nil
	}
	return maps.Clone(value.data.tags)
}

func (value Result) UploadCursor() (string, string) {
	if value.data == nil {
		return "", ""
	}
	return value.data.nextKey, value.data.nextUpload
}

func (value Result) NextPart() int {
	if value.data == nil {
		return 0
	}
	return value.data.nextPart
}

func (value Result) Delegation() (Delegation, bool) {
	if value.data == nil {
		return Delegation{}, false
	}
	return value.data.delegation, value.data.delegated
}

func publicAddress(value native.Address) Address {
	return Address{Key: value.Key, VersionID: value.VersionID}
}

func publicObject(value native.Object) Object {
	return Object{Address: publicAddress(value.Address), ETag: value.ETag, Size: value.Size, ContentType: value.ContentType, LastModified: value.LastModified,
		DeleteMarker: value.DeleteMarker, Latest: value.Latest, ChecksumMode: value.ChecksumMode, metadata: value.MetadataCopy(), headers: value.HeadersCopy(), checksums: value.ChecksumsCopy()}
}

func project(value invocation.Result[native.Result], metadata adapters.Info) Result {
	configuration := value.Source.Configuration
	data := &resultData{source: objectstore.Info{Scope: value.Source.Scope, Provider: configuration.Identity.Provider, Name: configuration.Identity.Name, Revision: configuration.Revision, FormatVersion: configuration.Format},
		attribution: objectstore.Attribution{Runtime: metadata.Runtime, Operation: metadata.Operation, ID: metadata.ID, Sequence: metadata.Sequence, Parent: metadata.Parent, Depth: metadata.Depth, Source: metadata.Source}, attempts: objectstore.Attempts{Observed: value.Attempts.Observed, Exact: value.Attempts.Exact}, present: value.Outcome.Present}
	for _, layer := range configuration.Provenance {
		data.source.Provenance = append(data.source.Provenance, objectstore.LayerInfo{Kind: uint8(layer.Kind), Fields: slices.Clone(layer.Fields)})
	}
	facts := value.Outcome.Value
	data.complete = facts.Complete()
	object, present := facts.Object()
	data.object, data.objectPresent = publicObject(object), present
	data.content = facts.DataCopy()
	transfer := facts.Transfer()
	data.transfer = Transfer{Effect: objectstore.Effect(transfer.Effect), Bytes: transfer.Bytes, Complete: transfer.Complete, SHA256: transfer.SHA256, Verified: transfer.Verified, UploadID: transfer.UploadID,
		PartsAcknowledged: transfer.PartsAcknowledged, CompletionAttempted: transfer.CompletionAttempted, AbortAttempted: transfer.AbortAttempted, AbortAcknowledged: transfer.AbortAcknowledged}
	for _, object := range facts.ObjectsCopy() {
		data.objects = append(data.objects, publicObject(object))
	}
	for _, removal := range facts.RemovalsCopy() {
		data.removals = append(data.removals, Removal{Address: publicAddress(removal.Address), Effect: objectstore.Effect(removal.Effect), DeleteMarker: removal.DeleteMarker,
			DeleteMarkerVersionID: removal.DeleteMarkerVersionID, Err: translate(removal.Err, "remove")})
	}
	for _, upload := range facts.UploadsCopy() {
		data.uploads = append(data.uploads, Upload{Key: upload.Key, ID: upload.ID, Initiated: upload.Initiated})
	}
	for _, part := range facts.PartsCopy() {
		data.parts = append(data.parts, Part{PartNumber: part.PartNumber, LastModified: part.LastModified, ETag: part.ETag, Size: part.Size,
			ChecksumCRC32: part.ChecksumCRC32, ChecksumCRC32C: part.ChecksumCRC32C, ChecksumSHA1: part.ChecksumSHA1, ChecksumSHA256: part.ChecksumSHA256, ChecksumCRC64NVME: part.ChecksumCRC64NVME,
			ChecksumMD5: part.ChecksumMD5, ChecksumSHA512: part.ChecksumSHA512, ChecksumXXHash64: part.ChecksumXXHash64, ChecksumXXHash3: part.ChecksumXXHash3, ChecksumXXHash128: part.ChecksumXXHash128})
	}
	data.nextKey, data.nextUpload = facts.UploadCursor()
	data.nextPart = facts.NextPart()
	data.tags = facts.TagsCopy()
	if delegation, ok := facts.Delegation(); ok {
		data.delegated = true
		data.delegation = Delegation{url: delegation.URL(), headers: delegation.HeadersCopy(), method: delegation.Method(), expiry: delegation.Expiry(), token: delegation.StaticToken()}
	}
	return Result{data: data}
}

func address(value Address) native.Address {
	return native.Address{Key: value.Key, VersionID: value.VersionID}
}
