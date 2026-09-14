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
	"strings"
	"time"

	native "github.com/minio/minio-go/v7"
)

// Address names a key in the selected bucket. Empty VersionID means current,
// "null" is a literal S3 version identifier, never an absence sentinel.
type Address struct {
	private
	Key       string
	VersionID string
}

func (client *Client) address(address Address, write bool) error {
	if !validPath(address.Key, false) || !validText(address.VersionID, 1024, true) {
		return failure(ErrInput, "address")
	}
	value := client.owner.settings
	if !strings.HasPrefix(address.Key, value.Prefix) || write && !value.Writes {
		return failure(ErrAuthority, "address")
	}
	if address.VersionID != "" && !value.Versions {
		return failure(ErrUnsupported, "versions")
	}
	return nil
}

// Object is a copied metadata observation, possibly present alongside an error.
// HeadersCopy deliberately exposes response data; it is not safe diagnostic text.
type Object struct {
	private
	Address      Address
	ETag         string
	Size         int64
	ContentType  string
	LastModified time.Time
	DeleteMarker bool
	Latest       bool
	ChecksumMode string
	metadata     map[string]string
	headers      http.Header
	checksums    map[string]string
}

func (object Object) MetadataCopy() map[string]string  { return maps.Clone(object.metadata) }
func (object Object) HeadersCopy() http.Header         { return object.headers.Clone() }
func (object Object) ChecksumsCopy() map[string]string { return maps.Clone(object.checksums) }
func objectInfo(info native.ObjectInfo) Object {
	sums := map[string]string{}
	for key, value := range map[string]string{"SHA256": info.ChecksumSHA256, "SHA1": info.ChecksumSHA1, "CRC32": info.ChecksumCRC32,
		"CRC32C": info.ChecksumCRC32C, "CRC64NVME": info.ChecksumCRC64NVME, "MD5": info.ChecksumMD5, "SHA512": info.ChecksumSHA512,
		"XXHASH64": info.ChecksumXXHash64, "XXHASH3": info.ChecksumXXHash3, "XXHASH128": info.ChecksumXXHash128} {
		if value != "" {
			sums[key] = value
		}
	}
	return Object{Address: Address{Key: info.Key, VersionID: info.VersionID}, ETag: info.ETag, Size: info.Size,
		ContentType: info.ContentType, LastModified: info.LastModified, DeleteMarker: info.IsDeleteMarker, Latest: info.IsLatest,
		ChecksumMode: info.ChecksumMode, metadata: maps.Clone(map[string]string(info.UserMetadata)), headers: info.Headers.Clone(), checksums: sums}
}
func uploadInfo(info native.UploadInfo) Object {
	return objectInfo(native.ObjectInfo{Key: info.Key, VersionID: info.VersionID, ETag: info.ETag, Size: info.Size, LastModified: info.LastModified,
		ChecksumMode: info.ChecksumMode, ChecksumSHA256: info.ChecksumSHA256, ChecksumSHA1: info.ChecksumSHA1,
		ChecksumCRC32: info.ChecksumCRC32, ChecksumCRC32C: info.ChecksumCRC32C, ChecksumCRC64NVME: info.ChecksumCRC64NVME,
		ChecksumMD5: info.ChecksumMD5, ChecksumSHA512: info.ChecksumSHA512, ChecksumXXHash64: info.ChecksumXXHash64,
		ChecksumXXHash3: info.ChecksumXXHash3, ChecksumXXHash128: info.ChecksumXXHash128})
}

// Effect records technical evidence, not idempotency, persistence or rollback.
type Effect uint8

const (
	NotSubmitted Effect = iota
	Unknown
	Acknowledged
)

// Transfer keeps input/read progress separate from final-object acknowledgement.
// SHA256 is computed over Bytes only. Verified means a caller-supplied expected
// SHA256 matched after complete consumption, not an ETag interpretation.
// UploadID remains evidence even after an acknowledged abort. Abort does not
// prove absence of an object following an unconfirmed completion.
type Transfer struct {
	private
	Effect              Effect
	Bytes               int64
	Complete            bool
	SHA256              string
	Verified            bool
	UploadID            string
	PartsAcknowledged   int
	CompletionAttempted bool
	AbortAttempted      bool
	AbortAcknowledged   bool
}

// Removal preserves every input index, including unsubmitted trailing targets.
// Acknowledged is a DELETE response, not proof the target previously existed.
// Version/delete-marker headers remain observations even alongside an error.
type Removal struct {
	private
	Address               Address
	Effect                Effect
	DeleteMarker          bool
	DeleteMarkerVersionID string
	Err                   error
}

// Upload is an incomplete-upload observation, not a grant to delete it.
type Upload struct {
	private
	Key       string
	ID        string
	Initiated time.Time
}

// Result is immutable shared evidence. Accessors copy mutable storage; explicit
// inspection returns runtime data, not a diagnostic or durable wire format.
type Result struct {
	private
	data *resultData
}
type resultData struct {
	object        Object
	objectPresent bool
	content       []byte
	transfer      Transfer
	objects       []Object
	complete      bool
	removals      []Removal
	uploads       []Upload
	nextKey       string
	nextUpload    string
	tags          map[string]string
	parts         []native.ObjectPart
	nextPart      int
}

func (result Result) Object() (Object, bool) {
	if result.data == nil {
		return Object{}, false
	}
	return result.data.object, result.data.objectPresent
}
func (result Result) DataCopy() []byte {
	if result.data == nil || result.data.content == nil {
		return nil
	}
	return append([]byte{}, result.data.content...)
}
func (result Result) Transfer() Transfer {
	if result.data == nil {
		return Transfer{}
	}
	return result.data.transfer
}
func (result Result) ObjectsCopy() []Object {
	if result.data == nil {
		return nil
	}
	return append([]Object{}, result.data.objects...)
}

// Complete means this finite result's consumption finished. Listings are not
// snapshots; false and ErrLimit retain bounded partial results.
func (result Result) Complete() bool { return result.data != nil && result.data.complete }
func (result Result) RemovalsCopy() []Removal {
	if result.data == nil {
		return nil
	}
	return append([]Removal{}, result.data.removals...)
}
func (result Result) UploadsCopy() []Upload {
	if result.data == nil {
		return nil
	}
	return append([]Upload{}, result.data.uploads...)
}

// UploadCursor returns native key/upload markers for the next inspection page.
func (result Result) UploadCursor() (string, string) {
	if result.data == nil {
		return "", ""
	}
	return result.data.nextKey, result.data.nextUpload
}
func (result Result) TagsCopy() map[string]string {
	if result.data == nil {
		return nil
	}
	return maps.Clone(result.data.tags)
}

// PartsCopy deliberately returns copied native, handle-free part metadata.
func (result Result) PartsCopy() []native.ObjectPart {
	if result.data == nil {
		return nil
	}
	return append([]native.ObjectPart{}, result.data.parts...)
}
func (result Result) NextPart() int {
	if result.data == nil {
		return 0
	}
	return result.data.nextPart
}
