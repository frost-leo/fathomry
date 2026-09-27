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

package nacos

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"slices"
	"strconv"
	"strings"

	"github.com/frost-leo/fathomry/internal/invocation"
	request "github.com/nacos-group/nacos-sdk-go/v2/common/remote/rpc/rpc_request"
	response "github.com/nacos-group/nacos-sdk-go/v2/common/remote/rpc/rpc_response"
)

// Document owns original UTF-8 content. Key is the requested identity; the query
// reply itself has no echoed key. Native MD5 is not authenticity or ordered time.
type Document struct {
	private
	selected                             key
	namespace, content, md5, contentType string
	modified                             int64
	missing                              bool
}

// Missing is positive native code-300 absence, not empty successful content.
func (document *Document) Missing() bool { return document != nil && document.missing }

// RawCopy returns independent sensitive bytes; nil means no document.
func (document *Document) RawCopy() []byte {
	if document == nil {
		return nil
	}
	return []byte(document.content)
}

// Key returns the requested native identity, not an echoed server field.
func (document *Document) Key() KeyV1 {
	if document == nil {
		return KeyV1{}
	}
	return KeyV1{Group: document.selected.Group, DataID: document.selected.DataID}
}

// Namespace returns the requested namespace; empty is the native default form.
func (document *Document) Namespace() string {
	if document == nil {
		return ""
	}
	return document.namespace
}

// MD5 returns the observed native content marker; empty means unavailable.
func (document *Document) MD5() string {
	if document == nil {
		return ""
	}
	return document.md5
}

// ContentType returns a native format hint, not application-schema validation.
func (document *Document) ContentType() string {
	if document == nil {
		return ""
	}
	return document.contentType
}

// LastModifiedMillis returns the native Unix-millisecond observation; zero means
// unavailable, not a preparation revision or globally consistent snapshot time.
func (document *Document) LastModifiedMillis() int64 {
	if document == nil {
		return 0
	}
	return document.modified
}

// Read acquires fresh remote bytes for one preselected key, never cache/backup data.
// Every finite read opens and closes an explicit Nacos connection session.
// A new query does not prove that a prior publication is already visible in the
// server's cache. Returned bytes still require application preparation/validation.
func (client *Client) Read(ctx context.Context, selected KeyV1) (*Document, error) {
	if client == nil || client.cancel == nil || !slices.Contains(client.settings.Keys, normalizeKey(selected)) {
		return nil, fail(ErrInput, "read")
	}
	values, err := client.read(ctx, []key{normalizeKey(selected)})
	if err != nil {
		return nil, err
	}
	return values[0], nil
}

// ReadAll returns all selected documents in input order, or nil on any failure.
// Inputs must be stable; a common-time or transactional snapshot is not implied.
func (client *Client) ReadAll(ctx context.Context) ([]*Document, error) {
	if client == nil || client.cancel == nil {
		return nil, fail(ErrInput, "read-all")
	}
	return client.read(ctx, client.settings.Keys)
}

// ReadRawAll preserves present empty/whitespace data and positive missing slots.
// Protocol checks, attempt budgets, transient sessions and aggregate bounds are
// identical to ReadAll; required-document policy is deliberately absent.
// The second result is the selected-key index of a failed document acquisition,
// or -1 when no failing slot is identified (including success and session errors).
// A batch-limit index identifies where the aggregate crossed its bound; it does
// not assert that this document alone is oversized. Multi-attempt aggregates
// conservatively have no unique slot.
func (client *Client) ReadRawAll(ctx context.Context) ([]*Document, int, error) {
	if client == nil || client.cancel == nil {
		return nil, -1, fail(ErrInput, "read-raw")
	}
	failedDocument := -1
	documents, err := client.readMode(ctx, client.settings.Keys, true, &failedDocument)
	return documents, failedDocument, err
}

// read retains one logical allowance through session setup, every required query,
// decoding and local session close. Failed failover attempts discard their whole
// document prefix; the next authorized member uses the remaining original budget.
func (client *Client) read(ctx context.Context, keys []key) (documents []*Document, result error) {
	return client.readMode(ctx, keys, false, nil)
}

func (client *Client) readMode(ctx context.Context, keys []key, raw bool, failedDocument *int) (documents []*Document, result error) {
	work, end, err := client.enter(ctx)
	if err != nil {
		return nil, err
	}
	defer end()
	defer func() { result = client.closingCause(result) }()
	budget, stop, err := (invocation.Budget{Limit: client.settings.Timeout}).Context(work, invocation.Execute)
	if err != nil {
		return nil, fail(ErrRead, "budget", err)
	}
	defer stop()
	lease, err := client.access.Acquire(budget, reservationBytes)
	if err != nil {
		return nil, err
	}
	defer lease.Release()
	var causes []error
	start := int(client.preferred.Load() % uint64(len(client.settings.Servers)))
	for offset := range client.settings.Servers {
		if failedDocument != nil {
			*failedDocument = -1
		}
		index := (start + offset) % len(client.settings.Servers)
		current, err := client.newSession(budget, index, nil)
		if err == nil {
			documents, err = current.readDocumentsMode(budget, keys, raw, failedDocument)
			current.close()
		}
		if err == nil {
			if budget.Err() != nil {
				return nil, fail(ErrRead, "read", budget.Err(), context.Cause(budget))
			}
			client.preferred.Store(uint64(index))
			return documents, nil
		}
		causes = append(causes, err)
		if !errors.Is(err, ErrUnavailable) {
			return nil, err
		}
		if work.Err() == nil {
			client.preferred.CompareAndSwap(uint64(index), uint64((index+1)%len(client.settings.Servers)))
		}
		if budget.Err() != nil {
			break
		}
	}
	if failedDocument != nil && len(causes) > 1 {
		*failedDocument = -1
	}
	return nil, fail(ErrRead, "read", append(causes, budget.Err(), context.Cause(budget))...)
}

// query explicitly disables implicit native listening. Watch owns registration
// separately, even when it uses this query to obtain a reconciliation hash.
func (current *session) query(ctx context.Context, selected key) (*response.ConfigQueryResponse, error) {
	return current.queryObserved(ctx, selected, nil)
}

func (current *session) queryObserved(ctx context.Context, selected key, attempted *bool) (*response.ConfigQueryResponse, error) {
	query := request.NewConfigQueryRequest(selected.Group, selected.DataID, current.owner.settings.Namespace)
	query.RequestId = strconv.FormatUint(current.owner.sequence.Add(1), 10)
	query.PutAllHeaders(map[string]string{"notify": "false"})
	value, err := current.callObserved(ctx, query, "ConfigQueryResponse", true, attempted)
	if err != nil {
		return nil, err
	}
	return value.(*response.ConfigQueryResponse), nil
}

// readDocuments publishes no prefix on failure and clones retained native strings.
// Empty content is a required-document refusal, not a missing-configuration code.
func (current *session) readDocuments(ctx context.Context, keys []key) ([]*Document, error) {
	return current.readDocumentsMode(ctx, keys, false, nil)
}

func (current *session) readDocumentsMode(ctx context.Context, keys []key, raw bool, failedDocument *int) ([]*Document, error) {
	documents := make([]*Document, 0, len(keys))
	total := 0
	for index, selected := range keys {
		attempted := false
		value, err := current.queryObserved(ctx, selected, &attempted)
		if err != nil {
			if raw && errors.Is(err, ErrMissing) {
				documents = append(documents, &Document{selected: selected, namespace: strings.Clone(current.owner.settings.Namespace), missing: true})
				continue
			}
			if failedDocument != nil && attempted {
				*failedDocument = index
			}
			return nil, err
		}
		if !raw && strings.TrimSpace(value.Content) == "" {
			return nil, fail(ErrEmpty, "content")
		}
		total += len(value.Content)
		if total > MaxTotalBytes {
			if failedDocument != nil {
				*failedDocument = index
			}
			return nil, fail(ErrLimit, "read-all")
		}
		documents = append(documents, &Document{selected: selected, namespace: strings.Clone(current.owner.settings.Namespace),
			content: strings.Clone(value.Content), md5: strings.Clone(value.Md5), contentType: strings.Clone(value.ContentType), modified: value.LastModified})
	}
	return documents, nil
}
func checksum(content string) string {
	digest := md5.Sum([]byte(content))
	return hex.EncodeToString(digest[:])
}
