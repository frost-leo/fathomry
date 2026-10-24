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

package catalog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	jsonv2 "encoding/json/v2"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"
	"unicode"

	mysql "github.com/frost-leo/fathomry/adapters/database/mysql/v1"
	postgres "github.com/frost-leo/fathomry/adapters/database/postgres/v1"
	"github.com/frost-leo/fathomry/failure/v1"
	declaration "github.com/frost-leo/fathomry/framework/workflow/declaration/v1"
	"github.com/frost-leo/fathomry/i18n/v1"
)

const (
	MaxPageSize    = 64
	MaxCursorBytes = 512
	MaxViewBytes   = 8 << 20
)

// Localized preserves absence versus present-empty text. Locale is the actual
// text locale, not necessarily RequestedLocale. Fallback is none, resource_absent,
// unknown_locale, disabled_locale or missing_translation. There is no prefix match.
type Localized struct {
	Present  bool   `json:"present"`
	Value    string `json:"value"`
	Locale   string `json:"locale,omitempty"`
	Fallback string `json:"fallback"`
}
type Display struct {
	Name        Localized `json:"name"`
	Description Localized `json:"description"`
}

// CategoryView is definition-owned directory metadata, not execution eligibility.
type CategoryView struct {
	ID          string  `json:"id"`
	Code        string  `json:"code"`
	Enabled     bool    `json:"enabled"`
	RowRevision string  `json:"row_revision"`
	Display     Display `json:"display"`
}

// Entity is detached metadata for one explicitly identified level. Empty ParentID
// or DefaultID means SQL NULL, never latest. All IDs/revisions are decimal strings;
// times are UTC with microsecond precision. DocumentVerified is false on lists and
// metadata-only reads, even when a stored fingerprint is reported.
type Entity struct {
	Kind             Kind                 `json:"kind"`
	Identity         declaration.Identity `json:"identity"`
	ID               string               `json:"id"`
	RowRevision      string               `json:"row_revision"`
	CreatedAt        string               `json:"created_at"`
	UpdatedAt        string               `json:"updated_at"`
	Enabled          bool                 `json:"enabled"`
	ParentID         string               `json:"parent_id,omitempty"`
	DefaultID        string               `json:"default_id,omitempty"`
	Category         *CategoryView        `json:"category,omitempty"`
	Display          Display              `json:"display"`
	DocumentFormat   string               `json:"format,omitempty"`
	Profile          string               `json:"profile,omitempty"`
	SHA256           string               `json:"sha256,omitempty"`
	DocumentVerified bool                 `json:"document_verified"`
}

// ListRequest selects immediate children. Namespace alone lists definitions;
// Code additionally lists versions, Version lists modes, and Mode lists revisions.
// Parent.Revision must be empty. Code filters the child code exactly. Category is
// definition-only. Limit defaults to 20 (1..64); Cursor is query/owner-bound, not
// authenticated authority or a persistent snapshot. Locale empty means English.
type ListRequest struct {
	Parent   declaration.Identity
	Code     string
	Category string
	Enabled  *bool
	Locale   string
	Limit    int
	Cursor   string
}
type Page struct {
	Parent          declaration.Identity `json:"parent"`
	Kind            Kind                 `json:"kind"`
	RequestedLocale string               `json:"requested_locale"`
	Items           []Entity             `json:"items"`
	Next            string               `json:"next,omitempty"`
}

// ReadRequest names one exact entity. Version/Mode/Revision require their parents;
// defaults are never followed. Document is valid for a version or mode revision
// only and verifies its canonical content (including the revision's parent spec).
type ReadRequest struct {
	Identity declaration.Identity
	Locale   string
	Document bool
}
type TagValue struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// StoredDocument carries verified immutable bytes. Canonical is detached and
// encodes as base64 in JSON, preserving exact bytes without HTML-escape expansion.
type StoredDocument struct {
	Identity       declaration.Identity `json:"identity"`
	DocumentFormat string               `json:"format"`
	Profile        string               `json:"profile"`
	SHA256         string               `json:"sha256"`
	Canonical      []byte               `json:"canonical"`
}
type View struct {
	RequestedLocale string           `json:"requested_locale"`
	Entity          Entity           `json:"entity"`
	Tags            []TagValue       `json:"tags,omitempty"`
	Documents       []StoredDocument `json:"documents,omitempty"`
}

func (Page) Format(state fmt.State, _ rune)   { _, _ = state.Write([]byte("catalog.Page")) }
func (Page) LogValue() slog.Value             { return slog.StringValue("catalog.Page") }
func (View) Format(state fmt.State, _ rune)   { _, _ = state.Write([]byte("catalog.View")) }
func (View) LogValue() slog.Value             { return slog.StringValue("catalog.View") }
func (Entity) Format(state fmt.State, _ rune) { _, _ = state.Write([]byte("catalog.Entity")) }
func (Entity) LogValue() slog.Value           { return slog.StringValue("catalog.Entity") }
func (StoredDocument) Format(state fmt.State, _ rune) {
	_, _ = state.Write([]byte("catalog.StoredDocument"))
}
func (StoredDocument) LogValue() slog.Value { return slog.StringValue("catalog.StoredDocument") }
func readError(code failure.Code, reason string, causes ...error) error {
	return problemAt(code, "catalog.read", reason, "", causes...)
}

// ValidateList checks request shape and cursor encoding without a client or I/O.
// List additionally binds a supplied cursor to the actual target/filter scope.
func ValidateList(request ListRequest) error {
	kind, err := listKind(request.Parent)
	if err != nil {
		return err
	}
	if _, err := readLocale(request.Locale); err != nil {
		return err
	}
	if !readIdentityText(request.Code, 128, true) || !readIdentityText(request.Category, 128, true) || request.Category != "" && kind != Definition {
		return readError(ErrInput, "filter")
	}
	if request.Limit < 0 || request.Limit > MaxPageSize {
		return readError(ErrLimit, "page_size")
	}
	if request.Cursor != "" {
		_, err := decodeCursor(request.Cursor)
		return err
	}
	return nil
}

// ValidateRead checks exact identity/document/locale selection without I/O.
func ValidateRead(request ReadRequest) error {
	kind, err := readKind(request.Identity)
	if err != nil {
		return err
	}
	if _, err := readLocale(request.Locale); err != nil {
		return err
	}
	if request.Document && kind != Version && kind != ModeRevision {
		return readError(ErrInput, "document_selection")
	}
	return nil
}

// List reads only bounded metadata, never document bodies or historical totals.
// Each page's mutable facts and localized text come from one nonlocking SELECT;
// subsequent pages are separate views and may observe intervening changes.
func (catalog *Catalog) List(ctx context.Context, request ListRequest) (Page, error) {
	var result Page
	if request.Enabled != nil {
		active := *request.Enabled
		request.Enabled = &active
	}
	locale, err := readLocale(request.Locale)
	if err != nil {
		return result, err
	}
	kind, err := listKind(request.Parent)
	if err != nil {
		return result, err
	}
	if !readIdentityText(request.Code, 128, true) || !readIdentityText(request.Category, 128, true) || request.Category != "" && kind != Definition {
		return result, readError(ErrInput, "filter")
	}
	if request.Limit == 0 {
		request.Limit = 20
	}
	if request.Limit < 1 || request.Limit > MaxPageSize {
		return result, readError(ErrLimit, "page_size")
	}
	if catalog == nil || catalog.state == nil {
		return result, readError(ErrInput, "catalog")
	}
	binding := cursorBinding(catalog.state.target, catalog.state.pg != nil, request, locale, kind)
	last, err := readCursor(request.Cursor, binding)
	if err != nil {
		return result, err
	}
	result = Page{Parent: request.Parent, Kind: kind, RequestedLocale: locale, Items: []Entity{}}
	err = catalog.read(ctx, func(work context.Context, tx *publicationTx) error {
		query, args := tx.readSQL(request.Parent, kind, locale, request.Code, request.Category, request.Enabled, last, request.Limit+1, false)
		rows, err := tx.readAnchor(work, request.Limit+1, query, args...)
		if err != nil {
			return err
		}
		for _, row := range rows {
			entity, _, err := tx.readEntity(row, request.Parent, kind, locale)
			if err != nil {
				return err
			}
			if entity.ID != "" {
				result.Items = append(result.Items, entity)
			}
		}
		if len(result.Items) > request.Limit {
			result.Items = result.Items[:request.Limit]
			result.Next = writeCursor(binding, identityCode(result.Items[len(result.Items)-1].Identity, kind))
		}
		return viewBound(result)
	})
	if err != nil {
		return Page{}, err
	}
	return result, nil
}

// Read returns one coherent metadata view. Verified immutable documents are read
// afterward by the anchor's exact identity/profile/digest; disappearance or drift
// refuses the entire result instead of mixing versions. Read errors return no
// partial view. Retain/Close Catalog when Released reports unfinished cleanup.
func (catalog *Catalog) Read(ctx context.Context, request ReadRequest) (View, error) {
	var result View
	locale, err := readLocale(request.Locale)
	if err != nil {
		return result, err
	}
	kind, err := readKind(request.Identity)
	if err != nil {
		return result, err
	}
	if request.Document && kind != Version && kind != ModeRevision {
		return result, readError(ErrInput, "document_selection")
	}
	result.RequestedLocale = locale
	err = catalog.read(ctx, func(work context.Context, tx *publicationTx) error {
		query, args := tx.readSQL(request.Identity, kind, locale, "", "", nil, "", 1, true)
		rows, err := tx.readAnchor(work, MaxTags+1, query, args...)
		if err != nil {
			return err
		}
		var refs []bodyReference
		for index, row := range rows {
			entity, documents, err := tx.readEntity(row, request.Identity, kind, locale)
			if err != nil {
				return err
			}
			if entity.ID == "" {
				return readError(ErrMissing, "entity")
			}
			if index == 0 {
				result.Entity = entity
				refs = documents
			}
			if row[38] != nil {
				if len(result.Tags) == MaxTags || !validText(string(row[38]), MaxTagKeyBytes) || len(row[38]) == 0 || !validText(string(row[39]), MaxTagValueBytes) || row[39] == nil {
					return readError(ErrStored, "tags")
				}
				key := string(row[38])
				if len(result.Tags) > 0 && result.Tags[len(result.Tags)-1].Key >= key {
					return readError(ErrStored, "tag_order")
				}
				result.Tags = append(result.Tags, TagValue{key, string(row[39])})
			}
		}
		if result.Entity.ID == "" {
			return readError(ErrMissing, "entity")
		}
		if request.Document {
			contents := []declaration.Content{}
			for _, reference := range refs {
				content, err := tx.readBody(work, reference)
				if err != nil {
					return err
				}
				contents = append(contents, content)
				result.Documents = append(result.Documents, StoredDocument{content.Entry.Identity, reference.format, content.Entry.Profile, content.Entry.SHA256, bytes.Clone(content.Bytes)})
			}
			if _, err := declaration.ParseContents(work, contents); err != nil {
				reason := "document_integrity"
				if errors.Is(err, declaration.ErrUnsupported) {
					reason = "document_unsupported"
				}
				return readError(ErrStored, reason, err)
			}
			result.Entity.DocumentVerified = true
		}
		return viewBound(result)
	})
	if err != nil {
		return View{}, err
	}
	return result, nil
}
func (catalog *Catalog) read(ctx context.Context, run func(context.Context, *publicationTx) error) error {
	if catalog == nil || catalog.state == nil || ctx == nil {
		return readError(ErrInput, "catalog")
	}
	state := catalog.state
	if !state.mu.TryLock() {
		return readError(ErrState, "busy")
	}
	defer state.mu.Unlock()
	if state.closed.Load() {
		return readError(ErrState, "closed")
	}
	if state.pending != nil {
		return readError(ErrCleanup, "pending")
	}
	if ctx.Err() != nil {
		return readError(ErrCanceled, "context", ctx.Err(), context.Cause(ctx))
	}
	work, cancel := context.WithTimeout(ctx, state.options.Timeout)
	state.cancel = cancel
	state.reading = true
	var err error
	if state.pg != nil {
		tx, problem := state.pg.Begin(work, postgres.TxOptions{Isolation: postgres.ReadCommitted, Access: postgres.ReadOnly})
		err = problem
		if tx != nil {
			state.pending, _ = postgresPublication(tx, state.target)
		}
	} else {
		tx, problem := state.my.Begin(work, mysql.TxOptions{Isolation: sql.LevelReadCommitted, ReadOnly: true})
		err = problem
		if tx != nil {
			state.pending, _ = mysqlPublication(tx, state.target)
		}
	}
	if state.pending == nil {
		cancel()
		state.cancel = nil
		state.reading = false
		return readError(ErrDatabase, "begin", err)
	}
	state.pending.readOnly = true
	if err == nil {
		err = state.pending.schema(work)
	}
	if err == nil {
		err = run(work, state.pending)
	}
	cleanup, stop := context.WithTimeout(context.Background(), state.options.CleanupTimeout)
	final, finished := state.pending.finish(cleanup, false)
	stop()
	state.observe(final)
	if !final.released {
		return readError(ErrCleanup, "pending", err, finished)
	}
	if finished != nil {
		return publicationErrors(err, readError(ErrDatabase, "read_finalization", finished), state.cleanup)
	}
	return publicationErrors(err, state.cleanup)
}
func (tx *publicationTx) readAnchor(ctx context.Context, maximum int, query string, args ...any) ([][][]byte, error) {
	if !tx.readOnly || tx.dataRead {
		return nil, readError(ErrState, "read_anchor")
	}
	tx.dataRead = true
	return tx.queryRows(ctx, maximum, query, args...)
}
func readLocale(value string) (string, error) {
	if value == "" {
		value = "en"
	}
	locale, err := i18n.NormalizeLocale(value)
	if err != nil {
		return "", readError(ErrInput, "locale", err)
	}
	return locale, nil
}
func readIdentityText(value string, maximum int, optional bool) bool {
	return (optional || value != "") && validText(value, maximum) && !strings.ContainsFunc(value, unicode.IsControl)
}
func validReadIdentity(value declaration.Identity) bool {
	return readIdentityText(value.Namespace, 255, false) && readIdentityText(value.Code, 128, true) && readIdentityText(value.Version, 128, true) && readIdentityText(value.Mode, 128, true) && readIdentityText(value.Revision, 128, true) &&
		(value.Version == "" || value.Code != "") && (value.Mode == "" || value.Version != "") && (value.Revision == "" || value.Mode != "")
}
func listKind(parent declaration.Identity) (Kind, error) {
	if !validReadIdentity(parent) || parent.Revision != "" {
		return "", readError(ErrInput, "parent")
	}
	if parent.Code == "" {
		return Definition, nil
	}
	if parent.Version == "" {
		return Version, nil
	}
	if parent.Mode == "" {
		return Mode, nil
	}
	return ModeRevision, nil
}
func readKind(identity declaration.Identity) (Kind, error) {
	if !validReadIdentity(identity) || identity.Code == "" {
		return "", readError(ErrInput, "identity")
	}
	if identity.Version == "" {
		return Definition, nil
	}
	if identity.Mode == "" {
		return Version, nil
	}
	if identity.Revision == "" {
		return Mode, nil
	}
	return ModeRevision, nil
}
func identityCode(identity declaration.Identity, kind Kind) string {
	switch kind {
	case Definition:
		return identity.Code
	case Version:
		return identity.Version
	case Mode:
		return identity.Mode
	default:
		return identity.Revision
	}
}

type readCursorValue struct {
	Version int    `json:"v"`
	Binding string `json:"binding"`
	After   string `json:"after"`
}

func cursorBinding(target string, pg bool, request ListRequest, locale string, kind Kind) string {
	raw, _ := json.Marshal([]any{"fathomry.catalog-page/v1", target, pg, kind, request.Parent, request.Code, request.Category, request.Enabled, locale})
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}
func writeCursor(binding, after string) string {
	raw, _ := jsonv2.Marshal(readCursorValue{1, binding, after})
	return base64.RawURLEncoding.EncodeToString(raw)
}
func readCursor(value, binding string) (string, error) {
	if value == "" {
		return "", nil
	}
	cursor, err := decodeCursor(value)
	if err != nil {
		return "", err
	}
	if cursor.Binding != binding {
		return "", readError(ErrInput, "cursor_binding")
	}
	return cursor.After, nil
}
func decodeCursor(value string) (readCursorValue, error) {
	var cursor readCursorValue
	if len(value) > MaxCursorBytes {
		return cursor, readError(ErrLimit, "cursor")
	}
	raw, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return cursor, readError(ErrInput, "cursor")
	}
	if err := jsonv2.Unmarshal(raw, &cursor, jsonv2.RejectUnknownMembers(true)); err != nil {
		return cursor, readError(ErrInput, "cursor", err)
	}
	hash, err := hex.DecodeString(cursor.Binding)
	if err != nil || len(hash) != 32 || hex.EncodeToString(hash) != cursor.Binding || cursor.Version != 1 || !readIdentityText(cursor.After, 128, false) || writeCursor(cursor.Binding, cursor.After) != value {
		return cursor, readError(ErrInput, "cursor_binding")
	}
	return cursor, nil
}
func viewBound(value any) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return readError(ErrStored, "view_encoding", err)
	}
	if len(raw) > MaxViewBytes {
		return readError(ErrLimit, "view_bytes")
	}
	return nil
}

func (tx *publicationTx) readSQL(scope declaration.Identity, kind Kind, locale, code, category string, active *bool, after string, limit int, one bool) (string, []any) {
	values := []any{scope.Namespace, scope.Code, scope.Version, scope.Mode, scope.Revision, locale, code, category, nil, after}
	if active != nil {
		if *active {
			values[8] = 1
		} else {
			values[8] = 0
		}
	}
	fields := []string{"namespace", "workflow", "version", "mode", "revision", "locale", "code", "category", "enabled", "after_code"}
	input := []string{}
	for index, field := range fields {
		dataType := "TEXT"
		if tx.my != nil {
			dataType = "CHAR CHARACTER SET utf8mb4"
		}
		if index == 8 {
			dataType = "INTEGER"
			if tx.my != nil {
				dataType = "SIGNED"
			}
		}
		input = append(input, "CAST("+tx.lastParameter(index+1)+" AS "+dataType+") AS "+field)
	}
	base := " FROM request q JOIN " + tx.table("workflow_definition") + " d ON d.namespace=q.namespace"
	if kind != Definition || one {
		base += " AND d.code=q.workflow"
	}
	parentBase := base
	entity, entityCode, parent, defaultID := "d", "d.code", "NULL", "d.default_version_id"
	format, profile, digest := "NULL", "NULL", "NULL"
	versionID, versionFormat, versionProfile, versionDigest, modeID := "NULL", "NULL", "NULL", "NULL", "NULL"
	parts := []string{"item.namespace", "item.workflow"}
	if kind != Definition {
		base += " JOIN " + tx.table("workflow_version") + " v ON v.workflow_definition_id=d.id"
		if kind != Version || one {
			base += " AND v.version_code=q.version"
		}
		entity, entityCode, parent, defaultID = "v", "v.version_code", "v.workflow_definition_id", "v.default_mode_id"
		format, profile, digest = "v.spec_format", "v.fingerprint_profile", "v.spec_sha256"
		versionID, versionFormat, versionProfile, versionDigest = "v.id", "v.spec_format", "v.fingerprint_profile", "v.spec_sha256"
		parts = append(parts, "item.version")
		if kind == Version && !one {
			parentBase = " FROM request q JOIN " + tx.table("workflow_definition") + " d ON d.namespace=q.namespace AND d.code=q.workflow"
		} else {
			parentBase = base
		}
	}
	if kind == Mode || kind == ModeRevision {
		base += " JOIN " + tx.table("workflow_mode") + " m ON m.workflow_version_id=v.id"
		if kind == ModeRevision || one {
			base += " AND m.code=q.mode"
		}
		entity, entityCode, parent, defaultID = "m", "m.code", "m.workflow_version_id", "m.default_mode_version_id"
		format, profile, digest = "NULL", "NULL", "NULL"
		modeID = "m.id"
		parts = append(parts, "item.mode")
	}
	if kind == ModeRevision {
		parentBase = base
		base += " JOIN " + tx.table("workflow_mode_version") + " r ON r.workflow_mode_id=m.id"
		if one {
			base += " AND r.version_code=q.revision"
		}
		entity, entityCode, parent, defaultID = "r", "r.version_code", "r.workflow_mode_id", "NULL"
		format, profile, digest = "r.config_format", "r.fingerprint_profile", "r.config_sha256"
		parts = append(parts, "item.revision")
	}
	base += " LEFT JOIN " + tx.table("workflow_category") + " c ON c.id=d.category_id AND c.namespace=d.namespace"
	owner := "1"
	if one {
		owner = "(SELECT COUNT(*)" + base + ")"
	} else if kind != Definition {
		owner = "(SELECT COUNT(*)" + parentBase + ")"
	}
	where := " WHERE (OCTET_LENGTH(q.code)=0 OR " + entityCode + "=q.code) AND (q.enabled IS NULL OR " + entity + ".enabled=(q.enabled=1)) AND (OCTET_LENGTH(q.after_code)=0 OR " + entityCode + ">q.after_code)"
	if kind == Definition {
		where += " AND (OCTET_LENGTH(q.category)=0 OR c.code=q.category)"
	}
	identityVersion, identityMode, identityRevision := "NULL", "NULL", "NULL"
	if kind != Definition {
		identityVersion = "v.version_code"
	}
	if kind == Mode || kind == ModeRevision {
		identityMode = "m.code"
	}
	if kind == ModeRevision {
		identityRevision = "r.version_code"
	}
	catID, catCode, catActive, catRevision := "NULL", "NULL", "NULL", "NULL"
	if kind == Definition {
		catID, catCode, catActive, catRevision = "d.category_id", "c.code", "c.enabled", "c.row_revision"
	}
	selected := "SELECT " + entity + ".id," + entity + ".row_revision," + entityCode + " AS machine_code," + tx.readTime(entity+".created_at") + " AS created_at," + tx.readTime(entity+".updated_at") + " AS updated_at," +
		entity + ".enabled," + parent + " AS parent_id," + defaultID + " AS default_id," + catID + " AS category_id," + catCode + " AS category_code," + catActive + " AS category_enabled," + catRevision + " AS category_revision," +
		format + " AS document_format," + profile + " AS profile," + digest + " AS digest,d.id AS definition_id," + versionID + " AS version_id," + versionFormat + " AS spec_format," + versionProfile + " AS spec_profile," + versionDigest + " AS spec_digest," + modeID + " AS mode_id," +
		"d.namespace,d.code AS workflow," + identityVersion + " AS version," + identityMode + " AS mode," + identityRevision + " AS revision" + base + where + " ORDER BY " + entityCode + " LIMIT " + strconv.Itoa(limit)
	// readEntity consumes owner/entity/locale facts (0..25), four resource/English/
	// requested-text triples (26..37), then the optional tag pair (38..39).
	columns := []string{owner, "item.id", "item.row_revision", "item.machine_code", "item.created_at", "item.updated_at", "item.enabled", "item.parent_id", "item.default_id", "item.category_id", "item.category_code", "item.category_enabled", "item.category_revision", "item.document_format", "item.profile", "item.digest", "item.definition_id", "item.version_id", "item.spec_format", "item.spec_profile", "item.spec_digest", "item.mode_id", "english.id", "english.enabled", "requested.id", "requested.enabled"}
	joins := " FROM request q LEFT JOIN selected item ON 1=1 LEFT JOIN " + tx.table("i18n_locale") + " english ON english.code='en' LEFT JOIN " + tx.table("i18n_locale") + " requested ON requested.code=q.locale"
	for index, field := range []struct {
		scope string
		parts []string
		field string
	}{{"workflow." + string(kind), parts, "name"}, {"workflow." + string(kind), parts, "description"}, {"workflow.category", []string{"item.namespace", "item.category_code"}, "name"}, {"workflow.category", []string{"item.namespace", "item.category_code"}, "description"}} {
		alias := "text" + strconv.Itoa(index)
		key := readResourceKey(append(append([]string(nil), field.parts...), "'"+field.field+"'"))
		joins += " LEFT JOIN " + tx.table("i18n_resource") + " " + alias + " ON " + alias + ".scope='" + field.scope + "' AND " + alias + ".code=" + key
		joins += " LEFT JOIN " + tx.table("i18n_text") + " " + alias + "en ON " + alias + "en.resource_id=" + alias + ".id AND " + alias + "en.locale_id=english.id"
		joins += " LEFT JOIN " + tx.table("i18n_text") + " " + alias + "tr ON " + alias + "tr.resource_id=" + alias + ".id AND " + alias + "tr.locale_id=requested.id"
		columns = append(columns, alias+".id", alias+"en.value", alias+"tr.value")
	}
	order := " ORDER BY item.machine_code"
	if one && kind == Definition {
		joins += " LEFT JOIN " + tx.table("workflow_definition_tag") + " tag ON tag.workflow_definition_id=item.id"
		columns = append(columns, "tag.tag_key", "tag.tag_value")
		order += ",tag.tag_key"
	} else {
		columns = append(columns, "NULL", "NULL")
	}
	return "WITH request AS (SELECT " + strings.Join(input, ",") + "), selected AS (" + selected + ") SELECT " + strings.Join(columns, ",") + joins + order + " LIMIT " + strconv.Itoa(max(limit, MaxTags+1)), values
}
func (tx *publicationTx) readTime(column string) string {
	if tx.pg != nil {
		return "to_char(" + column + " AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"')"
	}
	return "DATE_FORMAT(" + column + ",'%Y-%m-%dT%H:%i:%s.%fZ')"
}
func readResourceKey(parts []string) string {
	values := []string{"'v1;'"}
	for _, part := range parts {
		values = append(values, "OCTET_LENGTH("+part+")", "':'", part)
	}
	return "CONCAT(" + strings.Join(values, ",") + ")"
}

type bodyReference struct {
	table, parentKey, id, parentID, label, format, profile, sha string
	identity                                                    declaration.Identity
}

func (tx *publicationTx) readEntity(row [][]byte, scope declaration.Identity, kind Kind, locale string) (Entity, []bodyReference, error) {
	var entity Entity
	if len(row) != 40 {
		return entity, nil, readError(ErrStored, "row_shape")
	}
	if string(row[0]) != "1" {
		return entity, nil, readError(ErrMissing, "parent")
	}
	if _, err := readID(row[22], false); err != nil {
		return entity, nil, err
	}
	english, err := enabled(row[23])
	if err != nil || !english {
		return entity, nil, readError(ErrStored, "english_locale", err)
	}
	if row[1] == nil {
		return entity, nil, nil
	}
	entity.Kind = kind
	entity.Identity = scope
	code, err := readString(row[3], 128, false)
	if err != nil {
		return entity, nil, err
	}
	switch kind {
	case Definition:
		entity.Identity.Code = code
	case Version:
		entity.Identity.Version = code
	case Mode:
		entity.Identity.Mode = code
	case ModeRevision:
		entity.Identity.Revision = code
	}
	entity.ID, err = readID(row[1], false)
	if err != nil {
		return entity, nil, err
	}
	entity.RowRevision, err = readID(row[2], false)
	if err != nil {
		return entity, nil, err
	}
	entity.CreatedAt, err = readTimestamp(row[4])
	if err != nil {
		return entity, nil, err
	}
	entity.UpdatedAt, err = readTimestamp(row[5])
	if err != nil {
		return entity, nil, err
	}
	entity.Enabled, err = enabled(row[6])
	if err != nil {
		return entity, nil, err
	}
	entity.ParentID, err = readID(row[7], kind == Definition)
	if err != nil {
		return entity, nil, err
	}
	entity.DefaultID, err = readID(row[8], true)
	if err != nil {
		return entity, nil, err
	}
	state := "none"
	if locale != "en" {
		if row[24] == nil {
			state = "unknown_locale"
		} else {
			if _, err := readID(row[24], false); err != nil {
				return entity, nil, err
			}
			active, err := enabled(row[25])
			if err != nil {
				return entity, nil, err
			}
			if !active {
				state = "disabled_locale"
			}
		}
	}
	entity.Display, err = readDisplay(row[26:32], kind == Definition || kind == Mode, locale, state)
	if err != nil {
		return entity, nil, err
	}
	if row[9] != nil {
		category := &CategoryView{}
		category.ID, err = readID(row[9], false)
		if err != nil {
			return entity, nil, err
		}
		category.Code, err = readString(row[10], 128, false)
		if err != nil {
			return entity, nil, err
		}
		category.Enabled, err = enabled(row[11])
		if err != nil {
			return entity, nil, err
		}
		category.RowRevision, err = readID(row[12], false)
		if err != nil {
			return entity, nil, err
		}
		category.Display, err = readDisplay(row[32:38], false, locale, state)
		if err != nil {
			return entity, nil, err
		}
		entity.Category = category
	}
	var references []bodyReference
	if kind == Version || kind == ModeRevision {
		entity.DocumentFormat, err = readString(row[13], 64, false)
		if err != nil {
			return entity, nil, err
		}
		entity.Profile, err = readString(row[14], 64, false)
		if err != nil {
			return entity, nil, err
		}
		digest, err := tx.binary(row[15], 32)
		if err != nil || len(digest) != 32 {
			return entity, nil, readError(ErrStored, "document_digest", err)
		}
		entity.SHA256 = hex.EncodeToString(digest)
		if kind == Version {
			references = append(references, bodyReference{"workflow_version", "workflow_definition_id", entity.ID, entity.ParentID, entity.Identity.Version, entity.DocumentFormat, entity.Profile, entity.SHA256, entity.Identity})
		} else {
			parentIdentity := entity.Identity
			parentIdentity.Mode, parentIdentity.Revision = "", ""
			versionID, err := readID(row[17], false)
			if err != nil {
				return entity, nil, err
			}
			definitionID, err := readID(row[16], false)
			if err != nil {
				return entity, nil, err
			}
			format, err := readString(row[18], 64, false)
			if err != nil {
				return entity, nil, err
			}
			profile, err := readString(row[19], 64, false)
			if err != nil {
				return entity, nil, err
			}
			digest, err := tx.binary(row[20], 32)
			if err != nil || len(digest) != 32 {
				return entity, nil, readError(ErrStored, "document_digest", err)
			}
			references = append(references, bodyReference{"workflow_version", "workflow_definition_id", versionID, definitionID, parentIdentity.Version, format, profile, hex.EncodeToString(digest), parentIdentity},
				bodyReference{"workflow_mode_version", "workflow_mode_id", entity.ID, entity.ParentID, entity.Identity.Revision, entity.DocumentFormat, entity.Profile, entity.SHA256, entity.Identity})
		}
	}
	return entity, references, nil
}
func readID(raw []byte, optional bool) (string, error) {
	if raw == nil && optional {
		return "", nil
	}
	id, err := rowID([][]byte{raw}, 0)
	if err != nil {
		return "", readError(ErrStored, "id", err)
	}
	return strconv.FormatInt(id, 10), nil
}
func readString(raw []byte, maximum int, empty bool) (string, error) {
	value := string(raw)
	if raw == nil || !readIdentityText(value, maximum, empty) {
		return "", readError(ErrStored, "identity")
	}
	return value, nil
}
func readTimestamp(raw []byte) (string, error) {
	value := string(raw)
	if _, err := time.Parse("2006-01-02T15:04:05.000000Z", value); err != nil {
		return "", readError(ErrStored, "timestamp", err)
	}
	return value, nil
}
func readDisplay(row [][]byte, required bool, locale, state string) (Display, error) {
	name, err := readLocalized(row[:3], required, true, locale, state)
	if err != nil {
		return Display{}, err
	}
	description, err := readLocalized(row[3:], false, false, locale, state)
	return Display{name, description}, err
}
func readLocalized(row [][]byte, required, name bool, locale, state string) (Localized, error) {
	if row[0] == nil {
		if required {
			return Localized{}, readError(ErrStored, "missing_name")
		}
		return Localized{Fallback: "resource_absent"}, nil
	}
	if _, err := readID(row[0], false); err != nil {
		return Localized{}, err
	}
	field := Description
	if name {
		field = Name
	}
	if row[1] == nil {
		return Localized{}, readError(ErrStored, "missing_english")
	}
	if err := admitText(field, Text{English: string(row[1])}, ""); err != nil {
		return Localized{}, readError(ErrStored, "english_text", err)
	}
	result := Localized{Present: true, Value: string(row[1]), Locale: "en", Fallback: state}
	if locale == "en" {
		result.Fallback = "none"
		return result, nil
	}
	if state != "none" {
		return result, nil
	}
	if row[2] == nil {
		result.Fallback = "missing_translation"
		return result, nil
	}
	if err := admitText(field, Text{English: string(row[2])}, ""); err != nil {
		return Localized{}, readError(ErrStored, "translated_text", err)
	}
	result.Value, result.Locale = string(row[2]), locale
	return result, nil
}
func (tx *publicationTx) readBody(ctx context.Context, reference bodyReference) (declaration.Content, error) {
	if !tx.readOnly || !tx.dataRead {
		return declaration.Content{}, readError(ErrState, "read_anchor")
	}
	prefix, format, maximum := "spec", declaration.SpecFormat, declaration.MaxSpecBytes
	if reference.table == "workflow_mode_version" {
		prefix, format, maximum = "config", declaration.ModeFormat, declaration.MaxModeBytes
	}
	if reference.format != format {
		return declaration.Content{}, readError(ErrStored, "document_unsupported", declaration.ErrUnsupported)
	}
	hash, err := hex.DecodeString(reference.sha)
	if err != nil {
		return declaration.Content{}, readError(ErrStored, "document_digest", err)
	}
	query := "SELECT " + prefix + "_document FROM " + tx.table(reference.table) + " WHERE " + tx.predicate([]string{"id", reference.parentKey, "version_code", prefix + "_format", "fingerprint_profile", prefix + "_sha256"})
	rows, err := tx.query(ctx, query, reference.id, reference.parentID, reference.label, reference.format, reference.profile, hash)
	if err != nil {
		return declaration.Content{}, err
	}
	if len(rows) != 1 || len(rows[0]) != 1 {
		return declaration.Content{}, readError(ErrStored, "snapshot_changed")
	}
	body, err := tx.binary(rows[0][0], maximum)
	if err != nil {
		return declaration.Content{}, err
	}
	return declaration.Content{Entry: declaration.Entry{Identity: reference.identity, Profile: reference.profile, SHA256: reference.sha}, Bytes: body}, nil
}
