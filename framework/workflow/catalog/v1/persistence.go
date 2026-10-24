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
	"encoding/hex"
	"errors"
	"slices"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	mysql "github.com/frost-leo/fathomry/adapters/database/mysql/v1"
	postgres "github.com/frost-leo/fathomry/adapters/database/postgres/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/failure/v1"
	declaration "github.com/frost-leo/fathomry/framework/workflow/declaration/v1"
)

const maxPublicationCalls = 32768
const maxPublicationReadBytes = 64 << 20

// publicationTx borrows a writable read-committed transaction and admits its schema
// on that same retained Resource generation before business SQL. It opens no source
// and begins no transaction. The caller must join finish after success or failure;
// register's change counts remain tentative until native commit acknowledgement.
type publicationTx struct {
	pg               *postgres.Transaction
	my               *mysql.Transaction
	target, name     string
	calls, readBytes int
	used             bool
	finalRequested   bool
	finalError       error
	finalAck         string
	readOnly         bool
	dataRead         bool
}
type publicationFinal struct {
	acknowledgement  string
	released         bool
	source           adapters.Source
	primary, cleanup error
}

// finish never resends COMMIT. Subsequent calls only request rollback of still
// unresolved native work and join the original receipt. A wait failure retains
// this same transaction as the cleanup owner; it is not rollback evidence.
func (tx *publicationTx) finish(ctx context.Context, commit bool) (publicationFinal, error) {
	var result publicationFinal
	if tx == nil || ctx == nil || (tx.pg == nil) == (tx.my == nil) {
		return result, publicationError(ErrInput, "finalization", "")
	}
	var operationError, waitError error
	if tx.pg != nil {
		before, resolved := tx.pg.Receipt().Snapshot()
		result.source = before.Info().Source
		if !resolved {
			var native postgres.Result
			if !tx.finalRequested {
				tx.finalRequested = true
				if commit {
					native, operationError = tx.pg.Commit(ctx)
				} else {
					native, operationError = tx.pg.Rollback(ctx)
				}
			} else {
				native, operationError = tx.pg.Rollback(ctx)
			}
			if ack := string(native.TransactionOutcome()); ack != "" {
				tx.finalAck = ack
			}
		}
		after, err := tx.pg.Receipt().WaitReleased(ctx)
		waitError = err
		result.source, result.released = after.Info().Source, after.Info().Released
		result.primary, result.cleanup = after.Primary(), after.Cleanup()
		if native, present := after.ValueCopy(); present {
			if ack := string(native.TransactionOutcome()); ack != "" {
				tx.finalAck = ack
			}
		}
	} else {
		before, resolved := tx.my.Receipt().Snapshot()
		result.source = before.Info().Source
		if !resolved {
			var native mysql.Result
			if !tx.finalRequested {
				tx.finalRequested = true
				if commit {
					native, operationError = tx.my.Commit(ctx)
				} else {
					native, operationError = tx.my.Rollback(ctx)
				}
			} else {
				native, operationError = tx.my.Rollback(ctx)
			}
			if ack := string(native.TransactionOutcome()); ack != "" {
				tx.finalAck = ack
			}
		}
		after, err := tx.my.Receipt().WaitReleased(ctx)
		waitError = err
		result.source, result.released = after.Info().Source, after.Info().Released
		result.primary, result.cleanup = after.Primary(), after.Cleanup()
		if native, present := after.ValueCopy(); present {
			if ack := string(native.TransactionOutcome()); ack != "" {
				tx.finalAck = ack
			}
		}
	}
	if tx.finalError == nil {
		tx.finalError = operationError
	}
	result.acknowledgement = tx.finalAck
	problem := errors.Join(tx.finalError, operationError, waitError, result.primary, result.cleanup)
	if problem != nil {
		return result, publicationError(ErrDatabase, "finalization", "", problem)
	}
	if !result.released || result.acknowledgement == "" || result.acknowledgement == "finalization-unknown" {
		return result, publicationError(ErrDatabase, "finalization_unproven", "")
	}
	return result, nil
}

type publicationChanges struct {
	definitionID                                                   int64
	definitionCreated                                              bool
	categories, versions, modes, revisions, resources, texts, tags int
}

func (value publicationChanges) changed() bool {
	return value.definitionCreated || value.categories+value.versions+value.modes+value.revisions+value.resources+value.texts+value.tags > 0
}
func publicationError(code failure.Code, reason, path string, causes ...error) error {
	return problemAt(code, "catalog.register", reason, path, causes...)
}
func postgresPublication(tx *postgres.Transaction, target string) (*publicationTx, error) {
	quoted, err := publicationTarget("postgresql", target)
	if err != nil || tx == nil {
		return nil, publicationError(ErrInput, "transaction", "", err)
	}
	return &publicationTx{pg: tx, target: quoted, name: target}, nil
}
func mysqlPublication(tx *mysql.Transaction, target string) (*publicationTx, error) {
	quoted, err := publicationTarget("mysql", target)
	if err != nil || tx == nil {
		return nil, publicationError(ErrInput, "transaction", "", err)
	}
	return &publicationTx{my: tx, target: quoted, name: target}, nil
}
func publicationTarget(dialect, target string) (string, error) {
	if target == "" || !utf8.ValidString(target) || strings.ContainsFunc(target, unicode.IsControl) {
		return "", ErrInput
	}
	switch dialect {
	case "postgresql":
		if len(target) > 63 || strings.HasPrefix(target, "pg_") || target == "information_schema" {
			return "", ErrInput
		}
		return "\"" + strings.ReplaceAll(target, "\"", "\"\"") + "\"", nil
	case "mysql":
		if utf8.RuneCountInString(target) > 64 || strings.ContainsAny(target, "./\\") || strings.HasSuffix(target, " ") ||
			strings.ContainsFunc(target, func(char rune) bool { return char > 0xffff }) || slices.Contains([]string{"mysql", "sys", "information_schema", "performance_schema"}, strings.ToLower(target)) {
			return "", ErrInput
		}
		return "`" + strings.ReplaceAll(target, "`", "``") + "`", nil
	}
	return "", ErrInput
}
func (tx *publicationTx) admit(ctx context.Context) error {
	if tx == nil || ctx == nil || (tx.pg == nil) == (tx.my == nil) {
		return publicationError(ErrInput, "transaction", "")
	}
	if ctx.Err() != nil {
		return publicationError(ErrCanceled, "context", "", ctx.Err(), context.Cause(ctx))
	}
	if tx.calls >= maxPublicationCalls {
		return publicationError(ErrLimit, "sql_calls", "")
	}
	tx.calls++
	return nil
}
func (tx *publicationTx) parameters(count int) string {
	result := make([]string, count)
	for index := range result {
		result[index] = "?"
		if tx.pg != nil {
			result[index] = "$" + strconv.Itoa(index+1)
		}
	}
	return strings.Join(result, ",")
}
func (tx *publicationTx) predicate(columns []string) string {
	result := make([]string, len(columns))
	for index, column := range columns {
		parameter := "?"
		if tx.pg != nil {
			parameter = "$" + strconv.Itoa(index+1)
		}
		result[index] = column + "=" + parameter
	}
	return strings.Join(result, " AND ")
}
func (tx *publicationTx) table(name string) string { return tx.target + "." + name }

func (tx *publicationTx) query(ctx context.Context, sql string, args ...any) ([][][]byte, error) {
	return tx.queryRows(ctx, 2, sql, args...)
}
func (tx *publicationTx) queryRows(ctx context.Context, maximum int, sql string, args ...any) ([][][]byte, error) {
	if err := tx.admit(ctx); err != nil {
		return nil, err
	}
	var rows [][][]byte
	if tx.pg != nil {
		result, err := tx.pg.Query(ctx, sql, args...)
		if err != nil {
			return nil, publicationError(ErrDatabase, "query", "", err)
		}
		if !result.Complete() {
			return nil, publicationError(ErrDatabase, "partial_query", "")
		}
		values := result.RowsCopy()
		if len(values) > maximum {
			return nil, publicationError(ErrStored, "nonunique_row", "")
		}
		for _, row := range values {
			rows = append(rows, row.ValuesCopy())
		}
	} else {
		result, err := tx.my.Query(ctx, sql, args...)
		if err != nil {
			return nil, publicationError(ErrDatabase, "query", "", err)
		}
		if !result.Complete() {
			return nil, publicationError(ErrDatabase, "partial_query", "")
		}
		values := result.RowsCopy()
		if len(values) > maximum {
			return nil, publicationError(ErrStored, "nonunique_row", "")
		}
		for _, row := range values {
			rows = append(rows, row.ValuesCopy())
		}
	}
	for _, row := range rows {
		for _, value := range row {
			if len(value) > maxPublicationReadBytes-tx.readBytes {
				return nil, publicationError(ErrLimit, "stored_bytes", "")
			}
			tx.readBytes += len(value)
		}
	}
	return rows, nil
}
func (tx *publicationTx) exec(ctx context.Context, sql string, args ...any) (int64, error) {
	if err := tx.admit(ctx); err != nil {
		return 0, err
	}
	if tx.pg != nil {
		result, err := tx.pg.Exec(ctx, sql, args...)
		if err != nil {
			return 0, publicationError(ErrDatabase, "exec", "", err)
		}
		if !result.Complete() {
			return 0, publicationError(ErrDatabase, "partial_exec", "")
		}
		return result.RowsAffected(), nil
	}
	result, err := tx.my.Exec(ctx, sql, args...)
	if err != nil {
		return 0, publicationError(ErrDatabase, "exec", "", err)
	}
	if !result.Complete() {
		return 0, publicationError(ErrDatabase, "partial_exec", "")
	}
	count, known := result.RowsAffected()
	if !known {
		return 0, publicationError(ErrDatabase, "unknown_row_count", "")
	}
	return count, nil
}
func (tx *publicationTx) row(ctx context.Context, table, fields string, columns []string, args []any, lock string) ([][]byte, error) {
	rows, err := tx.query(ctx, "SELECT "+fields+" FROM "+tx.table(table)+" WHERE "+tx.predicate(columns)+" LIMIT 2 "+lock, args...)
	if err != nil {
		return nil, err
	}
	if len(rows) > 1 {
		return nil, publicationError(ErrStored, "nonunique_row", "")
	}
	if len(rows) == 0 {
		return nil, nil
	}
	return rows[0], nil
}
func (tx *publicationTx) insert(ctx context.Context, table string, columns, identity []string, args []any) (bool, error) {
	sql := "INSERT INTO " + tx.table(table) + " (" + strings.Join(columns, ",") + ") VALUES (" + tx.parameters(len(args)) + ")"
	if tx.pg != nil {
		rows, err := tx.query(ctx, sql+" ON CONFLICT ("+strings.Join(identity, ",")+") DO NOTHING RETURNING id", args...)
		if err != nil {
			return false, err
		}
		if len(rows) > 1 {
			return false, publicationError(ErrStored, "insert_count", "")
		}
		return len(rows) == 1, nil
	}
	count, err := tx.exec(ctx, sql, args...)
	if err != nil {
		return false, err
	}
	if count != 1 {
		return false, publicationError(ErrStored, "insert_count", "")
	}
	return true, nil
}
func (tx *publicationTx) identity(ctx context.Context, table, fields string, keys []string, keyValues []any, extra []string, values []any) ([][]byte, bool, error) {
	row, err := tx.row(ctx, table, fields, keys, keyValues, "FOR UPDATE")
	if err != nil || row != nil {
		return row, false, err
	}
	columns := append(slices.Clone(keys), extra...)
	arguments := append(slices.Clone(keyValues), values...)
	created, err := tx.insert(ctx, table, columns, keys, arguments)
	if err != nil {
		native, duplicate := mysql.InspectError(err)
		if tx.my == nil || !duplicate || native.Number != 1062 || string(native.SQLState[:]) != "23000" {
			return nil, false, err
		}
	}
	duplicateCause := err
	row, err = tx.row(ctx, table, fields, keys, keyValues, "FOR UPDATE")
	if err != nil {
		return nil, false, publicationError(ErrDatabase, "identity_readback", "", duplicateCause, err)
	}
	if row == nil {
		err = publicationError(ErrStored, "missing_inserted_identity", "", duplicateCause)
	}
	return row, created, err
}
func rowID(row [][]byte, index int) (int64, error) {
	if index < 0 || index >= len(row) || row[index] == nil {
		return 0, publicationError(ErrStored, "missing_id", "")
	}
	id, err := strconv.ParseInt(string(row[index]), 10, 64)
	if err != nil || id <= 0 || strconv.FormatInt(id, 10) != string(row[index]) {
		return 0, publicationError(ErrStored, "invalid_id", "")
	}
	return id, nil
}
func enabled(value []byte) (bool, error) {
	switch string(value) {
	case "t", "1":
		return true, nil
	case "f", "0":
		return false, nil
	}
	return false, publicationError(ErrStored, "invalid_boolean", "")
}
func (tx *publicationTx) binary(value []byte, maximum int) ([]byte, error) {
	if tx.pg == nil {
		if len(value) > maximum {
			return nil, publicationError(ErrStored, "stored_size", "")
		}
		return value, nil
	}
	if !bytes.HasPrefix(value, []byte("\\x")) || len(value) > 2*maximum+2 {
		return nil, publicationError(ErrStored, "binary_encoding", "")
	}
	decoded, err := hex.DecodeString(string(value[2:]))
	if err != nil {
		return nil, publicationError(ErrStored, "binary_encoding", "", err)
	}
	return decoded, nil
}

// register mutates only this transaction's tentative state. The caller must not
// describe these counts as durable until native commit acknowledgement is known.
func (tx *publicationTx) register(ctx context.Context, prepared *PreparedRegistration) (publicationChanges, error) {
	var changes publicationChanges
	if tx != nil && tx.readOnly {
		return changes, publicationError(ErrState, "read_only", "")
	}
	if prepared == nil || prepared.state == nil {
		return changes, publicationError(ErrHandle, "prepared", "")
	}
	if err := tx.admit(ctx); err != nil {
		return changes, err
	}
	if tx.used {
		return changes, publicationError(ErrHandle, "transaction_reused", "")
	}
	if publicationCalls(prepared.state) > maxPublicationCalls-tx.calls {
		return changes, publicationError(ErrLimit, "publication_calls", "")
	}
	tx.used = true
	if err := tx.schema(ctx); err != nil {
		return changes, err
	}
	input := prepared.state
	row, created, err := tx.identity(ctx, "workflow_definition", "id", []string{"namespace", "code"}, []any{input.info.Namespace, input.info.Code}, nil, nil)
	if err != nil {
		return changes, err
	}
	id, err := rowID(row, 0)
	if err != nil {
		return changes, err
	}
	changes.definitionID, changes.definitionCreated = id, created
	if created && input.category != nil {
		categoryID, newCategory, err := tx.category(ctx, input.info.Namespace, input.category)
		if err != nil {
			return changes, err
		}
		if newCategory {
			changes.categories++
		}
		count, err := tx.exec(ctx, "UPDATE "+tx.table("workflow_definition")+" SET category_id="+tx.parameters(1)+" WHERE id="+tx.lastParameter(2), categoryID, id)
		if err != nil {
			return changes, err
		}
		if count != 1 {
			return changes, publicationError(ErrStored, "new_definition_missing", "")
		}
	}
	parents := map[string]struct {
		id      int64
		content declaration.Content
	}{}
	modes := map[[2]string]int64{}
	for index, content := range input.contents {
		path := "/contents/" + strconv.Itoa(index)
		identity := content.Entry.Identity
		if identity.Mode == "" {
			id, inserted, err := tx.document(ctx, "workflow_version", "workflow_definition_id", id, content, nil, path)
			if err != nil {
				return changes, err
			}
			parents[identity.Version] = struct {
				id      int64
				content declaration.Content
			}{id, content}
			if inserted {
				changes.versions++
			}
		} else {
			parent, exists := parents[identity.Version]
			if !exists {
				return changes, publicationError(ErrStored, "missing_parent", path)
			}
			key := [2]string{identity.Version, identity.Mode}
			modeID, exists := modes[key]
			if !exists {
				row, inserted, err := tx.identity(ctx, "workflow_mode", "id", []string{"workflow_version_id", "code"}, []any{parent.id, identity.Mode}, nil, nil)
				if err != nil {
					return changes, err
				}
				modeID, err = rowID(row, 0)
				if err != nil {
					return changes, err
				}
				modes[key] = modeID
				if inserted {
					changes.modes++
				}
			}
			_, inserted, err := tx.document(ctx, "workflow_mode_version", "workflow_mode_id", modeID, content, &parent.content, path)
			if err != nil {
				return changes, err
			}
			if inserted {
				changes.revisions++
			}
		}
	}
	resources := slices.Clone(input.resources)
	if created {
		resources = append(resources, input.initialResources...)
	}
	slices.SortFunc(resources, func(left, right publicationResource) int {
		if value := strings.Compare(left.scope, right.scope); value != 0 {
			return value
		}
		return strings.Compare(left.key, right.key)
	})
	locale, err := tx.row(ctx, "i18n_locale", "id,enabled", []string{"code"}, []any{"en"}, tx.schemaReadLock())
	if err != nil {
		return changes, err
	}
	englishID, err := rowID(locale, 0)
	if err != nil || len(locale) != 2 {
		return changes, publicationError(ErrStored, "english_locale", "", err)
	}
	active, err := enabled(locale[1])
	if err != nil || !active {
		return changes, publicationError(ErrStored, "english_locale", "", err)
	}
	for index, resource := range resources {
		newResource, newText, err := tx.resource(ctx, englishID, resource, "/resources/"+strconv.Itoa(index))
		if err != nil {
			return changes, err
		}
		if newResource {
			changes.resources++
		}
		if newText {
			changes.texts++
		}
	}
	if created {
		for _, tag := range input.tags {
			count, err := tx.exec(ctx, "INSERT INTO "+tx.table("workflow_definition_tag")+" (workflow_definition_id,tag_key,tag_value) VALUES ("+tx.parameters(3)+")", id, tag.key, tag.value)
			if err != nil {
				return changes, err
			}
			if count != 1 {
				return changes, publicationError(ErrStored, "tag_count", "")
			}
			changes.tags++
		}
	}
	return changes, nil
}
func (tx *publicationTx) lastParameter(index int) string {
	if tx.pg != nil {
		return "$" + strconv.Itoa(index)
	}
	return "?"
}

// Charge the all-new path before the first write: each identity may need read,
// insert and read-back; each resource also needs its English text.
func publicationCalls(input *registration) int {
	count := 4 + 3*len(input.contents) + 3*input.info.Modes + 6*input.info.Resources + len(input.tags)
	if input.category != nil {
		count += 4
	}
	return count
}
func (tx *publicationTx) category(ctx context.Context, namespace string, category *initialCategory) (int64, bool, error) {
	var row [][]byte
	var created bool
	var err error
	if category.declare {
		row, created, err = tx.identity(ctx, "workflow_category", "id,enabled", []string{"namespace", "code"}, []any{namespace, category.code}, nil, nil)
	} else {
		row, err = tx.row(ctx, "workflow_category", "id,enabled", []string{"namespace", "code"}, []any{namespace, category.code}, "FOR UPDATE")
	}
	if err != nil {
		return 0, false, err
	}
	if row == nil {
		return 0, false, publicationError(ErrCategory, "missing_category", "/category")
	}
	if len(row) != 2 {
		return 0, false, publicationError(ErrStored, "category_row", "/category")
	}
	id, err := rowID(row, 0)
	if err != nil {
		return 0, false, err
	}
	active, err := enabled(row[1])
	if err != nil {
		return 0, false, err
	}
	if !active {
		return 0, false, publicationError(ErrCategory, "disabled_category", "/category")
	}
	return id, created, nil
}
func (tx *publicationTx) document(ctx context.Context, table, parentKey string, parentID int64, content declaration.Content, parent *declaration.Content, path string) (int64, bool, error) {
	prefix, format, label, maximum := "spec", declaration.SpecFormat, content.Entry.Identity.Version, declaration.MaxSpecBytes
	if parent != nil {
		prefix, format, label, maximum = "config", declaration.ModeFormat, content.Entry.Identity.Revision, declaration.MaxModeBytes
	}
	digest, err := hex.DecodeString(content.Entry.SHA256)
	if err != nil || len(digest) != 32 {
		return 0, false, publicationError(ErrInput, "digest", path, err)
	}
	fields := "id," + prefix + "_format,fingerprint_profile," + prefix + "_sha256," + prefix + "_document"
	row, created, err := tx.identity(ctx, table, fields, []string{parentKey, "version_code"}, []any{parentID, label},
		[]string{prefix + "_format", "fingerprint_profile", prefix + "_sha256", prefix + "_document"}, []any{format, content.Entry.Profile, digest, content.Bytes})
	if err != nil {
		return 0, false, err
	}
	if len(row) != 5 {
		return 0, false, publicationError(ErrStored, "document_row", path)
	}
	id, err := rowID(row, 0)
	if err != nil {
		return 0, false, err
	}
	if string(row[1]) != format {
		return 0, false, publicationError(ErrStored, "document_format", path)
	}
	storedHash, err := tx.binary(row[3], 32)
	if err != nil || len(storedHash) != 32 {
		return 0, false, publicationError(ErrStored, "document_digest", path, err)
	}
	stored, err := tx.binary(row[4], maximum)
	if err != nil {
		return 0, false, err
	}
	if created {
		if string(row[2]) != content.Entry.Profile || !bytes.Equal(storedHash, digest) || !bytes.Equal(stored, content.Bytes) {
			return 0, false, publicationError(ErrStored, "inserted_content_changed", path)
		}
		return id, true, nil
	}
	contents := []declaration.Content{{Entry: declaration.Entry{Identity: content.Entry.Identity, Profile: string(row[2]), SHA256: hex.EncodeToString(storedHash)}, Bytes: stored}}
	if parent != nil {
		contents = append(contents, *parent)
	}
	if _, err := declaration.ParseContents(ctx, contents); err != nil {
		return 0, false, publicationError(ErrStored, "document_integrity", path, err)
	}
	if !bytes.Equal(stored, content.Bytes) || string(row[2]) != content.Entry.Profile || !bytes.Equal(storedHash, digest) {
		return 0, false, publicationError(ErrConflict, "immutable_content", path)
	}
	return id, false, nil
}
func (tx *publicationTx) resource(ctx context.Context, localeID int64, input publicationResource, path string) (bool, bool, error) {
	var note any
	if input.text.Context != "" {
		note = input.text.Context
	}
	row, created, err := tx.identity(ctx, "i18n_resource", "id,translator_note", []string{"scope", "code"}, []any{input.scope, input.key}, []string{"translator_note"}, []any{note})
	if err != nil {
		return false, false, err
	}
	if len(row) != 2 {
		return false, false, publicationError(ErrStored, "resource_row", path)
	}
	id, err := rowID(row, 0)
	if err != nil {
		return false, false, err
	}
	if string(row[1]) != input.text.Context {
		return false, false, publicationError(ErrConflict, "translator_context", path)
	}
	text, err := tx.row(ctx, "i18n_text", "id,value", []string{"resource_id", "locale_id"}, []any{id, localeID}, "FOR UPDATE")
	if err != nil {
		return false, false, err
	}
	if text == nil {
		if !created {
			return false, false, publicationError(ErrStored, "missing_english", path)
		}
		_, err := tx.insert(ctx, "i18n_text", []string{"resource_id", "locale_id", "value"}, []string{"resource_id", "locale_id"}, []any{id, localeID, input.text.English})
		if err != nil {
			return false, false, err
		}
		text, err = tx.row(ctx, "i18n_text", "id,value", []string{"resource_id", "locale_id"}, []any{id, localeID}, "FOR UPDATE")
		if err != nil {
			return false, false, err
		}
	}
	if len(text) != 2 || text[1] == nil {
		return false, false, publicationError(ErrStored, "english_row", path)
	}
	if _, err := rowID(text, 0); err != nil {
		return false, false, err
	}
	if string(text[1]) != input.text.English {
		return false, false, publicationError(ErrConflict, "english_text", path)
	}
	return created, created, nil
}
