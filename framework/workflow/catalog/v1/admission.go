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
	"encoding/binary"
	"encoding/hex"
	json "encoding/json/v2"
	"io"
	"io/fs"
	"slices"
	"strconv"
	"strings"

	mysqlassets "github.com/frost-leo/fathomry/schema/fathomry/mysql/v1"
	pgassets "github.com/frost-leo/fathomry/schema/fathomry/postgresql/v1"
)

const schemaFamily = "fathomry.control"
const schemaHistory = "fathomry_schema_history"
const maxSchemaObjects = 512
const maxSchemaBytes = 1 << 20

type schemaStep struct {
	ID     string `json:"id"`
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
}
type schemaObject struct {
	Kind       string   `json:"kind"`
	Owner      string   `json:"owner"`
	Name       string   `json:"name"`
	Attributes []string `json:"attributes"`
}
type schemaContract struct {
	digest  []byte
	steps   []schemaStep
	objects []schemaObject
	tables  []string
}

func schemaError(reason string, causes ...error) error {
	return publicationError(ErrSchema, reason, "", causes...)
}
func schemaAsset(assets fs.FS, name string, maximum int) ([]byte, error) {
	if !fs.ValidPath(name) {
		return nil, schemaError("assets")
	}
	file, err := assets.Open(name)
	if err != nil {
		return nil, schemaError("assets", err)
	}
	raw, read := io.ReadAll(io.LimitReader(file, int64(maximum)+1))
	close := file.Close()
	if read != nil || close != nil || len(raw) > maximum {
		return nil, schemaError("assets", read, close)
	}
	return raw, nil
}
func schemaDigest(raw []byte) string { hash := sha256.Sum256(raw); return hex.EncodeToString(hash[:]) }

// loadSchema reads only embedded inspection facts. It owns no installer or DDL.
func loadSchema(assets fs.FS, dialect string) (*schemaContract, error) {
	raw, err := schemaAsset(assets, "manifest.json", 64<<10)
	if err != nil {
		return nil, err
	}
	var manifest struct {
		Format    string     `json:"format"`
		Family    string     `json:"family"`
		Dialect   string     `json:"dialect"`
		Profile   string     `json:"profile"`
		Bootstrap schemaStep `json:"bootstrap"`
		Structure struct {
			Path   string `json:"path"`
			SHA256 string `json:"sha256"`
		} `json:"structure"`
		Migrations []struct {
			Revision    int          `json:"revision"`
			Name        string       `json:"name"`
			Transaction string       `json:"transaction"`
			Steps       []schemaStep `json:"steps"`
		} `json:"migrations"`
	}
	if err := json.Unmarshal(raw, &manifest); err != nil {
		return nil, schemaError("assets", err)
	}
	profile, transaction := "postgresql-18-utf8", "required"
	if dialect == "mysql" {
		profile, transaction = "mysql-8.4-innodb-16k-dynamic-strict", "implicit-commit"
	}
	if manifest.Format != "fathomry.schema-manifest/v1" || manifest.Family != schemaFamily || manifest.Dialect != dialect || manifest.Profile != profile ||
		len(manifest.Migrations) != 1 || manifest.Migrations[0].Revision != 1 || manifest.Migrations[0].Name != "workflow_catalog" ||
		manifest.Migrations[0].Transaction != transaction || len(manifest.Migrations[0].Steps) == 0 || len(manifest.Migrations[0].Steps) > 48 ||
		manifest.Bootstrap.ID != "bootstrap.history" || manifest.Structure.Path != "0001/structure.json" {
		return nil, schemaError("assets")
	}
	digest := sha256.Sum256(raw)
	steps := append([]schemaStep{manifest.Bootstrap}, manifest.Migrations[0].Steps...)
	var sources strings.Builder
	seen := map[string]bool{}
	budget := 0
	for _, step := range steps {
		if step.ID == "" || seen[step.ID] || seen[step.Path] {
			return nil, schemaError("assets")
		}
		seen[step.ID], seen[step.Path] = true, true
		data, err := schemaAsset(assets, step.Path, 64<<10)
		if err != nil {
			return nil, err
		}
		budget += len(data)
		if budget > maxSchemaBytes || schemaDigest(data) != step.SHA256 {
			return nil, schemaError("assets")
		}
		sources.WriteString(step.ID + "\x00" + step.Path + "\x00" + step.SHA256 + "\n")
	}
	raw, err = schemaAsset(assets, manifest.Structure.Path, maxSchemaBytes)
	if err != nil {
		return nil, err
	}
	var structure struct {
		Format        string         `json:"format"`
		SourcesSHA256 string         `json:"sources_sha256"`
		Objects       []schemaObject `json:"objects"`
	}
	if schemaDigest(raw) != manifest.Structure.SHA256 {
		return nil, schemaError("assets")
	}
	if err := json.Unmarshal(raw, &structure); err != nil {
		return nil, schemaError("assets", err)
	}
	if structure.Format != "fathomry.schema-structure/v1" || structure.SourcesSHA256 != schemaDigest([]byte(sources.String())) ||
		len(structure.Objects) == 0 || len(structure.Objects) > maxSchemaObjects {
		return nil, schemaError("assets")
	}
	result := &schemaContract{digest: digest[:], steps: steps, objects: structure.Objects}
	result.steps[0].ID = manifest.Migrations[0].Name
	seen = map[string]bool{}
	for _, entry := range result.objects {
		key := schemaObjectKey(entry)
		if seen[key] || entry.Kind == "" || entry.Owner == "" || entry.Name == "" || len(entry.Attributes) > 16 {
			return nil, schemaError("assets")
		}
		seen[key] = true
		if entry.Kind == "relation" && entry.Owner == entry.Name && len(entry.Attributes) > 0 && (entry.Attributes[0] == "r" || entry.Attributes[0] == "BASE TABLE") {
			result.tables = append(result.tables, entry.Name)
		}
	}
	if len(result.tables) != 10 || !slices.Contains(result.tables, schemaHistory) {
		return nil, schemaError("assets")
	}
	slices.Sort(result.tables)
	sortSchemaObjects(result.objects)
	return result, nil
}
func schemaObjectKey(value schemaObject) string {
	return value.Kind + "\x00" + value.Owner + "\x00" + value.Name
}
func sortSchemaObjects(values []schemaObject) {
	slices.SortFunc(values, func(left, right schemaObject) int {
		return strings.Compare(schemaObjectKey(left), schemaObjectKey(right))
	})
}
func sameSchemaObjects(left, right []schemaObject) bool {
	return slices.EqualFunc(left, right, func(a, b schemaObject) bool {
		return schemaObjectKey(a) == schemaObjectKey(b) && slices.Equal(a.Attributes, b.Attributes)
	})
}

// schema admits only the exact retained Resource transaction. PostgreSQL's shared
// transaction lock coordinates the installer. MySQL takes no session named lock:
// transaction-owned metadata locks pin tables, and shared history-row locks pin
// completed history until native finalization. Neither path mutates schema data.
func (tx *publicationTx) schema(ctx context.Context) error {
	dialect := "mysql"
	var assets fs.FS = mysqlassets.Assets()
	if tx.pg != nil {
		dialect, assets = "postgresql", pgassets.Assets()
	}
	contract, err := loadSchema(assets, dialect)
	if err != nil {
		return err
	}
	var name string
	var generation uint64
	if tx.pg != nil {
		snapshot, finished := tx.pg.Receipt().Snapshot()
		if finished || snapshot.Info().Released {
			return schemaError("transaction_finished")
		}
		name, generation = snapshot.Info().Source.Name, snapshot.Info().Source.Generation
	} else {
		snapshot, finished := tx.my.Receipt().Snapshot()
		if finished || snapshot.Info().Released {
			return schemaError("transaction_finished")
		}
		name, generation = snapshot.Info().Source.Name, snapshot.Info().Source.Generation
	}
	if name == "" || generation == 0 {
		return schemaError("source")
	}
	physical, noBackslash, err := tx.schemaProfile(ctx)
	if err != nil {
		return err
	}
	lock := sha256.Sum256([]byte(schemaFamily + "\x00" + physical + "\x00" + tx.name))
	if tx.pg != nil {
		value, err := tx.schemaScalar(ctx, "SELECT pg_catalog.pg_try_advisory_xact_lock_shared($1)", int64(binary.BigEndian.Uint64(lock[:8])))
		if err != nil {
			return err
		}
		if value != "t" {
			return schemaError("changing")
		}
	} else if err := tx.schemaInstallerIdle(ctx, lock[:]); err != nil {
		return err
	}
	for _, table := range contract.tables {
		if tx.pg != nil {
			mode := "ROW EXCLUSIVE"
			if tx.readOnly || table == schemaHistory || table == "i18n_locale" {
				mode = "ACCESS SHARE"
			}
			if _, err := tx.exec(ctx, "LOCK TABLE "+tx.table(table)+" IN "+mode+" MODE NOWAIT"); err != nil {
				return schemaError("unavailable_or_changing", err)
			}
		} else if _, err := tx.query(ctx, "SELECT 1 FROM "+tx.table(table)+" LIMIT 0"); err != nil {
			return schemaError("unavailable_or_changing", err)
		}
	}
	if tx.my != nil {
		if err := tx.schemaInstallerIdle(ctx, lock[:]); err != nil {
			return err
		}
	}
	objects, err := tx.schemaObjects(ctx, noBackslash)
	if err != nil {
		return err
	}
	if !sameSchemaObjects(objects, contract.objects) {
		return schemaError("structure_unproven")
	}
	rows, err := tx.queryRows(ctx, 50, "SELECT family,migration_revision,step_ordinal,manifest_sha256,step_id,step_sha256,state,CASE WHEN acknowledged_at IS NULL THEN 0 ELSE 1 END FROM "+tx.table(schemaHistory)+" ORDER BY migration_revision,step_ordinal LIMIT 50 "+tx.schemaReadLock())
	if err != nil {
		return schemaError("history_unavailable", err)
	}
	if err := tx.schemaHistory(rows, contract); err != nil {
		return err
	}
	locale, err := tx.row(ctx, "i18n_locale", "id,enabled", []string{"code"}, []any{"en"}, tx.schemaReadLock())
	if err != nil {
		return schemaError("english", err)
	}
	if len(locale) != 2 {
		return schemaError("english")
	}
	if _, err := rowID(locale, 0); err != nil {
		return schemaError("english", err)
	}
	active, err := enabled(locale[1])
	if err != nil || !active {
		return schemaError("english", err)
	}
	after, err := tx.schemaObjects(ctx, noBackslash)
	if err != nil {
		return err
	}
	if !sameSchemaObjects(objects, after) {
		return schemaError("changing")
	}
	return nil
}

// PostgreSQL's shared installer lock protects history, and the verified guards
// protect English identity/eligibility. Avoid FOR SHARE there: it would require
// UPDATE privilege on tables the publisher must not mutate. MySQL's shared reads
// need only SELECT and retain their own row locks.
func (tx *publicationTx) schemaReadLock() string {
	if tx.my != nil && !tx.readOnly {
		return "FOR SHARE"
	}
	return ""
}

func (tx *publicationTx) schemaInstallerIdle(ctx context.Context, lock []byte) error {
	value, err := tx.schemaScalar(ctx, "SELECT IS_FREE_LOCK(?)", hex.EncodeToString(lock))
	if err != nil {
		return err
	}
	if value != "1" {
		return schemaError("changing")
	}
	return nil
}
func (tx *publicationTx) schemaScalar(ctx context.Context, sql string, args ...any) (string, error) {
	rows, err := tx.query(ctx, sql, args...)
	if err != nil {
		return "", schemaError("query", err)
	}
	if len(rows) != 1 || len(rows[0]) != 1 || rows[0][0] == nil {
		return "", schemaError("query_shape")
	}
	return string(rows[0][0]), nil
}
func (tx *publicationTx) schemaProfile(ctx context.Context) (string, bool, error) {
	if tx.pg != nil {
		rows, err := tx.query(ctx, "SELECT current_setting('server_version_num'),current_database(),current_setting('server_encoding'),pg_is_in_recovery()::text,current_setting('session_replication_role'),current_setting('transaction_isolation'),current_setting('transaction_read_only')")
		if err != nil {
			return "", false, schemaError("profile", err)
		}
		if len(rows) != 1 || len(rows[0]) != 7 {
			return "", false, schemaError("profile")
		}
		row := rows[0]
		access := "off"
		if tx.readOnly {
			access = "on"
		}
		version, err := strconv.Atoi(string(row[0]))
		if err != nil || version < 180000 || version >= 190000 || string(row[2]) != "UTF8" || string(row[3]) != "false" || string(row[4]) != "origin" || string(row[5]) != "read committed" || string(row[6]) != access {
			return "", false, schemaError("profile")
		}
		for _, statement := range []string{"SET LOCAL search_path TO pg_catalog", "SET LOCAL TIME ZONE 'UTC'", "SET LOCAL standard_conforming_strings TO on", "SET LOCAL bytea_output TO hex"} {
			if _, err := tx.exec(ctx, statement); err != nil {
				return "", false, schemaError("profile", err)
			}
		}
		return string(row[1]), true, nil
	}
	rows, err := tx.query(ctx, "SELECT VERSION(),DATABASE(),@@innodb_page_size,@@lower_case_table_names,@@autocommit,@@session.foreign_key_checks,@@session.unique_checks,@@read_only,@@super_read_only,@@time_zone,@@sql_mode,@@sql_quote_show_create,@@character_set_client,@@character_set_connection,@@character_set_results")
	if err != nil {
		return "", false, schemaError("profile", err)
	}
	if len(rows) != 1 || len(rows[0]) != 15 {
		return "", false, schemaError("profile")
	}
	row := rows[0]
	mode := strings.Split(string(row[10]), ",")
	if !strings.HasPrefix(string(row[0]), "8.4.") || string(row[1]) != tx.name || string(row[2]) != "16384" || string(row[3]) != "0" ||
		string(row[4]) != "1" || string(row[5]) != "1" || string(row[6]) != "1" || string(row[7]) != "0" || string(row[8]) != "0" ||
		string(row[9]) != "+00:00" || string(row[11]) != "1" || string(row[12]) != "utf8mb4" || string(row[13]) != "utf8mb4" || string(row[14]) != "utf8mb4" ||
		!slices.Contains(mode, "STRICT_ALL_TABLES") && !slices.Contains(mode, "STRICT_TRANS_TABLES") || slices.Contains(mode, "ANSI_QUOTES") {
		return "", false, schemaError("profile")
	}
	return string(row[1]), slices.Contains(mode, "NO_BACKSLASH_ESCAPES"), nil
}
func (tx *publicationTx) schemaHistory(rows [][][]byte, contract *schemaContract) error {
	if len(rows) != len(contract.steps) {
		return schemaError("incomplete")
	}
	for index, row := range rows {
		if len(row) != 8 {
			return schemaError("history_shape")
		}
		if string(row[0]) != schemaFamily || string(row[1]) != "1" {
			return schemaError("incompatible")
		}
		if string(row[2]) != strconv.Itoa(index) {
			return schemaError("incomplete")
		}
		manifest, err := tx.binary(row[3], 32)
		if err != nil || !bytes.Equal(manifest, contract.digest) {
			return schemaError("history_drift", err)
		}
		step, err := tx.binary(row[5], 32)
		if err != nil || hex.EncodeToString(step) != contract.steps[index].SHA256 || string(row[4]) != contract.steps[index].ID {
			return schemaError("history_drift", err)
		}
		state := "applied"
		if index == 0 {
			state = "complete"
		}
		if string(row[6]) != state || string(row[7]) != "1" {
			return schemaError("incomplete")
		}
	}
	return nil
}
func (tx *publicationTx) schemaObjects(ctx context.Context, noBackslash bool) ([]schemaObject, error) {
	result := []schemaObject{}
	total := 0
	appendObject := func(value schemaObject) error {
		size := len(value.Kind) + len(value.Owner) + len(value.Name)
		for _, attribute := range value.Attributes {
			size += len(attribute)
		}
		if len(result) >= maxSchemaObjects || size > maxSchemaBytes-total {
			return schemaError("inspection_limit")
		}
		total += size
		result = append(result, value)
		return nil
	}
	add := func(kind, sql string) error {
		rows, err := tx.queryRows(ctx, maxSchemaObjects, sql, tx.name)
		if err != nil {
			return schemaError("inspection", err)
		}
		for _, row := range rows {
			if len(row) < 2 || len(row) > 18 || row[0] == nil || row[1] == nil {
				return schemaError("inspection_shape")
			}
			value := schemaObject{Kind: kind, Owner: string(row[0]), Name: string(row[1]), Attributes: []string{}}
			for _, cell := range row[2:] {
				value.Attributes = append(value.Attributes, string(cell))
			}
			if tx.pg != nil && slices.Contains([]string{"constraint", "index", "trigger", "function"}, kind) {
				if len(value.Attributes) == 0 {
					return schemaError("inspection_shape")
				}
				last := len(value.Attributes) - 1
				value.Attributes[last] = pgassets.NormalizeInspection(value.Attributes[last], tx.name)
			}
			if err := appendObject(value); err != nil {
				return err
			}
		}
		return nil
	}
	if tx.pg != nil {
		queries, err := pgassets.InspectionQueries()
		if err != nil {
			return nil, schemaError("assets", err)
		}
		for _, query := range queries {
			if err := add(query.Kind, query.SQL); err != nil {
				return nil, err
			}
		}
	} else {
		queries, err := mysqlassets.InspectionQueries()
		if err != nil {
			return nil, schemaError("assets", err)
		}
		for _, query := range queries {
			if err := add(query.Kind, query.SQL); err != nil {
				return nil, err
			}
		}
		for _, entry := range slices.Clone(result) {
			if entry.Kind != "relation" || len(entry.Attributes) != 1 || entry.Attributes[0] != "BASE TABLE" {
				continue
			}
			rows, err := tx.query(ctx, "SHOW CREATE TABLE "+tx.target+".`"+strings.ReplaceAll(entry.Name, "`", "``")+"`")
			if err != nil {
				return nil, schemaError("inspection", err)
			}
			if len(rows) != 1 || len(rows[0]) != 2 || rows[0][1] == nil {
				return nil, schemaError("inspection_shape")
			}
			sql := string(rows[0][1])
			if !noBackslash && strings.Contains(sql, "\\") {
				return nil, schemaError("inspection_encoding")
			}
			if err := appendObject(schemaObject{Kind: "table", Owner: entry.Name, Name: entry.Name, Attributes: []string{mysqlassets.NormalizeInspection(sql)}}); err != nil {
				return nil, err
			}
		}
	}
	sortSchemaObjects(result)
	return result, nil
}
