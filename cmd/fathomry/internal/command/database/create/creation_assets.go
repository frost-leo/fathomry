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

package create

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"io/fs"
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"

	databasecmd "github.com/frost-leo/fathomry/cmd/fathomry/internal/command/database"
	mysqlassets "github.com/frost-leo/fathomry/schema/fathomry/mysql/v1"
	pgassets "github.com/frost-leo/fathomry/schema/fathomry/postgresql/v1"
)

type creationAsset struct {
	ID     string `json:"id"`
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
}
type creationManifest struct {
	Format     string          `json:"format"`
	Family     string          `json:"family"`
	Dialect    string          `json:"dialect"`
	Profile    string          `json:"profile"`
	Statements []creationAsset `json:"statements"`
	License    string          `json:"license"`
	Notice     []string        `json:"license_notice"`
}
type statements struct{ database, schema string }

func loadCreation(dialect, database, schema, owner string) (statements, error) {
	var source fs.FS
	profile := "mysql-8.4"
	count := 1
	if dialect == "postgresql" {
		source = pgassets.Assets()
		profile = "postgresql-18"
		count = 2
	} else if dialect == "mysql" {
		source = mysqlassets.Assets()
	} else {
		return statements{}, fail(databasecmd.ErrInput)
	}
	raw, err := readCreation(source, "provision/manifest.json", 16<<10)
	if err != nil {
		return statements{}, fail(databasecmd.ErrAssets, err)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var value creationManifest
	if err := decoder.Decode(&value); err != nil {
		return statements{}, fail(databasecmd.ErrAssets, err)
	}
	if decoder.Decode(new(any)) != io.EOF || value.Format != "fathomry.database-provision/v1" || value.Family != "fathomry.control" || value.Dialect != dialect || value.Profile != profile || len(value.Statements) != count {
		return statements{}, fail(databasecmd.ErrAssets)
	}
	result := statements{}
	for index, item := range value.Statements {
		name := "database"
		if index == 1 {
			name = "schema"
		}
		if item.ID != name+".create" || item.Path != "provision/"+name+".sql" {
			return statements{}, fail(databasecmd.ErrAssets)
		}
		raw, err := readCreation(source, item.Path, 4096)
		if err != nil {
			return statements{}, fail(databasecmd.ErrAssets, err)
		}
		digest := sha256.Sum256(raw)
		if hex.EncodeToString(digest[:]) != item.SHA256 {
			return statements{}, fail(databasecmd.ErrAssets)
		}
		sql := strings.NewReplacer("{{database}}", database, "{{schema}}", schema, "{{owner}}", owner).Replace(string(raw))
		if len(sql) > 4096 {
			return statements{}, fail(databasecmd.ErrLimit)
		}
		if name == "database" {
			result.database = sql
		} else {
			result.schema = sql
		}
	}
	return result, nil
}
func readCreation(source fs.FS, path string, limit int) ([]byte, error) {
	file, err := source.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	raw, err := io.ReadAll(io.LimitReader(file, int64(limit+1)))
	if err != nil {
		return nil, err
	}
	if len(raw) > limit {
		return nil, fail(databasecmd.ErrLimit)
	}
	return raw, nil
}
func quotePG(value string) (string, error) {
	if value == "" || len(value) > 63 || !utf8.ValidString(value) || strings.ContainsFunc(value, unicode.IsControl) {
		return "", fail(databasecmd.ErrInput)
	}
	return "\"" + strings.ReplaceAll(value, "\"", "\"\"") + "\"", nil
}
func quoteDatabase(dialect, name string) (string, error) {
	if dialect == "postgresql" {
		if slices.Contains([]string{"postgres", "template0", "template1"}, name) {
			return "", fail(databasecmd.ErrInput)
		}
		return quotePG(name)
	}
	if dialect != "mysql" || name == "" || !utf8.ValidString(name) || utf8.RuneCountInString(name) > 64 ||
		strings.ContainsFunc(name, func(value rune) bool { return value > 0xffff || unicode.IsControl(value) }) ||
		strings.ContainsAny(name, "./\\") || strings.HasSuffix(name, " ") || slices.Contains([]string{"mysql", "sys", "information_schema", "performance_schema"}, strings.ToLower(name)) {
		return "", fail(databasecmd.ErrInput)
	}
	return "`" + strings.ReplaceAll(name, "`", "``") + "`", nil
}
