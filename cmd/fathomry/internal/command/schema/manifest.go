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

package schema

import (
	"crypto/sha256"
	"encoding/hex"
	json "encoding/json/v2"
	"io"
	"io/fs"
	"slices"
	"strings"

	databasecmd "github.com/frost-leo/fathomry/cmd/fathomry/internal/command/database"
	mysqlassets "github.com/frost-leo/fathomry/schema/fathomry/mysql/v1"
	pgassets "github.com/frost-leo/fathomry/schema/fathomry/postgresql/v1"
)

const family = "fathomry.control"
const historyTable = "fathomry_schema_history"
const maxObjects = 512
const maxStructureBytes = 1 << 20
const maxSQLCalls = 256
const maxStatements = 48

// WorkBytes covers fixed asset/state scratch. The caller additionally reserves
// two selected-source evidence envelopes for detached row conversion, including
// oversized native results rejected here, plus its final report ownership.
// This is a declared bound, not measured RSS or another native admission pool.
const WorkBytes = 8 << 20

type asset struct {
	ID     string `json:"id"`
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
}
type structureAsset struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
}
type migration struct {
	Revision    int     `json:"revision"`
	Name        string  `json:"name"`
	Transaction string  `json:"transaction"`
	Steps       []asset `json:"steps"`
}
type manifest struct {
	Format               string         `json:"format"`
	Family               string         `json:"family"`
	Dialect              string         `json:"dialect"`
	Profile              string         `json:"profile"`
	Bootstrap            asset          `json:"bootstrap"`
	Structure            structureAsset `json:"structure"`
	Migrations           []migration    `json:"migrations"`
	DocumentationLocales []string       `json:"documentation_locales"`
	License              string         `json:"license"`
	LicenseNotice        []string       `json:"license_notice"`
}
type object struct {
	Kind       string   `json:"kind"`
	Owner      string   `json:"owner"`
	Name       string   `json:"name"`
	Attributes []string `json:"attributes"`
}
type structure struct {
	Format        string   `json:"format"`
	SourcesSHA256 string   `json:"sources_sha256"`
	Objects       []object `json:"objects"`
	License       string   `json:"license"`
	LicenseNotice []string `json:"license_notice"`
}
type contract struct {
	manifest  manifest
	digest    []byte
	sql       []string
	bootstrap string
	expected  []object
}

func readAsset(assets fs.FS, path string, maximum int) ([]byte, error) {
	if !fs.ValidPath(path) {
		return nil, fail(databasecmd.ErrAssets)
	}
	file, err := assets.Open(path)
	if err != nil {
		return nil, fail(databasecmd.ErrAssets, err)
	}
	raw, readErr := io.ReadAll(io.LimitReader(file, int64(maximum)+1))
	closeErr := file.Close()
	if readErr != nil || closeErr != nil || len(raw) > maximum {
		return nil, fail(databasecmd.ErrAssets, readErr, closeErr)
	}
	return raw, nil
}
func digest(raw []byte) string { value := sha256.Sum256(raw); return hex.EncodeToString(value[:]) }
func sourcesDigest(value manifest) string {
	var text strings.Builder
	for _, entry := range append([]asset{value.Bootstrap}, value.Migrations[0].Steps...) {
		text.WriteString(entry.ID)
		text.WriteByte(0)
		text.WriteString(entry.Path)
		text.WriteByte(0)
		text.WriteString(entry.SHA256)
		text.WriteByte('\n')
	}
	return digest([]byte(text.String()))
}
func load(dialect string) (*contract, error) {
	var assets fs.FS
	switch dialect {
	case "postgresql":
		assets = pgassets.Assets()
	case "mysql":
		assets = mysqlassets.Assets()
	default:
		return nil, fail(databasecmd.ErrInput)
	}
	return loadAssets(assets, dialect)
}
func loadAssets(assets fs.FS, dialect string) (*contract, error) {
	raw, err := readAsset(assets, "manifest.json", 64<<10)
	if err != nil {
		return nil, err
	}
	var value manifest
	if err = json.Unmarshal(raw, &value, json.RejectUnknownMembers(true)); err != nil {
		return nil, fail(databasecmd.ErrAssets, err)
	}
	profile, transaction := "postgresql-18-utf8", "required"
	if dialect == "mysql" {
		profile, transaction = "mysql-8.4-innodb-16k-dynamic-strict", "implicit-commit"
	}
	if value.Format != "fathomry.schema-manifest/v1" || value.Family != family || value.Dialect != dialect || value.Profile != profile ||
		!slices.Equal(value.DocumentationLocales, []string{"en", "zh-CN"}) || len(value.Migrations) != 1 ||
		value.Migrations[0].Revision != 1 || value.Migrations[0].Name != "workflow_catalog" ||
		value.Migrations[0].Transaction != transaction || len(value.Migrations[0].Steps) == 0 || len(value.Migrations[0].Steps) > maxStatements ||
		value.Bootstrap.ID != "bootstrap.history" || value.Bootstrap.Path != "bootstrap/fathomry_schema_history.sql" ||
		value.Structure.Path != "0001/structure.json" {
		return nil, fail(databasecmd.ErrAssets)
	}
	hash := sha256.Sum256(raw)
	result := &contract{manifest: value, digest: hash[:]}
	seen := map[string]bool{}
	sqlBytes := 0
	for index, entry := range append([]asset{value.Bootstrap}, value.Migrations[0].Steps...) {
		if seen[entry.ID] || seen[entry.Path] || !assetID(entry.ID) || !fs.ValidPath(entry.Path) ||
			!strings.HasSuffix(entry.Path, ".sql") || index > 0 && !strings.HasPrefix(entry.Path, "0001/") {
			return nil, fail(databasecmd.ErrAssets)
		}
		seen[entry.ID], seen[entry.Path] = true, true
		sql, err := readAsset(assets, entry.Path, 64<<10)
		if err != nil {
			return nil, err
		}
		if len(sql) > (1<<20)-sqlBytes {
			return nil, fail(databasecmd.ErrAssets)
		}
		sqlBytes += len(sql)
		if digest(sql) != entry.SHA256 || !strings.Contains(string(sql), "{{target}}.") ||
			strings.Contains(strings.ReplaceAll(string(sql), "{{target}}", ""), "{{") {
			return nil, fail(databasecmd.ErrAssets)
		}
		if index == 0 {
			result.bootstrap = string(sql)
		} else {
			result.sql = append(result.sql, string(sql))
		}
	}
	raw, err = readAsset(assets, value.Structure.Path, maxStructureBytes)
	if err != nil {
		return nil, err
	}
	var expected structure
	if digest(raw) != value.Structure.SHA256 {
		return nil, fail(databasecmd.ErrAssets)
	}
	if err = json.Unmarshal(raw, &expected, json.RejectUnknownMembers(true)); err != nil {
		return nil, fail(databasecmd.ErrAssets, err)
	}
	if expected.Format != "fathomry.schema-structure/v1" || expected.SourcesSHA256 != sourcesDigest(value) ||
		len(expected.Objects) == 0 || len(expected.Objects) > maxObjects {
		return nil, fail(databasecmd.ErrAssets)
	}
	seen = map[string]bool{}
	for _, entry := range expected.Objects {
		key := objectKey(entry)
		if seen[key] || entry.Kind == "" || entry.Owner == "" || entry.Name == "" || len(entry.Attributes) > 16 {
			return nil, fail(databasecmd.ErrAssets)
		}
		seen[key] = true
	}
	result.expected = expected.Objects
	sortObjects(result.expected)
	return result, nil
}
func assetID(value string) bool {
	if len(value) == 0 || len(value) > 128 {
		return false
	}
	for _, c := range value {
		if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '.' || c == '_' || c == '-') {
			return false
		}
	}
	return true
}

func quoteTarget(dialect, target string) (string, error) {
	if err := databasecmd.ValidateTarget(dialect, target); err != nil {
		return "", fail(databasecmd.ErrInput, err)
	}
	switch dialect {
	case "postgresql":
		return "\"" + strings.ReplaceAll(target, "\"", "\"\"") + "\"", nil
	case "mysql":
		return "`" + strings.ReplaceAll(target, "`", "``") + "`", nil
	default:
		return "", fail(databasecmd.ErrInput)
	}
}
func objectKey(value object) string { return value.Kind + "\x00" + value.Owner + "\x00" + value.Name }
func sortObjects(values []object) {
	slices.SortFunc(values, func(a, b object) int { return strings.Compare(objectKey(a), objectKey(b)) })
}
func sameObjects(left, right []object) bool {
	return slices.EqualFunc(left, right, func(a, b object) bool {
		return objectKey(a) == objectKey(b) && slices.Equal(a.Attributes, b.Attributes)
	})
}
func ownedObjects(values []object, owner string) []object {
	result := []object{}
	for _, value := range values {
		if value.Owner == owner {
			result = append(result, value)
		}
	}
	return result
}
