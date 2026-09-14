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

package trino

import (
	"context"
	"math"
	"reflect"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	native "github.com/trinodb/trino-go-client/trino"
)

// Statement is one SQL statement with positional values. Inputs are borrowed
// until the synchronous call returns and must not be mutated concurrently.
// Named SDK options, credential arguments, driver.Valuer and arbitrary pointers
// are not accepted. Use SQL CAST with string parameters for exact decimal/date/
// time/nested types not representable by the admitted Go values.
type Statement struct {
	private
	SQL  string
	Args []any
}

// BatchInsert builds one multi-row VALUES statement in the selected catalog and
// schema. Empty/ragged batches are rejected. It never splits, commits, or retries
// chunks. Table/column identifiers are quoted, not interpreted as SQL fragments.
type BatchInsert struct {
	private
	Table   string
	Columns []string
	Rows    [][]any
}

// Query collects a finite query's bounded direct rows and metadata. It rejects
// mutation families. No live rows or callback retain resources after return.
func (c *Client) Query(ctx, cleanup context.Context, id fault.Correlation, statement Statement) (*invocation.Receipt[Result], error) {
	return c.start(ctx, cleanup, id, "query", true, func(settings) (Statement, error) { return statement, nil })
}

// Execute drains every native page for ordinary DDL/CTAS/DML and selected
// maintenance, retaining aggregate/absent counts and terminal evidence. SQL
// authorization and connector semantics belong to the selected deployment.
func (c *Client) Execute(ctx, cleanup context.Context, id fault.Correlation, statement Statement) (*invocation.Receipt[Result], error) {
	return c.start(ctx, cleanup, id, "execute", false, func(settings) (Statement, error) { return statement, nil })
}

// Insert is a bounded convenience for one physical batch, not a bulk-upload API.
func (c *Client) Insert(ctx, cleanup context.Context, id fault.Correlation, batch BatchInsert) (*invocation.Receipt[Result], error) {
	return c.start(ctx, cleanup, id, "insert", false, func(s settings) (Statement, error) {
		if !identifier(s.Catalog) || !identifier(s.Schema) || !identifier(batch.Table) ||
			len(batch.Columns) < 1 || len(batch.Columns) > s.MaxColumns || len(batch.Rows) < 1 ||
			len(batch.Rows) > s.MaxRows || len(batch.Rows) > s.MaxParameters/len(batch.Columns) {
			return Statement{}, failure(ErrInput, "batch")
		}
		sqlBytes := len("INSERT INTO ") + len(s.Catalog) + len(s.Schema) + len(batch.Table) + 8 + len(" (") + len(") VALUES ")
		for _, name := range batch.Columns {
			if !identifier(name) {
				return Statement{}, failure(ErrInput, "columns")
			}
			sqlBytes += len(name) + 3
		}
		sqlBytes--
		sqlBytes += len(batch.Rows)*(2*len(batch.Columns)+2) - 1
		if sqlBytes > s.MaxSQLBytes {
			return Statement{}, failure(ErrLimit, "sql")
		}
		var sql strings.Builder
		sql.Grow(sqlBytes)
		sql.WriteString("INSERT INTO " + quote(s.Catalog) + "." + quote(s.Schema) + "." + quote(batch.Table) + " (")
		seen := make(map[string]bool, len(batch.Columns))
		for i, name := range batch.Columns {
			if !identifier(name) || seen[strings.ToLower(name)] {
				return Statement{}, failure(ErrInput, "columns")
			}
			seen[strings.ToLower(name)] = true
			if i != 0 {
				sql.WriteByte(',')
			}
			sql.WriteString(quote(name))
		}
		sql.WriteString(") VALUES ")
		args := make([]any, 0, len(batch.Rows)*len(batch.Columns))
		for i, row := range batch.Rows {
			if len(row) != len(batch.Columns) {
				return Statement{}, failure(ErrInput, "batch-arity")
			}
			if i != 0 {
				sql.WriteByte(',')
			}
			sql.WriteByte('(')
			for j, value := range row {
				if j != 0 {
					sql.WriteByte(',')
				}
				sql.WriteByte('?')
				args = append(args, value)
			}
			sql.WriteByte(')')
			if sql.Len() > s.MaxSQLBytes {
				return Statement{}, failure(ErrLimit, "sql")
			}
		}
		return Statement{SQL: sql.String(), Args: args}, nil
	})
}
func quote(name string) string { return `"` + name + `"` }

var decimalNumber = regexp.MustCompile(`^-?[0-9]+(\.[0-9]+)?([eE][+-]?[0-9]+)?$`)

func argumentSize(value any, depth int, remaining *int64) error {
	if depth > 8 || *remaining < 0 {
		return failure(ErrLimit, "arguments")
	}
	var size int64
	switch v := value.(type) {
	case nil, bool, int, int8, int16, int32, int64, uint16, uint32:
		size = 32
	case uint:
		if uint64(v) > math.MaxInt64 {
			return failure(ErrInput, "integer")
		}
		size = 32
	case uint64:
		if v > math.MaxInt64 {
			return failure(ErrInput, "integer")
		}
		size = 32
	case string:
		if !utf8.ValidString(v) || strings.ContainsRune(v, 0) {
			return failure(ErrInput, "string")
		}
		size = 2*int64(len(v)) + 2
	case []byte:
		size = 2*int64(len(v)) + 3
	case native.Numeric:
		if len(v) > 128 || !decimalNumber.MatchString(string(v)) {
			return failure(ErrInput, "numeric")
		}
		size = int64(len(v))
	case time.Time:
		_, offset := v.Zone()
		if v.Year() < 1 || v.Year() > 9999 || offset%60 != 0 || offset < -14*3600 || offset > 14*3600 {
			return failure(ErrInput, "timestamp")
		}
		size = 80
	default:
		ref := reflect.ValueOf(value)
		if !ref.IsValid() || ref.Kind() != reflect.Slice || ref.IsNil() || int64(ref.Len()) > *remaining {
			return failure(ErrUnsupported, "argument")
		}
		*remaining -= 8 + int64(ref.Len())*2
		for i := 0; i < ref.Len(); i++ {
			if err := argumentSize(ref.Index(i).Interface(), depth+1, remaining); err != nil {
				return err
			}
		}
		return nil
	}
	*remaining -= size
	if *remaining < 0 {
		return failure(ErrLimit, "arguments")
	}
	return nil
}

func prepare(statement Statement, s settings, queryOnly bool) (bool, error) {
	if len(statement.SQL) == 0 || len(statement.SQL) > s.MaxSQLBytes || len(statement.Args) > s.MaxParameters ||
		!utf8.ValidString(statement.SQL) || strings.ContainsRune(statement.SQL, 0) {
		return false, failure(ErrInput, "statement")
	}
	words, err := statementWords(statement.SQL)
	if err != nil {
		return false, err
	}
	verb := words[0]
	if verb == "WITH" {
		verb = ""
		for _, word := range words[1:] {
			switch word {
			case "SELECT", "INSERT", "UPDATE", "DELETE", "MERGE":
				verb = word
			}
			if verb != "" {
				break
			}
		}
	}
	read := false
	switch verb {
	case "SELECT", "VALUES", "TABLE", "SHOW", "DESCRIBE", "DESC":
		read = true
	case "EXPLAIN":
		read = true
		for _, word := range words[1:] {
			if word == "ANALYZE" {
				return false, failure(ErrUnsupported, "explain-analyze")
			}
		}
	case "INSERT", "UPDATE", "DELETE", "MERGE", "TRUNCATE", "CREATE", "ALTER", "DROP", "COMMENT":
	case "CALL", "ANALYZE", "REFRESH":
		if !s.Maintenance {
			return false, failure(ErrAuthority, "maintenance")
		}
	default:
		return false, failure(ErrUnsupported, "statement-family")
	}
	if verb == "ALTER" {
		for _, word := range words[1:] {
			if word == "EXECUTE" && !s.Maintenance {
				return false, failure(ErrAuthority, "maintenance")
			}
		}
	}
	if !read && (queryOnly || !s.Writes) {
		return false, failure(ErrAuthority, "writes")
	}
	remaining := int64(s.MaxSQLBytes) - int64(len(statement.SQL))
	if len(statement.Args) > 0 {
		remaining -= int64(len(statement.SQL)) + 32 + int64(len(statement.Args))*2
	}
	for _, value := range statement.Args {
		if err := argumentSize(value, 0, &remaining); err != nil {
			return false, err
		}
	}
	if remaining < 0 {
		return false, failure(ErrLimit, "sql")
	}
	return read, nil
}

// This is only a single-statement/family gate, not a SQL parser or privilege check.
func statementWords(sql string) ([]string, error) {
	var words []string
	previousWord := ""
	depth := 0
	ended := false
	for i := 0; i < len(sql); {
		char := sql[i]
		if strings.ContainsRune(" \t\r\n\f", rune(char)) {
			i++
			continue
		}
		if i+1 < len(sql) && sql[i:i+2] == "--" {
			i += 2
			for i < len(sql) && sql[i] != '\n' && sql[i] != '\r' {
				i++
			}
			continue
		}
		if i+1 < len(sql) && sql[i:i+2] == "/*" {
			i += 2
			end := strings.Index(sql[i:], "*/")
			if end < 0 {
				return nil, failure(ErrInput, "sql-comment")
			}
			i += end + 2
			continue
		}
		if ended {
			return nil, failure(ErrUnsupported, "multiple-statements")
		}
		if char == '\'' || char == '"' {
			delim := char
			i++
			closed := false
			for i < len(sql) {
				if sql[i] != delim {
					i++
					continue
				}
				i++
				if i < len(sql) && sql[i] == delim {
					i++
					continue
				}
				closed = true
				break
			}
			if !closed {
				return nil, failure(ErrInput, "sql-quote")
			}
			continue
		}
		switch char {
		case '(':
			depth++
			if depth > 128 {
				return nil, failure(ErrLimit, "sql-depth")
			}
			i++
			continue
		case ')':
			depth--
			if depth < 0 {
				return nil, failure(ErrInput, "sql-depth")
			}
			i++
			continue
		case ';':
			if depth != 0 {
				return nil, failure(ErrUnsupported, "multiple-statements")
			}
			ended = true
			i++
			continue
		}
		if char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char == '_' {
			start := i
			for i < len(sql) && (sql[i] >= 'a' && sql[i] <= 'z' || sql[i] >= 'A' && sql[i] <= 'Z' ||
				sql[i] >= '0' && sql[i] <= '9' || sql[i] == '_') {
				i++
			}
			word := strings.ToUpper(sql[start:i])
			if previousWord == "WITH" && word == "SESSION" {
				return nil, failure(ErrUnsupported, "statement-session")
			}
			previousWord = word
			if depth == 0 {
				words = append(words, word)
			}
		} else {
			i++
		}
	}
	if len(words) == 0 || depth != 0 {
		return nil, failure(ErrInput, "sql")
	}
	return words, nil
}
