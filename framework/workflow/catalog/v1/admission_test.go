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
	postgres "github.com/frost-leo/fathomry/adapters/database/postgres/v1"
	mysqlassets "github.com/frost-leo/fathomry/schema/fathomry/mysql/v1"
	pgassets "github.com/frost-leo/fathomry/schema/fathomry/postgresql/v1"
	"io/fs"
	"strconv"
	"testing"
	"testing/fstest"
)

func TestSchemaContractAndHistory(t *testing.T) {
	for _, dialect := range []string{"postgresql", "mysql"} {
		t.Run(dialect, func(t *testing.T) {
			var assets fs.FS = mysqlassets.Assets()
			tx := &publicationTx{}
			if dialect == "postgresql" {
				assets = pgassets.Assets()
				tx.pg = new(postgres.Transaction)
			}
			contract, err := loadSchema(assets, dialect)
			if err != nil {
				t.Fatal(err)
			}
			if len(contract.tables) != 10 || len(contract.objects) < 100 || len(contract.steps) < 20 {
				t.Fatal("incomplete inspection authority")
			}
			binary := func(raw []byte) []byte {
				if dialect == "postgresql" {
					return []byte("\\x" + hex.EncodeToString(raw))
				}
				return raw
			}
			rows := [][][]byte{}
			for index, step := range contract.steps {
				digest, err := hex.DecodeString(step.SHA256)
				if err != nil {
					t.Fatal(err)
				}
				state := "applied"
				if index == 0 {
					state = "complete"
				}
				rows = append(rows, [][]byte{[]byte(schemaFamily), []byte("1"), []byte(strconv.Itoa(index)), binary(contract.digest), []byte(step.ID), binary(digest), []byte(state), []byte("1")})
			}
			if err := tx.schemaHistory(rows, contract); err != nil {
				t.Fatal("valid history rejected", err)
			}
			for _, test := range []struct {
				column int
				value  []byte
			}{{0, []byte("foreign")}, {1, []byte("2")}, {2, []byte("01")}, {3, binary(make([]byte, 32))}, {4, []byte("other")}, {5, binary(make([]byte, 32))}, {6, []byte("intent")}, {7, []byte("0")}} {
				original := rows[0][test.column]
				rows[0][test.column] = test.value
				if err := tx.schemaHistory(rows, contract); !errors.Is(err, ErrSchema) {
					t.Fatal("unproven history admitted")
				}
				rows[0][test.column] = original
			}
			if err := tx.schemaHistory(rows[:len(rows)-1], contract); !errors.Is(err, ErrSchema) {
				t.Fatal("partial history admitted")
			}
			copied := fstest.MapFS{}
			if err := fs.WalkDir(assets, ".", func(path string, entry fs.DirEntry, problem error) error {
				if problem != nil {
					return problem
				}
				if entry.IsDir() {
					return nil
				}
				raw, err := fs.ReadFile(assets, path)
				if err != nil {
					return err
				}
				copied[path] = &fstest.MapFile{Data: raw}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			copied["manifest.json"].Data = bytes.Repeat([]byte(" "), 64<<10+1)
			if _, err := loadSchema(copied, dialect); !errors.Is(err, ErrSchema) {
				t.Fatal("unbounded assets accepted")
			}
		})
	}
}
func TestSchemaCannotUseUnprovenResourceTransaction(t *testing.T) {
	tx, err := postgresPublication(new(postgres.Transaction), "catalog")
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.schema(context.Background()); !errors.Is(err, ErrSchema) {
		t.Fatal("missing resource proof admitted", err)
	}
	if tx.calls != 0 {
		t.Fatal("unbound source reached SQL")
	}
}
