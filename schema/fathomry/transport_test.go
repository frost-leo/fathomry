//go:build schema_service

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

package fathomry_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"strings"
	"testing"
	"time"

	mysql "github.com/frost-leo/fathomry/adapters/database/mysql/v1"
	postgres "github.com/frost-leo/fathomry/adapters/database/postgres/v1"
	wf "github.com/frost-leo/fathomry/framework/workflow/declaration/v1"
	"github.com/jackc/pgx/v5/pgproto3"
)

func postgresDocumentSettings(value *postgres.Settings) {
	value.MaxArgumentBytes = 16 << 20
	value.MaxMessageBytes = 16 << 20
	value.MaxResultBytes = 16 << 20
}

func TestMaximumModeAggregateTransport(t *testing.T) {
	bundle := maximumModeAggregate(t)
	contents, err := bundle.ContentsCopy()
	if err != nil {
		t.Fatal(err)
	}
	for _, dialect := range []string{"postgresql", "mysql"} {
		t.Run(dialect, func(t *testing.T) {
			var fixture sqlFixture
			kind := "BYTEA"
			if dialect == "postgresql" {
				fixture = sqlPostgres(t, postgresDocumentSettings)
				fixture.target = `"public"`
			} else {
				fixture = sqlMySQL(t, mysqlDocumentSettings)
				fixture.target, kind = "`catalog_test`", "LONGBLOB"
			}
			var random [8]byte
			if _, err := rand.Read(random[:]); err != nil {
				t.Fatal(err)
			}
			table := fixture.target + ".aggregate_" + hex.EncodeToString(random[:])
			fixture.execute(t, "CREATE TABLE "+table+" (id BIGINT PRIMARY KEY, profile VARCHAR(64) NOT NULL, digest VARCHAR(64) NOT NULL, content "+kind+" NOT NULL)")
			t.Cleanup(func() { fixture.execute(t, "DROP TABLE "+table) })
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			var execute func(string, ...any) error
			var commit func() error
			if dialect == "postgresql" {
				transaction, err := fixture.postgresClient.Begin(ctx, postgres.TxOptions{Isolation: postgres.RepeatableRead, Access: postgres.ReadWrite})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
					defer stop()
					_, _ = transaction.Rollback(cleanup)
				})
				execute = func(sql string, args ...any) error { _, err := transaction.Exec(ctx, sql, args...); return err }
				commit = func() error {
					value, err := transaction.Commit(ctx)
					if value.TransactionOutcome() != postgres.CommitAcknowledged {
						return errors.Join(err, errors.New("aggregate commit not acknowledged"))
					}
					return err
				}
			} else {
				transaction, err := fixture.mysqlClient.Begin(ctx, mysql.TxOptions{Isolation: sql.LevelRepeatableRead})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
					defer stop()
					_, _ = transaction.Rollback(cleanup)
				})
				execute = func(sql string, args ...any) error { _, err := transaction.Exec(ctx, sql, args...); return err }
				commit = func() error {
					value, err := transaction.Commit(ctx)
					if value.TransactionOutcome() != mysql.CommitAcknowledged {
						return errors.Join(err, errors.New("aggregate commit not acknowledged"))
					}
					return err
				}
			}
			for index, content := range contents {
				if err := execute("INSERT INTO "+table+" VALUES ("+fixture.parameters(4)+")", int64(index), content.Entry.Profile, content.Entry.SHA256, content.Bytes); err != nil {
					t.Fatal("bounded aggregate write failed", err)
				}
			}
			if err := commit(); err != nil {
				t.Fatal(err)
			}
			stored := make([]wf.Content, len(contents))
			for index, content := range contents {
				rows := fixture.rows(t, "SELECT profile,digest,content FROM "+table+" WHERE id="+fixture.parameters(1), int64(index))
				if len(rows) != 1 || len(rows[0]) != 3 {
					t.Fatal("stored content unavailable")
				}
				data := rows[0][2]
				checkDocumentBytes(t, dialect, data, content.Bytes)
				if dialect == "postgresql" {
					data, err = hex.DecodeString(string(data[2:]))
					if err != nil {
						t.Fatal(err)
					}
				}
				stored[index] = wf.Content{Entry: wf.Entry{Identity: content.Entry.Identity, Profile: string(rows[0][0]), SHA256: string(rows[0][1])}, Bytes: data}
			}
			parsed, err := wf.ParseContents(ctx, stored)
			if err != nil {
				t.Fatal("stored aggregate reconstruction failed", err)
			}
			report, err := wf.Check(ctx, bundle, parsed)
			if err != nil || !report.Passed || report.Modes != len(contents)-1 {
				t.Fatal("stored aggregate comparison failed", err)
			}
			t.Logf("%d canonical documents passed bounded per-document SQL, commit and shared reconstruction", len(contents))
		})
	}
}

func mysqlDocumentSettings(value *mysql.Settings) {
	value.MaxArgumentBytes = 8 << 20
	value.MaxPacketBytes = 8 << 20
	value.MaxResultBytes = 8 << 20
}

func checkDocumentBytes(t *testing.T, dialect string, got, want []byte) {
	t.Helper()
	if dialect == "postgresql" {
		if !bytes.HasPrefix(got, []byte("\\x")) {
			t.Fatal("PostgreSQL binary representation is unavailable")
		}
		var err error
		got, err = hex.DecodeString(string(got[2:]))
		if err != nil {
			t.Fatal("invalid bytea encoding", err)
		}
	}
	if !bytes.Equal(got, want) {
		t.Fatal("document changed during transport")
	}
}

func TestMaximumDocumentTransport(t *testing.T) {
	document := maximumSpecification(t)
	for _, dialect := range []string{"postgresql", "mysql"} {
		t.Run(dialect, func(t *testing.T) {
			var fixture sqlFixture
			kind := "BYTEA"
			limitError := error(postgres.ErrLimit)
			if dialect == "postgresql" {
				fixture = sqlPostgres(t, postgresDocumentSettings)
				fixture.target = `"public"`
				if !strings.HasPrefix(fixture.scalar(t, "SHOW server_version"), "18.") {
					t.Fatal("PostgreSQL 18 required")
				}
			} else {
				fixture = sqlMySQL(t, mysqlDocumentSettings)
				fixture.target = "`catalog_test`"
				kind, limitError = "LONGBLOB", mysql.ErrLimit
				if !strings.HasPrefix(fixture.scalar(t, "SELECT VERSION()"), "8.4.") {
					t.Fatal("MySQL 8.4 required")
				}
			}
			var random [8]byte
			if _, err := rand.Read(random[:]); err != nil {
				t.Fatal(err)
			}
			table := fixture.target + ".document_" + hex.EncodeToString(random[:])
			fixture.execute(t, "CREATE TABLE "+table+" (id BIGINT PRIMARY KEY, content "+kind+" NOT NULL)")
			t.Cleanup(func() { fixture.execute(t, "DROP TABLE "+table) })
			insert := "INSERT INTO " + table + " (id,content) VALUES (" + fixture.parameters(2) + ")"
			fixture.execute(t, insert, int64(1), document)
			rows := fixture.rows(t, "SELECT content FROM "+table+" WHERE id=1")
			if len(rows) != 1 || len(rows[0]) != 1 {
				t.Fatal("complete document row unavailable")
			}
			checkDocumentBytes(t, dialect, rows[0][0], document)
			t.Run("default_arguments_refuse_before_effect", func(t *testing.T) {
				var limited sqlFixture
				if dialect == "postgresql" {
					limited = sqlPostgres(t)
				} else {
					limited = sqlMySQL(t)
				}
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				err := limited.exec(ctx, insert, int64(2), document)
				if !errors.Is(err, limitError) {
					t.Fatal("default arguments no longer reject maximum document", err)
				}
				if fixture.scalar(t, "SELECT COUNT(*) FROM "+table+" WHERE id=2") != "0" {
					t.Fatal("rejected arguments mutated storage")
				}
			})
			for _, layer := range []string{"protocol", "result"} {
				t.Run(layer+"_bound_remains_independent", func(t *testing.T) {
					var limited sqlFixture
					if dialect == "postgresql" {
						limited = sqlPostgres(t, postgresDocumentSettings, func(value *postgres.Settings) {
							if layer == "protocol" {
								value.MaxMessageBytes = 4 << 20
							} else {
								value.MaxResultBytes = 1 << 20
							}
						})
					} else {
						limited = sqlMySQL(t, mysqlDocumentSettings, func(value *mysql.Settings) {
							if layer == "protocol" {
								value.MaxPacketBytes = 4 << 20
							} else {
								value.MaxResultBytes = 1 << 20
							}
						})
					}
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer cancel()
					_, err := limited.query(ctx, "SELECT content FROM "+table+" WHERE id=1")
					if dialect == "postgresql" && layer == "protocol" {
						var exceeded *pgproto3.ExceededMaxBodyLenErr
						if !errors.Is(err, postgres.ErrQuery) || !errors.As(err, &exceeded) ||
							exceeded.MaxExpectedBodyLen != 4<<20 || exceeded.ActualBodyLen <= 4<<20 {
							t.Fatal("PostgreSQL protocol limit cause was not retained", err)
						}
					} else if !errors.Is(err, limitError) {
						t.Fatal("undersized independent read budget did not reject", err)
					}
				})
			}
			verifyRetainedDocumentTransport(t, fixture, table, document)
			t.Log("exact 4 MiB compiled/parsed/checked spec passed ordinary, prepared and transaction transport")
		})
	}
}

func verifyRetainedDocumentTransport(t *testing.T, fixture sqlFixture, table string, document []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if fixture.dialect == "postgresql" {
		prepared, err := fixture.postgresClient.Prepare(ctx, "SELECT $1::bytea")
		if err != nil {
			t.Fatal(err)
		}
		value, err := prepared.Query(ctx, document)
		if err != nil {
			t.Fatal("prepared document failed", err)
		}
		row, err := value.First()
		if err != nil {
			t.Fatal(err)
		}
		checkDocumentBytes(t, fixture.dialect, row.ValuesCopy()[0], document)
		if _, err := prepared.Close(ctx); err != nil {
			t.Fatal(err)
		}
		transaction, err := fixture.postgresClient.Begin(ctx, postgres.TxOptions{Isolation: postgres.RepeatableRead, Access: postgres.ReadWrite})
		if err != nil {
			t.Fatal(err)
		}
		defer transaction.Rollback(context.Background())
		if _, err := transaction.Exec(ctx, "INSERT INTO "+table+" VALUES ($1,$2)", int64(3), document); err != nil {
			t.Fatal(err)
		}
		value, err = transaction.Query(ctx, "SELECT content FROM "+table+" WHERE id=3")
		if err != nil {
			t.Fatal(err)
		}
		row, err = value.First()
		if err != nil {
			t.Fatal(err)
		}
		checkDocumentBytes(t, fixture.dialect, row.ValuesCopy()[0], document)
		result, err := transaction.Commit(ctx)
		if err != nil || result.TransactionOutcome() != postgres.CommitAcknowledged {
			t.Fatal("commit acknowledgement lost", err)
		}
	} else {
		prepared, err := fixture.mysqlClient.Prepare(ctx, "SELECT ?")
		if err != nil {
			t.Fatal(err)
		}
		value, err := prepared.Query(ctx, document)
		if err != nil {
			t.Fatal("prepared document failed", err)
		}
		row, err := value.First()
		if err != nil {
			t.Fatal(err)
		}
		checkDocumentBytes(t, fixture.dialect, row.ValuesCopy()[0], document)
		if _, err := prepared.Close(ctx); err != nil {
			t.Fatal(err)
		}
		transaction, err := fixture.mysqlClient.Begin(ctx, mysql.TxOptions{Isolation: sql.LevelRepeatableRead})
		if err != nil {
			t.Fatal(err)
		}
		defer transaction.Rollback(context.Background())
		if _, err := transaction.Exec(ctx, "INSERT INTO "+table+" VALUES (?,?)", int64(3), document); err != nil {
			t.Fatal(err)
		}
		value, err = transaction.Query(ctx, "SELECT content FROM "+table+" WHERE id=3")
		if err != nil {
			t.Fatal(err)
		}
		row, err = value.First()
		if err != nil {
			t.Fatal(err)
		}
		checkDocumentBytes(t, fixture.dialect, row.ValuesCopy()[0], document)
		result, err := transaction.Commit(ctx)
		if err != nil || result.TransactionOutcome() != mysql.CommitAcknowledged {
			t.Fatal("commit acknowledgement lost", err)
		}
	}
}
