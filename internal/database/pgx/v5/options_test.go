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

package pgx

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/frost-leo/fathomry/internal/conformance"
	"github.com/frost-leo/fathomry/internal/resource"
	sdk "github.com/jackc/pgx/v5"
)

func testOptions() OptionsV1 {
	return OptionsV1{Name: "fixture", Address: "127.0.0.1", Port: 5432, Database: "fixture", User: "fixture",
		Password: "credential-canary", Plaintext: true, ParserHome: os.Getenv("HOME"), MaxConnections: 1}
}
func TestOptionsAndNativeProfile(t *testing.T) {
	options := testOptions()
	config, err := nativeConfig(defaults(options))
	if err != nil {
		t.Fatal(err)
	}
	if config.Host != options.Address || config.Port != 5432 || config.User != options.User || config.Password != options.Password ||
		config.TLSConfig != nil || len(config.Fallbacks) != 0 || config.StatementCacheCapacity != 0 || config.DescriptionCacheCapacity != 0 ||
		config.MaxProtocolMessageBodyLen != 1<<20 || config.RequireAuth != "scram-sha-256" || config.MinProtocolVersion != "3.0" {
		t.Fatal("effective native profile differs from explicit bootstrap")
	}
	for _, layer := range []string{"unknown: secret-canary", "max_connections: 0", "port: 0", "timeout_ns: -1", "max_rows: 0", "max_message_bytes: 0", "max_result_bytes: 0", "password: null", "address: localhost"} {
		if _, err := Select(options, resource.Layer{Kind: resource.Base, Content: []byte(layer)},
			resource.Layer{Kind: resource.Local, Content: []byte("{}")}); err == nil {
			t.Fatal("invalid settings were accepted")
		}
	}
	selected, err := Select(options, resource.Layer{Kind: resource.Local, Content: []byte("database: overridden")})
	if err != nil {
		t.Fatal(err)
	}
	options.Password = "mutated"
	assembly, err := resource.Assemble(context.Background(), context.Background(), "options", selected)
	if err != nil {
		t.Fatal(err)
	}
	source, info, err := resource.Bind(assembly, selected)
	if err != nil || source.owner.settings.Database != "overridden" || source.owner.settings.Password != "credential-canary" || info.Configuration.Revision == "" {
		t.Fatal("resolved settings or source identity changed")
	}
	if source.owner.native.Stat().TotalResources() != 0 {
		t.Fatal("construction was mistaken for readiness")
	}
	if err := assembly.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	conformance.Runtime(t, options, new(OptionsV1), "credential-canary")
}
func TestNativeParserRejectsAmbientInputs(t *testing.T) {
	for _, name := range []string{"PGHOST", "PGPORT", "PGDATABASE", "PGUSER", "PGPASSWORD", "PGPASSFILE", "PGAPPNAME", "PGCONNECT_TIMEOUT",
		"PGSSLMODE", "PGSSLKEY", "PGSSLCERT", "PGSSLSNI", "PGSSLROOTCERT", "PGSSLPASSWORD", "PGSSLNEGOTIATION",
		"PGTARGETSESSIONATTRS", "PGSERVICE", "PGSERVICEFILE", "PGTZ", "PGOPTIONS", "PGMINPROTOCOLVERSION", "PGMAXPROTOCOLVERSION",
		"PGCHANNELBINDING", "PGREQUIREAUTH", "PGFUTUREOPTION"} {
		t.Run(name, func(t *testing.T) {
			t.Setenv(name, "environment-canary")
			_, err := nativeConfig(defaults(testOptions()))
			if !errors.Is(err, ErrEnvironment) {
				t.Fatal("ambient setting reached native parsing")
			}
			conformance.Private(t, err, "environment-canary")
		})
	}
	value := defaults(testOptions())
	value.ParserHome += "/unapproved"
	if _, err := nativeConfig(value); !errors.Is(err, ErrEnvironment) {
		t.Fatal("implicit home metadata probes were admitted")
	}
}
func TestParserDoesNotReadHomeCredentialsOrTLSContents(t *testing.T) {
	home := t.TempDir()
	if err := os.Mkdir(filepath.Join(home, ".postgresql"), 0700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{".pgpass", ".pg_service.conf", ".postgresql/root.crt", ".postgresql/postgresql.crt", ".postgresql/postgresql.key"} {
		if err := os.WriteFile(filepath.Join(home, name), []byte("malformed-private-canary"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("HOME", home)
	if _, err := sdk.ParseConfig(strings.Replace(parserSeed, "host=/fathomry-parser-only", "host=127.0.0.1", 1)); err == nil {
		t.Fatal("native TCP parser control no longer attempts default CA content")
	}
	config, err := nativeConfig(defaults(testOptions()))
	if err != nil || config.Password != "credential-canary" || config.TLSConfig != nil || config.RuntimeParams["application_name"] != "fathomry" {
		t.Fatal("home defaults influenced native configuration")
	}
}
func TestExplicitOptionsAndLayerBoundsBeforeConstruction(t *testing.T) {
	for _, change := range []func(*OptionsV1){
		func(v *OptionsV1) { v.Address = "0.0.0.0" }, func(v *OptionsV1) { v.Address = "::" },
		func(v *OptionsV1) { v.Address = "::ffff:0.0.0.0" },
		func(v *OptionsV1) { v.Address = "localhost" }, func(v *OptionsV1) { v.Port = 0 },
		func(v *OptionsV1) { v.User = "" }, func(v *OptionsV1) { v.Password = "" },
		func(v *OptionsV1) { v.Password = "bad\x00secret" }, func(v *OptionsV1) { v.Plaintext = false },
		func(v *OptionsV1) { v.MaxConnections = 33 }, func(v *OptionsV1) { v.MaxRows = -1 },
		func(v *OptionsV1) { v.QueuedCalls = -1 }, func(v *OptionsV1) { v.Timeout = -1 },
		func(v *OptionsV1) { v.RootCAPEM = "private-canary" }, func(v *OptionsV1) { v.MaxMessageBytes = 1023 },
	} {
		options := testOptions()
		change(&options)
		if _, err := Select(options); err == nil {
			t.Fatal("invalid options accepted")
		}
	}
	options := testOptions()
	first, err := Select(options)
	if err != nil {
		t.Fatal(err)
	}
	second, err := Select(options)
	if err != nil {
		t.Fatal(err)
	}
	if assembly, err := resource.Assemble(context.Background(), context.Background(), "duplicates", first, second); assembly != nil || !errors.Is(err, resource.ErrSelection) {
		t.Fatal("duplicate source names reached native construction")
	}
}
func FuzzOptionsV1(f *testing.F) {
	f.Add("127.0.0.1", uint16(5432), "fixture", "password", 1, 1024)
	f.Fuzz(func(t *testing.T, address string, port uint16, user, password string, connections, rows int) {
		if len(address)+len(user)+len(password) > 8192 {
			return
		}
		value := testOptions()
		value.Address, value.Port, value.User, value.Password, value.MaxConnections, value.MaxRows = address, port, user, password, connections, rows
		_, err := Select(value)
		if err != nil {
			conformance.Private(t, err, "never-a-config-value")
		}
	})
}
func FuzzStatement(f *testing.F) {
	f.Add("SELECT $1", "value")
	f.Add("COMMIT", "")
	f.Fuzz(func(t *testing.T, sql, value string) {
		if len(sql)+len(value) > 2*MaxSQLBytes {
			return
		}
		err := validStatement(sql, []any{value})
		if err == nil && (len(sql) > MaxSQLBytes || strings.ContainsRune(sql, 0)) {
			t.Fatal("invalid accepted statement")
		}
	})
}
