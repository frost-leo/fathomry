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

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/frost-leo/fathomry/adapters/configsource/v1"
	trino "github.com/frost-leo/fathomry/adapters/sqlengine/trino/v1"
	"github.com/frost-leo/fathomry/adapters/sqlengine/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("trino independent consumer passed")
}

func run() error {
	var input trino.Settings
	decoder := json.NewDecoder(os.Stdin)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		return errors.New("fixture configuration decoding failed")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	schema, err := trino.Configuration(input)
	if err != nil {
		return err
	}
	prepared, err := configsource.Prepare(ctx, schema, []configsource.Layer{{Kind: configsource.Base, Encoding: configsource.JSON, Content: []byte(`{}`)}})
	if err != nil {
		return err
	}
	options, err := prepared.ValueCopy()
	if err != nil || trino.Validate(options) != nil {
		return errors.New("public strict configuration handoff failed")
	}
	policy, err := trino.Recommend(options)
	if err != nil {
		return err
	}
	var declared sqlengine.Policy = policy
	if declared.SourceWorkBytes <= 0 || declared.SourceEvidenceBytes <= 0 {
		return errors.New("SQL-engine source reservations are absent")
	}
	runtime, err := adapters.New(ctx, policy.Runtime)
	if err != nil {
		return err
	}
	defer runtime.Close(context.Background())
	inbox, err := adapters.NewInbox[trino.Result](policy.Evidence)
	if err != nil {
		return err
	}
	owner, err := trino.Open(ctx, options, trino.Dependencies{Runtime: runtime, Evidence: inbox})
	if owner != nil {
		defer owner.Close(context.Background())
	}
	if err != nil {
		return err
	}
	if owner.Info().Name != input.Name || owner.Info().Revision == "" || owner.Handle().Info().Revision != owner.Info().Revision {
		return errors.New("detached preparation metadata unavailable")
	}
	profile, err := owner.Client().Profile(ctx)
	if err != nil || profile.SDKMode != "trino-direct" || profile.ServiceVersion.Kind != "observed" || profile.ServiceVersion.Value != "483" {
		return errors.New("actual readiness profile unavailable")
	}
	build, err := trino.Build()
	if err != nil || len(build.SDKs) != 1 || !build.SDKs[0].Present || build.SDKs[0].Version.Value != "v0.333.0" {
		return errors.New("executing-binary selected SDK facts unavailable")
	}
	client, err := owner.Client().WithID("independent/attempt")
	if err != nil {
		return err
	}
	receive := func() error {
		delivery, err := inbox.NextReleased(ctx)
		if err != nil {
			return err
		}
		return delivery.Ack()
	}
	value, err := client.Query(ctx, ctx, trino.Statement{SQL: "SELECT ?", Args: []any{trino.Numeric("9223372036854775807")}})
	if err != nil || !value.Complete() || string(value.DataCopy()) != "[[9223372036854775807]]" || value.Attribution().ID != "independent/attempt" || value.Effect() != trino.ReadOnly || value.RepresentationVersion() != 1 {
		return errors.New("independent exact finite query failed")
	}
	metadata := value.ColumnsCopy()
	metadata[0].Signature[0] = 'x'
	if value.ColumnsCopy()[0].Signature[0] != '{' {
		return errors.New("public metadata aliases consumer storage")
	}
	if _, err := json.Marshal(value); err == nil {
		return errors.New("runtime result unexpectedly serialized")
	}
	if err := receive(); err != nil {
		return err
	}
	value, err = client.Execute(ctx, ctx, trino.Statement{SQL: "CREATE TABLE fixture AS SELECT 1"})
	if err != nil || !value.Complete() || value.Effect() != trino.Acknowledged || value.Rows() != 0 {
		return errors.New("independent full execution failed")
	}
	if err := receive(); err != nil {
		return err
	}
	value, err = client.Insert(ctx, ctx, trino.BatchInsert{Table: "fixture", Columns: []string{"id", "binary"}, Rows: [][]any{{trino.Numeric("1"), []byte{0, 255}}, {int64(2), nil}}})
	if err != nil || !value.Complete() || value.Submissions() != 1 || value.Effect() != trino.Acknowledged {
		return errors.New("independent physical batch failed")
	}
	if _, present := value.UpdateCount(); !present {
		return errors.New("independent aggregate count presence lost")
	}
	if err := receive(); err != nil {
		return err
	}
	setup, stopSetup := context.WithCancel(ctx)
	reader, err := client.Read(setup, ctx, trino.Statement{SQL: "SELECT bounded"})
	stopSetup()
	if err != nil || reader == nil {
		return errors.New("independent reader admission failed")
	}
	page, err := reader.Next(ctx)
	if err != nil || page.Complete() || page.ReadProgress().Sequence != 1 || string(page.DataCopy()) != "[[9223372036854775807]]" {
		return errors.New("independent provisional page failed")
	}
	if err := receive(); err != nil {
		return err
	}
	if _, err := reader.Next(ctx); !errors.Is(err, io.EOF) {
		return errors.New("independent terminal EOF failed")
	}
	terminal, err := reader.Result(ctx)
	if err != nil || !terminal.Complete() || terminal.Rows() != 1 || string(terminal.DataCopy()) != "[]" || reader.Receipt() == nil {
		return errors.New("independent compact terminal evidence failed")
	}
	if _, err := reader.Close(ctx); err != nil {
		return err
	}
	if err := owner.Close(ctx); err != nil || !owner.ShutdownComplete() {
		return errors.New("independent source ownership not joined")
	}
	for state, _ := inbox.Inspect(); state.Outstanding > 0; state, _ = inbox.Inspect() {
		if err := receive(); err != nil {
			return err
		}
	}
	if err := runtime.Close(ctx); err != nil {
		return err
	}
	if state, _ := runtime.Inspect(); state.Active != 0 || state.WorkBytes != 0 {
		return errors.New("independent operation reservation leaked")
	}
	return nil
}
