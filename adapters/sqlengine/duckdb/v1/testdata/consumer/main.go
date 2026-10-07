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
	"errors"
	"fmt"
	"math/big"
	"os"
	"time"

	"github.com/frost-leo/fathomry/adapters/configsource/v1"
	duckdb "github.com/frost-leo/fathomry/adapters/sqlengine/duckdb/v1"
	"github.com/frost-leo/fathomry/adapters/sqlengine/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("duckdb direct public consumer passed")
}

func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	schema, err := duckdb.Configuration(duckdb.Settings{Name: "consumer"})
	if err != nil {
		return err
	}
	prepared, err := configsource.Prepare(ctx, schema,
		[]configsource.Layer{{Kind: configsource.Base, Encoding: configsource.JSON, Content: []byte(`{"name":"consumer","memory_bytes":67108864,"reader_chunk_rows":2,"reader_total_rows":100}`)}})
	if err != nil {
		return err
	}
	selected, err := prepared.ValueCopy()
	if err != nil {
		return err
	}
	policy, err := duckdb.Recommend(selected)
	if err != nil {
		return err
	}
	var declared sqlengine.Policy = policy
	if declared.SourceWorkBytes <= 0 || declared.SourceEvidenceBytes <= 0 {
		return fmt.Errorf("source reservations are absent from the SQL-engine policy")
	}
	runtime, err := adapters.New(ctx, policy.Runtime)
	if err != nil {
		return err
	}
	defer runtime.Close(context.Background())
	inbox, err := adapters.NewInbox[duckdb.Result](policy.Evidence)
	if err != nil {
		return err
	}
	owner, err := duckdb.Open(ctx, selected, duckdb.Dependencies{Runtime: runtime, Evidence: inbox})
	if owner != nil {
		defer owner.Close(context.Background())
	}
	if err != nil {
		return err
	}
	ack := func() error {
		delivery, err := inbox.NextReleased(ctx)
		if err != nil {
			return err
		}
		return delivery.Ack()
	}
	client, err := owner.Client().WithID("consumer/独立")
	if err != nil {
		return err
	}
	if _, err := client.Execute(ctx, ctx, "CREATE TABLE values_owned (id BIGINT PRIMARY KEY, amount DECIMAL(4,2), value VARCHAR DEFAULT 'default')"); err != nil {
		return err
	}
	if err := ack(); err != nil {
		return err
	}
	decimal := duckdb.Decimal{Width: 4, Scale: 2, Value: big.NewInt(-427)}
	if value, err := client.ExecuteMany(ctx, ctx, "INSERT INTO values_owned VALUES (?,?,?)", [][]any{{int64(1), decimal, "one"}, {int64(2), decimal, "two"}}); err != nil || !value.Snapshot().Committed {
		return errors.New("public parameter batch failed")
	}
	if err := ack(); err != nil {
		return err
	}
	if value, err := client.Append(ctx, ctx, "", "values_owned", []string{"id", "amount"}, [][]any{{int64(3), decimal}}); err != nil || !value.Snapshot().Committed || value.Snapshot().Steps[0].FlushedRows != 1 {
		return errors.New("public column-subset Appender failed")
	}
	if err := ack(); err != nil {
		return err
	}
	value, err := client.Query(ctx, ctx, "SELECT id,amount,value FROM values_owned ORDER BY id")
	if err != nil || len(value.Snapshot().Steps[0].Rows) != 3 || value.Attribution().ID != "consumer/独立" || value.Source().Provider != duckdb.ProviderID || value.Source().Revision == "" {
		return errors.New("public query or source attribution failed")
	}
	rows := value.Snapshot().Steps[0].Rows
	if rows[0][1].(duckdb.Decimal).Value.Cmp(decimal.Value) != 0 || rows[2][2] != "default" {
		return errors.New("public exact scalar or Appender subset changed")
	}
	rows[0][1].(duckdb.Decimal).Value.SetInt64(99)
	if value.Snapshot().Steps[0].Rows[0][1].(duckdb.Decimal).Value.Cmp(decimal.Value) != 0 {
		return errors.New("public result storage aliases caller")
	}
	if err := ack(); err != nil {
		return err
	}
	value, err = client.Transaction(ctx, ctx, []duckdb.Request{
		{Mode: duckdb.Append, Table: "values_owned", Columns: []string{"id"}, Rows: [][]any{{int64(4)}}},
		{Mode: duckdb.Query, SQL: "SELECT id FROM values_owned ORDER BY id"},
	})
	if err != nil || !value.Snapshot().Committed || len(value.Snapshot().Steps[1].Rows) != 4 {
		return errors.New("public ordered mixed transaction failed")
	}
	if err := ack(); err != nil {
		return err
	}
	setup, stop := context.WithCancel(ctx)
	reader, err := client.Read(setup, "SELECT range FROM range(9) ORDER BY range")
	stop()
	if err != nil {
		return err
	}
	total := int64(0)
	for {
		chunk, err := reader.Next(ctx)
		if err != nil {
			return err
		}
		for _, row := range chunk.Snapshot().Steps[0].Rows {
			if row[0] != total {
				return errors.New("public reader changed row order")
			}
			total++
		}
		if err := ack(); err != nil {
			return err
		}
		if chunk.Snapshot().Steps[0].Complete {
			break
		}
	}
	value, err = reader.Result(ctx)
	if err != nil || !value.Snapshot().Reader.Closed || total != 9 || len(value.Snapshot().Steps[0].Rows) != 0 {
		return errors.New("public reader terminal failed")
	}
	if err := ack(); err != nil {
		return err
	}
	profile, err := client.Profile(ctx)
	if err != nil || profile.Native.Kind != "observed" || profile.Native.Value != "v1.5.5" {
		return errors.New("observed native core profile unavailable")
	}
	build, err := duckdb.Build()
	if err != nil || len(build.SDKs) != 2 {
		return errors.New("selected executing-binary SDK evidence unavailable")
	}
	found := false
	for _, module := range build.SDKs {
		if module.Path.Value == "github.com/duckdb/duckdb-go/v2" {
			found = module.Present && module.Version.Value == "v2.10505.0"
		}
	}
	if !found {
		return errors.New("selected executing-binary driver evidence unavailable")
	}
	if err := owner.Close(ctx); err != nil || !owner.ShutdownComplete() {
		return errors.New("source cleanup incomplete")
	}
	if err := ack(); err != nil {
		return err
	}
	if err := runtime.Close(ctx); err != nil {
		return err
	}
	usage, err := runtime.Inspect()
	if err != nil || usage.Active != 0 || usage.WorkBytes != 0 {
		return errors.New("public operation reservation retained")
	}
	status, err := inbox.Inspect()
	if err != nil || status.Outstanding != 0 {
		return errors.New("independent evidence not acknowledged")
	}
	return nil
}
