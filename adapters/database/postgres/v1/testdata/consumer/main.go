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
	"os"
	"time"

	database "github.com/frost-leo/fathomry/adapters/database/postgres/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("postgres public consumer passed")
}
func run() error {
	var settings database.Settings
	decoder := json.NewDecoder(os.Stdin)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&settings); err != nil {
		return errors.New("fixture decoding failed")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	policy, err := database.Recommend(settings)
	if err != nil {
		return err
	}
	runtime, err := adapters.New(ctx, policy.Runtime)
	if err != nil {
		return err
	}
	defer runtime.Close(context.Background())
	inbox, err := adapters.NewInbox[database.Result](policy.Evidence)
	if err != nil {
		return err
	}
	owner, err := database.Open(ctx, settings, database.Dependencies{Runtime: runtime, Evidence: inbox})
	if owner != nil {
		defer owner.Close(context.Background())
	}
	if err != nil {
		return err
	}
	stats, err := owner.Client().Stats(ctx)
	if err != nil || stats.TotalResources != 0 {
		return errors.New("construction performed hidden readiness I/O")
	}
	profile, err := owner.Client().Profile(ctx)
	if err != nil || profile.SDKMode == "" {
		return errors.New("effective profile unavailable")
	}
	receive := func() error {
		delivery, err := inbox.NextReleased(ctx)
		if err != nil {
			return err
		}
		return delivery.Ack()
	}
	client, err := owner.Client().WithID("consumer/opaque")
	if err != nil {
		return err
	}
	if value, err := client.Ping(ctx); err != nil || !value.Complete() {
		return errors.New("public readiness failed")
	}
	if err := receive(); err != nil {
		return err
	}
	value, err := client.Query(ctx, "SELECT $1::text", "exact")
	if err != nil {
		return err
	}
	row, err := value.First()
	if err != nil || string(row.ValuesCopy()[0]) != "exact" || value.Attribution().ID != "consumer/opaque" {
		return errors.New("public positional read or attribution failed")
	}
	if err := receive(); err != nil {
		return err
	}
	value, err = client.Exec(ctx, "INSERT INTO fixture VALUES ($1)", "ordinary")
	if err != nil || !value.Complete() || value.RowsCopy() != nil {
		return errors.New("public execution failed")
	}
	if value.RowsAffected() != 1 {
		return errors.New("affected command metadata lost")
	}
	if err := receive(); err != nil {
		return err
	}
	setup, stop := context.WithCancel(ctx)
	statement, err := client.Prepare(setup, "SELECT $1::text")
	stop()
	if err != nil {
		return err
	}
	if !statement.Ready().Complete() {
		return errors.New("preparation readiness absent")
	}
	if snapshot, _ := statement.Receipt().Snapshot(); snapshot.Info().Resolved {
		return errors.New("preparation prematurely finalized")
	}
	if value, err := statement.Query(ctx, "prepared"); err != nil || !value.Complete() {
		return errors.New("preparation setup cancellation revoked use")
	}
	if err := receive(); err != nil {
		return err
	}
	if _, err := statement.Close(ctx); err != nil {
		return err
	}
	if err := receive(); err != nil {
		return err
	}
	transaction, err := client.Begin(ctx, database.TxOptions{Isolation: database.ReadCommitted, Access: database.ReadWrite})
	if err != nil {
		return err
	}
	prepared, err := transaction.Prepare(ctx, "INSERT INTO fixture VALUES ($1)")
	if err != nil {
		return err
	}
	if value, err := prepared.Exec(ctx, "inside"); err != nil || !value.Complete() {
		return errors.New("transaction preparation failed")
	}
	if err := receive(); err != nil {
		return err
	}
	point, err := transaction.Savepoint(ctx)
	if err != nil {
		return err
	}
	if value, err := point.Rollback(ctx); err != nil || value.SavepointOutcome() != database.SavepointRolledBack {
		return errors.New("savepoint finalization failed")
	}
	if err := receive(); err != nil {
		return err
	}
	value, err = transaction.Commit(ctx)
	if err != nil || value.TransactionOutcome() != database.CommitAcknowledged {
		return errors.New("controlled commit failed")
	}
	if snapshot, _ := prepared.Receipt().Snapshot(); !snapshot.Info().Released {
		return errors.New("parent did not join preparation")
	}
	if err := receive(); err != nil {
		return err
	}
	if err := receive(); err != nil {
		return err
	}
	transaction, err = client.Begin(ctx, database.TxOptions{Isolation: database.ReadCommitted, Access: database.ReadWrite})
	if err != nil {
		return err
	}
	if value, err := transaction.Rollback(ctx); err != nil || value.TransactionOutcome() != database.RollbackAcknowledged {
		return errors.New("controlled rollback failed")
	}
	if err := receive(); err != nil {
		return err
	}
	if err := owner.Close(ctx); err != nil {
		return err
	}
	if !owner.ShutdownComplete() {
		return errors.New("source not released")
	}
	if err := receive(); err != nil {
		return err
	}
	if err := runtime.Close(ctx); err != nil {
		return err
	}
	usage, err := runtime.Inspect()
	if err != nil || usage.Active != 0 || usage.WorkBytes != 0 {
		return errors.New("public work reservation retained")
	}
	evidence, err := inbox.Inspect()
	if err != nil || evidence.Outstanding != 0 {
		return errors.New("required evidence not acknowledged")
	}
	return nil
}
