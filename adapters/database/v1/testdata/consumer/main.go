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
	"os"
	"time"

	"github.com/frost-leo/fathomry/adapters/database/mysql/v1"
	"github.com/frost-leo/fathomry/adapters/database/postgres/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("database consumers composed without service I/O")
}
func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pgSettings := postgres.Settings{Name: "postgres", Address: "127.0.0.1", Port: 1, Database: "fixture", User: "fixture", Password: "synthetic-only", Plaintext: true, ParserHome: os.Getenv("HOME")}
	mySettings := mysql.Settings{Name: "mysql", Address: "127.0.0.1", Port: 1, Database: "fixture", User: "fixture", Password: "synthetic-only", Plaintext: true}
	pgPolicy, err := postgres.Recommend(pgSettings)
	if err != nil {
		return err
	}
	myPolicy, err := mysql.Recommend(mySettings)
	if err != nil {
		return err
	}
	runtime, err := adapters.New(ctx, adapters.Options{
		MaxActive:    pgPolicy.Runtime.MaxActive + myPolicy.Runtime.MaxActive,
		MaxWorkBytes: pgPolicy.Runtime.MaxWorkBytes + myPolicy.Runtime.MaxWorkBytes,
	})
	if err != nil {
		return err
	}
	defer runtime.Close(context.Background())
	pgInbox, err := adapters.NewInbox[postgres.Result](pgPolicy.Evidence)
	if err != nil {
		return err
	}
	myInbox, err := adapters.NewInbox[mysql.Result](myPolicy.Evidence)
	if err != nil {
		return err
	}
	pgOwner, err := postgres.Open(ctx, pgSettings, postgres.Dependencies{Runtime: runtime, Evidence: pgInbox})
	if pgOwner != nil {
		defer pgOwner.Close(context.Background())
	}
	if err != nil {
		return err
	}
	myOwner, err := mysql.Open(ctx, mySettings, mysql.Dependencies{Runtime: runtime, Evidence: myInbox})
	if myOwner != nil {
		defer myOwner.Close(context.Background())
	}
	if err != nil {
		return err
	}
	pgStats, err := pgOwner.Client().Stats(ctx)
	if err != nil || pgStats.TotalResources != 0 {
		return errors.New("PostgreSQL construction performed hidden service I/O")
	}
	myStats, err := myOwner.Client().Stats(ctx)
	if err != nil || myStats.OpenConnections != 0 {
		return errors.New("MySQL construction performed hidden service I/O")
	}
	if pgOwner.Info().Name != "postgres" || myOwner.Info().Name != "mysql" {
		return errors.New("source identities mixed")
	}
	usage, err := runtime.Inspect()
	if err != nil || usage.Active != 2 {
		return errors.New("source owners did not compose explicit reservations")
	}
	if err := pgOwner.Close(ctx); err != nil {
		return err
	}
	if err := myOwner.Close(ctx); err != nil {
		return err
	}
	if !pgOwner.ShutdownComplete() || !myOwner.ShutdownComplete() {
		return errors.New("source cleanup incomplete")
	}
	pgDelivery, err := pgInbox.NextReleased(ctx)
	if err != nil {
		return err
	}
	if err := pgDelivery.Ack(); err != nil {
		return err
	}
	myDelivery, err := myInbox.NextReleased(ctx)
	if err != nil {
		return err
	}
	if err := myDelivery.Ack(); err != nil {
		return err
	}
	if err := runtime.Close(ctx); err != nil {
		return err
	}
	usage, err = runtime.Inspect()
	if err != nil || usage.Active != 0 || usage.WorkBytes != 0 {
		return errors.New("combined source reservation retained")
	}
	return nil
}
