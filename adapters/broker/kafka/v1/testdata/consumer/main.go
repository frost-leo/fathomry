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

	"github.com/frost-leo/fathomry/adapters/broker/kafka/v1"
	"github.com/frost-leo/fathomry/adapters/broker/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("kafka public consumer passed")
}
func run() error {
	var settings kafka.Settings
	decoder := json.NewDecoder(os.Stdin)
	decoder.DisallowUnknownFields()
	if decoder.Decode(&settings) != nil {
		return errors.New("invalid synthetic fixture")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	policy, err := kafka.Recommend(settings)
	if err != nil {
		return err
	}
	var budget broker.Budget = policy.Budget
	if budget.WorkBytes <= 0 {
		return errors.New("missing provider policy")
	}
	runtime, err := adapters.New(ctx, policy.Runtime)
	if err != nil {
		return err
	}
	defer runtime.Close(context.Background())
	inbox, err := adapters.NewInbox[kafka.Result](policy.Evidence)
	if err != nil {
		return err
	}
	owner, err := kafka.Open(ctx, settings, kafka.Dependencies{Runtime: runtime, Evidence: inbox})
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
	client, err := owner.Client().WithID("external/correlation")
	if err != nil {
		return err
	}
	receipt, err := client.Produce(ctx, []kafka.Message{{Topic: settings.Topics[0], Value: []byte("independent-public")}})
	if err != nil {
		return err
	}
	snapshot, err := receipt.WaitReleased(ctx)
	if err != nil {
		return err
	}
	if snapshot.Err() != nil {
		return snapshot.Err()
	}
	produced, present := snapshot.ValueCopy()
	if !present || produced.Attribution().ID != "external/correlation" {
		return errors.New("missing public attribution")
	}
	if err := ack(); err != nil {
		return err
	}
	written := produced.WritesCopy()[0]
	if !written.IdentityChecked || !written.PositionKnown {
		return errors.New("missing native coordinates")
	}
	read, err := client.ReadExact(ctx, written.Position)
	if err != nil {
		return err
	}
	if read.ReadsCopy()[0].State != kafka.ReadFound || string(read.ReadsCopy()[0].Record.ValueCopy()) != "independent-public" {
		return errors.New("public exact read mismatch")
	}
	if err := ack(); err != nil {
		return err
	}
	group, err := client.ConsumeGroup(ctx)
	if err != nil {
		return err
	}
	defer group.Close(context.Background())
	for {
		status := group.Snapshot()
		if status.Ready && len(status.AssignmentsCopy()) == 2 {
			break
		}
		if _, err := group.Wait(ctx, status.Revision); err != nil {
			return err
		}
		if err := ack(); err != nil {
			return err
		}
	}
	page, err := group.Next(ctx)
	if err != nil {
		return err
	}
	if len(page.GroupBatch().Page().RecordsCopy()) != 1 {
		return errors.New("public group page mismatch")
	}
	if err := ack(); err != nil {
		return err
	}
	commit, err := group.CommitBatch(ctx, page.GroupBatch())
	if err != nil {
		return err
	}
	if commit.CheckpointsCopy()[0].State != kafka.CheckpointCommitted {
		return errors.New("public group commit unconfirmed")
	}
	if err := ack(); err != nil {
		return err
	}
	if _, err := group.Close(ctx); err != nil {
		return err
	}
	if err := ack(); err != nil {
		return err
	}
	if err := owner.Close(ctx); err != nil {
		return err
	}
	if err := ack(); err != nil {
		return err
	}
	return nil
}
