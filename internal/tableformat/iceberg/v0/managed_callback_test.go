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

package iceberg

import (
	"errors"
	"runtime"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/iceberg-go/catalog"
)

func TestManagedCallbackAbnormalExitStillReleasesAndRecordsFailure(t *testing.T) {
	for _, mode := range []string{"panic", "goexit", "visitor-panic", "visitor-goexit"} {
		t.Run(mode, func(t *testing.T) {
			_, options := newPeer(t)
			f := bindManagedFixture(t, managedSettingsFor(options), 1)
			record := makeBatch(t, 0, 4097, "exit")
			f.run(t, "setup", func(s *ManagedSession) error {
				if _, err := s.CreateTable([]string{"isolated", "data"}, testSchema(), catalog.WithLocation(options.Location+"data")); err != nil {
					return err
				}
				tx, err := stageManagedAppend(s, "data", record)
				if err != nil {
					return err
				}
				_, err = tx.Commit()
				return err
			})
			cause := errors.New("callback failure canary")
			result, err := f.outcome(t, "abnormal-exit", func(s *ManagedSession) error {
				switch mode {
				case "panic":
					panic(cause)
				case "goexit":
					runtime.Goexit()
				}
				tbl, err := s.LoadTable([]string{"isolated", "data"})
				if err != nil {
					return err
				}
				scan, err := tbl.Scan(ManagedScanOptions{BatchSize: 1})
				if err != nil {
					return err
				}
				_, err = scan.Records(func(arrow.RecordBatch) (bool, error) {
					if mode == "visitor-goexit" {
						runtime.Goexit()
					}
					panic(cause)
				})
				return err
			})
			if !errors.Is(err, ErrOperation) || !result.Released() {
				t.Fatalf("abnormal callback stranded its family: %s", causes(err))
			}
			if mode == "panic" || mode == "visitor-panic" {
				if !errors.Is(err, cause) {
					t.Fatal("deliberate error cause inspection lost")
				}
			}
			for _, effect := range result.Effects() {
				if effect.Operation == "records" && effect.Complete {
					t.Fatal("panicking record visitor was marked complete")
				}
			}
			f.run(t, "after-exit", func(s *ManagedSession) error {
				_, err := s.LoadTable([]string{"isolated", "data"})
				return err
			})
		})
	}
}
