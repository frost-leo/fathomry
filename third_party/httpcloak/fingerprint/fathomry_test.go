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

package fingerprint

import (
	tls "github.com/sardanioss/utls"
	"sync"
	"sync/atomic"
	"testing"
)

func TestFathomryCloneOwnsNativeContainers(t *testing.T) {
	source := GetStrict("chrome-148")
	source.RawClientHello = []byte{1, 2}
	source.RawPSKClientHello = []byte{3, 4}
	source.ClientHelloID.Seed = &tls.PRNGSeed{}
	source.ClientHelloID.Weights = &tls.Weights{}
	source.PSKClientHelloID = source.ClientHelloID
	source.QUICClientHelloID = source.ClientHelloID
	source.QUICPSKClientHelloID = source.ClientHelloID
	copied := clonePreset(source)
	copied.RawClientHello[0] = 9
	copied.RawPSKClientHello[0] = 9
	for _, pair := range [][2]tls.ClientHelloID{{source.ClientHelloID, copied.ClientHelloID}, {source.PSKClientHelloID, copied.PSKClientHelloID}, {source.QUICClientHelloID, copied.QUICClientHelloID}, {source.QUICPSKClientHelloID, copied.QUICPSKClientHelloID}} {
		if pair[0].Seed == pair[1].Seed || pair[0].Weights == pair[1].Weights {
			t.Fatal("ClientHelloID pointers alias")
		}
	}
	if source.RawClientHello[0] != 1 || source.RawPSKClientHello[0] != 3 {
		t.Fatal("raw captures alias")
	}
}
func TestFathomryRegistrySnapshotsAndAtomicInsertion(t *testing.T) {
	const name = "fathomry-unit-registration"
	Unregister(name)
	defer Unregister(name)
	preset := GetStrict("chrome-148")
	preset.Headers = map[string]string{"x-test": "original"}
	Register(name, preset)
	preset.Headers["x-test"] = "mutated"
	if LookupCustom(name).Headers["x-test"] != "original" {
		t.Fatal("registry borrows mutable input")
	}
	Unregister(name)
	var winners atomic.Int32
	var workers sync.WaitGroup
	start := make(chan struct{})
	for range 32 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			if RegisterStrict(name, preset) == nil {
				winners.Add(1)
			}
		}()
	}
	close(start)
	workers.Wait()
	if winners.Load() != 1 {
		t.Fatal("strict insertion was not atomic")
	}
}
