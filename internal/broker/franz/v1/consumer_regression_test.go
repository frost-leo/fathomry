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

package franz

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestConsumerLifetimeStopsNewRequestsAfterMetadata(t *testing.T) {
	for _, commit := range []bool{false, true} {
		name := "read"
		if commit {
			name = "commit"
		}
		t.Run(name, func(t *testing.T) {
			cluster := localCluster(t)
			options := clusterOptions(cluster)
			options.OffsetGroup = "cancel-" + name
			fixture := bindFixture(t, options, 8)
			position := produce(t, fixture.client, "source", Message{Topic: "records", Value: []byte("data")}).WritesCopy()[0].Position
			lifetime, cancel := context.WithCancelCause(context.Background())
			defer cancel(nil)
			consumer, err := fixture.client.Consume(lifetime, correlation("cursor"), Range{Start: position, End: 1})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { settle(t, consumer.Close(), nil); <-consumer.done })
			if commit {
				receipt, err := consumer.Next(deadline(t), correlation("page"))
				if result := settle(t, receipt, err); result.Err() != nil {
					t.Fatal(result.Err())
				}
			}
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			t.Cleanup(unblock)
			cluster.ControlKey(int16(kmsg.Metadata), func(kmsg.Request) (kmsg.Response, error, bool) {
				close(entered)
				cluster.SleepControl(func() { <-release })
				return nil, nil, false
			})
			key := int16(kmsg.Fetch)
			if commit {
				key = int16(kmsg.OffsetCommit)
			}
			var requests atomic.Int32
			cluster.ControlKey(key, func(kmsg.Request) (kmsg.Response, error, bool) { requests.Add(1); return nil, nil, false })
			ended := make(chan error, 1)
			go func() {
				var resultErr error
				if commit {
					receipt, err := consumer.Commit(deadline(t), correlation("commit"))
					if err != nil {
						resultErr = err
					} else {
						result, waitErr := receipt.WaitReleased(deadline(t))
						resultErr = errors.Join(waitErr, result.Err())
					}
				} else {
					receipt, err := consumer.Next(deadline(t), correlation("read"))
					if err != nil {
						resultErr = err
					} else {
						result, waitErr := receipt.WaitReleased(deadline(t))
						resultErr = errors.Join(waitErr, result.Err())
					}
				}
				ended <- resultErr
			}()
			select {
			case <-entered:
			case <-deadline(t).Done():
				t.Fatal("metadata did not start")
			}
			cause := errors.New("consumer-lifetime-ended")
			cancel(cause)
			unblock()
			select {
			case err := <-ended:
				if !errors.Is(err, cause) {
					t.Fatal("lifetime cause lost", err)
				}
			case <-deadline(t).Done():
				t.Fatal("child did not finish")
			}
			if requests.Load() != 0 {
				t.Fatal("canceled consumer issued another native operation")
			}
			if result := settle(t, consumer.Receipt(), nil); !errors.Is(result.Err(), cause) {
				t.Fatal("independent lifetime evidence lost")
			}
		})
	}
}
func TestZeroConsumerIsSafe(t *testing.T) {
	var consumer *Consumer
	if consumer.Close() != nil || consumer.Receipt() != nil {
		t.Fatal("nil consumer had a receipt")
	}
	consumer = new(Consumer)
	if consumer.Close() != nil || consumer.Receipt() != nil {
		t.Fatal("zero consumer had a receipt")
	}
	if _, err := consumer.Next(deadline(t), correlation("zero")); !errors.Is(err, ErrInput) {
		t.Fatal("zero consumer admitted work")
	}
}
