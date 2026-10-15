/*
fathomry
Copyright (C) 2026  Frost Leo
SPDX-License-Identifier: GPL-3.0-or-later

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.
*/

package main

import (
	"context"
	"errors"
	"testing"

	mail "github.com/frost-leo/fathomry/adapters/notification/mail/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
)

func TestOwnerlessOpenEvidenceIsObserved(t *testing.T) {
	for _, admitted := range []bool{false, true} {
		t.Run(map[bool]string{false: "no-record", true: "accepted-failure"}[admitted], func(t *testing.T) {
			policy, err := mail.Recommend(localSettings())
			if err != nil {
				t.Fatal(err)
			}
			runtime, err := adapters.New(context.Background(), policy.Runtime)
			if err != nil {
				t.Fatal(err)
			}
			defer runtime.Close(context.Background())
			inbox, err := adapters.NewInbox[mail.Result](policy.Evidence)
			if err != nil {
				t.Fatal(err)
			}
			if admitted {
				endpoint, err := adapters.Bind(runtime, adapters.Declaration[mail.Result]{Evidence: inbox, Copy: func(value mail.Result) mail.Result { return value }})
				if err != nil {
					t.Fatal(err)
				}
				receipt, err := endpoint.Run(context.Background(), adapters.Request{Operation: "notification.mail.open", WorkBytes: policy.SourceWorkBytes, EvidenceBytes: policy.SourceEvidenceBytes}, func(call *adapters.Call[mail.Result]) {
					if err := call.Resolve(adapters.Outcome[mail.Result]{Primary: context.Canceled}); err != nil {
						t.Error(err)
					}
				})
				if err != nil {
					t.Fatal(err)
				}
				if _, err := receipt.WaitReleased(context.Background()); err != nil {
					t.Fatal(err)
				}
			}
			summary := new(submission)
			session := &mailSession{runtime: runtime, inbox: inbox, summary: summary}
			err = session.close()
			if admitted && !errors.Is(err, context.Canceled) || !admitted && err != nil {
				t.Fatal("failed-open primary fact lost", err)
			}
			if !summary.RuntimeClosed || summary.EvidenceOutstanding != 0 || summary.PrimaryFailure != admitted || session.sourceAcked != admitted {
				t.Fatalf("ownerless open evidence not observed: %+v", summary)
			}
			want := 0
			if admitted {
				want = 1
			}
			if summary.EvidenceRecords != want {
				t.Fatal("ownerless record count changed")
			}
		})
	}
}
