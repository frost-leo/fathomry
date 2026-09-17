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
	"bytes"
	"encoding/csv"
	"image"
	"image/png"
	"strings"
	"testing"
	"time"

	mail "github.com/frost-leo/fathomry/internal/notification/mail/v0"
)

func samplePNG(t testing.TB) []byte {
	t.Helper()
	var output bytes.Buffer
	if err := png.Encode(&output, image.NewRGBA(image.Rect(0, 0, 2, 2))); err != nil {
		t.Fatal(err)
	}
	return output.Bytes()
}

func sampleRepositorySnapshot() (repositorySnapshot, time.Time) {
	now := time.Date(2026, 9, 17, 15, 0, 0, 0, time.UTC)
	since := time.Date(2025, 9, 18, 0, 0, 0, 0, time.UTC)
	created := "2026-09-07T18:07:21Z"
	snapshot := repositorySnapshot{Version: 1, Repository: "frost-leo/fathomry", RepositoryID: 1360531276,
		Branch: "develop", Head: strings.Repeat("a", 40), CapturedAt: now.Format(time.RFC3339), CompletedAt: now.Format(time.RFC3339),
		CreatedAt: created, Since: since.Format(time.RFC3339), Stars: 1, Commits: 2, Languages: map[string]int{"Go": 900, "Shell": 100},
		StarWeeks: []reportWeek{{Week: now.AddDate(0, 0, -7).Unix(), Total: 1, Days: []int{1, 0, 0, 0, 0, 0, 0}}}}
	for cursor := since; !cursor.After(now); cursor = cursor.AddDate(0, 0, 1) {
		day := cursor.Format("2006-01-02")
		count := 0
		if day == "2026-09-17" {
			count = 2
		}
		snapshot.Calendar = append(snapshot.Calendar, reportDay{Day: day, Count: count, Applicable: day >= created[:10]})
	}
	for offset := -11; offset <= 0; offset++ {
		month := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC).AddDate(0, offset, 0).Format("2006-01")
		snapshot.Months = append(snapshot.Months, reportMonth{Month: month, Applicable: month >= created[:7]})
	}
	snapshot.Months[11].Issues, snapshot.Months[11].PRs = 2, 1
	return snapshot, now
}

func TestRepositoryReportPreservesFourCIDAssetsAndCSV(t *testing.T) {
	snapshot, now := sampleRepositorySnapshot()
	assets := map[string][]byte{}
	for _, key := range []string{"commits", "stars", "languages", "issues"} {
		assets[key] = samplePNG(t)
	}
	base, err := welcomeContent("sender@fixture.test", "to@fixture.test", "report@fixture.test")
	if err != nil {
		t.Fatal(err)
	}
	content, err := repositoryContent(base, snapshot, assets, now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mail.NewMessage(content); err != nil {
		t.Fatal(err)
	}
	images := 0
	for _, asset := range content.Inline {
		key := strings.TrimSuffix(asset.ID, "@fathomry.test")
		if !bytes.Equal(asset.Data, assets[key]) {
			t.Fatal("report CID input bytes changed")
		}
		images++
	}
	csvSeen := len(content.Attachments) == 1
	if csvSeen {
		records, err := csv.NewReader(bytes.NewReader(content.Attachments[0].Data)).ReadAll()
		if err != nil {
			t.Fatal(err)
		}
		counts := map[string]string{}
		for _, record := range records[1:] {
			counts[record[0]+"/"+record[1]+"/"+record[2]] = record[3]
		}
		if counts["opened/issues/2026-09"] != "2" || counts["opened/prs/2026-09"] != "1" ||
			counts["commits/day/2025-09-18"] != "not_applicable" || counts["commits/day/2026-09-17"] != "2" {
			t.Fatal("report definitions or CSV values changed")
		}
	}
	if images != 4 || !csvSeen || !strings.Contains(content.HTML, "As of") || !strings.Contains(content.Text, "Current stars: 1") {
		t.Fatal("report is missing required charts or independent text/data alternatives")
	}
	for _, asset := range content.Inline {
		if !strings.Contains(content.HTML, "cid:"+asset.ID) {
			t.Fatal("HTML template lost a CID association")
		}
	}
}

func TestRepositoryReportRejectsIncompleteOrStaleSnapshots(t *testing.T) {
	for name, change := range map[string]func(*repositorySnapshot){
		"identity":            func(s *repositorySnapshot) { s.RepositoryID++ },
		"incomplete-calendar": func(s *repositorySnapshot) { s.Calendar = s.Calendar[:10] },
		"missing-day":         func(s *repositorySnapshot) { s.Calendar[1].Day = s.Calendar[0].Day },
		"false-zero":          func(s *repositorySnapshot) { s.Calendar[0].Applicable = true },
		"wrong-total":         func(s *repositorySnapshot) { s.Commits++ },
		"partial-months":      func(s *repositorySnapshot) { s.Months = s.Months[:11] },
		"negative-count":      func(s *repositorySnapshot) { s.Months[11].PRs = -1 },
		"star-total":          func(s *repositorySnapshot) { s.StarWeeks[0].Total++ },
	} {
		t.Run(name, func(t *testing.T) {
			snapshot, now := sampleRepositorySnapshot()
			change(&snapshot)
			if validateSnapshot(snapshot, now) == nil {
				t.Fatal("invalid snapshot accepted")
			}
		})
	}
	snapshot, now := sampleRepositorySnapshot()
	if validateSnapshot(snapshot, now.Add(11*time.Minute)) == nil {
		t.Fatal("stale snapshot accepted as current")
	}
	base, err := welcomeContent("sender@fixture.test", "to@fixture.test", "missing@fixture.test")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repositoryContent(base, snapshot, nil, now); err == nil {
		t.Fatal("missing chart accepted")
	}
}
