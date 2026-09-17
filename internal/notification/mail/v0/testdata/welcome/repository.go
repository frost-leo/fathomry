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
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"image/png"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	mail "github.com/frost-leo/fathomry/internal/notification/mail/v0"
)

type reportDay struct {
	Day        string
	Count      int
	Applicable bool
}
type reportMonth struct {
	Month       string
	Issues, PRs int
	Applicable  bool
}
type reportWeek struct {
	Week  int64
	Total int
	Days  []int
}
type repositorySnapshot struct {
	Version               int
	Repository            string
	RepositoryID          int64 `json:"repository_id"`
	Branch, Head          string
	CapturedAt            string `json:"captured_at"`
	CompletedAt           string `json:"capture_completed_at"`
	CreatedAt             string `json:"created_at"`
	Since                 string
	Stars, Forks, Commits int
	Calendar              []reportDay
	Months                []reportMonth
	Languages             map[string]int
	StarWeeks             []reportWeek `json:"star_weeks"`
}

func validateSnapshot(snapshot repositorySnapshot, now time.Time) error {
	invalid := errors.New("invalid or stale repository snapshot")
	captured, err := time.Parse(time.RFC3339Nano, snapshot.CapturedAt)
	if err != nil || captured.After(now.Add(time.Second)) || now.Sub(captured) > 10*time.Minute {
		return invalid
	}
	completed, err := time.Parse(time.RFC3339Nano, snapshot.CompletedAt)
	if err != nil || completed.Before(captured) || completed.After(now.Add(time.Second)) {
		return invalid
	}
	created, err := time.Parse(time.RFC3339, snapshot.CreatedAt)
	if err != nil || created.After(captured) {
		return invalid
	}
	since, err := time.Parse(time.RFC3339, snapshot.Since)
	if err != nil || since.After(captured) {
		return invalid
	}
	if snapshot.Version != 1 || snapshot.Repository != "frost-leo/fathomry" || snapshot.RepositoryID != 1360531276 ||
		len(snapshot.Head) != 40 || snapshot.Branch == "" || len(snapshot.Branch) > 128 || !reportLabel(snapshot.Branch, 128) ||
		snapshot.Stars < 0 || snapshot.Forks < 0 || snapshot.Commits < 0 || len(snapshot.Calendar) < 365 || len(snapshot.Calendar) > 366 ||
		len(snapshot.Months) != 12 || len(snapshot.Languages) > 64 || len(snapshot.StarWeeks) > 120 {
		return invalid
	}
	if _, err := hex.DecodeString(snapshot.Head); err != nil {
		return invalid
	}
	total := 0
	for index, day := range snapshot.Calendar {
		if day.Day != since.AddDate(0, 0, index).UTC().Format("2006-01-02") || day.Count < 0 || day.Count > 1000 ||
			day.Applicable != (day.Day >= created.UTC().Format("2006-01-02") || day.Count > 0) {
			return invalid
		}
		total += day.Count
	}
	if total != snapshot.Commits || total > 1000 || snapshot.Calendar[len(snapshot.Calendar)-1].Day != captured.UTC().Format("2006-01-02") {
		return invalid
	}
	monthTotal := 0
	for index, month := range snapshot.Months {
		expected := time.Date(captured.Year(), captured.Month(), 1, 0, 0, 0, 0, time.UTC).AddDate(0, index-11, 0).Format("2006-01")
		if month.Month != expected || month.Issues < 0 || month.PRs < 0 || month.Issues > 1000 || month.PRs > 1000 ||
			month.Applicable != (month.Month >= created.UTC().Format("2006-01")) || !month.Applicable && (month.Issues != 0 || month.PRs != 0) {
			return invalid
		}
		monthTotal += month.Issues + month.PRs
	}
	if monthTotal > 1000 {
		return invalid
	}
	for name, count := range snapshot.Languages {
		if !reportLabel(name, 80) || name == "" || count < 0 || int64(count) > 1<<40 {
			return invalid
		}
	}
	for index, week := range snapshot.StarWeeks {
		if week.Week <= 0 || week.Total < 0 || week.Total > 1000000000 || len(week.Days) != 7 || index > 0 && week.Week <= snapshot.StarWeeks[index-1].Week {
			return invalid
		}
		sum := 0
		for _, count := range week.Days {
			if count < 0 || count > 1000000000 {
				return invalid
			}
			sum += count
		}
		if sum != week.Total {
			return invalid
		}
	}
	return nil
}
func readReportFile(directory, name string, limit int) ([]byte, error) {
	file, err := os.Open(filepath.Join(directory, name))
	if err != nil {
		return nil, errors.New("repository report file unavailable")
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, int64(limit)+1))
	if err != nil || len(data) > limit {
		return nil, errors.New("repository report file exceeds its bound")
	}
	return data, nil
}

const repositorySection = `<tr><td class="gutter" style="padding:12px 40px 48px;">
<p style="margin:0 0 8px;font-size:11px;letter-spacing:2px;color:#707078;">A YEAR IN VIEW</p>
<h2 style="margin:0 0 16px;font:600 32px/1.2 Arial,sans-serif;letter-spacing:-1px;color:#171719;">Our story,<br>still unfolding.</h2>
<p style="margin:0 0 20px;font:12px/20px Arial,sans-serif;color:#707078;">{{.Repository}} · {{.Branch}}<br>As of {{.CapturedAt}}</p>
<table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="border-top:1px solid #e5e5ea;border-bottom:1px solid #e5e5ea;"><tr>
<td style="padding:22px 0;"><span style="font:600 30px Arial;color:#AF52DE;">{{.Stars}}</span><br><span style="font:12px Arial;color:#707078;">Current stars</span></td>
<td style="padding:22px 0;"><span style="font:600 30px Arial;color:#30B0C7;">{{.Forks}}</span><br><span style="font:12px Arial;color:#707078;">Forks</span></td>
<td style="padding:22px 0;"><span style="font:600 30px Arial;color:#34A853;">{{.Commits}}</span><br><span style="font:12px Arial;color:#707078;">Commits · past year</span></td>
</tr></table>
{{range .Charts}}
<h3 style="margin:34px 0 8px;font:600 21px/28px Arial,sans-serif;color:#1c1c1e;">{{.Title}}</h3>
<p style="margin:0 0 12px;font:12px/20px Arial,sans-serif;color:#707078;">{{.Summary}}</p>
<img src="cid:{{.ID}}" alt="{{.Summary}}" width="600" style="display:block;width:100%;max-width:600px;height:auto;border:0;">
{{end}}
<p style="margin:20px 0 0;font:11px/18px Arial,sans-serif;color:#707078;">The little details are in the attached CSV.</p>
</td></tr>`

type reportChart struct{ Key, ID, Title, Summary string }

func repositoryContent(content mail.Content, snapshot repositorySnapshot, assets map[string][]byte, now time.Time) (mail.Content, error) {
	if err := validateSnapshot(snapshot, now); err != nil {
		return mail.Content{}, err
	}
	charts := []reportChart{
		{"commits", "commits@fathomry.test", "Small steps. A lasting rhythm.", "Daily commits · past year"},
		{"stars", "stars@fathomry.test", "A little more starlight.", "New stars · weekly"},
		{"languages", "languages@fathomry.test", "Many parts. One purpose.", "Language share · bytes"},
		{"issues", "issues@fathomry.test", "Ideas, finding their way.", "Issues & pull requests opened · monthly"},
	}
	content.Inline = nil
	for _, chart := range charts {
		data, ok := assets[chart.Key]
		if !ok || len(data) == 0 || len(data) > 2<<20 {
			return mail.Content{}, errors.New("missing or oversized report chart")
		}
		config, err := png.DecodeConfig(bytes.NewReader(data))
		if err != nil || config.Width < 1 || config.Height < 1 || config.Width > 4096 || config.Height > 2048 {
			return mail.Content{}, errors.New("invalid report chart")
		}
		content.Inline = append(content.Inline, mail.Inline{ID: chart.ID, Name: chart.Key + ".png", ContentType: "image/png", Data: data})
	}
	captured, _ := time.Parse(time.RFC3339Nano, snapshot.CapturedAt)
	data := struct {
		Repository, Branch, CapturedAt string
		Stars, Forks, Commits          int
		Charts                         []reportChart
	}{
		snapshot.Repository, snapshot.Branch, captured.UTC().Format("02 Jan 2006 · 15:04 UTC"), snapshot.Stars, snapshot.Forks, snapshot.Commits, charts}
	var section bytes.Buffer
	if err := template.Must(template.New("repository").Parse(repositorySection)).Execute(&section, data); err != nil {
		return mail.Content{}, err
	}
	const marker = "<!-- REPOSITORY_REPORT -->"
	if strings.Count(content.HTML, marker) != 1 {
		return mail.Content{}, errors.New("welcome report insertion point missing")
	}
	content.HTML = strings.Replace(content.HTML, marker, section.String(), 1)
	content.Text += fmt.Sprintf("\nRepository snapshot: %s\nCaptured: %s\nBranch: %s at %s\nCurrent stars: %d\nForks: %d\nCommits in window: %d\n",
		snapshot.Repository, snapshot.CapturedAt, snapshot.Branch, snapshot.Head, snapshot.Stars, snapshot.Forks, snapshot.Commits)
	for _, chart := range charts {
		content.Text += chart.Title + "\n" + chart.Summary + "\n"
	}
	var csvData bytes.Buffer
	writer := csv.NewWriter(&csvData)
	_ = writer.Write([]string{"metric", "key", "date", "value"})
	_ = writer.Write([]string{"revision", snapshot.Branch, snapshot.CapturedAt, snapshot.Head})
	_ = writer.Write([]string{"repository_created", snapshot.Repository, snapshot.CreatedAt, ""})
	_ = writer.Write([]string{"commit_window", "UTC", snapshot.Since, snapshot.CapturedAt})
	_ = writer.Write([]string{"star_basis", "new_stars", "GitHub-defined weekly buckets", "not historical net totals"})
	_ = writer.Write([]string{"current_stars", "repository", snapshot.CapturedAt, strconv.Itoa(snapshot.Stars)})
	_ = writer.Write([]string{"forks", "repository", snapshot.CapturedAt, strconv.Itoa(snapshot.Forks)})
	for _, day := range snapshot.Calendar {
		value := "not_applicable"
		if day.Applicable {
			value = strconv.Itoa(day.Count)
		}
		_ = writer.Write([]string{"commits", "day", day.Day, value})
	}
	for _, month := range snapshot.Months {
		for _, pair := range []struct {
			key   string
			count int
		}{{"issues", month.Issues}, {"prs", month.PRs}} {
			value := "not_applicable"
			if month.Applicable {
				value = strconv.Itoa(pair.count)
			}
			_ = writer.Write([]string{"opened", pair.key, month.Month, value})
		}
	}
	names := make([]string, 0, len(snapshot.Languages))
	for name := range snapshot.Languages {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		safeName := name
		if strings.ContainsAny(name[:1], "=+-@") {
			safeName = "'" + name
		}
		_ = writer.Write([]string{"language_bytes", safeName, snapshot.CapturedAt, strconv.Itoa(snapshot.Languages[name])})
	}
	for _, week := range snapshot.StarWeeks {
		_ = writer.Write([]string{"new_stars", "github_week", time.Unix(week.Week, 0).UTC().Format(time.RFC3339), strconv.Itoa(week.Total)})
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		return mail.Content{}, err
	}
	content.Attachments = []mail.Attachment{{Name: "repository-snapshot.csv", ContentType: "text/csv; charset=utf-8", Data: csvData.Bytes()}}
	return content, nil
}
func loadRepositoryContent(content mail.Content, directory string, requireFresh bool) (mail.Content, error) {
	raw, err := readReportFile(directory, "snapshot.json", 256<<10)
	if err != nil {
		return mail.Content{}, err
	}
	var snapshot repositorySnapshot
	if json.Unmarshal(raw, &snapshot) != nil {
		return mail.Content{}, errors.New("repository snapshot is invalid")
	}
	assets := make(map[string][]byte)
	for _, key := range []string{"commits", "stars", "languages", "issues"} {
		assets[key], err = readReportFile(directory, key+".png", 2<<20)
		if err != nil {
			return mail.Content{}, err
		}
	}
	now := time.Now().UTC()
	if !requireFresh {
		now, err = time.Parse(time.RFC3339Nano, snapshot.CompletedAt)
		if err != nil {
			return mail.Content{}, errors.New("snapshot completion timestamp is invalid")
		}
	}
	return repositoryContent(content, snapshot, assets, now)
}

func reportLabel(value string, limit int) bool {
	if len(value) > limit || !utf8.ValidString(value) {
		return false
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return false
		}
	}
	return true
}
