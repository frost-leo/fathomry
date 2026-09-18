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
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
	"unicode"
	"unicode/utf8"

	lark "github.com/frost-leo/fathomry/internal/notification/lark/v3"
)

type reportDay struct {
	Day        string `json:"day"`
	Count      int    `json:"count"`
	Applicable bool   `json:"applicable"`
}
type reportMonth struct {
	Month      string `json:"month"`
	Issues     int    `json:"issues"`
	PRs        int    `json:"prs"`
	Applicable bool   `json:"applicable"`
}
type reportWeek struct {
	Week  int64 `json:"week"`
	Total int   `json:"total"`
	Days  []int `json:"days"`
}
type repositorySnapshot struct {
	Version      int            `json:"version"`
	Repository   string         `json:"repository"`
	RepositoryID int64          `json:"repository_id"`
	Branch       string         `json:"branch"`
	Head         string         `json:"head"`
	CapturedAt   string         `json:"captured_at"`
	CompletedAt  string         `json:"capture_completed_at"`
	CreatedAt    string         `json:"created_at"`
	Since        string         `json:"since"`
	Stars        int            `json:"stars"`
	Forks        int            `json:"forks"`
	Commits      int            `json:"commits"`
	Calendar     []reportDay    `json:"calendar"`
	Months       []reportMonth  `json:"months"`
	Languages    map[string]int `json:"languages"`
	StarWeeks    []reportWeek   `json:"star_weeks"`
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
		len(snapshot.Head) != 40 || snapshot.Branch == "" || len(snapshot.Branch) > 128 || !reportBranch(snapshot.Branch) ||
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

func reportBranch(value string) bool {
	for _, char := range value {
		if !(char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' || strings.ContainsRune("._/-", char)) {
			return false
		}
	}
	return true
}

func readSnapshot(path string, requireFresh bool) (repositorySnapshot, error) {
	var snapshot repositorySnapshot
	file, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return snapshot, errors.New("repository snapshot unavailable")
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() > 256<<10 {
		return snapshot, errors.New("invalid repository snapshot file")
	}
	raw, err := io.ReadAll(io.LimitReader(file, 256<<10+1))
	if err != nil || len(raw) > 256<<10 {
		return snapshot, errors.New("repository snapshot read failed")
	}
	if _, err := lark.NewJSON(raw); err != nil {
		return snapshot, errors.New("invalid repository JSON")
	}
	fields, err := requiredJSONFields(raw, "version", "repository", "repository_id", "branch", "head", "captured_at",
		"capture_completed_at", "created_at", "since", "stars", "forks", "commits", "calendar", "months", "languages", "star_weeks")
	if err != nil {
		return snapshot, err
	}
	for key, names := range map[string][]string{
		"calendar": {"day", "count", "applicable"}, "months": {"month", "issues", "prs", "applicable"},
		"star_weeks": {"week", "total", "days"},
	} {
		var entries []json.RawMessage
		if json.Unmarshal(fields[key], &entries) != nil {
			return snapshot, errors.New("invalid repository series")
		}
		for _, entry := range entries {
			object, err := requiredJSONFields(entry, names...)
			if err != nil {
				return snapshot, err
			}
			if key == "star_weeks" {
				var days []json.RawMessage
				if json.Unmarshal(object["days"], &days) != nil {
					return snapshot, errors.New("invalid star week")
				}
				for _, day := range days {
					if string(day) == "null" {
						return snapshot, errors.New("unknown star count")
					}
				}
			}
		}
	}
	languages, err := requiredJSONFields(fields["languages"])
	if err != nil {
		return snapshot, err
	}
	for _, count := range languages {
		if string(count) == "null" {
			return snapshot, errors.New("unknown language count")
		}
	}
	if json.Unmarshal(raw, &snapshot) != nil {
		return snapshot, errors.New("invalid repository values")
	}
	now := time.Now().UTC()
	if !requireFresh {
		now, err = time.Parse(time.RFC3339Nano, snapshot.CompletedAt)
		if err != nil {
			return snapshot, errors.New("invalid capture completion timestamp")
		}
	}
	if err := validateSnapshot(snapshot, now); err != nil {
		return repositorySnapshot{}, err
	}
	return snapshot, nil
}

func repositoryReport(path string, requireFresh bool) ([]json.RawMessage, []byte, error) {
	snapshot, err := readSnapshot(path, requireFresh)
	if err != nil {
		return nil, nil, err
	}
	var elements []json.RawMessage
	appendElement := func(value any) error {
		encoded, err := json.Marshal(value)
		if err == nil {
			elements = append(elements, encoded)
		}
		return err
	}
	if err := appendElement(map[string]any{"tag": "markdown", "text_size": "x-small", "margin": "32px 0px 8px 0px", "content": "<font color='welcome_muted'>A YEAR IN VIEW</font>"}); err != nil {
		return nil, nil, err
	}
	if err := appendElement(map[string]any{"tag": "markdown", "text_size": "heading-1", "margin": "0px 0px 16px 0px", "content": "**Our story,**<br>still unfolding."}); err != nil {
		return nil, nil, err
	}
	var columns []any
	for _, metric := range []struct {
		label, color string
		count        int
	}{
		{"Current stars", "purple", snapshot.Stars}, {"Forks", "turquoise", snapshot.Forks}, {"Commits · captured year", "green", snapshot.Commits},
	} {
		columns = append(columns, map[string]any{"tag": "column", "width": "weighted", "weight": 1, "elements": []any{
			map[string]any{"tag": "markdown", "text_size": "heading-1", "content": fmt.Sprintf("<font color='%s'>**%d**</font>", metric.color, metric.count)},
			map[string]any{"tag": "markdown", "text_size": "notation", "content": "<font color='welcome_muted'>" + metric.label + "</font>"},
		}})
	}
	if err := appendElement(map[string]any{"tag": "column_set", "flex_mode": "stretch", "margin": "0px 0px 20px 0px", "columns": columns}); err != nil {
		return nil, nil, err
	}
	var csvData bytes.Buffer
	writer := csv.NewWriter(&csvData)
	_ = writer.Write([]string{"metric", "key", "date", "value"})
	branch := snapshot.Branch
	if strings.ContainsAny(branch[:1], "=+-@") {
		branch = "'" + branch
	}
	_ = writer.Write([]string{"revision", branch, snapshot.CapturedAt, snapshot.Head})
	for _, item := range []struct {
		name  string
		count int
	}{{"current_stars", snapshot.Stars}, {"forks", snapshot.Forks}, {"commits_in_window", snapshot.Commits}} {
		_ = writer.Write([]string{item.name, snapshot.Repository, snapshot.CapturedAt, strconv.Itoa(item.count)})
	}
	type row map[string]any
	weekly := map[string]int{}
	for _, day := range snapshot.Calendar {
		count := "not_applicable"
		if day.Applicable {
			count = strconv.Itoa(day.Count)
			date, _ := time.Parse("2006-01-02", day.Day)
			monday := date.AddDate(0, 0, -(int(date.Weekday())+6)%7).Format("2006-01-02")
			weekly[monday] += day.Count
		}
		_ = writer.Write([]string{"commits", "day", day.Day, count})
	}
	weeks := make([]string, 0, len(weekly))
	for week := range weekly {
		weeks = append(weeks, week)
	}
	sort.Strings(weeks)
	var commits, languages, opened, stars []row
	for _, week := range weeks {
		commits = append(commits, row{"week": week, "commits": weekly[week]})
	}
	names := make([]string, 0, len(snapshot.Languages))
	for name := range snapshot.Languages {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		count := snapshot.Languages[name]
		if count > 0 {
			languages = append(languages, row{"language": name, "bytes": count})
		}
		safe := name
		if strings.ContainsAny(name[:1], "=+-@") {
			safe = "'" + name
		}
		_ = writer.Write([]string{"language_bytes", safe, snapshot.CapturedAt, strconv.Itoa(count)})
	}
	for _, month := range snapshot.Months {
		for _, item := range []struct {
			kind  string
			count int
		}{{"Issues", month.Issues}, {"PRs", month.PRs}} {
			count := "not_applicable"
			if month.Applicable {
				count = strconv.Itoa(item.count)
				opened = append(opened, row{"month": month.Month, "kind": item.kind, "opened": item.count})
			}
			_ = writer.Write([]string{"opened", item.kind, month.Month, count})
		}
	}
	for _, week := range snapshot.StarWeeks {
		date := time.Unix(week.Week, 0).UTC().Format("2006-01-02")
		stars = append(stars, row{"week": date, "new_stars": week.Total})
		_ = writer.Write([]string{"new_stars", "github_week", date, strconv.Itoa(week.Total)})
	}
	for _, chart := range []struct {
		id, title, kind, x, y string
		rows                  []row
	}{
		{"repo_commits", "Commits · UTC weeks · captured year", "line", "week", "commits", commits},
		{"repo_stars", "New stars · GitHub weekly buckets", "line", "week", "new_stars", stars},
		{"repo_languages", "Language share · bytes", "pie", "language", "bytes", languages},
		{"repo_opened", "Issues and pull requests opened · month", "bar", "month", "opened", opened},
	} {
		if len(chart.rows) == 0 {
			if err := appendElement(map[string]any{"tag": "markdown", "content": chart.title + ": no observations in this snapshot."}); err != nil {
				return nil, nil, err
			}
			continue
		}
		spec := map[string]any{"type": chart.kind, "title": map[string]string{"text": chart.title}, "data": map[string]any{"values": chart.rows}, "tooltip": map[string]bool{"visible": true}}
		if chart.kind == "pie" {
			spec["valueField"], spec["categoryField"] = chart.y, chart.x
			spec["legends"] = map[string]any{"visible": true, "orient": "bottom"}
		} else {
			spec["xField"], spec["yField"] = chart.x, chart.y
			if chart.kind == "bar" {
				spec["xField"], spec["seriesField"] = []string{"month", "kind"}, "kind"
			}
		}
		raw, err := json.Marshal(spec)
		if err != nil {
			return nil, nil, err
		}
		frozen, err := lark.NewJSON(raw)
		if err != nil {
			return nil, nil, err
		}
		element, err := lark.Chart(chart.id, frozen)
		if err != nil {
			return nil, nil, err
		}
		elements = append(elements, element.Bytes())
	}
	tableColumns, err := lark.NewJSON([]byte(`[{"name":"metric","display_name":"Captured metric","data_type":"text"},{"name":"count","display_name":"Count","data_type":"number"}]`))
	if err != nil {
		return nil, nil, err
	}
	raw, _ := json.Marshal([]row{{"metric": "Current stars", "count": snapshot.Stars}, {"metric": "Forks", "count": snapshot.Forks}, {"metric": "Commits in captured year", "count": snapshot.Commits}})
	rows, err := lark.NewJSON(raw)
	if err != nil {
		return nil, nil, err
	}
	table, err := lark.Table("repo_totals", tableColumns, rows)
	if err != nil {
		return nil, nil, err
	}
	elements = append(elements, table.Bytes())
	if err := appendElement(map[string]any{"tag": "collapsible_panel", "element_id": "repo_details", "expanded": false, "margin": "16px 0px 0px 0px", "vertical_spacing": "12px",
		"header": map[string]any{"title": map[string]string{"tag": "markdown", "content": "**The little details, all accounted for.**"}},
		"elements": []any{
			map[string]any{"tag": "markdown", "text_size": "notation", "content": fmt.Sprintf("Public repository: %s\nCaptured: %s\nBranch: %s\nRevision: %s", snapshot.Repository, snapshot.CapturedAt, snapshot.Branch, snapshot.Head)},
			map[string]any{"tag": "markdown", "text_size": "notation", "content": "The attached CSV contains the complete captured values. Before-creation dates are unavailable, not zero activity. Commit weeks use UTC Mondays and include partial boundary weeks. Star buckets are GitHub-defined new-star counts, not historical net totals. Language bytes are repository metadata, not lines of code. Capture timestamps describe these observations, not a continuously updated dashboard."},
		}}); err != nil {
		return nil, nil, err
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		return nil, nil, err
	}
	return elements, csvData.Bytes(), nil
}
