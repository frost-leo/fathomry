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

package trino

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/invocation"
	native "github.com/trinodb/trino-go-client/trino"
)

func readerFinal(t *testing.T, reader *Reader, receipt *invocation.Receipt[Result]) invocation.Result[Result] {
	t.Helper()
	result, err := receipt.WaitReleased(deadline(t))
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-reader.Done():
	case <-time.After(time.Second):
		t.Fatal("reader completion not joined")
	}
	return result
}

func TestReaderExactPagesAndTerminalSummary(t *testing.T) {
	var base string
	var requests atomic.Int64
	_, options := peer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "DELETE" {
			t.Error("unexpected cancellation")
			w.WriteHeader(204)
			return
		}
		page := requests.Add(1)
		body := map[string]any{"id": "reader", "columns": []any{column("x", "varchar", long(2147483647))}}
		if page < 3 {
			body["nextUri"] = base + "/v1/statement/executing/reader/slug/" + strconv.FormatInt(page, 10)
		}
		if page != 2 {
			body["data"] = [][]any{{strings.Repeat("x", 1024)}}
		}
		reply(t, w, body)
	})
	base = options.Endpoint
	options.MaxRows = 1
	options.MaxResultBytes = 512
	fixture := bindFixture(t, options, 1)
	reader, receipt, err := fixture.client.QueryPages(deadline(t), deadline(t), correlation("reader"), Statement{SQL: "SELECT exact_value"})
	if err != nil {
		t.Fatal(err)
	}
	offsets := []int{0, 1, 1}
	rows := []int{1, 0, 1}
	for i := range 3 {
		page, err := reader.Next(deadline(t))
		if err != nil {
			t.Fatal(err)
		}
		if page.Complete() || page.Sequence() != uint64(i+1) || page.Offset() != offsets[i] || page.Rows() != rows[i] || len(page.ColumnsCopy()) != 1 {
			t.Fatal("page exactness/provenance changed")
		}
		raw := page.DataCopy()
		raw[0] = '!'
		columns := page.ColumnsCopy()
		columns[0].Signature[0] = '!'
		if page.DataCopy()[0] != '[' || page.ColumnsCopy()[0].Signature[0] != '{' {
			t.Fatal("page copy aliases")
		}
	}
	if _, err := reader.Next(deadline(t)); err != io.EOF {
		t.Fatalf("not complete EOF: %v", err)
	}
	result := readerFinal(t, reader, receipt)
	terminal := success(t, result)
	if terminal.Rows() != 2 || terminal.Pages() != 3 || terminal.Submissions() != 1 || string(terminal.DataCopy()) != "[]" || terminal.Sequence() != 0 {
		t.Fatal("terminal retained entire output or lost facts")
	}
	if err := reader.Close(deadline(t)); err != nil {
		t.Fatal(err)
	}
}

func TestReaderLargeOutputAndBoundedPrefetch(t *testing.T) {
	const pages = 132
	var base string
	var requests atomic.Int64
	rows := make([][]any, 64)
	for i := range rows {
		rows[i] = []any{strings.Repeat("z", 4096)}
	}
	_, options := peer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "DELETE" {
			w.WriteHeader(204)
			return
		}
		page := requests.Add(1)
		body := map[string]any{"id": "large", "columns": []any{column("x", "varchar", long(4096))}, "data": rows}
		if page < pages {
			body["nextUri"] = base + "/v1/statement/executing/large/slug/" + strconv.FormatInt(page, 10)
		}
		reply(t, w, body)
	})
	base = options.Endpoint
	options.ReadTimeout = 30 * time.Second
	fixture := bindFixture(t, options, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	reader, receipt, err := fixture.client.QueryPages(ctx, deadline(t), correlation("large"), Statement{SQL: "SELECT exact_value"})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(10 * time.Millisecond)
	if requests.Load() > 1 {
		t.Fatal("absent reader prefetched more than one page")
	}
	var bytes, received int
	for {
		page, err := reader.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		bytes += len(page.DataCopy())
		received++
		if received%16 == 0 {
			time.Sleep(time.Millisecond)
			if requests.Load() > int64(received+1) {
				t.Fatal("slow reader prefetched beyond one page")
			}
		}
	}
	result := readerFinal(t, reader, receipt)
	if bytes <= 32<<20 || received != pages || result.Err() != nil || !result.Outcome.Value.Complete() || result.Outcome.Value.Rows() != pages*64 {
		t.Fatal("large complete output not consumed")
	}
}

func TestReaderFailuresNeverBecomeEOF(t *testing.T) {
	for _, mode := range []string{"late", "columns", "rows", "pages", "wire", "page-bytes", "spooling", "session", "cancel", "cleanup"} {
		t.Run(mode, func(t *testing.T) {
			var base string
			var requests, deletes atomic.Int64
			_, options := peer(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method == "DELETE" {
					deletes.Add(1)
					if mode == "cleanup" {
						w.WriteHeader(500)
					} else {
						w.WriteHeader(204)
					}
					return
				}
				page := requests.Add(1)
				body := map[string]any{"id": "fault", "columns": []any{column("x", "bigint")}, "data": [][]int{{1}}}
				if page == 1 || mode == "cancel" || mode == "cleanup" || mode == "pages" || mode == "wire" {
					body["nextUri"] = base + "/v1/statement/executing/fault/slug/" + strconv.FormatInt(page, 10)
				}
				if page > 1 {
					switch mode {
					case "late":
						body["error"] = map[string]any{"errorName": "INTERNAL_ERROR", "errorType": "INTERNAL_ERROR", "errorCode": 7, "message": "private native cause"}
					case "columns":
						body["columns"] = []any{column("x", "boolean")}
					case "spooling":
						body["data"] = map[string]any{"encoding": "json", "segments": []any{}}
					case "session":
						w.Header().Set("X-Trino-Set-Session", "x=y")
					case "page-bytes":
						body["stats"] = map[string]any{"state": strings.Repeat("x", 2048)}
					}
				}
				reply(t, w, body)
			})
			base = options.Endpoint
			switch mode {
			case "rows":
				options.MaxReadRows = 1
			case "pages":
				options.MaxReadPages = 1
			case "wire":
				options.MaxPageBytes = 512
				options.MaxReadWireBytes = 512
			case "page-bytes":
				options.MaxPageBytes = 512
			}
			fixture := bindFixture(t, options, 1)
			reader, receipt, err := fixture.client.QueryPages(deadline(t), deadline(t), correlation(mode), Statement{SQL: "SELECT 1"})
			if err != nil {
				t.Fatal(err)
			}
			page, err := reader.Next(deadline(t))
			if err != nil || page.Rows() != 1 || page.Complete() {
				t.Fatal("prefix missing")
			}
			if mode == "cancel" || mode == "cleanup" {
				_ = reader.Close(deadline(t))
			}
			for {
				_, err = reader.Next(deadline(t))
				if err != nil {
					break
				}
			}
			if err == io.EOF {
				t.Fatal("failed prefix became complete")
			}
			result := readerFinal(t, reader, receipt)
			if result.Err() == nil || result.Outcome.Value.Complete() || result.Outcome.Value.Rows() < 1 {
				t.Fatal("missing terminal failure evidence")
			}
			if mode == "late" {
				var nativeError *native.ErrTrino
				if !errors.As(result.Err(), &nativeError) {
					t.Fatal("native server cause lost")
				}
			}
			if mode == "cleanup" && (result.Outcome.Cleanup == nil || deletes.Load() != 1 || result.Outcome.Value.CancellationAcknowledged()) {
				t.Fatal("cleanup evidence incorrect")
			}
		})
	}
}

func TestReaderWaitCancellationInputFreezeAndSaturation(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	var payload string
	var posts atomic.Int64
	_, options := peer(t, func(w http.ResponseWriter, r *http.Request) {
		posts.Add(1)
		close(entered)
		<-release
		body, _ := io.ReadAll(r.Body)
		payload = string(body)
		reply(t, w, map[string]any{"id": "freeze", "columns": []any{column("x", "bigint")}, "data": [][]int{{1}}})
	})
	fixture := bindFixture(t, options, 1)
	binary := []byte{0, 255}
	array := []any{"original", []int64{7}}
	input := Statement{SQL: "SELECT ?,?", Args: []any{binary, array}}
	reader, receipt, err := fixture.client.QueryPages(deadline(t), deadline(t), correlation("freeze"), input)
	if err != nil {
		t.Fatal(err)
	}
	binary[0] = 42
	array[0] = "changed"
	array[1].([]int64)[0] = 99
	input.Args[0] = "changed"
	input.SQL = "DELETE FROM x"
	<-entered
	cancelled, stop := context.WithCancel(context.Background())
	stop()
	if _, err := reader.Next(cancelled); !errors.Is(err, context.Canceled) {
		t.Fatal("wait cancellation ignored")
	}
	if _, second, err := fixture.client.QueryPages(deadline(t), deadline(t), correlation("full"), Statement{SQL: "SELECT 1"}); err == nil || second != nil || posts.Load() != 1 {
		t.Fatal("native evidence saturation bypassed")
	}
	once.Do(func() { close(release) })
	if _, err := reader.Next(deadline(t)); err != nil {
		t.Fatal(err)
	}
	if _, err := reader.Next(deadline(t)); err != io.EOF {
		t.Fatal(err)
	}
	result := readerFinal(t, reader, receipt)
	if result.Err() != nil || !strings.Contains(payload, "X'00ff'") || !strings.Contains(payload, "original") || strings.Contains(payload, "changed") || strings.Contains(payload, "99") {
		t.Fatal("borrowed asynchronous input changed")
	}
}

func TestReaderAbandonmentAndTimedCloseRetainSource(t *testing.T) {
	for _, mode := range []string{"absent", "close"} {
		t.Run(mode, func(t *testing.T) {
			var base string
			var requests, deletes atomic.Int64
			deleteEntered := make(chan struct{})
			release := make(chan struct{})
			var once sync.Once
			t.Cleanup(func() { once.Do(func() { close(release) }) })
			_, options := peer(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method == "DELETE" {
					deletes.Add(1)
					close(deleteEntered)
					<-release
					w.WriteHeader(204)
					return
				}
				page := requests.Add(1)
				reply(t, w, map[string]any{"id": "abandon", "nextUri": base + "/v1/statement/executing/abandon/slug/" + strconv.FormatInt(page, 10)})
			})
			base = options.Endpoint
			options.ReadTimeout = 30 * time.Millisecond
			options.CleanupTimeout = time.Second
			fixture := bindFixture(t, options, 1)
			reader, receipt, err := fixture.client.QueryPages(deadline(t), deadline(t), correlation(mode), Statement{SQL: "SELECT 1"})
			if err != nil {
				t.Fatal(err)
			}
			if mode == "close" {
				for requests.Load() == 0 {
					time.Sleep(time.Millisecond)
				}
				wait, stop := context.WithTimeout(context.Background(), time.Millisecond)
				err = reader.Close(wait)
				stop()
				if err == nil {
					t.Fatal("close wait unexpectedly joined blocked cleanup")
				}
			}
			<-deleteEntered
			if result, _ := receipt.Result(); result.Released {
				t.Fatal("cleanup still active but source released")
			}
			wait, stop := context.WithTimeout(context.Background(), time.Millisecond)
			if err := fixture.assembly.Close(wait); err == nil {
				t.Fatal("source released under blocked cleanup")
			}
			stop()
			if requests.Load() != 1 {
				t.Fatal("absent consumer allowed prefetch")
			}
			once.Do(func() { close(release) })
			if err := reader.Close(deadline(t)); err == nil {
				t.Fatal("cancellation outcome erased")
			}
			result := readerFinal(t, reader, receipt)
			if result.Outcome.Value.Complete() || deletes.Load() != 1 || !result.Outcome.Value.CancellationAcknowledged() {
				t.Fatal("abandonment evidence incorrect")
			}
		})
	}
}

func TestReaderSuccessfulEmptyAndRejectedInput(t *testing.T) {
	for _, invalid := range []bool{false, true} {
		t.Run(fmt.Sprint(invalid), func(t *testing.T) {
			var posts atomic.Int64
			_, options := peer(t, func(w http.ResponseWriter, r *http.Request) {
				posts.Add(1)
				reply(t, w, map[string]any{"id": "empty", "columns": []any{column("x", "bigint")}, "data": [][]int{}})
			})
			fixture := bindFixture(t, options, 1)
			statement := Statement{SQL: "SELECT 1 WHERE FALSE"}
			if invalid {
				statement.SQL = "DELETE FROM x"
			}
			reader, receipt, err := fixture.client.QueryPages(deadline(t), deadline(t), correlation("empty"), statement)
			if err != nil {
				t.Fatal(err)
			}
			page, err := reader.Next(deadline(t))
			if invalid {
				if err == nil || err == io.EOF || posts.Load() != 0 {
					t.Fatal("invalid reader submitted")
				}
			} else {
				if err != nil || page.Rows() != 0 || page.Complete() || string(page.DataCopy()) != "[]" {
					t.Fatal("successful empty page lost")
				}
				if _, err = reader.Next(deadline(t)); err != io.EOF {
					t.Fatal(err)
				}
			}
			result := readerFinal(t, reader, receipt)
			if result.Outcome.Value.Complete() == invalid {
				t.Fatal("empty completeness changed")
			}
		})
	}
}
