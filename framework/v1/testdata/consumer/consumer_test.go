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

package consumer

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	configsource "github.com/frost-leo/fathomry/adapters/configsource/v1"
	viper "github.com/frost-leo/fathomry/adapters/configsource/viper/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	framework "github.com/frost-leo/fathomry/framework/v1"
)

func TestFrameworkComposition(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	declaration, err := configsource.Prepare(ctx, configsource.Schema[framework.Options]{Version: 1}, []configsource.Layer{{Kind: configsource.Base, Encoding: configsource.YAML, Content: []byte("operations: {max_active: 3}\nresources: {max_bindings: 4}\n")}})
	if err != nil {
		t.Fatal(err)
	}
	options, err := declaration.ValueCopy()
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := framework.New(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close(context.Background())
	inbox, err := adapters.NewInbox[viper.Evidence](adapters.EvidenceOptions{})
	if err != nil {
		t.Fatal(err)
	}
	delivered := make(chan string, 8)
	receiver, err := framework.StartReceiver(context.Background(), inbox, framework.ReceiverOptions{}, func(_ context.Context, value adapters.Snapshot[viper.Evidence]) error {
		delivered <- value.Info().Operation
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer receiver.Close(context.Background())
	client, err := viper.New(viper.Dependencies{Runtime: runtime.Operations(), Evidence: inbox})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "configuration.yaml")
	if err := os.WriteFile(path, []byte("value: original"), 0600); err != nil {
		t.Fatal(err)
	}
	source, err := client.Source(viper.WatchSettings{Paths: []string{path}})
	if err != nil {
		t.Fatal(err)
	}
	observer, err := source.Observe(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer observer.Close(context.Background())
	if value, err := observer.Next(ctx); err != nil || value.Err != nil {
		t.Fatal(err, value.Err)
	}
	if _, _, err := client.RawFile(ctx, path, viper.MaxDocumentBytes); err != nil {
		t.Fatal(err)
	}
	select {
	case operation := <-delivered:
		if operation != "config.viper.raw_file" {
			t.Fatal("live stream blocked finite evidence")
		}
	case <-ctx.Done():
		t.Fatal("finite evidence not received")
	}
	if err := runtime.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := observer.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := receiver.Finish(ctx); err != nil {
		t.Fatal(err)
	}
	status, err := receiver.Status()
	if err != nil || status.Delivered != 2 || !status.Stopped {
		t.Fatal("custody not completed", err)
	}
}
