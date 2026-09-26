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

package commandfamily

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sync/atomic"

	"github.com/spf13/cobra"
)

//go:embed resources/*.json
var resources embed.FS

// Backend is an invocation's typed fake operation dependency. Fields are configured
// before construction and then immutable; counters are independently inspectable.
type Backend struct {
	Opens          atomic.Int32
	Closes         atomic.Int32
	Calls          atomic.Int32
	Validations    atomic.Int32
	AcquireError   error
	OperationError error
	CleanupError   error
	EffectPath     string
	CleanupPath    string
	Started        chan struct{}
	Continue       <-chan struct{}
	CleanupGate    <-chan struct{}
	Wait           bool
	AfterEffect    func()
}

func (backend *Backend) acquire() error {
	backend.Opens.Add(1)
	return backend.AcquireError
}

func (backend *Backend) cleanup() error {
	if backend.CleanupGate != nil {
		<-backend.CleanupGate
	}
	backend.Closes.Add(1)
	var err error
	if backend.CleanupPath != "" {
		err = os.WriteFile(backend.CleanupPath, []byte("cleaned\n"), 0600)
	}
	return errors.Join(backend.CleanupError, err)
}

func (backend *Backend) record(ctx context.Context, value string) error {
	backend.Calls.Add(1)
	if backend.EffectPath != "" {
		file, err := os.OpenFile(backend.EffectPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
		if err != nil {
			return err
		}
		_, err = fmt.Fprintln(file, value)
		if err = errors.Join(err, file.Close()); err != nil {
			return err
		}
	}
	if backend.Started != nil {
		close(backend.Started)
	}
	if backend.AfterEffect != nil {
		backend.AfterEffect()
	}
	if backend.Wait {
		<-ctx.Done()
		return errors.Join(ctx.Err(), backend.OperationError)
	}
	return backend.OperationError
}

type prose struct{ en, zh map[string]string }

func loadProse() prose {
	load := func(path string) map[string]string {
		data, err := resources.ReadFile(path)
		if err != nil {
			panic(err)
		}
		var resource struct{ Messages map[string]string }
		if err := json.Unmarshal(data, &resource); err != nil {
			panic(err)
		}
		return resource.Messages
	}
	return prose{load("resources/en.json"), load("resources/zh-cn.json")}
}

func (words prose) command(name string) *cobra.Command {
	return &cobra.Command{Use: name, Short: words.en[name], Args: cobra.NoArgs,
		Annotations: map[string]string{"fathomry.short.zh-CN": words.zh[name]}}
}

// New is metadata-only. Capability acquisition occurs in RunE after native
// argument, required-flag and group validation by the host.
func New(backend *Backend) *cobra.Command {
	words := loadProse()
	family := words.command("sample")
	family.Aliases = []string{"s"}
	family.ValidArgsFunction = func(*cobra.Command, []string, string) ([]string, cobra.ShellCompDirective) {
		panic("completion must not execute")
	}
	nested := words.command("nested")
	record := words.command("record")
	record.Args = func(command *cobra.Command, args []string) error {
		backend.Validations.Add(1)
		return cobra.NoArgs(command, args)
	}
	var value string
	record.Flags().StringVar(&value, "value", "DEFAULT-CANARY", words.en["value"])
	record.Flags().Lookup("value").Annotations = map[string][]string{"fathomry.usage.zh-CN": {words.zh["value"]}}
	if err := record.MarkFlagRequired("value"); err != nil {
		panic(err)
	}
	record.Flags().Bool("left", false, words.en["left"])
	record.Flags().Bool("right", false, words.en["right"])
	record.MarkFlagsMutuallyExclusive("left", "right")
	record.RunE = func(command *cobra.Command, args []string) (err error) {
		defer func() { err = errors.Join(err, backend.cleanup()) }()
		if err = backend.acquire(); err != nil {
			return err
		}
		if err = backend.record(command.Context(), value); err != nil {
			return err
		}
		command.Print("{\"accepted\":true}\n")
		return nil
	}
	nested.AddCommand(record)
	raw := words.command("raw")
	raw.Args = cobra.ArbitraryArgs
	raw.RunE = func(command *cobra.Command, args []string) error {
		backend.Calls.Add(1)
		_, err := io.Copy(command.OutOrStdout(), command.InOrStdin())
		return err
	}
	stream := words.command("stream")
	stream.RunE = func(command *cobra.Command, args []string) error {
		backend.Calls.Add(1)
		if _, err := io.WriteString(command.OutOrStdout(), "prefix\n"); err != nil {
			return err
		}
		if backend.Started != nil {
			close(backend.Started)
		}
		select {
		case <-backend.Continue:
		case <-command.Context().Done():
			return command.Context().Err()
		}
		_, err := io.WriteString(command.OutOrStdout(), "suffix\n")
		return err
	}
	family.AddCommand(nested, raw, stream)
	addLeaf(family)
	return family
}

func addLeaf(family *cobra.Command) {
	words := loadProse()
	leaf := words.command("second")
	leaf.RunE = func(command *cobra.Command, args []string) error {
		_, err := io.WriteString(command.OutOrStdout(), "{\"leaf\":2}\n")
		return err
	}
	family.AddCommand(leaf)
}
