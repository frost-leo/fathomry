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

// Package query is a test-only, separately compiled command family. It is not
// linked into the product executable and supplies no real query semantics.
package query

import (
	"context"
	_ "embed"
	"io"
	"strings"

	"github.com/frost-leo/fathomry/cmd/fathomry/internal/cli"
	"github.com/frost-leo/fathomry/failure"
	"github.com/frost-leo/fathomry/i18n"
	"github.com/spf13/cobra"
)

const (
	GroupHelpID    = "fathomry.cli.test.query.help"
	GroupSummaryID = "fathomry.cli.test.query.summary"
	failureCode    = failure.Code("fathomry.cli.test.query_failure")
)

//go:embed locales/en.json
var english string

//go:embed locales/zh-Hans.json
var chinese string

// Options holds only a fake capability opener and a bounded test output size.
type Options struct {
	Open             func(context.Context) (func(), error)
	Bytes            int
	Fail             bool
	IgnoreWriteError bool
}

// New returns a nested test-only command without invoking Open.
func New(call *cli.Invocation, options Options) *cobra.Command {
	group := &cobra.Command{Use: "query"}
	count := 1
	operation := &cobra.Command{
		Use: "run", Args: cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			if count < 1 {
				return cli.Usage()
			}
			if options.Open == nil {
				return cli.Execution(failureCode, nil, Resources()...)
			}
			if err := call.CheckContext(); err != nil {
				return err
			}
			close, err := options.Open(command.Context())
			if err != nil {
				return cli.Execution(failureCode, err, Resources()...)
			}
			defer close()
			_, err = io.WriteString(command.OutOrStdout(), strings.Repeat("q", options.Bytes))
			if err != nil && !options.IgnoreWriteError {
				return cli.Execution(failureCode, err, Resources()...)
			}
			if options.Fail {
				return cli.Execution(failureCode, nil, Resources()...)
			}
			return nil
		},
	}
	operation.Flags().IntVar(&count, "count", 1, "")
	call.Add(group, operation, "fathomry.cli.test.query.run.help", "fathomry.cli.test.query.run.summary", Resources()...)
	return group
}

// Resources returns the test command's independent localized resource files.
func Resources() []i18n.Resource {
	return []i18n.Resource{
		{Name: "fathomry/cli/test-query/en.json", Data: []byte(english)},
		{Name: "fathomry/cli/test-query/zh-Hans.json", Data: []byte(chinese)},
	}
}
