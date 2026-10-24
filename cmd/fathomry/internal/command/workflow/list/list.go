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

// Package list implements bounded catalog metadata pagination.
package list

import (
	"context"
	"fmt"
	"io"

	"github.com/frost-leo/fathomry/cmd/fathomry/internal/command"
	catalog "github.com/frost-leo/fathomry/framework/workflow/catalog/v1"
	"github.com/spf13/cobra"
)

// New declares a read-only leaf over an entry-owned lazy callback.
func New(invocation *command.Invocation, run func(context.Context, catalog.ListRequest) (catalog.Page, error)) *cobra.Command {
	var request catalog.ListRequest
	var enabled bool
	cmd := &cobra.Command{Use: "list", Short: "fathomry.workflow_list.command", Args: cobra.NoArgs}
	cmd.Flags().StringVar(&request.Parent.Namespace, "namespace", "", "fathomry.workflow_list.namespace")
	cmd.Flags().StringVar(&request.Parent.Code, "workflow", "", "fathomry.workflow_list.workflow")
	cmd.Flags().StringVar(&request.Parent.Version, "version", "", "fathomry.workflow_list.version")
	cmd.Flags().StringVar(&request.Parent.Mode, "mode", "", "fathomry.workflow_list.mode")
	cmd.Flags().StringVar(&request.Code, "code", "", "fathomry.workflow_list.code")
	cmd.Flags().StringVar(&request.Category, "category", "", "fathomry.workflow_list.category")
	cmd.Flags().BoolVar(&enabled, "enabled", false, "fathomry.workflow_list.enabled")
	cmd.Flags().StringVar(&request.Locale, "locale", "", "fathomry.workflow_list.locale")
	cmd.Flags().IntVar(&request.Limit, "limit", 20, "fathomry.workflow_list.limit")
	cmd.Flags().StringVar(&request.Cursor, "cursor", "", "fathomry.workflow_list.cursor")
	invocation.Bind(cmd, func(ctx context.Context, _ []string) error {
		if run == nil {
			return command.Fail(command.ErrOptions)
		}
		for _, flag := range []struct{ name, value string }{{"namespace", request.Parent.Namespace}, {"workflow", request.Parent.Code}, {"version", request.Parent.Version}, {"mode", request.Parent.Mode}, {"code", request.Code}, {"category", request.Category}, {"locale", request.Locale}, {"cursor", request.Cursor}} {
			if cmd.Flags().Changed(flag.name) && flag.value == "" {
				return command.Fail(command.ErrUsage)
			}
		}
		if cmd.Flags().Changed("limit") && request.Limit == 0 {
			return command.Fail(command.ErrUsage)
		}
		if cmd.Flags().Changed("enabled") {
			request.Enabled = &enabled
		}
		page, err := run(ctx, request)
		if err != nil {
			return err
		}
		return invocation.Result("workflow.list", page, func(writer io.Writer) error {
			for _, item := range page.Items {
				name := item.Display.Name.Value
				if !item.Display.Name.Present {
					name = item.Identity.Code
					if item.Identity.Version != "" {
						name = item.Identity.Version
					}
					if item.Identity.Mode != "" {
						name = item.Identity.Mode
					}
					if item.Identity.Revision != "" {
						name = item.Identity.Revision
					}
				}
				if _, err := fmt.Fprintf(writer, "%s %q/%q/%q/%q/%q %q\n", item.Kind, item.Identity.Namespace, item.Identity.Code, item.Identity.Version, item.Identity.Mode, item.Identity.Revision, name); err != nil {
					return err
				}
			}
			if page.Next != "" {
				_, err := fmt.Fprintf(writer, "%s %s\n", invocation.Text("fathomry.workflow_list.next"), page.Next)
				return err
			}
			return nil
		})
	})
	return cmd
}
