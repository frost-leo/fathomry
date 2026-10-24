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

// Package show implements an exact catalog metadata/document read.
package show

import (
	"context"
	"fmt"
	"io"

	"github.com/frost-leo/fathomry/cmd/fathomry/internal/command"
	catalog "github.com/frost-leo/fathomry/framework/workflow/catalog/v1"
	"github.com/spf13/cobra"
)

// New declares the read-only leaf without resolving settings or opening sources.
func New(invocation *command.Invocation, run func(context.Context, catalog.ReadRequest) (catalog.View, error)) *cobra.Command {
	var request catalog.ReadRequest
	cmd := &cobra.Command{Use: "show", Short: "fathomry.workflow_show.command", Args: cobra.NoArgs}
	cmd.Flags().StringVar(&request.Identity.Namespace, "namespace", "", "fathomry.workflow_show.namespace")
	cmd.Flags().StringVar(&request.Identity.Code, "workflow", "", "fathomry.workflow_show.workflow")
	cmd.Flags().StringVar(&request.Identity.Version, "version", "", "fathomry.workflow_show.version")
	cmd.Flags().StringVar(&request.Identity.Mode, "mode", "", "fathomry.workflow_show.mode")
	cmd.Flags().StringVar(&request.Identity.Revision, "revision", "", "fathomry.workflow_show.revision")
	cmd.Flags().StringVar(&request.Locale, "locale", "", "fathomry.workflow_show.locale")
	cmd.Flags().BoolVar(&request.Document, "document", false, "fathomry.workflow_show.document")
	invocation.Bind(cmd, func(ctx context.Context, _ []string) error {
		if run == nil {
			return command.Fail(command.ErrOptions)
		}
		for _, flag := range []struct{ name, value string }{{"namespace", request.Identity.Namespace}, {"workflow", request.Identity.Code}, {"version", request.Identity.Version}, {"mode", request.Identity.Mode}, {"revision", request.Identity.Revision}, {"locale", request.Locale}} {
			if cmd.Flags().Changed(flag.name) && flag.value == "" {
				return command.Fail(command.ErrUsage)
			}
		}
		value, err := run(ctx, request)
		if err != nil {
			return err
		}
		return invocation.Result("workflow.show", value, func(writer io.Writer) error {
			item := value.Entity
			if _, err := fmt.Fprintf(writer, "%s %q/%q/%q/%q/%q\n%s %s; %s %s\n", item.Kind, item.Identity.Namespace, item.Identity.Code, item.Identity.Version, item.Identity.Mode, item.Identity.Revision, invocation.Text("fathomry.workflow_show.id"), item.ID, invocation.Text("fathomry.workflow_show.row_revision"), item.RowRevision); err != nil {
				return err
			}
			if item.Display.Name.Present {
				if _, err := fmt.Fprintf(writer, "%s %q [%s; %s]\n", invocation.Text("fathomry.workflow_show.name"), item.Display.Name.Value, item.Display.Name.Locale, item.Display.Name.Fallback); err != nil {
					return err
				}
			}
			if item.Display.Description.Present {
				if _, err := fmt.Fprintf(writer, "%s %q [%s; %s]\n", invocation.Text("fathomry.workflow_show.description"), item.Display.Description.Value, item.Display.Description.Locale, item.Display.Description.Fallback); err != nil {
					return err
				}
			}
			for _, document := range value.Documents {
				if _, err := fmt.Fprintf(writer, "%s %s\n%s\n", document.DocumentFormat, document.SHA256, document.Canonical); err != nil {
					return err
				}
			}
			return nil
		})
	})
	return cmd
}
