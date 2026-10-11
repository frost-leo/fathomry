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

package iceberg

import (
	nativeice "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/udf"
	"github.com/apache/iceberg-go/view"
	native "github.com/frost-leo/fathomry/internal/tableformat/iceberg/v0"
)

type Table struct {
	private
	native *native.ManagedTable
}

func wrapTable(t *native.ManagedTable) *Table {
	if t == nil {
		return nil
	}
	return &Table{native: t}
}
func (s *Session) valid() error {
	if s == nil || s.native == nil {
		return fail(ErrState, "session")
	}
	return nil
}
func (s *Session) LoadTable(id table.Identifier) (*Table, error) {
	if err := s.valid(); err != nil {
		return nil, err
	}
	v, err := s.native.LoadTable(id)
	return wrapTable(v), translate(err, "loadtable")
}
func (s *Session) CreateTable(id table.Identifier, schema *nativeice.Schema, opts ...catalog.CreateTableOpt) (*Table, error) {
	if err := s.valid(); err != nil {
		return nil, err
	}
	v, err := s.native.CreateTable(id, schema, opts...)
	return wrapTable(v), translate(err, "createtable")
}
func (s *Session) RegisterTable(id table.Identifier, location string) (*Table, error) {
	if err := s.valid(); err != nil {
		return nil, err
	}
	v, err := s.native.RegisterTable(id, location)
	return wrapTable(v), translate(err, "registertable")
}
func (s *Session) RenameTable(from, to table.Identifier) (*Table, error) {
	if err := s.valid(); err != nil {
		return nil, err
	}
	v, err := s.native.RenameTable(from, to)
	return wrapTable(v), translate(err, "renametable")
}
func (s *Session) UpdateTable(id table.Identifier, requirements []table.Requirement, updates []table.Update) (*Table, error) {
	if err := s.valid(); err != nil {
		return nil, err
	}
	v, err := s.native.UpdateTable(id, requirements, updates)
	return wrapTable(v), translate(err, "updatetable")
}
func (s *Session) LoadNamespaceProperties(id table.Identifier) (nativeice.Properties, error) {
	if err := s.valid(); err != nil {
		return nil, err
	}
	v, err := s.native.LoadNamespaceProperties(id)
	return v, translate(err, "loadnamespaceproperties")
}
func (s *Session) UpdateNamespaceProperties(id table.Identifier, removals []string, updates nativeice.Properties) (catalog.PropertiesUpdateSummary, error) {
	if err := s.valid(); err != nil {
		return catalog.PropertiesUpdateSummary{}, err
	}
	v, err := s.native.UpdateNamespaceProperties(id, removals, updates)
	return v, translate(err, "updatenamespaceproperties")
}
func (s *Session) ListNamespaces(parent table.Identifier) ([]table.Identifier, error) {
	if err := s.valid(); err != nil {
		return nil, err
	}
	v, err := s.native.ListNamespaces(parent)
	return v, translate(err, "listnamespaces")
}
func (s *Session) LoadView(id table.Identifier) (*view.View, error) {
	if err := s.valid(); err != nil {
		return nil, err
	}
	v, err := s.native.LoadView(id)
	return v, translate(err, "loadview")
}
func (s *Session) CreateView(id table.Identifier, version *view.Version, schema *nativeice.Schema, opts ...catalog.CreateViewOpt) (*view.View, error) {
	if err := s.valid(); err != nil {
		return nil, err
	}
	v, err := s.native.CreateView(id, version, schema, opts...)
	return v, translate(err, "createview")
}
func (s *Session) RegisterView(id table.Identifier, location string) (*view.View, error) {
	if err := s.valid(); err != nil {
		return nil, err
	}
	v, err := s.native.RegisterView(id, location)
	return v, translate(err, "registerview")
}
func (s *Session) RenameView(from, to table.Identifier) (*view.View, error) {
	if err := s.valid(); err != nil {
		return nil, err
	}
	v, err := s.native.RenameView(from, to)
	return v, translate(err, "renameview")
}
func (s *Session) UpdateView(id table.Identifier, requirements []view.Requirement, updates []view.Update) (*view.View, error) {
	if err := s.valid(); err != nil {
		return nil, err
	}
	v, err := s.native.UpdateView(id, requirements, updates)
	return v, translate(err, "updateview")
}
func (s *Session) LoadFunction(id table.Identifier) (*udf.UDF, error) {
	if err := s.valid(); err != nil {
		return nil, err
	}
	v, err := s.native.LoadFunction(id)
	return v, translate(err, "loadfunction")
}
func (s *Session) PlanTableScan(id table.Identifier, request rest.PlanTableScanRequest) (rest.PlanTableScanResponse, error) {
	if err := s.valid(); err != nil {
		return rest.PlanTableScanResponse{}, err
	}
	v, err := s.native.PlanTableScan(id, request)
	return v, translate(err, "plantablescan")
}
func (s *Session) FetchPlanningResult(id table.Identifier, planID string, options rest.FetchPlanningResultOptions) (rest.FetchPlanningResultResponse, error) {
	if err := s.valid(); err != nil {
		return rest.FetchPlanningResultResponse{}, err
	}
	v, err := s.native.FetchPlanningResult(id, planID, options)
	return v, translate(err, "fetchplanningresult")
}
func (s *Session) WaitForPlan(id table.Identifier, planID string, options rest.WaitForPlanOptions) (rest.CompletedPlanningResult, error) {
	if err := s.valid(); err != nil {
		return rest.CompletedPlanningResult{}, err
	}
	v, err := s.native.WaitForPlan(id, planID, options)
	return v, translate(err, "waitforplan")
}
func (s *Session) FetchScanTasks(id table.Identifier, request rest.FetchScanTasksRequest) (rest.FetchScanTasksResponse, error) {
	if err := s.valid(); err != nil {
		return rest.FetchScanTasksResponse{}, err
	}
	v, err := s.native.FetchScanTasks(id, request)
	return v, translate(err, "fetchscantasks")
}
func (s *Session) ListTables(namespace table.Identifier) ([]table.Identifier, error) {
	if err := s.valid(); err != nil {
		return nil, err
	}
	v, err := s.native.ListTables(namespace)
	return v, translate(err, "listtables")
}
func (s *Session) ListViews(namespace table.Identifier) ([]table.Identifier, error) {
	if err := s.valid(); err != nil {
		return nil, err
	}
	v, err := s.native.ListViews(namespace)
	return v, translate(err, "listviews")
}
func (s *Session) ListFunctions(namespace table.Identifier) ([]table.Identifier, error) {
	if err := s.valid(); err != nil {
		return nil, err
	}
	v, err := s.native.ListFunctions(namespace)
	return v, translate(err, "listfunctions")
}
func (s *Session) CheckTableExists(id table.Identifier) (bool, error) {
	if err := s.valid(); err != nil {
		return false, err
	}
	v, err := s.native.CheckTableExists(id)
	return v, translate(err, "checktableexists")
}
func (s *Session) CheckNamespaceExists(id table.Identifier) (bool, error) {
	if err := s.valid(); err != nil {
		return false, err
	}
	v, err := s.native.CheckNamespaceExists(id)
	return v, translate(err, "checknamespaceexists")
}
func (s *Session) CheckViewExists(id table.Identifier) (bool, error) {
	if err := s.valid(); err != nil {
		return false, err
	}
	v, err := s.native.CheckViewExists(id)
	return v, translate(err, "checkviewexists")
}
func (s *Session) CheckFunctionExists(id table.Identifier) (bool, error) {
	if err := s.valid(); err != nil {
		return false, err
	}
	v, err := s.native.CheckFunctionExists(id)
	return v, translate(err, "checkfunctionexists")
}
func (s *Session) CreateNamespace(id table.Identifier, props nativeice.Properties) error {
	if err := s.valid(); err != nil {
		return err
	}
	return translate(s.native.CreateNamespace(id, props), "createnamespace")
}
func (s *Session) DropNamespace(id table.Identifier) error {
	if err := s.valid(); err != nil {
		return err
	}
	return translate(s.native.DropNamespace(id), "dropnamespace")
}
func (s *Session) DropTable(id table.Identifier) error {
	if err := s.valid(); err != nil {
		return err
	}
	return translate(s.native.DropTable(id), "droptable")
}
func (s *Session) PurgeTable(id table.Identifier, quiescent bool) error {
	if err := s.valid(); err != nil {
		return err
	}
	return translate(s.native.PurgeTable(id, quiescent), "purgetable")
}
func (s *Session) DropView(id table.Identifier) error {
	if err := s.valid(); err != nil {
		return err
	}
	return translate(s.native.DropView(id), "dropview")
}
func (s *Session) VisitTables(namespace table.Identifier, pageSize int, visit func(table.Identifier) (bool, error)) error {
	if err := s.valid(); err != nil {
		return err
	}
	return translate(s.native.VisitTables(namespace, pageSize, visit), "visittables")
}
func (s *Session) VisitViews(namespace table.Identifier, pageSize int, visit func(table.Identifier) (bool, error)) error {
	if err := s.valid(); err != nil {
		return err
	}
	return translate(s.native.VisitViews(namespace, pageSize, visit), "visitviews")
}
func (s *Session) VisitFunctions(namespace table.Identifier, pageSize int, visit func(table.Identifier) (bool, error)) error {
	if err := s.valid(); err != nil {
		return err
	}
	return translate(s.native.VisitFunctions(namespace, pageSize, visit), "visitfunctions")
}
func (s *Session) CancelPlanning(id table.Identifier, planID string) error {
	if err := s.valid(); err != nil {
		return err
	}
	return translate(s.native.CancelPlanning(id, planID), "cancelplanning")
}
func (s *Session) CommitTable(id table.Identifier, requirements []table.Requirement, updates []table.Update) (table.Metadata, string, error) {
	if err := s.valid(); err != nil {
		return nil, "", err
	}
	meta, loc, err := s.native.CommitTable(id, requirements, updates)
	return meta, loc, translate(err, "commit-table")
}
func (s *Session) RemotePlanningCapabilities() (bool, bool, error) {
	if err := s.valid(); err != nil {
		return false, false, err
	}
	plan, full, err := s.native.RemotePlanningCapabilities()
	return plan, full, translate(err, "planning-capabilities")
}
