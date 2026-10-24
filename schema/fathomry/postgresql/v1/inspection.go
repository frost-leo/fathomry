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

package postgresql

import (
	"encoding/json"
	"strings"
)

// InspectionQuery describes one bounded read of owned schema objects. SQL binds
// the target name as its sole parameter; it does not acquire locks or prove readiness.
type InspectionQuery struct{ Kind, SQL string }

// InspectionQueries returns detached read-only SQL tied to the embedded structure
// profile. The caller owns target binding, supported session settings, locks,
// row/byte budgets, manifest verification and the resulting compatibility decision.
func InspectionQueries() ([]InspectionQuery, error) {
	raw, err := assets.ReadFile("0001/structure.json")
	if err != nil {
		return nil, err
	}
	var expected struct{ Objects []struct{ Kind, Name string } }
	if err := json.Unmarshal(raw, &expected); err != nil {
		return nil, err
	}
	tables := listLiteral([]string{"fathomry_schema_history", "i18n_locale", "i18n_resource", "i18n_text", "workflow_category", "workflow_definition", "workflow_definition_tag", "workflow_version", "workflow_mode", "workflow_mode_version"})
	relations := []string{}
	triggers := []string{}
	for _, entry := range expected.Objects {
		if entry.Kind == "relation" {
			relations = append(relations, entry.Name)
		}
		if entry.Kind == "trigger" {
			triggers = append(triggers, entry.Name)
		}
	}
	owned := "SELECT c.oid FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=$1 AND c.relname IN (" + tables + ")"
	queries := []InspectionQuery{
		{"relation", `SELECT COALESCE(t.relname,d.relname,c.relname),c.relname,c.relkind::text,c.relpersistence::text,c.relrowsecurity::text,c.relforcerowsecurity::text,c.relispartition::text,COALESCE(c.reloptions::text,''),COALESCE(pg_catalog.obj_description(c.oid,'pg_class'),'')
FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
LEFT JOIN pg_catalog.pg_index i ON i.indexrelid=c.oid
LEFT JOIN pg_catalog.pg_class t ON t.oid=i.indrelid
LEFT JOIN pg_catalog.pg_depend dep ON dep.objid=c.oid AND dep.classid='pg_catalog.pg_class'::regclass AND dep.refclassid='pg_catalog.pg_class'::regclass AND dep.deptype IN ('a','i') AND c.relkind='S'
LEFT JOIN pg_catalog.pg_class d ON d.oid=dep.refobjid
WHERE n.nspname=$1 AND (c.relname IN (` + tables + `) OR t.relname IN (` + tables + `) OR d.relname IN (` + tables + `) OR c.relname IN (` + listLiteral(relations) + `)) ORDER BY 1,2 LIMIT 513`},
		{"column", `SELECT c.relname,a.attname,a.attnum::text,pg_catalog.format_type(a.atttypid,a.atttypmod),a.attnotnull::text,a.attidentity::text,a.attgenerated::text,COALESCE(cn.nspname||'.'||co.collname,''),COALESCE(pg_catalog.pg_get_expr(ad.adbin,ad.adrelid,false),''),COALESCE(pg_catalog.col_description(c.oid,a.attnum),'')
FROM pg_catalog.pg_class c JOIN pg_catalog.pg_attribute a ON a.attrelid=c.oid
LEFT JOIN pg_catalog.pg_attrdef ad ON ad.adrelid=c.oid AND ad.adnum=a.attnum
LEFT JOIN pg_catalog.pg_collation co ON co.oid=a.attcollation
LEFT JOIN pg_catalog.pg_namespace cn ON cn.oid=co.collnamespace
WHERE c.oid IN (` + owned + `) AND a.attnum>0 AND NOT a.attisdropped ORDER BY 1,a.attnum LIMIT 513`},
		{"constraint", `SELECT c.relname,co.conname,co.contype::text,co.convalidated::text,co.conenforced::text,co.condeferrable::text,co.condeferred::text,pg_catalog.pg_get_constraintdef(co.oid,false)
FROM pg_catalog.pg_class c JOIN pg_catalog.pg_constraint co ON co.conrelid=c.oid
WHERE c.oid IN (` + owned + `) ORDER BY 1,2 LIMIT 513`},
		{"index", `SELECT c.relname,ic.relname,i.indisvalid::text,i.indisready::text,i.indislive::text,i.indisunique::text,i.indisprimary::text,i.indnullsnotdistinct::text,pg_catalog.pg_get_indexdef(i.indexrelid,0,false)
FROM pg_catalog.pg_class c JOIN pg_catalog.pg_index i ON i.indrelid=c.oid JOIN pg_catalog.pg_class ic ON ic.oid=i.indexrelid
WHERE c.oid IN (` + owned + `) ORDER BY 1,2 LIMIT 513`},
		{"trigger", `SELECT c.relname,t.tgname,t.tgenabled::text,pg_catalog.pg_get_triggerdef(t.oid,false)
FROM pg_catalog.pg_trigger t JOIN pg_catalog.pg_class c ON c.oid=t.tgrelid JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
WHERE n.nspname=$1 AND NOT t.tgisinternal AND (c.relname IN (` + tables + `) OR t.tgname IN (` + listLiteral(triggers) + `)) ORDER BY 1,2 LIMIT 513`},
		{"enforcement", `SELECT c.relname,co.conname,string_agg(t.tgtype::text||':'||t.tgenabled::text,',' ORDER BY t.tgtype,t.tgenabled)
FROM pg_catalog.pg_trigger t JOIN pg_catalog.pg_class c ON c.oid=t.tgrelid JOIN pg_catalog.pg_constraint co ON co.oid=t.tgconstraint
WHERE c.oid IN (` + owned + `) AND t.tgisinternal GROUP BY c.relname,co.conname ORDER BY 1,2 LIMIT 513`},
		{"sequence", `SELECT d.relname,c.relname,pg_catalog.format_type(s.seqtypid,NULL),s.seqstart::text,s.seqincrement::text,s.seqmax::text,s.seqmin::text,s.seqcache::text,s.seqcycle::text,dep.refobjsubid::text
FROM pg_catalog.pg_sequence s JOIN pg_catalog.pg_class c ON c.oid=s.seqrelid
JOIN pg_catalog.pg_depend dep ON dep.objid=c.oid AND dep.classid='pg_catalog.pg_class'::regclass AND dep.refclassid='pg_catalog.pg_class'::regclass AND dep.deptype IN ('a','i')
JOIN pg_catalog.pg_class d ON d.oid=dep.refobjid
WHERE d.oid IN (` + owned + `) ORDER BY 1,2 LIMIT 513`},
		{"function", `SELECT 'reject_catalog_change',p.proname||'('||pg_catalog.pg_get_function_identity_arguments(p.oid)||')',pg_catalog.pg_get_functiondef(p.oid)
FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_namespace n ON n.oid=p.pronamespace
WHERE n.nspname=$1 AND p.proname='reject_catalog_change' ORDER BY 2 LIMIT 513`},
		{"inheritance", `SELECT p.relname,c.relname,n.nspname,i.inhseqno::text
FROM pg_catalog.pg_inherits i JOIN pg_catalog.pg_class p ON p.oid=i.inhparent JOIN pg_catalog.pg_class c ON c.oid=i.inhrelid JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
WHERE i.inhparent IN (` + owned + `) OR i.inhrelid IN (` + owned + `) ORDER BY 1,2 LIMIT 513`},
		{"rule", `SELECT c.relname,r.rulename,r.ev_enabled::text,pg_catalog.pg_get_ruledef(r.oid,false)
FROM pg_catalog.pg_rewrite r JOIN pg_catalog.pg_class c ON c.oid=r.ev_class
WHERE c.oid IN (` + owned + `) ORDER BY 1,2 LIMIT 513`},
	}
	return queries, nil
}

func listLiteral(values []string) string {
	quoted := make([]string, len(values))
	for index, value := range values {
		quoted[index] = "'" + strings.ReplaceAll(value, "'", "''") + "'"
	}
	if len(quoted) == 0 {
		return "NULL"
	}
	return strings.Join(quoted, ",")
}

// NormalizeInspection replaces only a qualified identifier, never a string/dollar
// literal or a substring of another name in a reconstructed SQL fragment.
func NormalizeInspection(sql, target string) string {
	var result strings.Builder
	previous := ""
	qualified := func() bool {
		return previous == "ON" || previous == "REFERENCES" || previous == "FUNCTION" || previous == "ONLY"
	}
	for index := 0; index < len(sql); {
		start := index
		switch sql[index] {
		case '\'':
			escaped := index > 0 && (sql[index-1] == 'E' || sql[index-1] == 'e') && (index < 2 || !identifierByte(sql[index-2]))
			index++
			for index < len(sql) {
				if escaped && sql[index] == '\\' {
					index = min(len(sql), index+2)
					continue
				}
				if sql[index] == '\'' {
					index++
					if index < len(sql) && sql[index] == '\'' {
						index++
						continue
					}
					break
				}
				index++
			}
		case '$':
			end := index + 1
			for end < len(sql) && (sql[end] == '_' || sql[end] >= 'a' && sql[end] <= 'z' || sql[end] >= 'A' && sql[end] <= 'Z' || sql[end] >= '0' && sql[end] <= '9') {
				end++
			}
			if end < len(sql) && sql[end] == '$' {
				tag := sql[index : end+1]
				if close := strings.Index(sql[end+1:], tag); close >= 0 {
					index = end + 1 + close + len(tag)
					break
				}
			}
			index++
		case '"':
			index++
			var name strings.Builder
			for index < len(sql) {
				if sql[index] == '"' {
					index++
					if index < len(sql) && sql[index] == '"' {
						name.WriteByte('"')
						index++
						continue
					}
					break
				}
				name.WriteByte(sql[index])
				index++
			}
			if qualified() && name.String() == target && strings.HasPrefix(strings.TrimLeft(sql[index:], " \t\r\n"), ".") {
				result.WriteString("{{target}}")
				previous = ""
				continue
			}
			previous = ""
		default:
			if identifierByte(sql[index]) {
				index++
				for index < len(sql) && identifierByte(sql[index]) {
					index++
				}
				if qualified() && sql[start:index] == target && strings.HasPrefix(strings.TrimLeft(sql[index:], " \t\r\n"), ".") {
					result.WriteString("{{target}}")
					previous = ""
					continue
				}
				previous = strings.ToUpper(sql[start:index])
			} else {
				index++
			}
		}
		result.WriteString(sql[start:index])
	}
	return result.String()
}
func identifierByte(value byte) bool {
	return value >= 'a' && value <= 'z' || value >= 'A' && value <= 'Z' || value >= '0' && value <= '9' || value == '_' || value >= 128
}
