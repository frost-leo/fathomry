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

package i18n

import (
	"strings"
	"text/template"
	"text/template/parse"

	nativetemplate "github.com/nicksnyder/go-i18n/v2/i18n/template"
)

type compiledTemplate struct {
	template     *template.Template
	arguments    []string
	literalBytes int
}

func compileTemplate(content string) (*compiledTemplate, error) {
	parsed, err := template.New("").Option("missingkey=error").Parse(content)
	if err != nil || len(parsed.Templates()) != 1 {
		return nil, problem(InvalidTemplate)
	}
	// A definition can replace a same-named root without adding a second tree.
	// Distinct roots prevent any definition from hiding behind that identity.
	checked, err := template.New("profile-check").Parse(content)
	if err != nil || len(checked.Templates()) != 1 {
		return nil, problem(InvalidTemplate)
	}
	nodes := parsed.Tree.Root.Nodes
	if len(nodes) > MaxTemplateNodes {
		return nil, problem(LimitExceeded)
	}
	result := &compiledTemplate{template: parsed}
	for _, node := range nodes {
		switch node := node.(type) {
		case *parse.TextNode:
			result.literalBytes += len(node.Text)
		case *parse.ActionNode:
			pipe := node.Pipe
			if len(pipe.Decl) != 0 || len(pipe.Cmds) != 1 || len(pipe.Cmds[0].Args) != 1 {
				return nil, problem(InvalidTemplate)
			}
			field, ok := pipe.Cmds[0].Args[0].(*parse.FieldNode)
			if !ok || len(field.Ident) != 1 || !validName(field.Ident[0]) {
				return nil, problem(InvalidTemplate)
			}
			result.arguments = append(result.arguments, field.Ident[0])
		default:
			return nil, problem(InvalidTemplate)
		}
	}
	if result.literalBytes == 0 && len(result.arguments) == 0 {
		return nil, problem(InvalidTemplate)
	}
	return result, nil
}

type preparedParser struct {
	templates map[string]*compiledTemplate
}

func (preparedParser) Cacheable() bool { return true }

func (parser preparedParser) Parse(content, left, right string) (nativetemplate.ParsedTemplate, error) {
	if left != "" || right != "" {
		return nil, problem(InvalidTemplate)
	}
	prepared := parser.templates[content]
	if prepared == nil {
		return nil, problem(InvalidTemplate)
	}
	return prepared, nil
}

func (compiled *compiledTemplate) Execute(data any) (string, error) {
	arguments, ok := data.(map[string]string)
	if !ok {
		return "", problem(InvalidArguments)
	}
	size := compiled.literalBytes
	for _, name := range compiled.arguments {
		value, exists := arguments[name]
		if !exists {
			return "", problem(InvalidArguments)
		}
		if len(value) > MaxOutputBytes-size {
			return "", problem(LimitExceeded)
		}
		size += len(value)
	}
	writer := boundedWriter{}
	writer.builder.Grow(size)
	if err := compiled.template.Execute(&writer, arguments); err != nil {
		return "", problem(RenderFailed)
	}
	return writer.builder.String(), nil
}

type boundedWriter struct{ builder strings.Builder }

func (writer *boundedWriter) Write(data []byte) (int, error) {
	if len(data) > MaxOutputBytes-writer.builder.Len() {
		return 0, problem(LimitExceeded)
	}
	return writer.builder.Write(data)
}
