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

package viper

import (
	"bufio"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/pelletier/go-toml/v2/unstable"
)

func supportedEncoding(encoding string) bool {
	switch encoding {
	case "json", "yaml", "yml", "toml", "dotenv", "env":
		return true
	}
	return false
}

func checkTOML(raw []byte) error {
	var parser unstable.Parser
	parser.Reset(raw)
	nodes, tableDepth := 0, 0
	arrayTables := map[string]bool{}
	var visit func(*unstable.Node, int) error
	visit = func(node *unstable.Node, depth int) error {
		nodes++
		if node.Kind == unstable.KeyValue {
			keys := node.Key()
			keys.Next()
			for keys.Next() {
				depth++
			}
		}
		if depth > MaxDepth || nodes > MaxNodes {
			return fail(ErrLimit, "structure")
		}
		children := node.Children()
		for children.Next() {
			if err := visit(children.Node(), depth+1); err != nil {
				return err
			}
		}
		return nil
	}
	for parser.NextExpression() {
		expression := parser.Expression()
		depth := tableDepth + 1
		switch expression.Kind {
		case unstable.Table, unstable.ArrayTable:
			depth = 0
			prefix := ""
			keys := expression.Key()
			for keys.Next() {
				part := keys.Node().Data
				prefix += strconv.Itoa(len(part)) + ":" + string(part)
				depth++
				if arrayTables[prefix] {
					depth++
				}
				if depth > MaxDepth {
					return fail(ErrLimit, "structure")
				}
			}
			if expression.Kind == unstable.ArrayTable && !arrayTables[prefix] {
				arrayTables[prefix] = true
				depth++
			}
			tableDepth = depth
		}
		if depth > MaxDepth {
			return fail(ErrLimit, "structure")
		}
		if err := visit(expression, max(1, depth)); err != nil {
			return err
		}
	}
	// Native ReadConfig owns syntax interpretation and its ConfigParseError.
	return nil
}

// Match the pinned gotenv v1.6.0 logical-record/value rules before expansion.
// A quote embedded in an unquoted value does NOT protect its dollars. Keep this
// guard qualified with the codec: generic shell quoting is not gotenv semantics.
var dotenvRecord = regexp.MustCompile(`\A\s*(?:export\s+)?([\w\.]+)(?:\s*=\s*|:\s+?)('(?:\\'|[^'])*'|"(?:\\"|[^"])*"|[^#\n]+)?\s*(?:\s*\#.*)?\z`)

// Native dotenv expansion can amplify small input with process/local values.
// Only literal single-quoted/escaped dollars are admitted; native parsing still
// owns values and syntax errors. UTF-16 is outside this UTF-8 input profile.
func checkDotenv(raw []byte) error {
	if !utf8.Valid(raw) {
		return fail(ErrInput, "dotenv-utf8")
	}
	text := strings.TrimPrefix(string(raw), "\ufeff")
	text = strings.ReplaceAll(strings.ReplaceAll(text, "\r\n", "\n"), "\r", "\n")
	scanner := bufio.NewScanner(strings.NewReader(text))
	nodes := 0
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || line[0] == '#' {
			continue
		}
		quote := ""
		separator := strings.Index(line, "=")
		if separator < 0 {
			separator = strings.Index(line, ":")
		}
		if separator > 0 && separator < len(line)-1 {
			value := strings.TrimSpace(line[separator+1:])
			if value[0] == '\'' || value[0] == '"' {
				quote = value[:1]
				closing := strings.LastIndex(strings.TrimSpace(value[1:]), quote)
				if closing >= 0 && value[closing] != '\\' {
					quote = ""
				}
			}
		}
		for quote != "" && scanner.Scan() {
			next := scanner.Text()
			line += "\n" + next
			closing := strings.LastIndex(next, quote)
			if closing >= 0 && (closing == 0 || next[closing-1] != '\\') {
				quote = ""
			}
		}
		if quote != "" {
			return nil
		}
		fields := dotenvRecord.FindStringSubmatch(line)
		if len(fields) == 0 {
			continue
		}
		nodes += 2
		if nodes > MaxNodes {
			return fail(ErrLimit, "structure")
		}
		value := strings.TrimSpace(fields[2])
		if len(value) >= 2 && value[0] == '\'' && value[len(value)-1] == '\'' {
			continue
		}
		for index := range len(value) {
			if value[index] == '$' && (index == 0 || value[index-1] != '\\') {
				return fail(ErrInput, "dotenv-expansion")
			}
		}
	}
	return nil
}
