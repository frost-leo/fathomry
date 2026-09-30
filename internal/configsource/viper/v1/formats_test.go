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
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	sdk "github.com/spf13/viper"
)

func TestFormats(t *testing.T) {
	t.Run("toml_types_and_native_failures", func(t *testing.T) {
		text := "value=9007199254740993\nwhen=2026-09-29T00:00:00Z\n[service]\nports=[7000,7001]\n"
		documents, err := Load(context.Background(), []LoadInput{readerInput("toml", text)})
		if err != nil {
			t.Fatal(err)
		}
		value, err := documents[0].ValueCopy("value")
		if err != nil || value != int64(9007199254740993) {
			t.Fatal("TOML integer rounded")
		}
		got, _ := documents[0].ValueCopy("when")
		reflectTime, ok := got.(time.Time)
		if !ok || reflectTime.Year() != 2026 {
			t.Fatal("TOML native date lost")
		}
		type config struct {
			Value   int64
			Service struct{ Ports []int }
		}
		decoded, err := Decode[config](context.Background(), documents[0])
		if err != nil || decoded.Value != 9007199254740993 || len(decoded.Service.Ports) != 2 {
			t.Fatal("TOML native typed route failed", err)
		}
		if string(documents[0].RawCopy()) != text {
			t.Fatal("original syntax lost")
		}
		_, err = Load(context.Background(), []LoadInput{readerInput("toml", "x=[")})
		var native sdk.ConfigParseError
		if !errors.As(err, &native) {
			t.Fatal("native TOML error lost")
		}
		if _, err := Load(context.Background(), []LoadInput{readerInput("toml", "x="+strings.Repeat("[", MaxDepth+1)+"0"+strings.Repeat("]", MaxDepth+1))}); !errors.Is(err, ErrLimit) {
			t.Fatal("TOML depth bound not applied")
		}
		deep := "x={" + strings.Repeat("a.", 35) + "a={" + strings.Repeat("b.", 35) + "b=0}}"
		if _, err := Load(context.Background(), []LoadInput{readerInput("toml", deep)}); !errors.Is(err, ErrLimit) {
			t.Fatal("inline dotted keys bypassed TOML depth bound")
		}
	})
	t.Run("dotenv_is_a_file_not_global_environment", func(t *testing.T) {
		t.Setenv("FATHOMRY_DOTENV_GUARD", "original")
		documents, err := Load(context.Background(), []LoadInput{readerInput("dotenv", "FATHOMRY_DOTENV_GUARD=file\nPASSWORD='literal$dollar'\n")})
		if err != nil {
			t.Fatal(err)
		}
		if got, _ := documents[0].ValueCopy("fathomry_dotenv_guard"); got != "file" || os.Getenv("FATHOMRY_DOTENV_GUARD") != "original" {
			t.Fatal("dotenv mutated process state")
		}
		if _, err := Load(context.Background(), []LoadInput{readerInput("env", "SECRET=$FATHOMRY_DOTENV_GUARD")}); !errors.Is(err, ErrInput) {
			t.Fatal("unbounded expansion admitted")
		}
		for _, text := range []string{
			"SECRET=prefix'$FATHOMRY_DOTENV_GUARD'suffix",
			"SECRET=\"prefix\nINNER='$FATHOMRY_DOTENV_GUARD'\nsuffix\"",
		} {
			if _, err := Load(context.Background(), []LoadInput{readerInput("env", text)}); !errors.Is(err, ErrInput) {
				t.Fatal("native expansion bypassed preflight")
			}
		}
		for _, text := range []string{
			"SECRET='literal$dollar'",
			"SECRET='first\nsecond$dollar'",
			`SECRET="escaped\$dollar"`,
			`SECRET=escaped\$dollar`,
		} {
			if _, err := Load(context.Background(), []LoadInput{readerInput("env", text)}); err != nil {
				t.Fatal("literal dotenv value refused", err)
			}
		}
	})
}

func FuzzFormats(f *testing.F) {
	for _, raw := range []string{"x=1", "x={a.b=1}", "x=[1,2]", "VALUE='literal$dollar'", "VALUE=prefix'$FATHOMRY_EXPANSION_CANARY'suffix", "VALUE=\"first\nSECOND='$FATHOMRY_EXPANSION_CANARY'\nlast\""} {
		f.Add(raw, false)
		f.Add(raw, true)
	}
	f.Setenv("FATHOMRY_EXPANSION_CANARY", "expanded-native-canary")
	f.Fuzz(func(t *testing.T, raw string, dotenv bool) {
		if len(raw) > 64<<10 {
			return
		}
		encoding := "toml"
		if dotenv {
			encoding = "env"
		}
		documents, err := Load(context.Background(), []LoadInput{readerInput(encoding, raw)})
		if err != nil {
			if documents != nil {
				t.Fatal("failed format exposed partial result")
			}
			return
		}
		if string(documents[0].RawCopy()) != raw {
			t.Fatal("original format changed")
		}
		snapshot, err := documents[0].Capture(context.Background())
		if err == nil {
			values, err := snapshot.ValuesCopy()
			if err != nil {
				t.Fatal(err)
			}
			for _, value := range values {
				if text, ok := value.(string); ok && strings.Contains(text, "expanded-native-canary") && !strings.Contains(raw, "expanded-native-canary") {
					t.Fatal("dotenv expansion escaped admission")
				}
			}
		}
	})
}
