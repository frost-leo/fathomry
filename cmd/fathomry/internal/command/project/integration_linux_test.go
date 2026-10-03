//go:build linux

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

package project

import (
	"bufio"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func TestSourceFileRaces(t *testing.T) {
	directory := t.TempDir()
	root, err := os.OpenRoot(directory)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	path := filepath.Join(directory, "go.mod")
	if err := syscall.Mkfifo(path, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := readSourceFile(root, "go.mod"); !errors.Is(err, ErrDependency) {
		t.Fatal("FIFO was admitted")
	}
	done := make(chan struct{})
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		for {
			select {
			case <-done:
				return
			default:
			}
			_ = os.Remove(path)
			_ = os.WriteFile(path, []byte("module example.org/control\n"), 0600)
			_ = os.Remove(path)
			_ = syscall.Mkfifo(path, 0600)
		}
	}()
	t.Cleanup(func() { close(done); <-stopped })
	returned := make(chan struct{})
	go func() {
		defer close(returned)
		for range 1000 {
			_, _ = readSourceFile(root, "go.mod")
		}
	}()
	select {
	case <-returned:
	case <-time.After(5 * time.Second):
		t.Fatal("a racing nonregular file blocked source admission")
	}
}

func TestConfigurationDirectoryFIFO(t *testing.T) {
	directory := t.TempDir()
	if err := os.MkdirAll(filepath.Join(directory, "framework/configuration"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Mkfifo(filepath.Join(directory, "framework/configuration/v1"), 0600); err != nil {
		t.Fatal(err)
	}
	root, err := os.OpenRoot(directory)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	returned := make(chan error, 1)
	go func() { returned <- checkConfigurationAPI(root) }()
	select {
	case err := <-returned:
		if !errors.Is(err, ErrDependency) {
			t.Fatal("non-directory bootstrap source admitted", err)
		}
	case <-time.After(time.Second):
		t.Fatal("FIFO in place of the source directory blocked dependency admission")
	}
}

func TestGeneratedRepeatedSignals(t *testing.T) {
	if testing.Short() {
		t.Skip("generated process signal qualification")
	}
	tree := testPlan(t, "local", "yaml")
	if err := create(context.Background(), tree, writeProjectFile); err != nil {
		t.Fatal(err)
	}
	helper := `package main
import ("fmt"; "testing"; "time")
func TestSignalControl(t *testing.T) {
	ctx, stop := processContext()
	defer stop()
	fmt.Println("ready")
	<-ctx.Done()
	fmt.Println("canceled")
	time.Sleep(time.Hour)
}
`
	if err := os.WriteFile(filepath.Join(tree.destination, "cmd/demo/signal_control_test.go"), []byte(helper), 0600); err != nil {
		t.Fatal(err)
	}
	environment := consumerEnvironment()
	runConsumer(t, tree.destination, environment, goTool(), "mod", "tidy")
	binary := filepath.Join(t.TempDir(), "signal-control")
	runConsumer(t, tree.destination, environment, goTool(), "test", "-c", "-o", binary, "./cmd/demo")
	for _, signal := range []syscall.Signal{syscall.SIGINT, syscall.SIGTERM} {
		t.Run(signal.String(), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			process := exec.CommandContext(ctx, binary, "-test.run=^TestSignalControl$")
			process.Dir, process.Env = tree.destination, environment
			output, err := process.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			if err := process.Start(); err != nil {
				t.Fatal(err)
			}
			defer process.Process.Kill()
			lines := bufio.NewScanner(output)
			if !lines.Scan() || lines.Text() != "ready" {
				t.Fatal("signal handler did not become ready")
			}
			if err := process.Process.Signal(signal); err != nil {
				t.Fatal(err)
			}
			if !lines.Scan() || lines.Text() != "canceled" {
				t.Fatal("first signal did not request cancellation")
			}
			if err := process.Process.Signal(signal); err != nil {
				t.Fatal(err)
			}
			err = process.Wait()
			if ctx.Err() != nil || err == nil {
				t.Fatal("repeated signal did not terminate the blocked process", err)
			}
			status, ok := process.ProcessState.Sys().(syscall.WaitStatus)
			if !ok || !status.Signaled() || status.Signal() != signal {
				t.Fatal("process did not terminate through the requested signal")
			}
		})
	}
}
