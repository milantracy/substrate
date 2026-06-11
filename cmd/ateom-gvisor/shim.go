//go:build linux

// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"

	"github.com/containerd/ttrpc"

	taskapi "github.com/containerd/containerd/api/runtime/task/v2"

	"github.com/agent-substrate/substrate/internal/ateompath"
)

var _ ContainerRuntime = (*shimRuntime)(nil)

type shimRuntime struct {
	shimPath               string
	actorTemplateNamespace string
	actorTemplateName      string
	actorID                string

	mu         sync.Mutex
	started    bool
	shimAddr   string
	ttrpcConn  net.Conn
	ttrpcCli   *ttrpc.Client
	taskClient taskapi.TTRPCTaskService
}

func (s *shimRuntime) sandboxID() string {
	return s.actorTemplateNamespace + ":" + s.actorTemplateName + ":" + s.actorID
}

func (s *shimRuntime) stateDir() string {
	return ateompath.ShimStateDir(s.actorTemplateNamespace, s.actorTemplateName, s.actorID)
}

// ensureStarted launches the shim daemon if it hasn't been started yet.
// bundlePath is used as the working directory for the shim start command.
func (s *shimRuntime) ensureStarted(ctx context.Context, bundlePath string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.started {
		return nil
	}

	stateDir := s.stateDir()
	if err := os.MkdirAll(stateDir, 0o700); err != nil {
		return fmt.Errorf("creating shim state dir: %w", err)
	}

	cmd := exec.CommandContext(ctx, s.shimPath,
		"-namespace", "ateom",
		"-id", s.sandboxID(),
		"-address", "/dev/null",
		"start",
	)
	cmd.Dir = bundlePath

	out, err := cmd.Output()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			return fmt.Errorf("shim start failed (stderr: %s): %w", exitErr.Stderr, err)
		}
		return fmt.Errorf("shim start failed: %w", err)
	}

	s.shimAddr = strings.TrimSpace(string(out))
	slog.InfoContext(ctx, "Shim daemon started", slog.String("address", s.shimAddr))

	// Persist shim info so CheckpointWorkload can clean up the daemon later.
	if err := os.WriteFile(filepath.Join(stateDir, "shim_path"), []byte(s.shimPath), 0o600); err != nil {
		return fmt.Errorf("persisting shim path: %w", err)
	}
	if err := os.WriteFile(filepath.Join(stateDir, "address"), []byte(s.shimAddr), 0o600); err != nil {
		return fmt.Errorf("persisting shim address: %w", err)
	}

	conn, err := net.Dial("unix", s.shimAddr)
	if err != nil {
		return fmt.Errorf("connecting to shim ttrpc socket %q: %w", s.shimAddr, err)
	}

	s.ttrpcConn = conn
	s.ttrpcCli = ttrpc.NewClient(conn)
	s.taskClient = taskapi.NewTTRPCTaskClient(s.ttrpcCli)
	s.started = true

	return nil
}

func (s *shimRuntime) Create(ctx context.Context, out io.Writer, containerName string) error {
	reapLock.RLock()
	defer reapLock.RUnlock()

	bundlePath := ateompath.OCIBundlePath(s.actorTemplateNamespace, s.actorTemplateName, s.actorID, containerName)

	if err := s.ensureStarted(ctx, bundlePath); err != nil {
		return fmt.Errorf("ensuring shim started: %w", err)
	}

	slog.InfoContext(ctx, "About to create container via shim", slog.String("container", containerName))

	resp, err := s.taskClient.Create(ctx, &taskapi.CreateTaskRequest{
		ID:     containerName,
		Bundle: bundlePath,
	})
	if err != nil {
		return fmt.Errorf("shim create %q: %w", containerName, err)
	}

	slog.InfoContext(ctx, "Container created via shim",
		slog.String("container", containerName),
		slog.Uint64("pid", uint64(resp.Pid)))
	return nil
}

func (s *shimRuntime) Start(ctx context.Context, out io.Writer, containerName string) error {
	reapLock.RLock()
	defer reapLock.RUnlock()

	slog.InfoContext(ctx, "About to start container via shim", slog.String("container", containerName))

	resp, err := s.taskClient.Start(ctx, &taskapi.StartRequest{
		ID: containerName,
	})
	if err != nil {
		return fmt.Errorf("shim start %q: %w", containerName, err)
	}

	slog.InfoContext(ctx, "Container started via shim",
		slog.String("container", containerName),
		slog.Uint64("pid", uint64(resp.Pid)))
	return nil
}

func (s *shimRuntime) Checkpoint(_ context.Context, _, _ string) error {
	return fmt.Errorf("checkpoint not supported via shim (use raw runsc for sandbox-level checkpoint)")
}

func (s *shimRuntime) Restore(_ context.Context, _ io.Writer, _, _ string) error {
	return fmt.Errorf("restore not exposed via shim ttrpc API (use raw runsc)")
}

func (s *shimRuntime) Delete(ctx context.Context, containerName string) error {
	reapLock.RLock()
	defer reapLock.RUnlock()

	slog.InfoContext(ctx, "About to delete container via shim", slog.String("container", containerName))

	_, err := s.taskClient.Delete(ctx, &taskapi.DeleteRequest{
		ID: containerName,
	})
	if err != nil {
		return fmt.Errorf("shim delete %q: %w", containerName, err)
	}

	slog.InfoContext(ctx, "Container deleted via shim", slog.String("container", containerName))
	return nil
}

func (s *shimRuntime) State(ctx context.Context, containerName string) error {
	reapLock.RLock()
	defer reapLock.RUnlock()

	slog.InfoContext(ctx, "About to query container state via shim", slog.String("container", containerName))

	resp, err := s.taskClient.State(ctx, &taskapi.StateRequest{
		ID: containerName,
	})
	if err != nil {
		return fmt.Errorf("shim state %q: %w", containerName, err)
	}

	slog.InfoContext(ctx, "Container state via shim",
		slog.String("container", containerName),
		slog.Any("status", resp.Status),
		slog.Uint64("pid", uint64(resp.Pid)))
	return nil
}

// Close tears down the ttrpc connection to the shim. It does NOT stop the
// shim daemon — use Shutdown for that.
func (s *shimRuntime) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.ttrpcCli != nil {
		s.ttrpcCli.Close()
		s.ttrpcCli = nil
	}
	if s.ttrpcConn != nil {
		s.ttrpcConn.Close()
		s.ttrpcConn = nil
	}
	s.started = false

	return nil
}

// Shutdown sends a Shutdown RPC to stop the shim daemon process, then
// closes the ttrpc connection and runs the delete subcommand to clean
// up state files.
func (s *shimRuntime) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	if s.taskClient != nil {
		if _, err := s.taskClient.Shutdown(ctx, &taskapi.ShutdownRequest{ID: s.sandboxID(), Now: true}); err != nil {
			slog.WarnContext(ctx, "Shim Shutdown RPC failed", slog.Any("err", err))
		} else {
			slog.InfoContext(ctx, "Shim daemon stopped via Shutdown RPC")
		}
	}
	s.mu.Unlock()

	s.Close()

	bundlePath := ateompath.OCIBundlePath(s.actorTemplateNamespace, s.actorTemplateName, s.actorID, "pause")
	cmd := exec.CommandContext(ctx, s.shimPath,
		"-namespace", "ateom",
		"-id", s.sandboxID(),
		"-address", "/dev/null",
		"-bundle", bundlePath,
		"delete",
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		slog.WarnContext(ctx, "Shim delete subcommand returned error",
			slog.Any("err", err), slog.String("output", string(out)))
	}

	os.RemoveAll(s.stateDir())
	return nil
}

// cleanupShim stops any orphaned shim daemon for the given actor by reading
// the persisted shim state. Called by CheckpointWorkload after raw runsc
// checkpoint+delete has already terminated the sandbox.
func cleanupShim(ctx context.Context, actorTemplateNamespace, actorTemplateName, actorID string) {
	stateDir := ateompath.ShimStateDir(actorTemplateNamespace, actorTemplateName, actorID)

	shimPathBytes, err := os.ReadFile(filepath.Join(stateDir, "shim_path"))
	if err != nil {
		return
	}
	shimPath := strings.TrimSpace(string(shimPathBytes))
	if shimPath == "" {
		return
	}

	// Send Shutdown RPC to stop the daemon process. The delete subcommand
	// only cleans up state files — it does not kill the daemon.
	addrBytes, err := os.ReadFile(filepath.Join(stateDir, "address"))
	if err == nil {
		addr := strings.TrimSpace(string(addrBytes))
		if addr != "" {
			if conn, dialErr := net.Dial("unix", addr); dialErr == nil {
				tc := ttrpc.NewClient(conn)
				client := taskapi.NewTTRPCTaskClient(tc)
				sandboxID := actorTemplateNamespace + ":" + actorTemplateName + ":" + actorID
				if _, shutErr := client.Shutdown(ctx, &taskapi.ShutdownRequest{ID: sandboxID, Now: true}); shutErr != nil {
					slog.WarnContext(ctx, "Shim Shutdown RPC failed", slog.Any("err", shutErr))
				} else {
					slog.InfoContext(ctx, "Shim daemon stopped via Shutdown RPC")
				}
				tc.Close()
				conn.Close()
			}
		}
	}

	// Clean up shim state files.
	sandboxID := actorTemplateNamespace + ":" + actorTemplateName + ":" + actorID
	bundlePath := ateompath.OCIBundlePath(actorTemplateNamespace, actorTemplateName, actorID, "pause")
	cmd := exec.CommandContext(ctx, shimPath,
		"-namespace", "ateom",
		"-id", sandboxID,
		"-address", "/dev/null",
		"-bundle", bundlePath,
		"delete",
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		slog.WarnContext(ctx, "Shim delete subcommand returned error", slog.Any("err", err), slog.String("output", string(out)))
	}

	os.RemoveAll(stateDir)
}
