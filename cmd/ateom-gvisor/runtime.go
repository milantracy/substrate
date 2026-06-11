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
	"io"
	"log/slog"
)

type ContainerRuntime interface {
	Create(ctx context.Context, out io.Writer, containerName string) error
	Start(ctx context.Context, out io.Writer, containerName string) error
	Checkpoint(ctx context.Context, containerName, checkpointPath string) error
	Restore(ctx context.Context, out io.Writer, containerName, checkpointPath string) error
	Delete(ctx context.Context, containerName string) error
	State(ctx context.Context, containerName string) error
}

// newContainerRuntime returns a shimRuntime if shimPath is set, otherwise
// a raw runsc runtime.
func newContainerRuntime(shimPath, runscPath, actorTemplateNamespace, actorTemplateName, actorID string) ContainerRuntime {
	if shimPath != "" {
		slog.Info("Using gVisor shim runtime",
			slog.String("shim_path", shimPath),
			slog.String("runsc_path", runscPath))
		return &shimRuntime{
			shimPath:               shimPath,
			runscPath:              runscPath,
			actorTemplateNamespace: actorTemplateNamespace,
			actorTemplateName:      actorTemplateName,
			actorID:                actorID,
		}
	}
	return &runsc{
		path:                   runscPath,
		actorTemplateNamespace: actorTemplateNamespace,
		actorTemplateName:      actorTemplateName,
		actorID:                actorID,
	}
}
