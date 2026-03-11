// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package context

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/sr"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/schemaregistry"
)

const QualifiedSubjectsConfigKey = "schema_registry_enable_qualified_subjects"

type contextResponse struct {
	Name string `json:"name" yaml:"name"`
}

// ListContexts calls cl.Contexts and translates a 404 into a message
// indicating that the Redpanda cluster does not support schema contexts.
func ListContexts(ctx context.Context, cl *sr.Client) ([]string, error) {
	contexts, err := cl.Contexts(ctx)
	if err != nil {
		var re *sr.ResponseError
		if errors.As(err, &re) && re.StatusCode == 404 {
			return nil, fmt.Errorf("schema registry contexts are not supported by this cluster")
		}
		return nil, err
	}
	return contexts, nil
}

// CheckQualifiedSubjectsEnabled verifies that the cluster has the
// schema_registry_enable_qualified_subjects config set to true via the
// Admin API. This is for Redpanda 26.1 clusters where the SR API
// returns 200 for /contexts.
func CheckQualifiedSubjectsEnabled(ctx context.Context, fs afero.Fs, profile *config.RpkProfile) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	cl, err := adminapi.NewClient(ctx, fs, profile)
	if err != nil {
		return fmt.Errorf("unable to verify schema context support via admin API: %w\nUse --skip-context-check to skip this verification", err)
	}
	cfg, err := cl.SingleKeyConfig(ctx, QualifiedSubjectsConfigKey)
	if err != nil {
		return fmt.Errorf("unable to verify schema context support via admin API: %w\nUse --skip-context-check to skip this verification", err)
	}
	val, exists := cfg[QualifiedSubjectsConfigKey]
	if !exists {
		return fmt.Errorf("schema contexts are not supported by this cluster (config key %q not found)", QualifiedSubjectsConfigKey)
	}
	enabled, ok := val.(bool)
	if !ok {
		return fmt.Errorf("schema contexts are not supported by this cluster (unexpected value for %q: %v)", QualifiedSubjectsConfigKey, val)
	}
	if !enabled {
		return fmt.Errorf("schema contexts are not enabled on this cluster; set %s to true", QualifiedSubjectsConfigKey)
	}
	return nil
}

// ValidateContext validates the schema context name format, loads the
// profile and SR client, confirms the cluster supports contexts via
// GET /contexts, and optionally verifies the admin API feature flag.
func ValidateContext(ctx context.Context, schemaCtx string, fs afero.Fs, p *config.Params, skipAdminCheck bool) error {
	if schemaCtx[0] != '.' {
		return fmt.Errorf("invalid schema context %q: context names must start with a '.'", schemaCtx)
	}
	if strings.Contains(schemaCtx, ":") {
		return fmt.Errorf("invalid schema context %q: context names must not contain ':'", schemaCtx)
	}
	profile, err := p.LoadVirtualProfile(fs)
	if err != nil {
		return fmt.Errorf("rpk unable to load config: %w", err)
	}
	cl, err := schemaregistry.NewClient(fs, profile)
	if err != nil {
		return fmt.Errorf("unable to initialize schema registry client: %w", err)
	}
	return CheckContextSupport(ctx, cl.Client, fs, profile, skipAdminCheck)
}

// CheckContextSupport checks whether the cluster supports schema contexts by
// calling GET /contexts and optionally verifying the admin API flag.
func CheckContextSupport(ctx context.Context, cl *sr.Client, fs afero.Fs, profile *config.RpkProfile, skipAdminCheck bool) error {
	if _, err := ListContexts(ctx, cl); err != nil {
		return err
	}
	if !skipAdminCheck {
		return CheckQualifiedSubjectsEnabled(ctx, fs, profile)
	}
	return nil
}

func NewCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "context",
		Args:  cobra.ExactArgs(0),
		Short: "Manage schema registry contexts",
	}
	cmd.AddCommand(
		listCommand(fs, p),
		deleteCommand(fs, p),
	)
	return cmd
}
