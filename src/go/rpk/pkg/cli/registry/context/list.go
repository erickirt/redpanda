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
	"fmt"
	"slices"

	"github.com/spf13/afero"
	"github.com/spf13/cobra"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/schemaregistry"
)

func listCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "List schema registry contexts",
		Args:    cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, _ []string) {
			f := p.Formatter
			if h, ok := f.Help([]contextResponse{}); ok {
				out.Exit(h)
			}
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)

			cl, err := schemaregistry.NewClient(fs, p)
			out.MaybeDie(err, "unable to initialize schema registry client: %v", err)

			contexts, err := ListContexts(cmd.Context(), cl.Client)
			out.MaybeDie(err, "%v", err)

			slices.Sort(contexts)
			rows := make([]contextResponse, 0, len(contexts))
			for _, c := range contexts {
				rows = append(rows, contextResponse{Name: c})
			}
			if isText, _, s, err := f.Format(rows); !isText {
				out.MaybeDie(err, "unable to print in the required format %q: %v", f.Kind, err)
				out.Exit(s)
			}
			for _, c := range contexts {
				fmt.Println(c)
			}
		},
	}
	p.InstallFormatFlag(cmd)
	return cmd
}
