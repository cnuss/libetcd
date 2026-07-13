// Copyright 2026 The etcd Authors
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

// Command etcdinit initializes an etcd member's data directory offline — no
// snapshot file, no running server. Run once per member with the shared
// --initial-cluster; the produced data directories carry the full cluster
// membership, so members afterwards start with just --name and --data-dir.
//
// The flag surface follows the author's upstream PR
// https://github.com/etcd-io/etcd/pull/22091 (`etcdutl init`), but the
// implementation dogfoods libetcd: the command is a thin CLI over
// libetcd.New().….Init(). Deliberate divergences from the upstream PR:
// --wal-dir, --initial-memory-map-size, and --no-verify are not offered —
// the builder keeps the default data-dir layout and always verifies an
// existing directory.
package main

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"github.com/cnuss/libetcd"
	"go.etcd.io/etcd/pkg/v3/cobrautl"
)

// Defaults mirror etcdutl's (etcdutl/etcdutl/snapshot_command.go) and embed's
// initial-cluster-token literal.
const (
	defaultName                     = "default"
	defaultInitialAdvertisePeerURLs = "http://localhost:2380"
	defaultInitialClusterToken      = "etcd-cluster"
)

var (
	initCluster      string
	initClusterToken string
	initDataDir      string
	initPeerURLs     string
	initName         string
)

func initialClusterFromName(name string) string {
	n := name
	if name == "" {
		n = defaultName
	}
	return fmt.Sprintf("%s=http://localhost:2380", n)
}

func newInitCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "etcdinit --data-dir {output dir} [options]",
		Short: "Initializes a new etcd data directory for a member of a new cluster",
		Long: `Initializes a new etcd data directory, without requiring a snapshot file or a
running etcd server. The produced data directory contains an empty keyspace and
the full initial cluster membership, so the member can afterwards be started
with just --data-dir.

To bootstrap a multi-member cluster, run init once per member with that
member's --name and --initial-advertise-peer-urls and the same
--initial-cluster, then place each produced data directory on its member.
`,
		Run: initCommandFunc,
	}
	cmd.Flags().StringVar(&initDataDir, "data-dir", "", "Path to the output data directory")
	cmd.Flags().StringVar(&initCluster, "initial-cluster", initialClusterFromName(defaultName), "Initial cluster configuration for init bootstrap")
	cmd.Flags().StringVar(&initClusterToken, "initial-cluster-token", defaultInitialClusterToken, "Initial cluster token for the etcd cluster during init bootstrap")
	cmd.Flags().StringVar(&initPeerURLs, "initial-advertise-peer-urls", defaultInitialAdvertisePeerURLs, "List of this member's peer URLs to advertise to the rest of the cluster")
	cmd.Flags().StringVar(&initName, "name", defaultName, "Human-readable name for this member")

	cmd.MarkFlagDirname("data-dir")

	return cmd
}

func main() {
	if err := newInitCommand().Execute(); err != nil {
		os.Exit(1)
	}
}

func initCommandFunc(_ *cobra.Command, args []string) {
	if len(args) != 0 {
		cobrautl.ExitWithError(cobrautl.ExitBadArgs, errors.New("init doesn't take any positional arguments"))
	}
	if err := runInit(initName, initDataDir, initCluster, initClusterToken, initPeerURLs); err != nil {
		cobrautl.ExitWithError(cobrautl.ExitError, err)
	}
}

// defaultDataDir is the data directory used when --data-dir is not given —
// etcd's <name>.etcd convention rather than libetcd's temp-dir default, so
// the produced directory lands somewhere predictable for the server start
// that follows.
func defaultDataDir(name string) string {
	return name + ".etcd"
}

// runInit dogfoods libetcd: accumulate the member's configuration on the
// builder, then Init. Order matters — WithInitialCluster pins the
// membership, so name and peer advertise URLs must be set before it.
func runInit(name, dataDir, cluster, clusterToken, peerURLs string) error {
	if dataDir == "" {
		dataDir = defaultDataDir(name)
	}
	return libetcd.New().
		WithName(name).
		WithDir(dataDir).
		WithClusterToken(clusterToken).
		WithLog("info", os.Stderr).
		WithPeerListener(nil, strings.Split(peerURLs, ",")...).
		WithInitialCluster(cluster).
		Init()
}
