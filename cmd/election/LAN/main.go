package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"reliable/database"
	"reliable/election"
	"reliable/network"
	"reliable/p2p"
	"reliable/types"
	"reliable/utils"
	"reliable/utils/codec"
	"time"
)

func main() {
	// --- Command-line parameters ---
	listenAddr := flag.String("addr", "", "Listen address (e.g. /ip4/0.0.0.0/tcp/9000)")
	processID := flag.Int("id", 0, "Current process ID (starting from 1)")
	totalProcesses := flag.Int("n", 0, "Total number of processes in the cluster")
	flag.Parse()

	// --- Validation: straight to the point ---
	if *processID <= 0 {
		log.Fatal("Error: --id is required and must be >= 1")
	}
	if *totalProcesses <= 0 {
		log.Fatal("Error: --n is required and must be >= 1")
	}
	if *processID > *totalProcesses {
		log.Fatalf("Error: --id (%d) cannot be greater than --n (%d)", *processID, *totalProcesses)
	}

	pid := types.ProcessID(*processID)
	n := *totalProcesses

	fmt.Printf("🚀 Starting process: id=%d, addr=%q, n=%d\n", pid, *listenAddr, n)

	// --- Network setup ---
	ctx := context.Background()

	registry := codec.New()

	err := setupLAN(*listenAddr, pid, n, registry)
	if err != nil {
		log.Fatal(err)
	}

	// --- Building the process map ---
	correct := utils.ProcessesIDRange(1, n)
	processes := make(map[types.ProcessID]types.ProcessRank)
	for _, p := range correct {
		processes[p] = types.ProcessRank(p)
	}

	// --- Creating and starting the election ---
	e := makeElection(ctx, processes, pid, registry)
	e.Init()
	e.Start()

	select {}
}

func makeElection(
	ctx context.Context,
	processes map[types.ProcessID]types.ProcessRank,
	self types.ProcessID,
	registry *codec.Registry,
) *election.LowerEpochElection {
	baseLink := p2p.NewBaseLink(self)
	storage := database.NewInMemory()
	e := election.NewLowerEpochElection(ctx, self, processes, storage, baseLink, 50*time.Millisecond, registry, nil)
	return e
}

func setupLAN(listenAddr string, processID types.ProcessID, N int, registry *codec.Registry) error {
	net := network.NewLibp2p([]string{listenAddr}, processID, N,
		network.WithRegistry(registry))

	err := net.Boostrap()
	if err != nil {
		return err
	}

	network.SetGlobal(net)
	return nil
}
