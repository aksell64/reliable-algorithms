package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"flag"
	"fmt"
	"log"
	"os"
	"reliable/broadcaster"
	"reliable/consensus"
	"reliable/consensus/coin/commitreveal"
	"reliable/consensus/failsilent/randomized"
	"reliable/database"
	"reliable/messages"
	"reliable/network"
	"reliable/p2p"
	"reliable/types"
	"reliable/types/inbox"
	"reliable/utils"
	"reliable/utils/codec"
	"strconv"
	"strings"

	"github.com/rs/zerolog"
)

func main() {
	// --- Command-line parameters ---
	listenAddr := flag.String("addr", "", "Listen address (e.g. /ip4/0.0.0.0/tcp/9000)")
	processID := flag.Int("id", 0, "Current process ID (starting from 1)")
	crashFaultsProcesses := flag.Int("c", 0, "Total number of crash faults processes in the cluster")
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
	if *crashFaultsProcesses < 0 {
		log.Fatal("Error: --c is required and must be >=0")
	}

	pid := types.ProcessID(*processID)
	n := *totalProcesses
	crashes := *crashFaultsProcesses

	fmt.Printf("🚀 Starting process: id=%d, addr=%q, n=%d\n", pid, *listenAddr, n)

	// --- Network setup ---
	ctx := context.Background()

	registry := codec.New()

	net, err := setupLAN(*listenAddr, pid, n, registry)
	if err != nil {
		log.Fatal(err)
	}

	// --- Building the process map ---
	correct := utils.ProcessesIDRange(1, n)
	processes := make(map[types.ProcessID]types.ProcessRank)
	for _, p := range correct {
		processes[p] = types.ProcessRank(p)
	}

	node := makeConsensus(ctx, utils.KeysSlice(processes), pid, crashes, registry)

	fmt.Printf("⚙️ Preparing network...\n")

	if err := net.Boostrap(); err != nil {
		log.Fatal(err)
	}

	fmt.Printf("✅ Cluster ready!\n")

	node.Init()
	node.Start()

	go listenCmds(node)

	decided := consensus.CollectDecided(ctx, node)
	for _, d := range decided {
		fmt.Printf("🤝 Decided: %v\n", d.Value)
	}
}

func makeConsensus(
	ctx context.Context,
	processes []types.ProcessID,
	pid types.ProcessID,
	crashFaults int,
	registry *codec.Registry,
) consensus.Consensus {
	fl := p2p.NewBaseLink(pid)
	sl := p2p.NewStubbornP2PLinks(ctx, fl)
	pl := p2p.NewPerfectP2PLinks(pid, sl)

	beb1 := broadcaster.NewBestEffortBroadcaster(pid,
		processes,
		broadcaster.DefaultBroadcastNodeSelector(),
		pl)

	beb2 := broadcaster.NewBestEffortBroadcaster(pid,
		processes,
		broadcaster.DefaultBroadcastNodeSelector(),
		pl)

	rb := broadcaster.NewEagerReliableBroadcaster(ctx, pid, processes, beb1, registry)

	hasher := sha256.New()

	codec.RegisterTyped[messages.BaseMsg](registry)

	storage := database.NewInMemory()

	ibox := inbox.New(registry, storage)

	coin := commitreveal.NewCoin(
		ctx,
		pid,
		hasher,
		processes,
		len(processes),
		crashFaults,
		beb2,
		ibox,
		registry,
		utils.Ptr(zerolog.Nop()),
	)

	node := randomized.New(ctx, processes, pid, crashFaults, coin, beb2, rb, nil, registry)
	return node
}

func setupLAN(listenAddr string, processID types.ProcessID, N int, registry *codec.Registry) (network.Network, error) {
	net := network.NewLibp2p([]string{listenAddr}, processID, N,
		network.WithRegistry(registry),
		network.WithIdentityFile(fmt.Sprintf("./node_identity%s.json", processID.String())),
		/*network.DisableLogger()*/)

	network.SetGlobal(net)
	return net, nil
}

func listenCmds(node consensus.Consensus) {
	scanner := bufio.NewScanner(os.Stdin)
	fmt.Println("📝 Enter Propose (Enter для отправки):")
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		val, err := strconv.Atoi(line)
		if err != nil {
			fmt.Printf("❌ '%s' — не число, попробуй ещё раз\n", line)
			continue
		}

		fmt.Printf("📤 Propose(%d)!\n", val)
		go node.Propose(types.IntValue(val))
	}
}
