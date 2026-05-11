package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"reliable/consensus"
	"reliable/consensus/failnoisy/byzantine"
	"reliable/database"
	"reliable/election"
	"reliable/messages"
	"reliable/network"
	"reliable/p2p"
	"reliable/types"
	"reliable/types/inbox"
	"reliable/utils"
	"reliable/utils/codec"
	"sort"
	"strconv"
	"strings"
)

func main() {
	listenAddr := flag.String("addr", "", "Listen address (e.g. /ip4/0.0.0.0/tcp/9000)")
	processID := flag.Int("id", 0, "Current process ID (starting from 1)")
	byzantineFaults := flag.Int("f", 0, "Number of byzantine faults tolerated (N > 3f required)")
	totalProcesses := flag.Int("n", 0, "Total number of processes in the cluster")
	flag.Parse()

	if *processID <= 0 {
		log.Fatal("Error: --id is required and must be >= 1")
	}
	if *totalProcesses <= 0 {
		log.Fatal("Error: --n is required and must be >= 1")
	}
	if *processID > *totalProcesses {
		log.Fatalf("Error: --id (%d) cannot be greater than --n (%d)", *processID, *totalProcesses)
	}
	if *byzantineFaults < 0 {
		log.Fatal("Error: --f must be >= 0")
	}
	if *totalProcesses <= 3**byzantineFaults {
		log.Fatalf("Error: Byzantine consensus requires N > 3f, got N=%d f=%d", *totalProcesses, *byzantineFaults)
	}

	pid := types.ProcessID(*processID)
	n := *totalProcesses
	f := *byzantineFaults

	fmt.Printf("Starting byzantine node: id=%d addr=%q n=%d f=%d\n", pid, *listenAddr, n, f)

	ctx := context.Background()

	registry := codec.New()
	registerCodecTypes(registry)

	net := network.NewLibp2p(
		[]string{*listenAddr},
		pid,
		n,
		network.WithRegistry(registry),
		network.WithIdentityFile(fmt.Sprintf("./node_identity%s.json", pid.String())),
		network.DisableLogger(),
	)
	network.SetGlobal(net)

	fmt.Println("Preparing network...")
	if err := net.Boostrap(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("Cluster ready!")

	processesSlice := utils.ProcessesIDRange(1, n)
	processes := make(map[types.ProcessID]types.ProcessRank)
	for _, pid := range processesSlice {
		processes[pid] = types.ProcessRank(pid)
	}

	node := makeConsensus(ctx, processes, pid, f, registry, net)

	node.Init()
	node.Start()

	go listenCmds(node)

	decided := consensus.CollectDecided(ctx, node)
	for _, d := range decided {
		fmt.Printf("Decided: %v\n", d.Value)
	}

	consensus.ProposeSync(ctx, node, types.IntValue(10))

	select {}
}

func makeConsensus(
	ctx context.Context,
	processes map[types.ProcessID]types.ProcessRank,
	pid types.ProcessID,
	faults int,
	registry *codec.Registry,
	net network.Network,
) consensus.Consensus {
	registerCodecTypes(registry)

	al := p2p.NewBaseLink(pid, p2p.WithNetwork(net))
	id := network.Global().Identify()

	detector := election.NewRotatingByzantineLeaderDetector(pid, al, processes, faults)
	detector.Init()

	procs := utils.KeysSlice(processes)
	sort.Slice(procs, func(i, j int) bool { return procs[i] < procs[j] })
	changer := byzantine.NewEpochChanger(ctx, pid, al, procs, faults, detector)
	detector.Subscribe(changer)

	ibx := inbox.New(registry, database.NewInMemory())

	validator := types.IntValueValidator()

	node := byzantine.New(ctx, pid, processes, al, id, changer, faults, registry, ibx, &validator, detector)
	return node
}

func registerCodecTypes(registry *codec.Registry) {
	types.RegisterIntValue(registry)
	codec.Register[byzantine.RWStateMsg](registry, byzantine.RWStateMsgName)
	codec.RegisterTyped[messages.BaseMsg](registry)
	codec.RegisterTyped[byzantine.EpochMsg](registry)
	codec.RegisterTyped[byzantine.CCSendMsg](registry)
	codec.RegisterTyped[byzantine.CCCollectedMsg](registry)
	codec.Register[byzantine.RWReadMsg](registry, byzantine.RWReadMsgName)
	codec.Register[byzantine.RWWriteMsg](registry, byzantine.RWWriteMsgName)
	codec.Register[byzantine.RWAcceptMsg](registry, byzantine.RWAcceptMsgName)
	codec.Register[election.ComplainMsg](registry, election.ComplainMsgName)
}

func listenCmds(node consensus.Consensus) {
	scanner := bufio.NewScanner(os.Stdin)
	fmt.Println("Enter value to Propose (press Enter to submit):")
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		val, err := strconv.Atoi(line)
		if err != nil {
			fmt.Printf("'%s' is not a number, try again\n", line)
			continue
		}

		fmt.Printf("Propose(%d)\n", val)
		go node.Propose(types.IntValue(val))
	}
}
