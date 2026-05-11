package main

import (
	"context"
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
)

// Byzantine consensus requires N > 3f. With 4 processes and 1 fault: 4 > 3*1.
const (
	totalProcesses  = 10
	byzantineFaults = 3
)

func main() {
	ctx := context.Background()

	net := network.NewInMemory()
	network.SetGlobal(net)

	processesSlice := utils.ProcessesIDRange(1, totalProcesses)
	processes := make(map[types.ProcessID]types.ProcessRank)
	for _, pid := range processesSlice {
		processes[pid] = types.ProcessRank(pid)
	}

	nodes := make([]consensus.Consensus, 0, len(processes))
	for pid, _ := range processes {
		nodes = append(nodes, makeConsensus(ctx, processes, pid, byzantineFaults))
	}

	for _, node := range nodes {
		node.Init()
	}

	for _, node := range nodes {
		node.Start()
	}

	for i, node := range nodes {

		if int(node.ProcessID()) < len(nodes)/2 {
			go node.Propose(types.IntValue(-1))
			continue
		}

		if int(node.ProcessID()) == 10 {
			go node.Propose(types.IntValue(-1))
			continue
		}

		value := i*10 + 1
		go node.Propose(types.IntValue(value))
	}

	consensus.PrintDecided(consensus.MustUniqAgreement(consensus.CollectDecided(ctx, nodes...)))
}

func makeConsensus(
	ctx context.Context,
	processes map[types.ProcessID]types.ProcessRank,
	pid types.ProcessID,
	faults int,
) consensus.Consensus {
	registry := codec.New()
	registerCodecTypes(registry)

	al := p2p.NewBaseLink(pid)
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
	// Internal protocol: value and state serialization
	types.RegisterIntValue(registry)
	codec.Register[byzantine.RWStateMsg](registry, byzantine.RWStateMsgName)

	// Network-level: all message types sent over the wire
	codec.RegisterTyped[messages.BaseMsg](registry)
	codec.RegisterTyped[byzantine.EpochMsg](registry)
	codec.RegisterTyped[byzantine.CCSendMsg](registry)
	codec.RegisterTyped[byzantine.CCCollectedMsg](registry)
	codec.Register[byzantine.RWReadMsg](registry, byzantine.RWReadMsgName)
	codec.Register[byzantine.RWWriteMsg](registry, byzantine.RWWriteMsgName)
	codec.Register[byzantine.RWAcceptMsg](registry, byzantine.RWAcceptMsgName)
}
