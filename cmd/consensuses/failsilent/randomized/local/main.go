package main

import (
	"context"
	"crypto/sha256"
	"reliable/broadcaster"
	"reliable/consensus"
	"reliable/consensus/coin/commitreveal"
	"reliable/consensus/failsilent/randomized"
	"reliable/database"
	"reliable/messages"
	"reliable/p2p"
	"reliable/types"
	"reliable/types/inbox"
	"reliable/utils"
	"reliable/utils/codec"
)

func main() {

	ctx := context.Background()

	processes := utils.ProcessesIDRange(1, 10)

	nodes := make([]consensus.Consensus, 0, len(processes))

	for _, pid := range processes {
		nodes = append(nodes, makeConsensus(ctx, processes, pid, 2))
	}

	for _, node := range nodes {
		node.Init()
	}

	for _, node := range nodes {
		node.Start()
	}

	for i, node := range nodes {
		value := i*10 + 1
		go node.Propose(types.IntValue(value))
	}

	consensus.PrintDecided(consensus.MustUniqAgreement(consensus.CollectDecided(ctx, nodes...)))
}

func makeConsensus(
	ctx context.Context,
	processes []types.ProcessID,
	pid types.ProcessID,
	crashFaults int,
) consensus.Consensus {
	registry := codec.New()

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
		nil,
	)

	node := randomized.New(ctx, processes, pid, crashFaults, coin, beb2, rb, nil, registry)
	return node
}
