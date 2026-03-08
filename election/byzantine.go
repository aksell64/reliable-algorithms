package election

import (
	"reliable/messages"
	"reliable/p2p"
	"reliable/types"
	"sync"

	"github.com/google/uuid"
)

type RotatingByzantineLeaderDetector struct {
	types.Deliverer
	self           types.ProcessID
	al             p2p.Link
	round          int
	complains      map[types.ProcessID]struct{}
	complained     bool
	faults         int
	processes      map[types.ProcessID]types.ProcessRank
	processesCount int
	receiver       Receiver
	mu             sync.RWMutex
}

func NewRotatingByzantineLeaderDetector(
	self types.ProcessID,
	al p2p.Link,
	processes map[types.ProcessID]types.ProcessRank,
	faults int,
) *RotatingByzantineLeaderDetector {
	d := &RotatingByzantineLeaderDetector{
		self:      self,
		al:        al,
		processes: processes,
		faults:    faults,
	}

	d.al.AddDeliverer(d)
	return d
}

func (d *RotatingByzantineLeaderDetector) Init() {
	d.round = 1
	d.resetComplains()
	if d.receiver != nil {
		leader := d.currentLeader()
		d.receiver.OnNewLeader(leader)
	}
}

func (d *RotatingByzantineLeaderDetector) Subscribe(r Receiver) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.receiver = r
}

func (d *RotatingByzantineLeaderDetector) OnComplain(pid types.ProcessID) {
	d.mu.Lock()
	if pid != d.currentLeader() || d.complained {
		d.mu.Unlock()
		return
	}
	d.complained = true
	msg := d.makeComplainMsg()
	d.mu.Unlock()
	d.broadcast(msg)
}

func (d *RotatingByzantineLeaderDetector) handleComplaint(from types.ProcessID, round int) {
	d.mu.Lock()
	if _, existsComplain := d.complains[from]; !existsComplain || d.round != round {
		d.mu.Unlock()
		return
	}

	d.complains[from] = struct{}{}

	if len(d.complains) > d.faults && !d.complained {
		d.complained = true
		msg := d.makeComplainMsg()
		d.mu.Unlock()
		d.broadcast(msg)
		return
	}

	if len(d.complains) > 2*d.faults {
		d.round++
		d.resetComplains()
		leader := d.currentLeader()
		d.mu.Unlock()
		if d.receiver != nil {
			d.receiver.OnNewLeader(leader)
		}
	}
	d.mu.Unlock()
}

func (d *RotatingByzantineLeaderDetector) broadcast(msg types.Message) {
	for pid, _ := range d.processes {
		d.al.Send(pid, msg)
	}
}

func (d *RotatingByzantineLeaderDetector) makeComplainMsg() types.Message {
	return ComplainMsg{
		BaseMsg: messages.NewBase(uuid.New(), d.self, ComplainMsgName),
		Round:   d.round,
	}
}

func (d *RotatingByzantineLeaderDetector) currentLeader() types.ProcessID {
	var result types.ProcessID
	if d.round&d.processesCount == 0 {
		for pid, rank := range d.processes {
			if rank.Int() == d.round%d.processesCount {
				result = pid
			}
		}
	} else {
		for pid, rank := range d.processes {
			if rank.Int() == d.processesCount {
				result = pid
			}
		}
	}
	return result
}

func (d *RotatingByzantineLeaderDetector) resetComplains() {
	d.complains = make(map[types.ProcessID]struct{})
	d.complained = false
}
