package byzantine

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reliable/p2p"
	"reliable/types"
	"reliable/utils"
)

type ConditionsCollector interface {
	Start()
	Input(i []byte) error
	SetReceiver(receiver CollectReceiver)
	Stop()
}

type CollectReceiver interface {
	OnCollected([]Sent)
}

type Sent struct {
	Sender types.ProcessID
	Msg    []byte
	Sign   []byte
}

type OutputPredicate func(sent []Sent) error

var UndefinedMsg = []byte("undefined")

type sendEnvelope struct {
	from types.ProcessID
	msg  []byte
	sing []byte
}

type signedEnvelope struct {
	Msg      []byte
	From     types.ProcessID
	Instance string
	Typ      string
}

type collectedEnvelope struct {
	from  types.ProcessID
	inner []Sent
}

type collector struct {
	types.Deliverer
	ctx            context.Context
	cancel         context.CancelFunc
	self           types.ProcessID
	id             types.Identify
	processes      []types.ProcessID
	processesCount int
	al             p2p.Link
	messages       map[types.ProcessID]Sent
	leader         types.ProcessID
	sendCh         chan sendEnvelope
	collectedCh    chan collectedEnvelope
	faults         int
	collected      bool
	predicate      OutputPredicate
	receiver       CollectReceiver
	stopCh         chan struct{}
}

func NewSignedConditionsCollector(
	ctx context.Context,
	self types.ProcessID,
	id types.Identify,
	processes []types.ProcessID,
	al p2p.Link,
	leader types.ProcessID,
	fault int,
	predicate OutputPredicate,
) ConditionsCollector {
	c := new(collector)
	c.ctx, c.cancel = context.WithCancel(ctx)
	c.self = self
	c.id = id
	c.processes = processes
	c.al = al
	c.Deliverer = types.NewUnaryDeliverer(self)
	c.al.AddDeliverer(c)

	c.leader = leader
	c.faults = fault
	c.predicate = predicate
	c.messages = make(map[types.ProcessID]Sent)
	c.sendCh = make(chan sendEnvelope, 50)
	c.collectedCh = make(chan collectedEnvelope, 50)
	c.stopCh = make(chan struct{})
	return c
}

func (c *collector) SetReceiver(receiver CollectReceiver) {
	c.receiver = receiver
}

func (c *collector) Input(i []byte) error {
	signed, err := c.signedMsg(i)
	if err != nil {
		return fmt.Errorf("sign: %w", err)
	}

	msg := NewCCSignedMsg(c.self, signed, i)
	c.al.Send(c.leader, msg)
	return nil
}

func (c *collector) Start() {
	go c.background()
}

func (c *collector) Stop() {
	c.al.RemoveDeliverer(c)
	c.cancel()
	<-c.stopCh
	close(c.sendCh)
	close(c.collectedCh)
}

func (c *collector) background() {
	defer close(c.stopCh)

OUTER:
	for {
		select {
		case <-c.ctx.Done():
			return
		case env := <-c.sendCh:
			err := c.validateSigned(env.from, env.msg, env.sing)
			if err != nil {
				continue
			}
			c.messages[env.from] = Sent{
				Sender: env.from,
				Sign:   env.sing,
				Msg:    env.msg,
			}
			c.checkCollected()

		case env := <-c.collectedCh:
			if c.collected {
				continue
			}
			if len(env.inner) < c.processesCount-c.faults {
				continue
			}
			definedMsgs := make([]Sent, 0)
			for _, msg := range env.inner {
				if !bytes.Equal(msg.Msg, UndefinedMsg) {
					definedMsgs = append(definedMsgs, msg)
				}
			}
			if len(definedMsgs) < c.processesCount-c.faults {
				continue
			}
			if err := c.predicate(env.inner); err != nil {
				continue
			}
			for _, msg := range definedMsgs {
				err := c.validateSigned(msg.Sender, msg.Msg, msg.Sign)
				if err != nil {
					continue OUTER
				}
			}
			c.collected = true
			c.receiver.OnCollected(env.inner)
		}
	}
}

func (c *collector) checkCollected() {
	if len(c.messages) < c.processesCount-c.faults {
		return
	}

	messages := utils.ValuesSlice(c.messages)
	if err := c.predicate(messages); err != nil {
		return
	}

	undefinedMgs := make(map[types.ProcessID]Sent)
	for _, pid := range c.processes {
		undefinedMgs[pid] = Sent{
			Sender: pid,
			Msg:    UndefinedMsg,
			Sign:   UndefinedMsg,
		}
	}

	msg := NewCCCollectedMsg(c.self)
	for _, m := range messages {
		msg = msg.AddMsg(CCCollectInner{
			Sender: m.Sender,
			Msg:    m.Msg,
			Sign:   m.Sign,
		})
		delete(undefinedMgs, m.Sender)
	}

	for _, m := range undefinedMgs {
		msg = msg.AddMsg(CCCollectInner{
			Sender: m.Sender,
			Msg:    m.Msg,
			Sign:   m.Sign,
		})
	}

	for _, pid := range c.processes {
		c.al.Send(pid, msg)
	}

	c.messages = make(map[types.ProcessID]Sent)
}

func (c *collector) signedMsg(msg []byte) ([]byte, error) {
	s, err := buildSign(c.self, msg)
	if err != nil {
		return nil, fmt.Errorf("build sign: %w", err)
	}

	signed, err := c.id.Sign(s)
	if err != nil {
		return nil, fmt.Errorf("sign raw: %w", err)
	}

	return signed, nil
}

func (c *collector) validateSigned(from types.ProcessID, msg []byte, sign []byte) error {
	s, err := buildSign(from, msg)
	if err != nil {
		return fmt.Errorf("build sign: %w", err)
	}

	err = c.id.Verify(from, s, sign)
	if err != nil {
		return fmt.Errorf("verify sign: %w", err)
	}

	return nil
}

func (c *collector) Deliver(msg types.Message) {
	switch m := msg.(type) {
	case CCCollectedMsg:
		env := collectedEnvelope{
			from:  m.From(),
			inner: make([]Sent, 0),
		}
		for _, innerMsg := range m.Inner {
			env.inner = append(env.inner, Sent{
				Sender: innerMsg.Sender,
				Msg:    innerMsg.Msg,
				Sign:   innerMsg.Sign,
			})
		}
		go func() {
			select {
			case <-c.ctx.Done():
			case c.collectedCh <- env:
			}
		}()

	case CCSendMsg:
		env := sendEnvelope{
			from: m.From(),
			msg:  m.Msg,
			sing: m.Signed,
		}
		go func() {
			select {
			case <-c.ctx.Done():
			case c.sendCh <- env:
			}
		}()
	}
}

func buildSign(from types.ProcessID, msg []byte) ([]byte, error) {
	s := signedEnvelope{
		Msg:      msg,
		From:     from,
		Instance: "cc",
		Typ:      "INPUT",
	}

	return json.Marshal(s)
}
