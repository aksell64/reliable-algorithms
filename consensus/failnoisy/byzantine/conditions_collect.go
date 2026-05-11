package byzantine

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reliable/logger"
	"reliable/p2p"
	"reliable/types"
	"reliable/utils"

	"github.com/rs/zerolog"
)

type ConditionsCollector interface {
	types.Deliverer
	Start()
	Input(i []byte) error
	SetReceiver(receiver CollectReceiver)
	Stop()
}

type CollectReceiver interface {
	ValidateCollected([]Sent) error
	OnCollected([]Sent) error
}

type Sent struct {
	Sender types.ProcessID
	Msg    []byte
	Sign   []byte
}

func (s Sent) IsUndefined() bool {
	return bytes.Equal(s.Msg, UndefinedMsg)
}

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
	epoch          int
	processes      []types.ProcessID
	processesCount int
	al             p2p.Link
	messages       map[types.ProcessID]Sent
	leader         types.ProcessID
	sendCh         chan sendEnvelope
	collectedCh    chan collectedEnvelope
	faults         int
	collected      bool
	receiver       CollectReceiver
	stopCh         chan struct{}
	logger         zerolog.Logger
}

func NewSignedConditionsCollector(
	ctx context.Context,
	self types.ProcessID,
	id types.Identify,
	processes []types.ProcessID,
	al p2p.Link,
	leader types.ProcessID,
	fault int,
	epoch int,
) ConditionsCollector {
	c := new(collector)
	c.ctx, c.cancel = context.WithCancel(ctx)
	c.self = self
	c.id = id
	c.processes = processes
	c.al = al
	c.Deliverer = types.NewUnaryDeliverer(self)

	c.leader = leader
	c.faults = fault
	c.processesCount = len(processes)
	c.messages = make(map[types.ProcessID]Sent)
	c.sendCh = make(chan sendEnvelope, 50)
	c.collectedCh = make(chan collectedEnvelope, 50)
	c.stopCh = make(chan struct{})
	c.logger = logger.NewNodeScopeLogger(self, logger.Scope{"cc", "byz"})
	c.epoch = epoch

	c.logger.Info().
		Str("leader", leader.String()).
		Int("processes", len(processes)).
		Int("faults", fault).
		Int("threshold", len(processes)-fault).
		Msg("created")

	return c
}

func (c *collector) SetReceiver(receiver CollectReceiver) {
	c.receiver = receiver
}

func (c *collector) Input(i []byte) error {
	c.logger.Info().
		Str("to", c.leader.String()).
		Msg("input: signing and sending state to leader")

	signed, err := c.signedMsg(i)
	if err != nil {
		c.logger.Error().Err(err).Msg("input: sign failed")
		return fmt.Errorf("sign: %w", err)
	}

	msg := NewCCSignedMsg(c.self, signed, i, c.epoch)
	c.al.Send(c.leader, msg)
	return nil
}

func (c *collector) Start() {
	c.logger.Info().Msg("start")
	go c.background()
}

func (c *collector) Stop() {
	c.logger.Info().Msg("stopping")
	c.al.RemoveDeliverer(c)
	c.cancel()
	<-c.stopCh
	close(c.sendCh)
	close(c.collectedCh)
	c.logger.Info().Msg("stopped")
}

func (c *collector) background() {
	defer close(c.stopCh)

OUTER:
	for {
		select {
		case <-c.ctx.Done():
			c.logger.Info().Msg("context cancelled, background exiting")
			return

		case env := <-c.sendCh:
			err := c.validateSigned(env.from, env.msg, env.sing)
			if err != nil {
				c.logger.Warn().
					Str("from", env.from.String()).
					Err(err).
					Msg("SEND: invalid signature, discarding")
				continue
			}
			c.messages[env.from] = Sent{
				Sender: env.from,
				Sign:   env.sing,
				Msg:    env.msg,
			}
			c.logger.Info().
				Str("from", env.from.String()).
				Int("collected", len(c.messages)).
				Int("threshold", c.processesCount-c.faults).
				Msg("SEND: state accepted")
			c.checkCollected()

		case env := <-c.collectedCh:
			if c.collected {
				c.logger.Warn().
					Str("from", env.from.String()).
					Msg("COLLECTED: already collected, ignoring duplicate")
				continue
			}
			if len(env.inner) < c.processesCount-c.faults {
				c.logger.Warn().
					Str("from", env.from.String()).
					Int("got", len(env.inner)).
					Int("threshold", c.processesCount-c.faults).
					Msg("COLLECTED: not enough entries, ignoring")
				continue
			}
			definedMsgs := make([]Sent, 0)
			for _, msg := range env.inner {
				if !bytes.Equal(msg.Msg, UndefinedMsg) {
					definedMsgs = append(definedMsgs, msg)
				}
			}
			if len(definedMsgs) < c.processesCount-c.faults {
				c.logger.Warn().
					Str("from", env.from.String()).
					Int("defined", len(definedMsgs)).
					Int("threshold", c.processesCount-c.faults).
					Msg("COLLECTED: not enough defined messages, ignoring")
				continue
			}
			for _, msg := range definedMsgs {
				err := c.validateSigned(msg.Sender, msg.Msg, msg.Sign)
				if err != nil {
					c.logger.Warn().
						Str("from", env.from.String()).
						Str("sender", msg.Sender.String()).
						Err(err).
						Msg("COLLECTED: signature verification failed, discarding")
					continue OUTER
				}
			}
			err := c.receiver.OnCollected(env.inner)
			if err != nil {
				c.logger.Warn().
					Str("from", env.from.String()).
					Err(err).
					Msg("COLLECTED: OnCollected rejected")
				continue
			}
			c.logger.Info().
				Str("from", env.from.String()).
				Int("total", len(env.inner)).
				Int("defined", len(definedMsgs)).
				Msg("COLLECTED: accepted, triggering epoch consensus")
			c.collected = true
		}
	}
}

func (c *collector) checkCollected() {
	if len(c.messages) < c.processesCount-c.faults {
		return
	}

	messages := utils.ValuesSlice(c.messages)
	if err := c.receiver.ValidateCollected(messages); err != nil {
		c.logger.Warn().
			Err(err).
			Int("messagesCount", len(messages)).
			Msg("checkCollected: validation failed, not broadcasting")
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

	msg := NewCCCollectedMsg(c.self, c.epoch)
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

	c.logger.Info().
		Int("defined", len(messages)).
		Int("undefined", len(undefinedMgs)).
		Int("total", len(c.processes)).
		Msg("checkCollected: threshold reached, broadcasting COLLECTED")

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
		c.logger.Info().
			Str("from", m.From().String()).
			Int("innerCount", len(m.Inner)).
			Msg("deliver: COLLECTED received")
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
				return
			default:
			}

			select {
			case <-c.ctx.Done():
			case c.collectedCh <- env:
			}
		}()

	case CCSendMsg:
		c.logger.Info().
			Str("from", m.From().String()).
			Msg("deliver: SEND received")
		env := sendEnvelope{
			from: m.From(),
			msg:  m.Msg,
			sing: m.Signed,
		}
		go func() {
			select {
			case <-c.ctx.Done():
				return
			default:
			}

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
