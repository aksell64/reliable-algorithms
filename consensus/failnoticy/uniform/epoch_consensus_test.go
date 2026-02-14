package uniform

import (
	"context"
	"reliable/broadcaster"
	"reliable/mocks"
	"reliable/network"
	"reliable/p2p"
	"reliable/types"
	"reliable/utils"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	readTimeout = 200 * time.Millisecond
	raceTimeout = 2 * time.Second
)

// ──────────────────────────────────────────────────────────────
// 1. Abort: базовый — не зависает, каналы закрываются корректно
// ──────────────────────────────────────────────────────────────

func TestAbort_NoDeadlock(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), raceTimeout)
	defer cancel()

	N := 5
	processes := utils.ProcessesIDRange(1, N)

	net := network.NewInMemory()
	ecs := setupFullEpochConsensus(ctx, processes, net)
	leaderID := processes[0]

	for _, ec := range ecs {
		ec.StartEpoch(ctx, leaderID, 1, State{})
	}

	<-time.After(50 * time.Millisecond)

	// Abort на каждом — ни один не должен зависнуть
	done := make(chan struct{})
	go func() {
		defer close(done)
		for _, ec := range ecs {
			ec.Abort()
		}
	}()

	select {
	case <-done:
		// ОК, не зависли
	case <-time.After(raceTimeout):
		t.Fatal("DEADLOCK: Abort() завис")
	}

	// Проверяем, что aborted канал отдал состояние и закрылся
	for i, ec := range ecs {
		abortedState, ok := <-ec.Aborted()
		require.True(t, ok, "instance %d: aborted channel должен отдать значение перед закрытием", i)
		assert.Equal(t, 1, abortedState.Ts, "instance %d: неверный epoch ts в aborted state", i)

		// Канал должен быть закрыт после cleanup
		_, ok = <-ec.Aborted()
		assert.False(t, ok, "instance %d: aborted channel должен быть закрыт", i)

		// decided тоже закрыт
		_, ok = <-ec.Decided()
		assert.False(t, ok, "instance %d: decided channel должен быть закрыт", i)
	}
}

// ──────────────────────────────────────────────────────────────
// 2. Abort вызван N раз параллельно — stopOnce гарантирует
//    ровно один cleanup, без паники на double close
// ──────────────────────────────────────────────────────────────

func TestAbort_ConcurrentMultipleCalls_NoPanic(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), raceTimeout)
	defer cancel()

	N := 5
	processes := utils.ProcessesIDRange(1, N)

	net := network.NewInMemory()
	ecs := setupFullEpochConsensus(ctx, processes, net)
	leaderID := processes[0]

	for _, ec := range ecs {
		ec.StartEpoch(ctx, leaderID, 1, State{})
	}

	// Для каждого ec — запускаем 10 горутин, которые одновременно дёргают Abort
	for _, ec := range ecs {
		ec := ec
		done := make(chan struct{})
		go func() {
			defer close(done)
			var wg sync.WaitGroup
			for i := 0; i < 10; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					ec.Abort() // не должно паниковать на double close
				}()
			}
			wg.Wait()
		}()

		select {
		case <-done:
		case <-time.After(raceTimeout):
			t.Fatal("DEADLOCK: concurrent Abort() завис")
		}
	}
}

// ──────────────────────────────────────────────────────────────
// 3. Decide: полный цикл — не зависает, decided отдаёт значение
// ──────────────────────────────────────────────────────────────

func TestDecide_NoDeadlock(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), raceTimeout)
	defer cancel()

	N := 5
	processes := utils.ProcessesIDRange(1, N)

	net := network.NewInMemory()
	ecs := setupFullEpochConsensus(ctx, processes, net)
	leaderID := processes[0]

	for _, ec := range ecs {
		ec.StartEpoch(ctx, leaderID, 1, State{})
	}

	proposed := types.IntValue(42)
	ecs[0].Propose(proposed)

	for i, ec := range ecs {
		select {
		case val := <-ec.Decided():
			assert.True(t, val.Compare(proposed), "instance %d: wrong decided value", i)
		case <-time.After(raceTimeout):
			t.Fatalf("DEADLOCK: Decided() завис на instance %d", i)
		}
	}
}

// ──────────────────────────────────────────────────────────────
// 4. ГОНКА: Abort и Decide одновременно
//    Только одно из двух должно сработать. Без паник и дедлоков.
// ──────────────────────────────────────────────────────────────

func TestAbortAndDecide_Race(t *testing.T) {
	t.Parallel()

	// Прогоняем много раз — рейсы вероятностные
	for iteration := 0; iteration < 50; iteration++ {
		func() {
			ctx, cancel := context.WithTimeout(context.Background(), raceTimeout)
			defer cancel()

			N := 3
			processes := utils.ProcessesIDRange(1, N)

			net := network.NewInMemory()
			ecs := setupFullEpochConsensus(ctx, processes, net)
			leaderID := processes[0]

			for _, ec := range ecs {
				ec.StartEpoch(ctx, leaderID, 1, State{})
			}

			proposed := types.IntValue(77)
			ecs[0].Propose(proposed)

			// На каждом инстансе одновременно дёргаем Abort
			// и слушаем Decided. stopOnce гарантирует взаимоисключение.
			var wg sync.WaitGroup
			for i, ec := range ecs {
				i, ec := i, ec
				wg.Add(1)
				go func() {
					defer wg.Done()
					ec.Abort()
				}()

				wg.Add(1)
				go func() {
					defer wg.Done()
					select {
					case _, ok := <-ec.Decided():
						// Либо получили значение, либо канал закрыт — оба варианта ОК
						_ = ok
					case <-time.After(raceTimeout):
						t.Errorf("instance %d: timeout reading Decided()", i)
					}
				}()
			}

			done := make(chan struct{})
			go func() {
				wg.Wait()
				close(done)
			}()

			select {
			case <-done:
			case <-time.After(raceTimeout):
				t.Fatal("DEADLOCK: Abort+Decide race завис")
			}
		}()
	}
}

// ──────────────────────────────────────────────────────────────
// 5. Abort во время фазы Reading (лидер ещё не набрал кворум)
//    Убеждаемся, что event loop корректно завершается
// ──────────────────────────────────────────────────────────────

func TestAbort_DuringReading_NoDeadlock(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), raceTimeout)
	defer cancel()

	N := 5
	processes := utils.ProcessesIDRange(1, N)

	net := network.NewInMemory()
	ecs := setupFullEpochConsensus(ctx, processes, net)
	leaderID := processes[0]

	for _, ec := range ecs {
		ec.StartEpoch(ctx, leaderID, 1, State{})
	}

	// Лидер начал Propose → перешёл в Reading, но мы сразу абортим,
	// не давая кворуму state-ответов набраться
	proposed := types.IntValue(55)
	ecs[0].Propose(proposed)

	// Небольшая пауза — чтобы Propose успел дойти до event loop
	time.Sleep(10 * time.Millisecond)

	done := make(chan struct{})
	go func() {
		defer close(done)
		for _, ec := range ecs {
			ec.Abort()
		}
	}()

	select {
	case <-done:
	case <-time.After(raceTimeout):
		t.Fatal("DEADLOCK: Abort during Reading завис")
	}
}

// ──────────────────────────────────────────────────────────────
// 6. Abort во время Writing (лидер набрал state-кворум,
//    разослал Write, но accept-кворума ещё нет)
// ──────────────────────────────────────────────────────────────

func TestAbort_DuringWriting_NoDeadlock(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), raceTimeout)
	defer cancel()

	N := 5
	processes := utils.ProcessesIDRange(1, N)

	// Используем моки для p2p у фолловеров, чтобы контролировать Accept-ы
	// Но для простоты — запустим полный стек и абортим на лидере
	// в момент, когда он уже в Writing
	net := network.NewInMemory()
	ecs := setupFullEpochConsensus(ctx, processes, net)
	leaderID := processes[0]

	for _, ec := range ecs {
		ec.StartEpoch(ctx, leaderID, 1, State{})
	}

	proposed := types.IntValue(66)
	ecs[0].Propose(proposed)

	// Ждём чуть дольше — чтобы Read-фаза прошла и Write-broadcast случился
	time.Sleep(50 * time.Millisecond)

	done := make(chan struct{})
	go func() {
		defer close(done)
		for _, ec := range ecs {
			ec.Abort()
		}
	}()

	select {
	case <-done:
	case <-time.After(raceTimeout):
		t.Fatal("DEADLOCK: Abort during Writing завис")
	}
}

// ──────────────────────────────────────────────────────────────
// 7. onDecided вызван дважды через event loop — stopOnce
//    не даёт двойного cleanup / double close каналов
// ──────────────────────────────────────────────────────────────

func TestDecide_DuplicateDecidedEvent_NoPanic(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), raceTimeout)
	defer cancel()

	N := 3
	instances := setupNInstances(ctx, N)
	self := instances[0]

	self.leader = instances[1].ProcessID()
	self.stopOnce = sync.Once{}
	self.decided = make(chan types.Value, 1)
	self.stopCh = make(chan struct{})
	self.aborted = make(chan AbortedState, 1)

	ecCtx, ecCancel := context.WithCancel(ctx)
	self.ctx = ecCtx
	self.cancel = ecCancel

	go self.eventLoop()

	self.sm.SetState(StateIdle)

	val := types.IntValue(123)

	// Первый decidedEvent — нормальный
	self.sm.Apply(decidedEvent{val: val})

	// Второй — дубликат; не должен паниковать
	self.sm.Apply(decidedEvent{val: val})

	select {
	case decided := <-self.decided:
		assert.True(t, decided.Compare(val))
	case <-time.After(raceTimeout):
		t.Fatal("DEADLOCK: decided channel timeout")
	}
}

// ──────────────────────────────────────────────────────────────
// 8. Decide на одном узле + Abort на нём же параллельно —
//    ровно один путь срабатывает, cleanup один раз
// ──────────────────────────────────────────────────────────────

func TestDecideAndAbort_SameNode_Race(t *testing.T) {
	t.Parallel()

	for iteration := 0; iteration < 100; iteration++ {
		func() {
			ctx, cancel := context.WithTimeout(context.Background(), raceTimeout)
			defer cancel()

			N := 3
			processes := utils.ProcessesIDRange(1, N)

			net := network.NewInMemory()
			ecs := setupFullEpochConsensus(ctx, processes, net)
			leaderID := processes[0]

			for _, ec := range ecs {
				ec.StartEpoch(ctx, leaderID, 1, State{})
			}

			proposed := types.IntValue(99)
			ecs[0].Propose(proposed)

			target := ecs[1] // фолловер

			var wg sync.WaitGroup

			// Горутина 1: Abort
			wg.Add(1)
			go func() {
				defer wg.Done()
				target.Abort()
			}()

			// Горутина 2: ждём decided или закрытие
			wg.Add(1)
			go func() {
				defer wg.Done()
				select {
				case <-target.Decided():
				case <-time.After(raceTimeout):
				}
			}()

			// Горутина 3: ждём aborted или закрытие
			wg.Add(1)
			go func() {
				defer wg.Done()
				select {
				case <-target.Aborted():
				case <-time.After(raceTimeout):
				}
			}()

			allDone := make(chan struct{})
			go func() {
				wg.Wait()
				close(allDone)
			}()

			select {
			case <-allDone:
			case <-time.After(raceTimeout):
				t.Fatal("DEADLOCK: Decide+Abort on same node завис")
			}
		}()
	}
}

// ──────────────────────────────────────────────────────────────
// 9. Abort после Decide — не должен паниковать / зависать
// ──────────────────────────────────────────────────────────────

func TestAbort_AfterDecide_NoPanic(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), raceTimeout)
	defer cancel()

	N := 3
	network.SetGlobal(network.NewInMemory())
	processes := utils.ProcessesIDRange(1, N)

	net := network.NewInMemory()
	ecs := setupFullEpochConsensus(ctx, processes, net)
	leaderID := processes[0]

	for _, ec := range ecs {
		ec.StartEpoch(ctx, leaderID, 1, State{})
	}

	proposed := types.IntValue(42)
	ecs[0].Propose(proposed)

	// Ждём, пока все решат
	for i, ec := range ecs {
		select {
		case <-ec.Decided():
		case <-time.After(raceTimeout):
			t.Fatalf("timeout waiting for decided on instance %d", i)
		}
	}

	// Теперь Abort на всех — уже решили, stopOnce уже сработал
	done := make(chan struct{})
	go func() {
		defer close(done)
		for _, ec := range ecs {
			ec.Abort()
		}
	}()

	select {
	case <-done:
	case <-time.After(raceTimeout):
		t.Fatal("DEADLOCK: Abort after Decide завис")
	}
}

// ──────────────────────────────────────────────────────────────
// 10. Abort сохраняет корректный current state
// ──────────────────────────────────────────────────────────────

func TestAbort_PreservesState(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), raceTimeout)
	defer cancel()

	N := 3
	processes := utils.ProcessesIDRange(1, N)

	net := network.NewInMemory()
	ecs := setupFullEpochConsensus(ctx, processes, net)
	leaderID := processes[0]

	initialVal := types.IntValue(555)
	initialState := State{ts: 3, val: utils.Ptr(initialVal)}

	for _, ec := range ecs {
		ec.StartEpoch(ctx, leaderID, 7, initialState)
	}

	<-time.After(100 * time.Millisecond)

	// Абортим фолловера сразу — его state не должен измениться
	follower := ecs[1]

	done := make(chan struct{})
	go func() {
		defer close(done)
		follower.Abort()
	}()

	select {
	case <-done:
	case <-time.After(raceTimeout):
		t.Fatal("DEADLOCK: Abort завис")
	}

	abortedState := <-follower.Aborted()
	// Может быть ok=false (closed), но первое чтение должно быть значением
	assert.Equal(t, 7, abortedState.Ts)
	require.NotNil(t, abortedState.State)
	assert.Equal(t, 3, abortedState.State.ts)
	require.NotNil(t, abortedState.State.val)
	assert.True(t, (*abortedState.State.val).Compare(initialVal))
}

// ──────────────────────────────────────────────────────────────
// Хелпер: поднимает полный стек ec с реальным сетевым слоем
// ──────────────────────────────────────────────────────────────

func setupFullEpochConsensus(ctx context.Context, processes []types.ProcessID, net network.Network) []*epochConsensus {
	N := len(processes)
	ecs := make([]*epochConsensus, 0, N)

	for _, pid := range processes {
		bl := p2p.NewBaseLink(pid, p2p.WithNetwork(net))
		sl := p2p.NewStubbornP2PLinks(ctx, bl)
		pl := p2p.NewPerfectP2PLinks(pid, sl)
		beb := broadcaster.NewBestEffortBroadcaster(
			pid, processes,
			broadcaster.DefaultBroadcastNodeSelector(), pl,
		)
		types.InitWorkers(bl, sl, pl, beb)
		types.StartWorkers(bl, sl, pl, beb)

		ec := newEpochConsensus(pid, beb, pl, N, nil)
		ecs = append(ecs, ec)
	}

	for _, ec := range ecs {
		for _, other := range ecs {
			ec.beb.AddCorrect(other.self)
		}
	}

	return ecs
}

func TestTransitions(t *testing.T) {
	t.Parallel()
	t.Run("transitions", func(t *testing.T) {
		runInstancesTest(t, "onInit", 10, testOnInit)
		runInstancesTest(t, "onPropose", 10, testOnPropose)
		runInstancesTest(t, "onReceivedRead", 10, testOnReceivedRead)
		runInstancesTest(t, "onReceivedAccept", 10, testOnReceivedAccept)
		runInstancesTest(t, "onHandleRead", 10, testOnHandleRead)
		runInstancesTest(t, "onHandleWrite", 10, testOnHandleWrite)
		runInstancesTest(t, "onDecided", 10, testOnDecided)
	})
}

func runInstancesTest(
	t *testing.T,
	name string,
	instancesCount int,
	inner func(t *testing.T, instancesCount int)) {
	t.Run(name, func(t *testing.T) {
		inner(t, instancesCount)
	})
}

func runEachInstance(t *testing.T, name string, count int, f func(self *instance, instances []*instance)) {
	t.Run(name, func(t *testing.T) {
		instances := setupNInstances(context.Background(), count)
		for _, instance := range instances {
			f(instance, instances)
		}
	})
}

func runSingleInstance(t *testing.T, name string, count int, idx int, f func(self *instance, instances []*instance)) {
	t.Run(name, func(t *testing.T) {
		instances := setupNInstances(context.Background(), count)
		self := instances[idx]
		f(self, instances)
	})
}

// ──────────────────────────────────────────────
// onInit
// ──────────────────────────────────────────────

func testOnInit(t *testing.T, instancesCount int) {
	runEachInstance(t, "zero state", instancesCount, func(self *instance, instances []*instance) {
		leader := instances[0]
		testOnInitApply(t, leader.ProcessID(), 0, State{}, self, instances)
	})

	runEachInstance(t, "nozero state", instancesCount, func(self *instance, instances []*instance) {
		state := State{
			ts:  5,
			val: utils.Ptr(types.IntValue(10)),
		}
		leader := instances[0]
		testOnInitApply(t, leader.ProcessID(), 0, state, self, instances)
	})
}

func testOnInitApply(t *testing.T, leader types.ProcessID, ets int, state State, instance *instance, instances []*instance) {
	t.Helper()
	instance.sm.SetState(StateIdle)

	evt := initEvent{
		epochTs: ets,
		leader:  leader,
		current: state,
	}

	instance.sm.Apply(evt)

	assert.Equal(t, instance.epochTs, ets)
	assert.Equal(t, instance.leader, leader)

	assert.Equal(t, instance.current.ts, state.ts)
	assert.Equal(t, instance.current.val, state.val)

	if evt.leader == instance.ProcessID() {
		assert.Equal(t, instance.sm.Current(), StatePropose)
	} else {
		assert.Equal(t, instance.sm.Current(), StateIdle)
	}

	for _, i := range instances {
		if !i.bebDeliverer.Empty() {
			t.Fatalf("beb deliverer not empty: instance: %d", i.ProcessID())
		}
	}
}

// ──────────────────────────────────────────────
// onPropose
// ──────────────────────────────────────────────

func testOnPropose(t *testing.T, instancesCount int) {
	runSingleInstance(t, "leader", instancesCount, 0, func(self *instance, instances []*instance) {
		self.sm.SetState(StatePropose)
		self.leader = self.ProcessID()
		evt := proposeEvent{
			val: types.IntValue(10),
		}
		self.sm.Apply(evt)

		if self.tempValue == nil {
			t.Error("nil temp")
		}

		temp := *self.tempValue
		if !temp.Compare(evt.val) {
			t.Error("not eq temp")
		}

		assert.Equal(t, self.sm.Current(), StateReading)

		for _, i := range instances {
			select {
			case <-time.After(readTimeout):
				t.Errorf("read beb timeout: instance: %d", i.ProcessID())
			case msg := <-i.bebDeliverer.Ch():
				pmsg, ok := msg.(ReadMsg)
				if !ok {
					t.Fatalf("not a ReadMsg beb received: instance: %d", i.ProcessID())
				}
				if pmsg.From() != self.ProcessID() {
					t.Fatalf("wrong msg from: instance: %d", i.ProcessID())
				}
			}
		}
	})

	runSingleInstance(t, "noleader", instancesCount, 0, func(self *instance, instances []*instance) {
		self.sm.SetState(StateIdle)
		self.leader = instances[1].ProcessID()
		evt := proposeEvent{
			val: types.IntValue(10),
		}
		self.sm.Apply(evt)

		assert.Equal(t, self.sm.Current(), StateIdle)

		for _, i := range instances {
			if !i.bebDeliverer.Empty() {
				t.Fatalf("beb deliverer not empty: instance: %d", i.ProcessID())
			}
		}
	})
}

// ──────────────────────────────────────────────
// onReceivedRead
// ──────────────────────────────────────────────

func testOnReceivedRead(t *testing.T, instancesCount int) {
	// Кейс 1: не набрали кворум — остаёмся в StateReading
	runSingleInstance(t, "below quorum stays reading", instancesCount, 0, func(self *instance, instances []*instance) {
		self.sm.SetState(StateReading)
		self.leader = self.ProcessID()
		self.receivedStates = make(map[types.ProcessID]State)
		proposed := types.IntValue(42)
		self.tempValue = utils.Ptr(proposed)

		// Шлём кворум-1 STATE-ов — недостаточно
		for i := 0; i < self.quorum()-1; i++ {
			evt := receivedReadEvent{
				msg: makeStateMsg(instances[i+1].ProcessID(), 0, nil),
			}
			self.sm.Apply(evt)
		}

		assert.Equal(t, StateReading, self.sm.Current())
		assert.Equal(t, self.quorum()-1, len(self.receivedStates))

		// beb не должен был получить WRITE
		for _, inst := range instances {
			if !inst.bebDeliverer.Empty() {
				t.Fatalf("beb deliverer not empty before quorum: instance: %d", inst.ProcessID())
			}
		}
	})

	// Кейс 2: набрали кворум, все STATE с nil — tempValue не меняется, переход в Writing
	runSingleInstance(t, "quorum with nil vals keeps tempValue", instancesCount, 0, func(self *instance, instances []*instance) {
		self.sm.SetState(StateReading)
		self.leader = self.ProcessID()
		self.receivedStates = make(map[types.ProcessID]State)
		proposed := types.IntValue(42)
		self.tempValue = utils.Ptr(proposed)

		for i := 0; i < self.quorum(); i++ {
			evt := receivedReadEvent{
				msg: makeStateMsg(instances[i].ProcessID(), 0, nil),
			}
			self.sm.Apply(evt)
		}

		assert.Equal(t, StateWriting, self.sm.Current())
		assert.NotNil(t, self.tempValue)
		assert.True(t, (*self.tempValue).Compare(proposed))

		// Все должны получить WriteMsg через beb
		for _, inst := range instances {
			select {
			case <-time.After(readTimeout):
				t.Errorf("write beb timeout: instance: %d", inst.ProcessID())
			case msg := <-inst.bebDeliverer.Ch():
				wmsg, ok := msg.(WriteMsg)
				if !ok {
					t.Fatalf("not a WriteMsg: instance: %d", inst.ProcessID())
				}
				assert.Equal(t, self.ProcessID(), wmsg.From())
				assert.NotNil(t, wmsg.Val)
				assert.True(t, (*wmsg.Val).Compare(proposed))
			}
		}
	})

	// Кейс 3: набрали кворум, один STATE с более высоким ts — tempValue перезаписывается
	runSingleInstance(t, "quorum with higher ts overrides tempValue", instancesCount, 0, func(self *instance, instances []*instance) {
		self.sm.SetState(StateReading)
		self.leader = self.ProcessID()
		self.receivedStates = make(map[types.ProcessID]State)
		proposed := types.IntValue(42)
		override := types.IntValue(99)
		self.tempValue = utils.Ptr(proposed)

		// Первый STATE с высоким ts
		self.sm.Apply(receivedReadEvent{
			msg: makeStateMsg(instances[1].ProcessID(), 10, utils.Ptr(override)),
		})

		// Остальные STATE с ts=0, nil
		for i := 2; i <= self.quorum(); i++ {
			self.sm.Apply(receivedReadEvent{
				msg: makeStateMsg(instances[i].ProcessID(), 0, nil),
			})
		}

		assert.Equal(t, StateWriting, self.sm.Current())
		assert.NotNil(t, self.tempValue)
		assert.True(t, (*self.tempValue).Compare(override))

		for _, inst := range instances {
			select {
			case <-time.After(readTimeout):
				t.Errorf("write beb timeout: instance: %d", inst.ProcessID())
			case msg := <-inst.bebDeliverer.Ch():
				wmsg, ok := msg.(WriteMsg)
				if !ok {
					t.Fatalf("not a WriteMsg: instance: %d", inst.ProcessID())
				}
				assert.True(t, (*wmsg.Val).Compare(override))
			}
		}
	})

	// Кейс 4: дубликат от того же процесса — не увеличивает счётчик
	runSingleInstance(t, "duplicate from same process does not double count", instancesCount, 0, func(self *instance, instances []*instance) {
		self.sm.SetState(StateReading)
		self.leader = self.ProcessID()
		self.receivedStates = make(map[types.ProcessID]State)
		proposed := types.IntValue(42)
		self.tempValue = utils.Ptr(proposed)

		sender := instances[1].ProcessID()

		// Два STATE от одного процесса
		self.sm.Apply(receivedReadEvent{
			msg: makeStateMsg(sender, 0, nil),
		})
		self.sm.Apply(receivedReadEvent{
			msg: makeStateMsg(sender, 1, nil),
		})

		// Должен быть только 1 entry в receivedStates (перезаписанный)
		assert.Equal(t, 1, len(self.receivedStates))
		assert.Equal(t, StateReading, self.sm.Current())
	})

	// Кейс 5: после кворума receivedStates сбрасывается
	runSingleInstance(t, "receivedStates cleared after quorum", instancesCount, 0, func(self *instance, instances []*instance) {
		self.sm.SetState(StateReading)
		self.leader = self.ProcessID()
		self.receivedStates = make(map[types.ProcessID]State)
		proposed := types.IntValue(42)
		self.tempValue = utils.Ptr(proposed)

		for i := 0; i < self.quorum(); i++ {
			self.sm.Apply(receivedReadEvent{
				msg: makeStateMsg(instances[i].ProcessID(), 0, nil),
			})
		}

		assert.Equal(t, 0, len(self.receivedStates))

		// drain beb
		for _, inst := range instances {
			select {
			case <-time.After(readTimeout):
				t.Errorf("timeout draining beb: instance: %d", inst.ProcessID())
			case <-inst.bebDeliverer.Ch():
			}
		}
	})
}

// ──────────────────────────────────────────────
// onReceivedAccept
// ──────────────────────────────────────────────

func testOnReceivedAccept(t *testing.T, instancesCount int) {
	// Кейс 1: не набрали кворум — остаёмся в StateWriting
	runSingleInstance(t, "below quorum stays writing", instancesCount, 0, func(self *instance, instances []*instance) {
		self.sm.SetState(StateWriting)
		self.leader = self.ProcessID()
		self.receivedAccepted = 0
		proposed := types.IntValue(42)
		self.tempValue = utils.Ptr(proposed)

		for i := 0; i < self.quorum()-1; i++ {
			self.sm.Apply(receivedAcceptEvent{})
		}

		assert.Equal(t, StateWriting, self.sm.Current())
		assert.Equal(t, self.quorum()-1, self.receivedAccepted)

		for _, inst := range instances {
			if !inst.bebDeliverer.Empty() {
				t.Fatalf("beb deliverer not empty before accept quorum: instance: %d", inst.ProcessID())
			}
		}
	})

	// Кейс 2: набрали кворум — broadcast DecidedMsg, переход в StateDeciding
	runSingleInstance(t, "quorum reached broadcasts decide", instancesCount, 0, func(self *instance, instances []*instance) {
		self.sm.SetState(StateWriting)
		self.leader = self.ProcessID()
		self.receivedAccepted = 0
		proposed := types.IntValue(42)
		self.tempValue = utils.Ptr(proposed)

		for i := 0; i < self.quorum(); i++ {
			self.sm.Apply(receivedAcceptEvent{})
		}

		assert.Equal(t, StateDeciding, self.sm.Current())
		assert.Equal(t, self.quorum(), self.receivedAccepted)

		for _, inst := range instances {
			select {
			case <-time.After(readTimeout):
				t.Errorf("decide beb timeout: instance: %d", inst.ProcessID())
			case msg := <-inst.bebDeliverer.Ch():
				dmsg, ok := msg.(DecidedMsg)
				if !ok {
					t.Fatalf("not a DecidedMsg: instance: %d", inst.ProcessID())
				}
				assert.Equal(t, self.ProcessID(), dmsg.From())
				assert.True(t, dmsg.Val.Compare(proposed))
			}
		}
	})

	// Кейс 3: ровно один accept — счётчик инкрементился на 1
	runSingleInstance(t, "single accept increments counter", instancesCount, 0, func(self *instance, instances []*instance) {
		self.sm.SetState(StateWriting)
		self.leader = self.ProcessID()
		self.receivedAccepted = 0
		self.tempValue = utils.Ptr(types.IntValue(1))

		self.sm.Apply(receivedAcceptEvent{})

		assert.Equal(t, 1, self.receivedAccepted)
		assert.Equal(t, StateWriting, self.sm.Current())
	})
}

// ──────────────────────────────────────────────
// onDecided
// ──────────────────────────────────────────────

func testOnDecided(t *testing.T, instancesCount int) {
	// Кейс 1: фолловер получает decided — пишет в канал и переходит в StateDecided
	runSingleInstance(t, "follower receives decided value", instancesCount, 1, func(self *instance, instances []*instance) {
		leader := instances[0]
		self.leader = leader.ProcessID()
		self.decided = make(chan types.Value, 1)
		self.stopCh = make(chan struct{})
		self.aborted = make(chan AbortedState)

		ctx, cancel := context.WithCancel(context.Background())
		self.ctx = ctx
		self.cancel = cancel

		go self.eventLoop()

		self.sm.SetState(StateIdle)

		val := types.IntValue(77)
		self.sm.Apply(decidedEvent{val: val})

		select {
		case <-time.After(readTimeout):
			t.Fatal("decided channel timeout")
		case decided := <-self.decided:
			assert.True(t, decided.Compare(val))
		}
	})

	// Кейс 2: лидер получает decided — аналогично
	runSingleInstance(t, "leader receives decided value", instancesCount, 0, func(self *instance, instances []*instance) {
		self.leader = self.ProcessID()
		self.decided = make(chan types.Value, 1)
		self.stopCh = make(chan struct{})
		self.aborted = make(chan AbortedState)

		ctx, cancel := context.WithCancel(context.Background())
		self.ctx = ctx
		self.cancel = cancel

		go self.eventLoop()

		self.sm.SetState(StateDeciding)

		val := types.IntValue(123)
		self.sm.Apply(decidedEvent{val: val})

		select {
		case <-time.After(readTimeout):
			t.Fatal("decided channel timeout")
		case decided := <-self.decided:
			assert.True(t, decided.Compare(val))
		}
	})
}

// ──────────────────────────────────────────────
// onHandleRead
// ──────────────────────────────────────────────

func testOnHandleRead(t *testing.T, instancesCount int) {
	// Кейс 1: фолловер с непустым current — отправляет StateMsg лидеру через pl
	runSingleInstance(t, "follower sends state to leader", instancesCount, 1, func(self *instance, instances []*instance) {
		leader := instances[0]
		self.leader = leader.ProcessID()
		self.current = State{
			ts:  5,
			val: utils.Ptr(types.IntValue(33)),
		}
		self.sm.SetState(StateIdle)

		self.sm.Apply(handleReadEvent{from: leader.ProcessID()})

		// Состояние НЕ меняется (SameState)
		assert.Equal(t, StateIdle, self.sm.Current())

		// Лидер должен получить StateMsg через pl
		select {
		case <-time.After(readTimeout):
			t.Fatal("pl timeout: leader did not receive StateMsg")
		case msg := <-leader.plDeliverer.Ch():
			smsg, ok := msg.(StateMsg)
			if !ok {
				t.Fatal("not a StateMsg received on leader pl")
			}
			assert.Equal(t, self.ProcessID(), smsg.From())
			assert.Equal(t, 5, smsg.Ts)
			assert.NotNil(t, smsg.Val)
			assert.True(t, (*smsg.Val).Compare(types.IntValue(33)))
		}
	})

	// Кейс 2: фолловер с пустым current (ts=0, val=nil) — отправляет StateMsg с нулями
	runSingleInstance(t, "follower sends empty state to leader", instancesCount, 1, func(self *instance, instances []*instance) {
		leader := instances[0]
		self.leader = leader.ProcessID()
		self.current = State{ts: 0, val: nil}
		self.sm.SetState(StateIdle)

		self.sm.Apply(handleReadEvent{from: leader.ProcessID()})

		assert.Equal(t, StateIdle, self.sm.Current())

		select {
		case <-time.After(readTimeout):
			t.Fatal("pl timeout: leader did not receive StateMsg")
		case msg := <-leader.plDeliverer.Ch():
			smsg, ok := msg.(StateMsg)
			if !ok {
				t.Fatal("not a StateMsg received on leader pl")
			}
			assert.Equal(t, self.ProcessID(), smsg.From())
			assert.Equal(t, 0, smsg.Ts)
			assert.Nil(t, smsg.Val)
		}
	})

	// Кейс 3: каждый фолловер в массиве — все шлют StateMsg лидеру
	runSingleInstance(t, "all followers send state to leader", instancesCount, 0, func(self *instance, instances []*instance) {
		leader := self
		for _, follower := range instances {
			if follower.ProcessID() == leader.ProcessID() {
				continue
			}

			follower.leader = leader.ProcessID()
			follower.current = State{
				ts:  3,
				val: utils.Ptr(types.IntValue(55)),
			}
			follower.sm.SetState(StateIdle)

			follower.sm.Apply(handleReadEvent{from: leader.ProcessID()})
			assert.Equal(t, StateIdle, follower.sm.Current())
		}

		// Лидер должен получить N-1 StateMsg-ов
		received := 0
		for i := 0; i < instancesCount-1; i++ {
			select {
			case <-time.After(readTimeout):
				t.Fatalf("pl timeout on leader: received only %d out of %d", received, instancesCount-1)
			case msg := <-leader.plDeliverer.Ch():
				smsg, ok := msg.(StateMsg)
				if !ok {
					t.Fatal("not a StateMsg")
				}
				assert.Equal(t, 3, smsg.Ts)
				assert.NotNil(t, smsg.Val)
				assert.True(t, (*smsg.Val).Compare(types.IntValue(55)))
				received++
			}
		}
		assert.Equal(t, instancesCount-1, received)
	})
}

// ──────────────────────────────────────────────
// onHandleWrite
// ──────────────────────────────────────────────

func testOnHandleWrite(t *testing.T, instancesCount int) {
	// Кейс 1: фолловер обрабатывает write — обновляет current, шлёт AcceptMsg лидеру
	runSingleInstance(t, "follower updates current and sends accept", instancesCount, 1, func(self *instance, instances []*instance) {
		leader := instances[0]
		self.leader = leader.ProcessID()
		self.epochTs = 7
		self.current = State{ts: 0, val: nil}
		self.sm.SetState(StateIdle)

		writeVal := types.IntValue(88)

		self.sm.Apply(handleWriteEvent{from: leader.ProcessID(), v: utils.Ptr(writeVal)})

		// Состояние НЕ меняется (SameState)
		assert.Equal(t, StateIdle, self.sm.Current())

		// current должен обновиться: ts = epochTs, val = writeVal
		assert.Equal(t, 7, self.current.ts)
		assert.NotNil(t, self.current.val)
		assert.True(t, (*self.current.val).Compare(writeVal))

		// Лидер должен получить AcceptMsg через pl
		select {
		case <-time.After(readTimeout):
			t.Fatal("pl timeout: leader did not receive AcceptMsg")
		case msg := <-leader.plDeliverer.Ch():
			amsg, ok := msg.(AcceptMsg)
			if !ok {
				t.Fatal("not an AcceptMsg received on leader pl")
			}
			assert.Equal(t, self.ProcessID(), amsg.From())
		}
	})

	// Кейс 2: write с nil значением — current.val становится nil
	runSingleInstance(t, "follower handles write with nil value", instancesCount, 1, func(self *instance, instances []*instance) {
		leader := instances[0]
		self.leader = leader.ProcessID()
		self.epochTs = 3
		self.current = State{ts: 1, val: utils.Ptr(types.IntValue(50))}
		self.sm.SetState(StateIdle)

		self.sm.Apply(handleWriteEvent{from: leader.ProcessID(), v: nil})

		assert.Equal(t, StateIdle, self.sm.Current())
		assert.Equal(t, 3, self.current.ts)
		assert.Nil(t, self.current.val)

		select {
		case <-time.After(readTimeout):
			t.Fatal("pl timeout: leader did not receive AcceptMsg")
		case msg := <-leader.plDeliverer.Ch():
			_, ok := msg.(AcceptMsg)
			if !ok {
				t.Fatal("not an AcceptMsg")
			}
		}
	})

	// Кейс 3: все фолловеры обрабатывают write — лидер получает N-1 AcceptMsg
	runSingleInstance(t, "all followers accept write", instancesCount, 0, func(self *instance, instances []*instance) {
		leader := self
		writeVal := types.IntValue(77)

		for _, follower := range instances {
			if follower.ProcessID() == leader.ProcessID() {
				continue
			}
			follower.leader = leader.ProcessID()
			follower.epochTs = 5
			follower.current = State{ts: 0, val: nil}
			follower.sm.SetState(StateIdle)

			follower.sm.Apply(handleWriteEvent{
				from: leader.ProcessID(),
				v:    utils.Ptr(writeVal),
			})

			assert.Equal(t, StateIdle, follower.sm.Current())
			assert.Equal(t, 5, follower.current.ts)
			assert.True(t, (*follower.current.val).Compare(writeVal))
		}

		received := 0
		for i := 0; i < instancesCount-1; i++ {
			select {
			case <-time.After(readTimeout):
				t.Fatalf("pl timeout on leader: received only %d out of %d", received, instancesCount-1)
			case msg := <-leader.plDeliverer.Ch():
				_, ok := msg.(AcceptMsg)
				if !ok {
					t.Fatal("not an AcceptMsg")
				}
				received++
			}
		}
		assert.Equal(t, instancesCount-1, received)
	})

	// Кейс 4: повторный write перезаписывает current
	runSingleInstance(t, "second write overwrites current", instancesCount, 1, func(self *instance, instances []*instance) {
		leader := instances[0]
		self.leader = leader.ProcessID()
		self.epochTs = 4
		self.current = State{ts: 0, val: nil}
		self.sm.SetState(StateIdle)

		first := types.IntValue(10)
		second := types.IntValue(20)

		self.sm.Apply(handleWriteEvent{from: leader.ProcessID(), v: utils.Ptr(first)})
		assert.Equal(t, 4, self.current.ts)
		assert.True(t, (*self.current.val).Compare(first))

		// Drain первый AcceptMsg
		select {
		case <-time.After(readTimeout):
			t.Fatal("pl timeout first accept")
		case <-leader.plDeliverer.Ch():
		}

		self.sm.Apply(handleWriteEvent{from: leader.ProcessID(), v: utils.Ptr(second)})
		assert.Equal(t, 4, self.current.ts)
		assert.True(t, (*self.current.val).Compare(second))

		select {
		case <-time.After(readTimeout):
			t.Fatal("pl timeout second accept")
		case <-leader.plDeliverer.Ch():
		}
	})
}

func TestRound(t *testing.T) {
	t.Parallel()

	t.Run("propose to decided", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		N := 5
		processes := utils.ProcessesIDRange(1, N)

		// Поднимаем полноценные инстансы с реальным event loop
		ecs := make([]*epochConsensus, 0, N)
		for _, pid := range processes {
			bl := p2p.NewBaseLink(pid)
			sl := p2p.NewStubbornP2PLinks(ctx, bl)
			pl := p2p.NewPerfectP2PLinks(pid, sl)
			beb := broadcaster.NewBestEffortBroadcaster(
				pid, processes,
				broadcaster.DefaultBroadcastNodeSelector(), pl,
			)
			types.InitWorkers(bl, sl, pl, beb)
			types.StartWorkers(bl, sl, pl, beb)

			ec := newEpochConsensus(pid, beb, pl, N, nil)
			ecs = append(ecs, ec)
		}

		// Добавляем correct для beb
		for _, ec := range ecs {
			for _, other := range ecs {
				if ec.self != other.self {
					ec.beb.AddCorrect(other.self)
				}
			}
		}

		leaderID := processes[0]
		epochTs := 1
		initialState := State{ts: 0, val: nil}

		// Запускаем эпоху на всех
		for _, ec := range ecs {
			ec.StartEpoch(ctx, leaderID, epochTs, initialState)
		}

		// Лидер предлагает значение
		proposed := types.IntValue(42)
		ecs[0].Propose(proposed)

		// Все должны получить decided
		for i, ec := range ecs {
			select {
			case <-ctx.Done():
				t.Fatalf("timeout waiting for decided on instance %d", i)
			case val := <-ec.Decided():
				assert.True(t, val.Compare(proposed),
					"instance %d decided wrong value", i)
			}
		}
	})

	t.Run("propose with existing state overridden by higher ts", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		N := 5
		processes := utils.ProcessesIDRange(1, N)

		net := network.NewInMemory()
		ecs := make([]*epochConsensus, 0, N)
		for _, pid := range processes {
			bl := p2p.NewBaseLink(pid, p2p.WithNetwork(net))
			sl := p2p.NewStubbornP2PLinks(ctx, bl)
			pl := p2p.NewPerfectP2PLinks(pid, sl)
			beb := broadcaster.NewBestEffortBroadcaster(
				pid, processes,
				broadcaster.DefaultBroadcastNodeSelector(), pl,
			)
			types.InitWorkers(bl, sl, pl, beb)
			types.StartWorkers(bl, sl, pl, beb)

			ec := newEpochConsensus(pid, beb, pl, N, nil)
			ecs = append(ecs, ec)
		}

		for _, ec := range ecs {
			for _, other := range ecs {
				if ec.self != other.self {
					ec.beb.AddCorrect(other.self)
				}
			}
		}

		leaderID := processes[0]
		epochTs := 5

		// Большинство нод имеют state с высоким ts и значением 99
		// Лидер предложит 42, но должен быть overridden на 99
		override := types.IntValue(99)
		proposed := types.IntValue(42)

		for i, ec := range ecs {
			var state State
			if i >= 1 && i <= 3 { // 3 из 5 = кворум
				state = State{ts: 3, val: utils.Ptr(override)}
			} else {
				state = State{ts: 0, val: nil}
			}
			ec.StartEpoch(ctx, leaderID, epochTs, state)
		}

		ecs[0].Propose(proposed)

		// Все должны decided на override (99), а не на proposed (42)
		for i, ec := range ecs {
			select {
			case <-ctx.Done():
				t.Fatalf("timeout waiting for decided on instance %d", i)
			case val := <-ec.Decided():
				assert.True(t, val.Compare(override),
					"instance %d: expected override value 99, got something else", i)
			}
		}
	})
}

func setupNInstances(ctx context.Context, N int) []*instance {
	processes := utils.ProcessesIDRange(1, N)
	instances := make([]*instance, 0, len(processes))
	net := network.NewInMemory()
	for _, pid := range processes {
		instance := setupECInstance(ctx, pid, processes, net)
		instances = append(instances, instance)
	}
	for _, instance := range instances {
		for _, other := range instances {
			if instance.ProcessID() == other.ProcessID() {
				continue
			}
			instance.beb.AddCorrect(other.ProcessID())
		}
	}
	return instances
}

func setupECInstance(ctx context.Context, self types.ProcessID, processes []types.ProcessID, net network.Network) *instance {
	bl := p2p.NewBaseLink(self, p2p.WithNetwork(net))
	sl := p2p.NewStubbornP2PLinks(ctx, bl)
	pl := p2p.NewPerfectP2PLinks(self, sl)
	plDeliverer := mocks.NewMockUnBufChanDeliverer(self)
	pl.AddDeliverer(plDeliverer, types.DelivererWithMsgNames(AcceptMsgName, StateMsgName))
	beb := broadcaster.NewBestEffortBroadcaster(self, processes, broadcaster.DefaultBroadcastNodeSelector(), pl)
	bebDeliverer := mocks.NewMockUnBufChanDeliverer(self)
	beb.AddDeliverer(bebDeliverer, types.DelivererWithMsgNames(ReadMsgName, WriteMsgName, DecideMsgName))
	ec := newEpochConsensus(self, beb, pl, len(processes), nil)
	types.InitWorkers(bl, sl, pl, beb)
	types.StartWorkers(bl, sl, pl, beb)
	ec.ctx = ctx
	return &instance{
		epochConsensus: ec,
		bebDeliverer:   bebDeliverer,
		plDeliverer:    plDeliverer,
	}
}

type instance struct {
	*epochConsensus
	bebDeliverer *mocks.MockUnBufChanDeliverer
	plDeliverer  *mocks.MockUnBufChanDeliverer
}
