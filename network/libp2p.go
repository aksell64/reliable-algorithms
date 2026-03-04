package network

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"reliable/logger"
	"reliable/messages"
	"reliable/types"
	"reliable/utils"
	"reliable/utils/codec"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-libp2p/p2p/discovery/mdns"
	drouting "github.com/libp2p/go-libp2p/p2p/discovery/routing"
	dutil "github.com/libp2p/go-libp2p/p2p/discovery/util"
	"github.com/multiformats/go-multiaddr"
	"github.com/rs/zerolog"
)

const (
	ProtocolID              = protocol.ID("/reliable/1.0.0")
	HandshakeProtocol       = protocol.ID("/reliable/handshake/1.0.0")
	ReadyProtocol           = protocol.ID("/reliable/ready/1.0.0")
	Rendezvous              = "reliable/cluster/v1"
	maxReadSize             = 1 << 20
	streamOpenTimeout       = 10 * time.Second
	discoveryInterval       = 5 * time.Second
	msgType                 = "libp2p_msg"
	clusterHandshakeTimeout = 5 * 30 * time.Second
)

// Файл хранит приватный ключ в base64-encoded protobuf (стандарт libp2p)
type identityFile struct {
	PrivKeyBytes []byte `json:"priv_key"` // crypto.MarshalPrivateKey result
}

func loadOrCreateIdentity(path string) (crypto.PrivKey, error) {
	data, err := os.ReadFile(path)
	if err == nil {
		// Файл есть — десериализуем
		var id identityFile
		if err := json.Unmarshal(data, &id); err != nil {
			return nil, fmt.Errorf("parse identity file %s: %w", path, err)
		}
		priv, err := crypto.UnmarshalPrivateKey(id.PrivKeyBytes)
		if err != nil {
			return nil, fmt.Errorf("unmarshal private key from %s: %w", path, err)
		}
		return priv, nil
	}

	if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("read identity file %s: %w", path, err)
	}

	// Файла нет — генерим новый Ed25519 ключ
	priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate key: %w", err)
	}

	// Сериализуем и сохраняем
	raw, err := crypto.MarshalPrivateKey(priv)
	if err != nil {
		return nil, fmt.Errorf("marshal private key: %w", err)
	}

	id := identityFile{PrivKeyBytes: raw}
	fileData, err := json.Marshal(id)
	if err != nil {
		return nil, fmt.Errorf("marshal identity file: %w", err)
	}

	if err := os.WriteFile(path, fileData, 0600); err != nil {
		return nil, fmt.Errorf("write identity file %s: %w", path, err)
	}

	return priv, nil
}

type Libp2pMessage struct {
	messages.BaseMsg
	messages.RawMsg
}

func (msg Libp2pMessage) Type() string { return msgType }

type wireMessage struct {
	FromPID types.ProcessID `json:"from_pid"`
	ToPID   types.ProcessID `json:"to_pid"`
	Payload json.RawMessage `json:"payload"`
}

type readyMsg struct {
	ProcessID types.ProcessID `json:"process_id"`
}

// handshakeMsg — при подключении каждый пир сообщает свой ProcessID.
type handshakeMsg struct {
	ProcessID types.ProcessID `json:"process_id"`
}

type libp2pNetwork struct {
	types.Deliverer
	host   host.Host
	kdht   *dht.IpfsDHT
	ctx    context.Context
	cancel context.CancelFunc

	localPID types.ProcessID // наш ProcessID
	clusterN int             // сколько пиров ожидаем

	// peer.ID -> ProcessID (заполняется динамически через handshake)
	peerToPID map[peer.ID]types.ProcessID
	// ProcessID -> peer.ID (обратный маппинг)
	pidToPeer map[types.ProcessID]peer.ID

	deliverers map[types.ProcessID]types.Deliverer

	marshal   func(types.Message) ([]byte, error)
	unmarshal func([]byte) (types.Message, error)

	// Сигнал, что кластер собран
	clusterReady   chan struct{}
	readyOnce      sync.Once
	registry       *codec.Registry
	customBoostrap bool

	// === Фаза 2: Ready-барьер ===
	readyReceived map[types.ProcessID]bool // от кого получили "ready"
	allReady      chan struct{}            // закрывается когда ВСЕ прислали ready
	allReadyOnce  sync.Once
	readySent     bool // отправляли ли мы уже ready всем

	// Трекер: кому мы уже отправляли handshake
	handshakeSent map[peer.ID]bool

	identityKeyPath string // путь к файлу с ключом (пусто = рандом каждый раз)

	logger *zerolog.Logger

	mu sync.RWMutex
}

type Libp2pOption func(*libp2pNetwork)

func WithRegistry(r *codec.Registry) Libp2pOption {
	return func(network *libp2pNetwork) {
		network.registry = r
	}
}

// WithBootstrapPeers — начальные ноды для DHT bootstrap.
// Если запускаешь в локалке, первый нод — bootstrap для остальных.
func WithBootstrapPeers(peers []peer.AddrInfo) Libp2pOption {
	return func(n *libp2pNetwork) {
		n.customBoostrap = true
		// Подключаемся к bootstrap-нодам
		for _, p := range peers {
			n.boostrapConnect(p)
		}
	}
}

func WithLogger(l zerolog.Logger) Libp2pOption {
	return func(network *libp2pNetwork) {
		network.logger = &l
	}
}

func DisableLogger() Libp2pOption {
	return func(network *libp2pNetwork) {
		network.logger = utils.Ptr(zerolog.Nop())
	}
}

// WithIdentityFile — опция: если файл есть — читаем ключ,
// если файла нет — генерим Ed25519, сохраняем, используем.
func WithIdentityFile(path string) Libp2pOption {
	return func(n *libp2pNetwork) {
		n.identityKeyPath = path
	}
}

// NewLibp2p создаёт p2p ноду.
// localPID — ProcessID этой ноды.
// clusterN — общее число процессов в кластере.
func NewLibp2p(
	listenAddrs []string,
	localPID types.ProcessID,
	clusterN int,
	opts ...Libp2pOption,
) Network {
	ctx, cancel := context.WithCancel(context.Background())

	maddrs := make([]multiaddr.Multiaddr, 0, len(listenAddrs))
	for _, addr := range listenAddrs {
		ma, err := multiaddr.NewMultiaddr(addr)
		if err != nil {
			logger.Panicfmt("invalid multiaddr %s: %v", addr, err)
		}
		maddrs = append(maddrs, ma)
	}

	// Создаём "пустой" объект, чтобы опции могли записать identityKeyPath
	n := &libp2pNetwork{
		ctx:           ctx,
		cancel:        cancel,
		localPID:      localPID,
		clusterN:      clusterN,
		peerToPID:     make(map[peer.ID]types.ProcessID),
		pidToPeer:     make(map[types.ProcessID]peer.ID),
		deliverers:    make(map[types.ProcessID]types.Deliverer),
		clusterReady:  make(chan struct{}),
		registry:      codec.New(),
		handshakeSent: make(map[peer.ID]bool),
		readyReceived: make(map[types.ProcessID]bool),
		allReady:      make(chan struct{}),
	}

	// Применяем опции ПЕРВЫМ делом — чтобы identityKeyPath был заполнен
	for _, opt := range opts {
		opt(n)
	}

	// Собираем libp2p.Option для хоста
	hostOpts := []libp2p.Option{
		libp2p.ListenAddrs(maddrs...),
	}

	// Если указан путь к identity-файлу — грузим/создаём ключ
	if n.identityKeyPath != "" {
		privKey, err := loadOrCreateIdentity(n.identityKeyPath)
		if err != nil {
			logger.Panicfmt("identity key: %v", err)
		}
		hostOpts = append(hostOpts, libp2p.Identity(privKey))
	}

	h, err := libp2p.New(hostOpts...)
	if err != nil {
		logger.Panicfmt("failed to create libp2p host: %v", err)
	}

	n.host = h

	// Создаём Kademlia DHT
	kdht, err := dht.New(ctx, h, dht.Mode(dht.ModeAutoServer))
	if err != nil {
		logger.Panicfmt("failed to create DHT: %v", err)
	}
	n.kdht = kdht

	if err := kdht.Bootstrap(ctx); err != nil {
		logger.Panicfmt("failed to bootstrap DHT: %v", err)
	}

	n.Deliverer = types.NewUnaryDeliverer(localPID)

	// Регистрируем себя
	n.peerToPID[h.ID()] = localPID
	n.pidToPeer[localPID] = h.ID()

	for _, opt := range opts {
		opt(n)
	}

	if n.logger == nil {
		n.logger = utils.Ptr(logger.NewNodeScopeLogger(localPID, logger.Scope{"net", "libp2p"}))
	}

	codec.RegisterTyped[Libp2pMessage](n.registry)

	n.marshal = func(message types.Message) ([]byte, error) {
		inner, err := n.registry.Marshal(message)
		if err != nil {
			return nil, err
		}

		msg := Libp2pMessage{
			BaseMsg: messages.NewBase(uuid.New(), n.localPID, msgType),
			RawMsg: messages.RawMsg{
				Raw:     inner,
				RawType: message.Type(),
			},
		}

		return n.registry.Marshal(msg)
	}

	n.unmarshal = func(bytes []byte) (types.Message, error) {
		msgObj, err := n.registry.Unmarshal(bytes, msgType)
		if err != nil {
			return nil, err
		}

		msg, ok := msgObj.(Libp2pMessage)
		if !ok {
			return nil, fmt.Errorf("failed to unmarshal libp2p message")
		}

		innerMsg, err := n.registry.Unmarshal(msg.Raw, msg.RawType)
		if err != nil {
			return nil, err
		}

		resMsg, ok := innerMsg.(types.Message)
		if !ok {
			return nil, fmt.Errorf("failed to unmarshal inner message")
		}

		return resMsg, nil
	}

	if !n.customBoostrap {
		boostraps := make([]peer.AddrInfo, 0)
		for _, ma := range dht.DefaultBootstrapPeers {
			info, err := peer.AddrInfoFromP2pAddr(ma)
			if err != nil {
				log.Fatal("incorrect default boostrap info")
			}
			boostraps = append(boostraps, *info)
		}

		for _, p := range boostraps {
			n.boostrapConnect(p)
		}
	}

	// Обработчик основного протокола — приём сообщений
	h.SetStreamHandler(ProtocolID, n.handleStream)

	// Обработчик handshake — когда кто-то сообщает свой ProcessID
	h.SetStreamHandler(HandshakeProtocol, n.handleHandshake)

	h.SetStreamHandler(ReadyProtocol, n.handleReady)

	n.logger.Info().Any("addrs", h.Addrs()).Str("peerID", h.ID().String()).Msg("host started")

	// Запускаем discovery в фоне
	go n.discoverPeers()

	mdnsService := mdns.NewMdnsService(h, Rendezvous, &mdnsNotifee{n: n})
	if err := mdnsService.Start(); err != nil {
		n.logger.Error().Err(err).Msg("mDNS start failed")
	}

	return n
}

func (n *libp2pNetwork) boostrapConnect(p peer.AddrInfo) {
	if p.ID == n.host.ID() {
		return
	}
	if err := n.host.Connect(n.ctx, p); err != nil {
		return
	}

	n.logger.Info().Str("peer", p.ID.String()).Msg("connected to bootstrap peer")
}

func (n *libp2pNetwork) Boostrap() error {
	return n.waitForCluster()
}

// discoverPeers — ищет пиров через DHT по Rendezvous-строке.
func (n *libp2pNetwork) discoverPeers() {
	routingDiscovery := drouting.NewRoutingDiscovery(n.kdht)

	// Объявляем себя
	dutil.Advertise(n.ctx, routingDiscovery, Rendezvous)
	n.logger.Info().Str("rendezvous", Rendezvous).Msg("Advertised")

	ticker := time.NewTicker(discoveryInterval)
	defer ticker.Stop()

	for {
		select {
		case <-n.ctx.Done():
			return
		case <-ticker.C:
			// Проверяем, может кластер уже собран
			n.mu.RLock()
			found := len(n.pidToPeer)
			n.mu.RUnlock()
			if found >= n.clusterN {
				n.markClusterReady()
				return
			}

			// Ищем пиров
			peerChan, err := routingDiscovery.FindPeers(n.ctx, Rendezvous)
			if err != nil {
				n.logger.Error().Err(err).Msg("discovery: find peers")
				continue
			}

			for p := range peerChan {
				if p.ID == n.host.ID() || p.ID == "" {
					continue
				}

				//n.logger.Info().Str("peer", p.ID.String()).Msg("found peer")

				// Уже знаем этого пира?
				n.mu.RLock()
				_, known := n.peerToPID[p.ID]
				n.mu.RUnlock()
				if known {
					n.logger.Info().Str("peer", p.ID.String()).Msg("already known")
					continue
				}

				go func() {
					// Подключаемся
					if n.host.Network().Connectedness(p.ID) != network.Connected {
						ctx, cancel := context.WithTimeout(n.ctx, streamOpenTimeout)
						err := n.host.Connect(ctx, p)
						cancel()
						if err != nil {
							//n.logger.Error().Err(err).Str("peer", p.ID.String()).Msg("connect")
							return
						}
					}

					// Отправляем handshake — сообщаем свой ProcessID
					n.sendHandshake(p.ID)
				}()
			}
		}
	}
}

func (n *libp2pNetwork) sendHandshake(peerID peer.ID) {
	// Атомарно проверяем и ставим флаг
	n.mu.Lock()
	if n.handshakeSent[peerID] {
		n.mu.Unlock()
		return // уже отправляли — выходим
	}
	n.handshakeSent[peerID] = true
	n.mu.Unlock()

	ctx, cancel := context.WithTimeout(n.ctx, streamOpenTimeout)
	defer cancel()

	s, err := n.host.NewStream(ctx, peerID, HandshakeProtocol)
	if err != nil {
		n.logger.Error().Err(err).Str("peer", peerID.String()).Msg("handshake stream")
		// Сбрасываем флаг, чтобы можно было попробовать ещё раз
		n.mu.Lock()
		delete(n.handshakeSent, peerID)
		n.mu.Unlock()
		return
	}
	defer s.Close()

	hs := handshakeMsg{ProcessID: n.localPID}
	data, err := json.Marshal(hs)
	if err != nil {
		return
	}

	if err := writeLengthPrefixed(s, data); err != nil {
		n.logger.Error().Err(err).Str("peer", peerID.String()).Msg("handshake write")
		return
	}

	n.logger.Info().Str("peer", peerID.String()).Msg("handshake sent")
}

func (n *libp2pNetwork) handleHandshake(s network.Stream) {
	defer s.Close()

	data, err := readLengthPrefixed(s)
	if err != nil {
		n.logger.Error().Err(err).Msg("handshake read")
		return
	}

	var hs handshakeMsg
	if err := json.Unmarshal(data, &hs); err != nil {
		n.logger.Error().Err(err).Msg("handshake unmarshal")
		return
	}

	remotePeerID := s.Conn().RemotePeer()

	n.mu.Lock()
	_, alreadyKnown := n.peerToPID[remotePeerID]
	n.peerToPID[remotePeerID] = hs.ProcessID
	n.pidToPeer[hs.ProcessID] = remotePeerID
	currentCount := len(n.pidToPeer)
	n.mu.Unlock()

	n.logger.Info().
		Str("peer", remotePeerID.String()).
		Str("pid", hs.ProcessID.String()).
		Int("count", currentCount).
		Int("clusterN", n.clusterN).
		Msg("handshake received")

	// Отправляем ответный ТОЛЬКО если мы ещё не знали этого пира
	// (то есть это первый хэндшейк от него)
	if !alreadyKnown {
		go n.sendHandshake(remotePeerID)
	}

	if currentCount >= n.clusterN {
		n.markClusterReady()
	}
}

func (n *libp2pNetwork) broadcastReady() {
	n.mu.Lock()
	if n.readySent {
		n.mu.Unlock()
		return
	}
	n.readySent = true

	// Считаем себя тоже "ready"
	n.readyReceived[n.localPID] = true
	currentReady := len(n.readyReceived)

	// Собираем список пиров для рассылки ПОД ЛОКОМ
	peers := make([]peer.ID, 0, len(n.peerToPID))
	for peerID, pid := range n.peerToPID {
		if pid != n.localPID {
			peers = append(peers, peerID)
		}
	}
	n.mu.Unlock()

	n.logger.Info().Int("readyCount", currentReady).Msg("broadcasting READY to all peers")

	// СНАЧАЛА рассылаем READY всем — ВСЕГДА, без исключений
	for _, peerID := range peers {
		go n.sendReady(peerID)
	}

	// ПОТОМ проверяем, может мы уже всех собрали
	if currentReady >= n.clusterN {
		n.markAllReady()
	}
}

func (n *libp2pNetwork) sendReady(peerID peer.ID) {
	ctx, cancel := context.WithTimeout(n.ctx, streamOpenTimeout)
	defer cancel()

	s, err := n.host.NewStream(ctx, peerID, ReadyProtocol)
	if err != nil {
		n.logger.Error().Err(err).Str("peer", peerID.String()).Msg("ready stream open failed")
		// Retry через секунду
		go func() {
			time.Sleep(1 * time.Second)
			n.sendReady(peerID)
		}()
		return
	}
	defer s.Close()

	rm := readyMsg{ProcessID: n.localPID}
	data, err := json.Marshal(rm)
	if err != nil {
		return
	}

	if err := writeLengthPrefixed(s, data); err != nil {
		n.logger.Error().Err(err).Str("peer", peerID.String()).Msg("ready write failed")
	}

	n.logger.Info().Str("peer", peerID.String()).Msg("READY sent")
}

func (n *libp2pNetwork) handleReady(s network.Stream) {
	defer s.Close()

	data, err := readLengthPrefixed(s)
	if err != nil {
		n.logger.Error().Err(err).Msg("ready read")
		return
	}

	var rm readyMsg
	if err := json.Unmarshal(data, &rm); err != nil {
		n.logger.Error().Err(err).Msg("ready unmarshal")
		return
	}

	n.mu.Lock()
	n.readyReceived[rm.ProcessID] = true
	currentReady := len(n.readyReceived)
	n.mu.Unlock()

	n.logger.Info().
		Str("fromPID", rm.ProcessID.String()).
		Int("readyCount", currentReady).
		Int("clusterN", n.clusterN).
		Msg("READY received")

	if currentReady >= n.clusterN {
		n.markAllReady()
	}
}

func (n *libp2pNetwork) markAllReady() {
	n.allReadyOnce.Do(func() {
		n.logger.Info().Msg("✅ ALL peers READY — cluster fully synchronized")
		close(n.allReady)
	})
}

func (n *libp2pNetwork) markClusterReady() {
	n.readyOnce.Do(func() {
		close(n.clusterReady)
		// Фаза 1 завершена → запускаем фазу 2
		n.logger.Info().Msg("Phase 1 complete (all peers discovered). Starting ready-barrier...")
		go n.broadcastReady()
	})
}

// waitForCluster — блокирует до тех пор, пока все N пиров не обнаружены.
func (n *libp2pNetwork) waitForCluster() error {
	// Фаза 1: ждём discovery всех пиров
	select {
	case <-n.clusterReady:
		n.logger.Info().Msg("[Bootstrap] Phase 1 done: all peers discovered")
	case <-time.After(clusterHandshakeTimeout):
		n.mu.RLock()
		got := len(n.pidToPeer)
		n.mu.RUnlock()
		return fmt.Errorf("phase 1 timeout: discovered %d/%d peers", got, n.clusterN)
	case <-n.ctx.Done():
		return n.ctx.Err()
	}

	// Фаза 2: ждём ready от всех
	select {
	case <-n.allReady:
		n.logger.Info().Msg("[Bootstrap] Phase 2 done: all peers confirmed ready ✅")
		return nil
	case <-time.After(clusterHandshakeTimeout):
		n.mu.RLock()
		got := len(n.readyReceived)
		n.mu.RUnlock()
		return fmt.Errorf("phase 2 timeout: got ready from %d/%d peers", got, n.clusterN)
	case <-n.ctx.Done():
		return n.ctx.Err()
	}
}

func (n *libp2pNetwork) handleStream(s network.Stream) {
	defer s.Close()

	data, err := readLengthPrefixed(s)
	if err != nil {
		n.logger.Error().Err(err).Msg("stream read")
		return
	}

	var wire wireMessage
	if err := json.Unmarshal(data, &wire); err != nil {
		n.logger.Error().Err(err).Msg("wire unmarshal")
		return
	}

	msg, err := n.unmarshal(wire.Payload)
	if err != nil {
		n.logger.Error().Err(err).Msg("wire payload unmarshal")
		return
	}

	n.Deliverer.Deliver(msg)
}

func (n *libp2pNetwork) Send(from, to types.ProcessID, msg types.Message) {
	payload, err := n.marshal(msg)
	if err != nil {
		n.logger.Error().Err(err).Msg("marshal send msg")
		return
	}

	// Ищем peer.ID по ProcessID — динамически!
	n.mu.RLock()
	peerID, ok := n.pidToPeer[to]
	n.mu.RUnlock()

	if !ok {
		n.logger.Error().Err(err).
			Str("peer", peerID.String()).
			Msg("no peer found for PID (cluster not ready?)")
		return
	}

	wire := wireMessage{
		FromPID: from,
		ToPID:   to,
		Payload: json.RawMessage(payload),
	}
	data, err := json.Marshal(wire)
	if err != nil {
		n.logger.Error().Err(err).Msg("wire marshal msg")
		return
	}

	go n.sendToPeerByID(peerID, data)
}

func (n *libp2pNetwork) sendToPeerByID(peerID peer.ID, data []byte) {
	if n.host.Network().Connectedness(peerID) != network.Connected {
		// Пир уже был подключен через discovery, но на всякий случай
		ctx, cancel := context.WithTimeout(n.ctx, streamOpenTimeout)
		defer cancel()
		peerInfo := n.host.Peerstore().PeerInfo(peerID)
		if err := n.host.Connect(ctx, peerInfo); err != nil {
			return
		}
	}

	ctx, cancel := context.WithTimeout(n.ctx, streamOpenTimeout)
	defer cancel()

	s, err := n.host.NewStream(ctx, peerID, ProtocolID)
	if err != nil {
		n.logger.Error().Err(err).Str("peer", peerID.String()).Msg("stream open")
		return
	}
	defer s.Close()

	if err := writeLengthPrefixed(s, data); err != nil {
		n.logger.Error().Err(err).Str("peer", peerID.String()).Msg("write to stream")
	}
}

func (n *libp2pNetwork) Connect(deliverer types.Deliverer) {
	n.Deliverer.AddDeliverer(deliverer)
}

func (n *libp2pNetwork) Close() error {
	n.cancel()
	n.kdht.Close()
	return n.host.Close()
}

func (n *libp2pNetwork) Host() host.Host {
	return n.host
}

// Реализуем интерфейс mdns.Notifee
type mdnsNotifee struct {
	n *libp2pNetwork
}

func (m *mdnsNotifee) HandlePeerFound(pi peer.AddrInfo) {
	if pi.ID == m.n.host.ID() {
		return
	}

	ctx, cancel := context.WithTimeout(m.n.ctx, streamOpenTimeout)
	defer cancel()

	if err := m.n.host.Connect(ctx, pi); err != nil {
		m.n.logger.Error().Err(err).Str("peer", pi.ID.String()).Msg("mdns connect failed")
		return
	}

	m.n.logger.Info().Str("peer", pi.ID.String()).Msg("mDNS: found peer, sending handshake")
	m.n.sendHandshake(pi.ID)
}

// --- Length-prefixed framing ---

func writeLengthPrefixed(w interface{ Write([]byte) (int, error) }, data []byte) error {
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, uint32(len(data)))
	if _, err := w.Write(header); err != nil {
		return err
	}
	_, err := w.Write(data)
	return err
}

func readLengthPrefixed(r interface{ Read([]byte) (int, error) }) ([]byte, error) {
	header := make([]byte, 4)
	if _, err := readFull(r, header); err != nil {
		return nil, err
	}
	length := binary.BigEndian.Uint32(header)
	if length > maxReadSize {
		return nil, fmt.Errorf("message too large: %d bytes", length)
	}
	data := make([]byte, length)
	if _, err := readFull(r, data); err != nil {
		return nil, err
	}
	return data, nil
}

func readFull(r interface{ Read([]byte) (int, error) }, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := r.Read(buf[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}
