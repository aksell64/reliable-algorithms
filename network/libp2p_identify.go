package network

import (
	"fmt"
	"reliable/types"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
)

type PeerResolver interface {
	PeerIDByProcess(pid types.ProcessID) (peer.ID, bool)
}

type libp2pIdentify struct {
	privKey  crypto.PrivKey
	host     host.Host
	resolver PeerResolver
}

func NewLibp2pIdentify(h host.Host, resolver PeerResolver) types.Identify {
	privKey := h.Peerstore().PrivKey(h.ID())
	if privKey == nil {
		panic("host has no private key in peerstore")
	}
	return &libp2pIdentify{
		privKey:  privKey,
		host:     h,
		resolver: resolver,
	}
}

func (id *libp2pIdentify) Sign(raw []byte) ([]byte, error) {
	sig, err := id.privKey.Sign(raw)
	if err != nil {
		return nil, fmt.Errorf("sign: %w", err)
	}
	return sig, nil
}

func (id *libp2pIdentify) Verify(from types.ProcessID, raw []byte, signature []byte) error {
	peerID, ok := id.resolver.PeerIDByProcess(from)
	if !ok {
		return fmt.Errorf("unknown process: %s", from.String())
	}

	pubKey := id.host.Peerstore().PubKey(peerID)
	if pubKey == nil {
		var err error
		pubKey, err = peerID.ExtractPublicKey()
		if err != nil {
			return fmt.Errorf("no public key for process %s (peer %s): %w",
				from.String(), peerID.String(), err)
		}
	}

	ok, err := pubKey.Verify(raw, signature)
	if err != nil {
		return fmt.Errorf("verify error for process %s: %w", from.String(), err)
	}
	if !ok {
		return fmt.Errorf("invalid signature from process %s", from.String())
	}

	return nil
}
