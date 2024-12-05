package mainpBFT

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"go.opentelemetry.io/otel/trace"
)

type Replica struct {
	// Embedded
	pbftCore
	replicaState

	// Configuration
	cfg Config

	// Thông tin của Replica
	id         peer.ID
	peers      []peer.ID
	clusterID  string
	protocolID protocol.ID

	// Thời gian inactivity, delay
	requestTimer *time.Time

	// Communication
	host *host.Host

	// Biến để biết Replica này có phải malicious Replica
	byzantine bool
}

func NewReplica(log zerolog.Logger, host *host.Host, executor blockless.Executor, peers []peer.ID, clusterID string, options ...Option) (*Replica, error) {

	total := uint(len(peers))

	cfg := DefaultConfig

	// Thiết lập các thay đổi cho config
	for _, option := range options {
		option(&cfg)
	}

	// Tạo instance
	replica := Replica{
		pbftCore:     newPbftCore(total),
		replicaState: newState(),

		cfg: cfg,

		host:       host,
		executor:   executor,
		clusterID:  clusterID,
		protocolID: protocol.ID(fmt.Sprintf("%s/cluster/%s", Protocol, clusterID)),

		id:    host.ID(),
		peers: peers,

		byzantine: isByzantine(),
	}
	replica.log.Info().Strs("replicas", peerIDList(peers)).Uint("n", total).Uint("f", replica.f).Bool("byzantine", replica.byzantine).Msg("created PBFT replica")

	// Xử lý các message của pBFT
	replica.setPBFTMessageHandler()

	return &replica, nil
}

// Tắt node, dừng requestTimer
func (r *Replica) Shutdown() error {
	r.host.RemoveStreamHandler(r.protocolID)
	r.stopRequestTimer()
	return nil
}

func (r *Replica) setPBFTMessageHandler() {

	//Chỉ nhận tin nhắn từ replica cùng cluster
	// Dùng map để tìm kiếm nhanh hơn
	pm := make(map[peerID]struct{})
	for _, peer := range r.peers {
		pm[peer] = struct{}{}
	}
	ctx := tracing.TraceContext(context.Background(), r.cfg.TraceInfo)

	r.host.Host.SetStreamHandler(r.protocolID, func(stream network.Stream) {
		defer stream.Close()

		from := stream.Conn().RemotePeer()
		// Chỉ nhận message từ replica cùng cluster
		_, known := pm[from]
		if !known {
			r.log.Info().Str("peer", from.String()).Msg("received message from a peer not in our cluster, discarding")
			return
		}

		buf := bufio.NewReader(stream)
		msg, err := buf.ReadBytes('\n')

		// Nếu có error mà không phải là EOF thì báo error
		if err != nil && !errors.Is(err, io.EOF) {
			stream.Reset()
			r.log.Error().Err(err).Msg("error receiving direct message")
			return
		}

		r.log.Debug().Str("peer", from.String()).Msg("received message")

		err = r.processMessage(ctx, from, msg)
		if err != nil {
			r.log.Error().Err(err).Str("peer", from.String()).Msg("message processing failed")
		}
	})

}

func (r *Replica) processMessage(ctx context.Context, from peer.ID, payload []byte) (procError error) {

	// Không làm gì cả nếu Replica này là malicious Replica
	if r.byzantine {
		return errors.New("we're a byzantine replica, ignoring received message")
	}

	// Thu thập thông tin Trace
	ti, ok := getTraceInfoFromMessage(payload)
	if ok {
		ctx = tracing.TraceContext(ctx, ti)
	}

	// Xử lý payload
	msg, err := unpackMessage(payload)
	if err != nil {
		return fmt.Errorf("could not unpack message: %w", err)
	}

	ctx, span := r.tracer.Start(ctx, msgProcessSpanName(msg.Type()), trace.WithAttributes(b7ssemconv.MessagePeer.String(from.String())))
	defer span.End()
	// NOTE: This function checks the named return error value in order to set the span status accordingly.
	defer func() {
		if procError == nil {
			span.SetStatus(otelcodes.Ok, spanStatusOK)
			return
		}

		if allowErrorLeakToTelemetry {
			span.SetStatus(otelcodes.Error, procError.Error())
			return
		}

		span.SetStatus(otelcodes.Error, spanStatusErr)
	}()

	// Access to individual segments (pre-prepares, prepares, commits etc) could be managed on an individual level,
	// but it's probably not worth it. This way we just do it request by request.
	// NOTE: Perhaps lock as early as possible or force serialization. For some things we want to force in-order processing of messages,
	// e.g. `new-view` first, THEN any `preprepares` for that view.
	r.sl.Lock()
	defer r.sl.Unlock()

	err = r.isMessageAllowed(msg)
	if err != nil {
		return fmt.Errorf("message not allowed (message: %T): %w", msg, err)
	}

	switch m := msg.(type) {

	case Request:
		return r.processRequest(ctx, from, m)

	case PrePrepare:
		return r.processPrePrepare(ctx, from, m)

	case Prepare:
		return r.processPrepare(ctx, from, m)

	case Commit:
		return r.processCommit(ctx, from, m)

	case ViewChange:
		return r.processViewChange(ctx, from, m)

	case NewView:
		return r.processNewView(ctx, from, m)
	}

	return fmt.Errorf("unexpected message type (from: %s): %T", from, msg)
}
