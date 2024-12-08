package mainpBFT

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	//"github.com/armon/go-metrics"
	"github.com/hashicorp/go-metrics"
	"github.com/rs/zerolog"
	otelcodes "go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"

	"github.com/blocklessnetwork/b7s/consensus"
	"github.com/blocklessnetwork/b7s/host"
	"github.com/blocklessnetwork/b7s/models/blockless"
	"github.com/blocklessnetwork/b7s/telemetry/b7ssemconv"
	"github.com/blocklessnetwork/b7s/telemetry/tracing"
)

type Replica struct {
	// pBFT
	pbftCore
	replicaState

	// Configuration
	cfg Config

	// Thời gian inactivity, delay
	requestTimer *time.Timer

	// Giao tiếp
	log      zerolog.Logger
	host     *host.Host
	executor blockless.Executor

	// Thông tin của Replica
	id         peer.ID
	peers      []peer.ID
	clusterID  string
	protocolID protocol.ID

	// Biến để biết Replica này có phải malicious Replica
	byzantine bool

	// tracer và metrics
	tracer  *tracing.Tracer
	metrics *metrics.Metrics
}

func NewReplica(log zerolog.Logger, host *host.Host, peers []peer.ID, clusterID string /*, options ...Option */) (*Replica, error) {

	total := uint(len(peers))

	//  Nếu số replica nhỏ hơn 4 thì ko dùng pbft được
	if total < 4 {
		return nil, fmt.Errorf("số lượng quá nhỏ để sử dụng pBFT (đang có: %v, cần ít nhất: %v)", total, 4)
	}

	cfg := DefaultConfig

	// Thiết lập các thay đổi cho config
	// for _, option := range options {
	// 	option(&cfg)
	// }

	// Tạo instance
	replica := Replica{
		pbftCore:     newPbftCore(total),
		replicaState: newState(),

		cfg: cfg,

		log:  log.With().Str("component", "pbft").Str("cluster", clusterID).Logger(),
		host: host,
		//executor:   executor,
		clusterID:  clusterID,
		protocolID: protocol.ID(fmt.Sprintf("%s/cluster/%s", Protocol, clusterID)),

		id:    host.ID(),
		peers: peers,

		byzantine: isByzantine(),

		tracer:  tracing.NewTracer(tracerName),
		metrics: metrics.Default(),
	}
	replica.log.Info().Strs("replicas", peerIDList(peers)).Uint("n", total).Uint("f", replica.f).Bool("byzantine", replica.byzantine).Msg("created PBFT replica")

	// Xử lý các message của pBFT
	replica.setPBFTMessageHandler()

	return &replica, nil

}

// Trả về loại thuật toán đồng thuận đang sử dụng (pBFT)
func (r *Replica) Consensus() consensus.Type {
	return consensus.PBFT
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
	pm := make(map[peer.ID]struct{})
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
		fmt.Println("stream:", stream)
		buf := bufio.NewReader(stream)
		fmt.Printf("buf:", buf)

		msg, err := buf.ReadBytes('\n')
		fmt.Println("msg new line:", string(msg))
		// Nếu có error mà không phải là EOF thì báo error
		if err != nil && !errors.Is(err, io.EOF) {
			stream.Reset()
			r.log.Error().Err(err).Msg("error receiving direct message")
			return
		}

		r.log.Debug().Str("peer", from.String()).Msg("received message")

		err = r.processMessage(ctx, from, msg)
		if err != nil {
			r.log.Error().Err(err).Str("peer", from.String()).Msg("message processing failed lmao")
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
	// Kiểm tra named return value procError rồi đổi span status
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

	// Khóa sl lại để không cho nhiều replica cùng thay đổi một field
	r.sl.Lock()
	defer r.sl.Unlock()

	// Nếu phase hiện tại không cho xử lý message này thì return
	err = r.isMessageAllowed(msg)
	if err != nil {
		return fmt.Errorf("message not allowed (message: %T): %w", msg, err)
	}
	// Chọn cách xử lý phù hợp tùy theo msg.type
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

// Trả về ID của primary
func (r *Replica) primaryReplicaID() peer.ID {
	return r.peers[r.currentPrimary()]
}

// Kiểm tra xem replica có phải là primary
func (r *Replica) IsPrimary() bool {
	return r.id == r.primaryReplicaID()
}

// tạo danh sách string từ danh sách các peer.ID
func peerIDList(ids []peer.ID) []string {
	peerIDs := make([]string, 0, len(ids))
	for _, rp := range ids {
		peerIDs = append(peerIDs, rp.String())
	}
	return peerIDs
}

// Kiểm tra xem msg có được phép xử lý trong trạng thái hiện tại
func (r *Replica) isMessageAllowed(msg interface{}) error {

	// Nếu đang ở "active view", ta không nhận new-view msg
	if r.activeView {

		switch msg.(type) {
		case NewView:
			return ErrActiveView
		default:
			return nil
		}
	}

	// Nếu đang ở "active view", ta chỉ nhận view-change và new-view message
	switch msg.(type) {
	case ViewChange, NewView:
		return nil
	default:
		return ErrViewChange
	}

}

// Loại bỏ preprepares, prepares, commist cũ(view nhỏ hơn thresholdView) và pending requests
// Call this before updating the list of pending requests since for those we don't know
// in which view they were scheduled - we remove all of them.
func (r *Replica) cleanupState(thresholdView uint) {

	r.log.Debug().Uint("threshold_view", thresholdView).Msg("cleaning up replica state")

	// Xóa pending requests
	for id := range r.pending {
		delete(r.pending, id)
	}

	// Xóa preprepares cũ
	for id := range r.preprepares {
		if id.view < thresholdView {
			delete(r.preprepares, id)
		}
	}

	// Xóa prepares cũ
	for id := range r.prepares {
		if id.view < thresholdView {
			delete(r.prepares, id)
		}
	}

	// Xóa commits cũ
	for id := range r.commits {
		if id.view < thresholdView {
			delete(r.commits, id)
		}
	}
}

// Replica hiện tại có phải malicious
func isByzantine() bool {
	env := strings.ToLower(os.Getenv(EnvVarByzantine))

	switch env {
	case "y", "yes", "true", "1":
		return true
	default:
		return false
	}
}
