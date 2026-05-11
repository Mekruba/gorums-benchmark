package server

import (
	"context"
	"log/slog"
	"time"

	pb "github.com/Mekruba/gorums-benchmark/pbft.gorums.new/proto"
	"github.com/relab/gorums"
)

const viewChangeTimeout = 3 * time.Second

// ── Timer management ──────────────────────────────────────────────────────────

// startViewChangeTimer starts the timer if not already running.
// Called when a ClientRequest arrives and there are pending requests.
func (p *PBFTServer) startViewChangeTimer() {
	if p.primary {
		return
	}
	p.viewChangeMu.Lock()
	defer p.viewChangeMu.Unlock()
	if p.viewChangeTimer == nil {
		p.viewChangeTimer = time.AfterFunc(viewChangeTimeout, p.onViewChangeTimeout)
		slog.Debug("view change timer started", "node", p.id, "view", p.view)
	}
}

// resetViewChangeTimer restarts the timer — called when a request commits
// but pending map is still non-empty.
func (p *PBFTServer) resetViewChangeTimer() {
	if p.primary {
		return
	}
	p.viewChangeMu.Lock()
	defer p.viewChangeMu.Unlock()
	if p.viewChangeTimer != nil {
		p.viewChangeTimer.Reset(viewChangeTimeout)
		slog.Debug("view change timer reset", "node", p.id, "view", p.view)
	}
}

// stopViewChangeTimer stops the timer — called when pending map is empty.
func (p *PBFTServer) stopViewChangeTimer() {
	p.viewChangeMu.Lock()
	defer p.viewChangeMu.Unlock()
	if p.viewChangeTimer != nil {
		p.viewChangeTimer.Stop()
		p.viewChangeTimer = nil
		slog.Debug("view change timer stopped", "node", p.id, "view", p.view)
	}
}

// onViewChangeTimeout fires when the timer expires — primary is suspected.
func (p *PBFTServer) onViewChangeTimeout() {
	slog.Warn("view change timeout — primary suspected",
		"node", p.id, "view", p.view)
	p.sendViewChange()
}

// ── View Change ───────────────────────────────────────────────────────────────

func (p *PBFTServer) sendViewChange() {
	p.mu.Lock()
	newView := p.view + 1
	p.inViewChange = true
	p.mu.Unlock()

	n := p.msgLog.LastStableSeq()
	prepared := p.msgLog.PreparedEntries(n)

	// Build P set
	var preparedSets []*pb.PreparedSet
	for _, e := range prepared {
		e.mu.Lock()
		pp := e.prePrepare
		preps := make([]*pb.PrepareMsg, 0, len(e.prepares))
		for _, pr := range e.prepares {
			preps = append(preps, pb.PrepareMsg_builder{
				View:      pr.view,
				Sequence:  pr.sequence,
				ReplicaId: pr.replicaID,
				Digest:    pr.digest,
			}.Build())
		}
		e.mu.Unlock()
		if pp == nil {
			continue
		}
		preparedSets = append(preparedSets, pb.PreparedSet_builder{
			PrePrepare: pb.PrePrepareMsg_builder{
				View:     pp.view,
				Sequence: pp.sequence,
				Digest:   pp.digest,
			}.Build(),
			Prepares: preps,
		}.Build())
	}

	sig := sign(getPrivKey(p.id), viewChangeDigest(newView, n))
	msg := pb.ViewChangeMsg_builder{
		NewView:       newView,
		LastStableSeq: n,
		ReplicaId:     p.id,
		Prepared:      preparedSets,
		Signature:     sig,
	}.Build()

	cfg := p.outbound()
	pb.ViewChange(cfg.Context(context.Background()), msg, gorums.IgnoreErrors())
	slog.Info("view change sent", "node", p.id, "new_view", newView, "last_stable", n)
}

// ViewChange handles an incoming VIEW-CHANGE message.
func (p *PBFTServer) ViewChange(ctx gorums.ServerCtx, request *pb.ViewChangeMsg) {
	ctx.Release()
	newView := request.GetNewView()
	fromID := request.GetReplicaId()

	// verify signature
	if !verifyMsg(fromID, viewChangeDigest(newView, request.GetLastStableSeq()), request.GetSignature()) {
		slog.Warn("ViewChange signature invalid", "node", p.id, "from", fromID)
		return
	}

	slog.Info("view change received", "node", p.id, "new_view", newView, "from", fromID)

	// Only the new primary processes view change messages
	newPrimary := p.newPrimary(newView)
	if newPrimary != p.id {
		return
	}

	p.viewChangeMu.Lock()
	if p.viewChangeMsgs == nil {
		p.viewChangeMsgs = make(map[uint32]*pb.ViewChangeMsg)
	}
	p.viewChangeMsgs[fromID] = request
	count := len(p.viewChangeMsgs)
	p.viewChangeMu.Unlock()

	// Need 2f valid view-change messages
	if count == 2*p.f() {
		p.sendNewView(newView)
	}
}

func (p *PBFTServer) sendNewView(newView uint32) {
	p.viewChangeMu.Lock()
	msgs := make([]*pb.ViewChangeMsg, 0, len(p.viewChangeMsgs))
	for _, m := range p.viewChangeMsgs {
		msgs = append(msgs, m)
	}
	p.viewChangeMu.Unlock()

	// compute min-s and max-s
	minS := p.msgLog.LastStableSeq()
	var maxS uint64
	for _, m := range msgs {
		if m.GetLastStableSeq() > minS {
			minS = m.GetLastStableSeq()
		}
		for _, ps := range m.GetPrepared() {
			if s := ps.GetPrePrepare().GetSequence(); s > maxS {
				maxS = s
			}
		}
	}

	// build new pre-prepares for seqs between minS and maxS
	var prePrepares []*pb.PrePrepareMsg
	for seq := minS + 1; seq <= maxS; seq++ {
		// find highest view pre-prepare for this seq across all view-change msgs
		var best *pb.PrePrepareMsg
		for _, m := range msgs {
			for _, ps := range m.GetPrepared() {
				pp := ps.GetPrePrepare()
				if pp.GetSequence() == seq {
					if best == nil || pp.GetView() > best.GetView() {
						best = pp
					}
				}
			}
		}
		if best == nil {
			// no prepared request for this seq — use null request
			best = pb.PrePrepareMsg_builder{
				View:     newView,
				Sequence: seq,
				Digest:   nullDigest(),
			}.Build()
		} else {
			best = pb.PrePrepareMsg_builder{
				View:     newView,
				Sequence: seq,
				Digest:   best.GetDigest(),
			}.Build()
		}
		sig := sign(getPrivKey(p.id), prePrepareDigest(newView, seq, best.GetDigest()))
		best = pb.PrePrepareMsg_builder{
			View:      newView,
			Sequence:  seq,
			Digest:    best.GetDigest(),
			Signature: sig,
		}.Build()
		prePrepares = append(prePrepares, best)
	}

	sig := sign(getPrivKey(p.id), newViewDigest(newView))
	msg := pb.NewViewMsg_builder{
		NewView:     newView,
		ReplicaId:   p.id,
		ViewChanges: msgs,
		PrePrepares: prePrepares,
		Signature:   sig,
	}.Build()

	cfg := p.outbound()
	pb.NewView(cfg.Context(context.Background()), msg, gorums.IgnoreErrors())
	slog.Info("new view sent", "node", p.id, "new_view", newView)

	// new primary enters new view
	p.enterNewView(newView, prePrepares)
}

// NewView handles an incoming NEW-VIEW message.
func (p *PBFTServer) NewView(ctx gorums.ServerCtx, request *pb.NewViewMsg) {
	ctx.Release()
	newView := request.GetNewView()
	fromID := request.GetReplicaId()

	// verify new primary signature
	if !verifyMsg(fromID, newViewDigest(newView), request.GetSignature()) {
		slog.Warn("NewView signature invalid", "node", p.id, "from", fromID)
		return
	}

	// verify it came from the correct new primary
	if fromID != p.newPrimary(newView) {
		slog.Warn("NewView from wrong primary", "node", p.id, "from", fromID, "expected", p.newPrimary(newView))
		return
	}

	slog.Info("new view received", "node", p.id, "new_view", newView)
	p.enterNewView(newView, request.GetPrePrepares())
}

// enterNewView transitions this node into newView and replays pre-prepares.
func (p *PBFTServer) enterNewView(newView uint32, prePrepares []*pb.PrePrepareMsg) {
	p.mu.Lock()
	p.view = newView
	p.primary = (p.id == p.newPrimary(newView))
	p.inViewChange = false
	p.mu.Unlock()

	p.stopViewChangeTimer()

	p.viewChangeMu.Lock()
	p.viewChangeMsgs = make(map[uint32]*pb.ViewChangeMsg)
	p.viewChangeMu.Unlock()

	slog.Info("entered new view", "node", p.id, "view", newView, "primary", p.primary)

	// advance sequence counter to max-s so new primary continues from there
	var maxS uint64
	for _, pp := range prePrepares {
		if s := pp.GetSequence(); s > maxS {
			maxS = s
		}
	}
	for {
		cur := p.sequence.Load()
		if cur >= maxS {
			break
		}
		if p.sequence.CompareAndSwap(cur, maxS) {
			break
		}
	}

	// replay pre-prepares
	cfg := p.outbound()
	cfgCtx := cfg.Context(context.Background())

	for _, pp := range prePrepares {
		seq := pp.GetSequence()
		if !p.msgLog.WithinWaterMarks(seq) {
			continue
		}
		if p.primary {
			p.tryRecordPrePrepare(seq, 0)
			pb.PrePrepare(cfgCtx, pb.PrePrepareMsg_builder{
				View:      newView,
				Sequence:  seq,
				Digest:    pp.GetDigest(),
				Signature: pp.GetSignature(),
			}.Build(), gorums.IgnoreErrors())
		} else {
			if p.tryRecordPrePrepare(seq, 0) {
				sig := sign(getPrivKey(p.id), prepareDigest(newView, seq, p.id, pp.GetDigest()))
				pb.Prepare(cfgCtx, pb.PrepareMsg_builder{
					View:      newView,
					Sequence:  seq,
					ReplicaId: p.id,
					Digest:    pp.GetDigest(),
					Signature: sig,
				}.Build(), gorums.IgnoreErrors())
			}
		}
	}

	// restart primary loop if this node is now primary
	if p.primary {
		p.pendingMu.Lock()
		for ts := range p.pending {
			select {
			case p.reqQueue <- queuedRequest{ts: ts}:
			default:
			}
		}
		p.pendingMu.Unlock()
		go p.runPrimary()
	}
}

// newPrimary returns the node ID of the primary for a given view.
func (p *PBFTServer) newPrimary(view uint32) uint32 {
	return view%uint32(p.clusterSize) + 1
}
