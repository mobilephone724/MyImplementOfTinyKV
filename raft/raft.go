// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	NumbersOfVote int

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64

	peersId []uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	return &Raft{
		id:               c.ID,
		Term:             0,
		Vote:             0,
		RaftLog:          newLog(c.Storage),
		Lead:             0,
		votes:            make(map[uint64]bool),
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick + int(c.ID),
		heartbeatElapsed: 0,
		electionElapsed:  0,
		leadTransferee:   0,
		PendingConfIndex: c.Applied,
		peersId:          c.peers,
	}
	// Your Code Here (2A).
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	r.msgs = append(r.msgs,
		pb.Message{
			MsgType: pb.MessageType_MsgAppend,
			To:      to,
			From:    r.id,
			Term:    r.Term,
			LogTerm: r.RaftLog.logTerm,
		})
	return false
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	r.msgs = append(r.msgs,
		pb.Message{
			MsgType: pb.MessageType_MsgBeat,
			To:      to,
			From:    r.id,
			Term:    r.Term,
			LogTerm: r.RaftLog.logTerm,
		})
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateLeader:
		r.heartbeatElapsed++
		r.electionElapsed++
		r.boardcastHeartBeat()
	case StateFollower:
		r.electionElapsed++
		if r.electionElapsed >= r.electionTimeout {
			r.becomeCandidate()
			r.clearElapse()
			r.startAVote(true)
		}
	case StateCandidate:
		r.electionElapsed++
		if r.electionElapsed >= r.electionTimeout {
			r.clearElapse()
			r.startAVote(false)
		}
	}

}

func (r *Raft) startAVote(firstVote bool) {
	if !firstVote {
		r.Term++
	}
	r.votes = make(map[uint64]bool)
	for _, to := range r.peersId {
		if to == r.id {
			r.votes[r.id] = true
			continue
		}
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgRequestVote,
			From:    r.id,
			To:      to,
			Index:   0,
			LogTerm: 0,
			Term:    r.Term,
		})
	}
}

func (r *Raft) boardcastHeartBeat() {
	for _, to := range r.peersId {
		if to == r.id {
			continue
		}
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgHeartbeat,
			From:    r.id,
			To:      to,
			Index:   0,
			LogTerm: 0,
			Term:    r.Term,
		})
	}
}

func (r *Raft) clearElapse() {
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	if r.Term < term {
		r.Vote = 0
		r.Term = term
		r.Lead = lead
		r.State = StateFollower
	}
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	if r.State == StateFollower {
		r.State = StateCandidate
		r.Term++
	}
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	if r.State == StateCandidate {
		r.State = StateLeader
		for _, to := range r.peersId {
			r.msgs = append(r.msgs, pb.Message{
				MsgType: pb.MessageType_MsgPropose,
				From:    r.id,
				To:      to,
				Index:   0,
				LogTerm: 0,
				Term:    r.Term,
			})
		}

		// r.RaftLog.
	}
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
}

func (r *Raft) HandleVoteRequst(to uint64, isRejected bool) {
	r.msgs = append(r.msgs,
		pb.Message{
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			To:      to,
			From:    r.id,
			Term:    r.Term,
			LogTerm: r.RaftLog.logTerm,
			Reject:  isRejected,
		})
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	if m.Term < r.Term {
		return nil
	} else if m.Term > r.Term {
		if m.MsgType == pb.MessageType_MsgAppend {
			r.becomeFollower(m.Term, m.From)
		}
	}
	r.Term = m.Term
	switch r.State {
	case StateFollower:
		switch m.MsgType {
		case pb.MessageType_MsgAppend:
		case pb.MessageType_MsgHeartbeat:
			r.clearElapse()
			r.msgs = append(r.msgs,
				pb.Message{
					MsgType: pb.MessageType_MsgHeartbeatResponse,
					To:      m.From,
					From:    r.id,
					Term:    r.Term,
					LogTerm: r.RaftLog.logTerm,
					Reject:  false,
				})
		case pb.MessageType_MsgPropose:
			r.becomeFollower(m.Term, m.From)
		case pb.MessageType_MsgRequestVote:
			if r.Vote == 0 {
				r.Vote = m.From
				r.HandleVoteRequst(m.From, false)
			}
			r.HandleVoteRequst(m.From, true)
		}
	case StateCandidate:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			r.startAVote(false)
		case pb.MessageType_MsgRequestVote:
			r.HandleVoteRequst(m.From, true)
		case pb.MessageType_MsgRequestVoteResponse:
			r.votes[m.From] = !m.Reject
			if !m.Reject {
				r.NumbersOfVote++
				if r.NumbersOfVote > len(r.peersId)/2 {
					r.becomeLeader()
				}
			}
			// if(r.num)
		case pb.MessageType_MsgPropose:
			r.becomeFollower(m.Term, m.From)
		}
	case StateLeader:
		switch m.MsgType {
		case pb.MessageType_MsgRequestVote:
			r.HandleVoteRequst(m.From, true)
		case pb.MessageType_MsgBeat:
			for _, id := range r.peersId {
				r.msgs = append(r.msgs,
					pb.Message{
						MsgType: pb.MessageType_MsgHeartbeat,
						To:      id,
						From:    r.id,
						Term:    r.Term,
						LogTerm: r.RaftLog.logTerm,
					})
			}
		case pb.MessageType_MsgHeartbeatResponse:
		case pb.MessageType_MsgTransferLeader:
		case pb.MessageType_MsgPropose:
			r.becomeFollower(m.Term, m.From)
		}
	}
	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
