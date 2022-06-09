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
	"github.com/pingcap-incubator/tinykv/log"
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
	// Match: log that latest matched with leader
	// Next: next position that will be put log
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// Leader 维护的数据结构 // log replication progress of each peer
	// peerID -> Progress
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	LeaderID uint64

	// Timeout 表示经过多久触发某个事件
	// Elapse 表示距离上一个TimeOut过去了多久

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
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}

	// Your Code Here (2A).

	hardState, confState, err := c.Storage.InitialState()
	if err != nil {
		log.Fatal("raft.newRaft Failed, cannot inistialize state: ", err)
		return nil
	}

	peerInfo := make(map[uint64]bool)
	for _, node := range confState.Nodes {
		peerInfo[node] = false
	}

	return &Raft{
		id:               c.ID,
		Term:             hardState.Term,
		Vote:             hardState.Vote,
		RaftLog:          newLog(c.Storage),
		State:            StateFollower,
		votes:            peerInfo,
		msgs:             make([]pb.Message, 0),
		LeaderID:         None,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
	}
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	return false
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	r.heartbeatElapsed++
	r.electionElapsed++

	switch r.State {
	case StateLeader:
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			heartBeatMsg := pb.Message{
				MsgType: pb.MessageType_MsgHeartbeat,
				// To:                   0, to everyone else
				From: r.id,
				Term: r.Term,
			}
			for u := range r.Prs {
				heartBeatMsg.To = u
				r.msgs = append(r.msgs, heartBeatMsg)
			}
		}
	default:
		if r.electionElapsed >= r.electionTimeout {
			r.electionElapsed = 0
			electionMsg := pb.Message{
				MsgType: pb.MessageType_MsgHup,
				// To:                   0,  to everyone else
				From: r.id,
				Term: r.Term,
			}
			for u := range r.Prs {
				electionMsg.To = u
				r.msgs = append(r.msgs, electionMsg)
			}
		}
	}

}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, leaderID uint64) {
	r.Term = term
	r.LeaderID = leaderID
	r.State = StateFollower
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	r.State = StateCandidate
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	r.State = StateLeader

	noopMsg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		//To:                   0, everyone
		From:  r.id,
		Term:  r.Term,
		Index: r.RaftLog.LastIndex(),
		Entries: []*pb.Entry{
			{
				EntryType: pb.EntryType_EntryNormal,
				Term:      r.Term,
				Index:     r.RaftLog.LastIndex(),
				Data:      nil,
			},
		},
	}

	for u := range r.Prs {
		noopMsg.To = u
		r.msgs = append(r.msgs, noopMsg)
	}
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		switch m.MsgType {
		// Follower 变成Candidate 并发起投票请求
		case pb.MessageType_MsgHup:
			r.msgs = buildElectionRequest(r.id, r.Term, nodes(r), r.RaftLog)
			return nil
		// 收到candidate的Vote请求
		case pb.MessageType_MsgRequestVote:
			return nil
		// 收到Leader的心跳请求
		case pb.MessageType_MsgHeartbeat:
			return nil
		// 收到Leader的AppendEntry请求
		case pb.MessageType_MsgAppend:
			return nil
		}
	case StateCandidate:
		switch m.MsgType {
		// 自己投自己
		case pb.MessageType_MsgHup:
			return nil
		// 有Leader已经当选
		case pb.MessageType_MsgAppend:
			return nil
		// 收到Follower的投票结果
		case pb.MessageType_MsgRequestVoteResponse:
			return nil
		}
	case StateLeader:
		switch m.MsgType {
		// 收到另一个Leader的Append请求 -> 脑裂heal之后有可能发生
		case pb.MessageType_MsgAppend:
			return nil
		// 收到Follower的Propose请求
		case pb.MessageType_MsgPropose:
			return nil
		// Leader发给自己 提醒要发送心跳了
		case pb.MessageType_MsgBeat:
			return nil
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

func (r *Raft) handleRequestVote(m pb.Message) (pb.Message, error) {
	lastIndex, committed, lastLogTerm := parseRaftLogIndex(r.RaftLog)

	resp := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		LogTerm: lastLogTerm,
		Index:   lastIndex,
		Commit:  committed,
	}

	// 拒绝投票的情况
	if r.Term > m.Term {
		resp.Reject = true
		return resp, nil
	}
	if r.Term == m.Term && r.Vote != m.From {
		resp.Reject = true
		return resp, nil
	}

	// 判断本地日志是否大于Candidate的日志
	if r.RaftLog.committed > committed {
		resp.Reject = true
		return resp, nil
	}

	// 投票给发送消息过来的 Candidate
	r.Vote = m.From
	r.Term = m.Term

	resp.Reject = false
	return resp, nil
}
