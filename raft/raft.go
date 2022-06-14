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
	"fmt"
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

type stepFunc func(r *Raft, m pb.Message) error

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

	stepFunc stepFunc

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

// sendAppend sends an AppendRPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	return r.maySendAppend(to, false)
}

func (r *Raft) maySendAppend(to uint64, sendIfEmpty bool) bool {
	pr := r.Prs[to]
	m := pb.Message{}
	m.To = to

	term, errT := r.RaftLog.Term(pr.Next - 1)
	entries := r.RaftLog.entries[pr.Next : r.RaftLog.LastIndex()+1]
	if len(entries) == 0 && !sendIfEmpty {
		return false
	}

	if errT != nil {
		log.Fatalf("Raft.MaybeSendAppend Term Error: %v \n", errT)
		return false
	}

	m.MsgType = pb.MessageType_MsgAppend
	m.Index = pr.Next - 1
	m.LogTerm = term
	m.Entries = ConverEntryArrToEntryPntArr(entries)
	m.Commit = r.RaftLog.committed
	r.send(m)
	return true
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
			for u := range r.Prs {
				r.sendHeartbeat(u)
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
				r.send(electionMsg)
			}
		}
	}

}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, leaderID uint64) {
	r.Term = term
	r.LeaderID = leaderID
	r.State = StateFollower
	r.stepFunc = stepFuncFollower
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	r.State = StateCandidate
	r.stepFunc = stepFuncCandidate
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	r.State = StateLeader
	r.stepFunc = stepFuncLeader

	noopMsg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		//To:                   0, // to everyone
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
		r.send(noopMsg)
	}
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	return r.stepFunc(r, m)
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	if m.Index < r.RaftLog.committed {
		r.send(pb.Message{To: m.From, MsgType: pb.MessageType_MsgAppendResponse, Index: r.RaftLog.committed})
		return
	}

	// TODO: Implement Lab2-b
}

// handleHeartbeat handle Heartbeat RPC request
// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Attach the commit as min(to.matched, r.committed).
	// When the leader sends out heartbeat message,
	// the receiver(follower) might not be matched with the leader,
	// or it might not have all the committed entries.
	// The leader MUST NOT forward the follower's commit to
	// an unmatched index.
	commit := min(r.Prs[to].Match, r.RaftLog.committed)
	m := pb.Message{
		To:      to,
		MsgType: pb.MessageType_MsgHeartbeat,
		Commit:  commit,
	}

	r.send(m)
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

func (r *Raft) handleRequestVote(m pb.Message) {
	lastIndex, committed, lastLogTerm := ParseRaftLogIndex(r.RaftLog)

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
	}
	if r.Term == m.Term && r.Vote != m.From {
		resp.Reject = true
	}

	// 判断本地日志是否大于Candidate的日志
	if r.RaftLog.committed > committed {
		resp.Reject = true
	}

	// 投票给发送消息过来的 Candidate
	r.Vote = m.From
	r.Term = m.Term

	resp.Reject = false
	r.send(resp)
}

func (r *Raft) handleHeartBeat(m pb.Message) error {
	if r.LeaderID == m.From {
		r.electionElapsed = 0
		r.RaftLog.committed = m.Commit
		r.send(pb.Message{
			MsgType: pb.MessageType_MsgHeartbeatResponse,
			To:      m.From,
			From:    r.id,
		})
	}
	return nil
}

// send schedules persisting state to a stable storage and AFTER that
// sending the message (as part of next Ready message processing).
func (r *Raft) send(m pb.Message) {
	if m.From == None {
		m.From = r.id
	}
	if m.MsgType == pb.MessageType_MsgRequestVote || m.MsgType == pb.MessageType_MsgRequestVoteResponse {
		if m.Term == 0 {
			panic(fmt.Sprintf("term should be set when sending %s", m.MsgType))
		}
	} else {
		if m.Term != 0 {
			panic(fmt.Sprintf("term should not be set when sending %s (was %d)", m.MsgType, m.Term))
		}
	}
	r.msgs = append(r.msgs, m)
}

func stepFuncLeader(r *Raft, m pb.Message) error {
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
	return nil
}

func stepFuncCandidate(r *Raft, m pb.Message) error {
	switch m.MsgType {
	// 自己投自己
	case pb.MessageType_MsgHup:
		messages := BuildElectionRequest(r.id, r.Term, nodes(r), r.RaftLog)
		for _, msg := range messages {
			r.send(msg)
		}
	// 有Leader已经当选
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	// 收到Follower的投票结果
	case pb.MessageType_MsgRequestVoteResponse:
		switch m.Reject {
		case true:
			r.votes[m.From] = true
			if IsElectionSuccess(r.Prs, r.votes) {
				r.becomeLeader()
			}
		case false:
			r.votes[m.From] = false
			if IsElectionFailed(r.Prs, r.votes) {
				r.becomeFollower(m.Term, None)
			}
		}
	}
	return nil
}

func stepFuncFollower(r *Raft, m pb.Message) error {
	switch m.MsgType {
	// Follower 变成Candidate 并发起投票请求
	case pb.MessageType_MsgHup:
		messages := BuildElectionRequest(r.id, r.Term, nodes(r), r.RaftLog)
		for _, msg := range messages {
			r.send(msg)
		}
	// 收到candidate的Vote请求
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	// 收到Leader的心跳请求
	case pb.MessageType_MsgHeartbeat:
		return r.handleHeartBeat(m)
	// 收到 Leader 的AppendEntry请求
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	}
	return nil
}
