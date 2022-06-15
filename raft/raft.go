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
	"strings"
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

type CampaignType string

// Possible values for CampaignType
const (
	// campaignPreElection represents the first phase of a normal election when
	// Config.PreVote is true.
	campaignPreElection CampaignType = "CampaignPreElection"
	// campaignElection represents a normal (time-based) election (the second phase
	// of the election when Config.PreVote is true).
	campaignElection CampaignType = "CampaignElection"
	// campaignTransfer represents the type of leader transfer
	campaignTransfer CampaignType = "CampaignTransfer"
)

// 控制 log 的输出
func init() {
	log.SetLevel(log.LOG_LEVEL_DEBUG)
}

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

	hardState, _, err := c.Storage.InitialState()
	if err != nil {
		log.Fatal("raft.newRaft Failed, cannot inistialize state: ", err)
		return nil
	}

	votes := make(map[uint64]bool)
	peers := make(map[uint64]*Progress)
	for _, node := range c.peers {
		votes[node] = false
		peers[node] = &Progress{
			Match: 0,
			Next:  0,
		}
	}

	raft := &Raft{
		id:               c.ID,
		Term:             hardState.Term,
		Vote:             hardState.Vote,
		RaftLog:          newLog(c.Storage),
		Prs:              peers,
		State:            StateFollower,
		votes:            votes,
		msgs:             make([]pb.Message, 0),
		LeaderID:         None,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
	}
	raft.becomeFollower(raft.Term, None)

	var nodesStrs []string
	for n := range raft.Prs {
		nodesStrs = append(nodesStrs, fmt.Sprintf("%x ", n))
	}

	log.Infof("newRaft %x [peers: [%s], term: %d, commit: %d, applied: %d, lastindex: %d, lastterm: %d]",
		raft.id, strings.Join(nodesStrs, ","), raft.Term, raft.RaftLog.committed, raft.RaftLog.applied, raft.RaftLog.LastIndex(), raft.RaftLog.LastTerm())
	return raft
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
			hup := pb.Message{
				MsgType: pb.MessageType_MsgHup,
				To:      r.id, // send MsgHup to self
				From:    r.id,
				Term:    r.Term,
			}
			r.send(hup)
		}
	}

}

func (r *Raft) reset(term uint64) {
	if r.Term != term {
		r.Term = term
		r.Vote = None
	}
	r.LeaderID = None

	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	// r.resetRandomizedElectionTimeout()

	// r.abortLeaderTransfer()

	for id := range r.votes {
		r.votes[id] = false
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, leaderID uint64) {
	r.reset(term)
	r.LeaderID = leaderID
	r.State = StateFollower
	r.stepFunc = stepFuncFollower
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	r.State = StateCandidate
	r.stepFunc = stepFuncCandidate
	r.Vote = r.id
	r.reset(r.Term + 1)
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	r.State = StateLeader
	r.stepFunc = stepFuncLeader
	r.reset(r.Term)
	// 发一条noop消息
	r.broadcast(pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		From:    r.id,
		Term:    r.Term,
		Index:   r.RaftLog.LastIndex(),
	})
}

// sendAppend sends an AppendRPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	return r.maySendAppend(to, false)
}

// 找 To 的 Next 从Next位置开始 Append
func (r *Raft) maySendAppend(to uint64, sendIfEmpty bool) bool {
	pr := r.Prs[to]
	m := pb.Message{}
	m.To = to

	term, errT := r.RaftLog.Term(pr.Next - 1)
	entries := r.RaftLog.entries[pr.Next:r.RaftLog.LastIndex()]
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
	m.Term = term
	m.Entries = ConvertEntryArrToEntryPntArr(entries)
	m.Commit = r.RaftLog.committed
	r.send(m)
	return true
}

func (r *Raft) send(m pb.Message) {
	if m.From == None {
		m.From = r.id
	}
	if r.id == m.To {
		r.Step(m)
	} else {
		r.msgs = append(r.msgs, m)
	}
	return
}

func (r *Raft) broadcast(m pb.Message) {
	for _, node := range nodes(r) {
		m.To = node
		r.send(m)
	}
}

func (r *Raft) broadcastAppend() {
	for _, node := range nodes(r) {
		r.sendAppend(node)
	}
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	log.Debugf("Server: %d receive %v \n", r.id, m)
	return r.stepFunc(r, m)
}

func (r *Raft) handleAppendEntries(m pb.Message) {
	if m.Index < r.RaftLog.committed {
		r.send(pb.Message{To: m.From, MsgType: pb.MessageType_MsgAppendResponse, Index: r.RaftLog.committed, Reject: true})
	}

	if m.Term > r.Term {
		r.becomeFollower(m.Term, m.From)
		r.send(pb.Message{From: r.id, To: m.From, MsgType: pb.MessageType_MsgAppendResponse})
	}

	return
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
	if r.Term == m.Term && r.Vote != None {
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

func (r *Raft) handleHeartBeat(m pb.Message) {
	if r.LeaderID == m.From {
		r.electionElapsed = 0
		r.RaftLog.committed = m.Commit
		r.send(pb.Message{
			MsgType: pb.MessageType_MsgHeartbeatResponse,
			Term:    r.Term,
			To:      m.From,
			From:    r.id,
		})
	}
	return
}

func (r *Raft) handlePropose(m pb.Message) error {
	// Leader 添加到 RaftLog中
	if !r.appendEntry(ConvertEntryPntArrToEntryArr(m.Entries)...) {
		return ErrProposalDropped
	}
	// 然后发送AppendEntry 给其他的节点
	r.broadcastAppend()
	return nil
}

// helper function
func (r *Raft) appendEntry(entries ...pb.Entry) (accepted bool) {
	lastIndex := r.RaftLog.LastIndex()
	// 修正 RaftLog 为空的情况
	li := int64(lastIndex)
	if len(r.RaftLog.entries) == 0 {
		li = -1
	}

	for i := range entries {
		entries[i].Term = r.Term
		entries[i].Index = uint64(li + int64(1+i))
	}
	lastIndex = r.RaftLog.append(entries...)
	return true
}

func stepFuncLeader(r *Raft, m pb.Message) error {
	switch m.MsgType {
	// 收到另一个Leader的Append请求 -> 脑裂heal之后有可能发生
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	// 收到Follower的Propose请求
	case pb.MessageType_MsgPropose:
		r.handlePropose(m)
	// Leader发给自己 提醒要发送心跳了
	case pb.MessageType_MsgBeat:
	}
	return nil
}

func stepFuncCandidate(r *Raft, m pb.Message) error {
	switch m.MsgType {
	// 自己投自己
	case pb.MessageType_MsgHup:
		r.campaign(campaignElection)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	// 有Leader已经当选
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	// 收到Follower的投票结果
	case pb.MessageType_MsgRequestVoteResponse:
		switch m.Reject {
		case false:
			r.votes[m.From] = true
			if IsElectionSuccess(r.Prs, r.votes) {
				r.becomeLeader()
			}
		case true:
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
		r.campaign(campaignElection)
	// 收到candidate的Vote请求
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	// 收到Leader的心跳请求
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartBeat(m)
	// 收到 Leader 的AppendEntry请求
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	}
	return nil
}

func (r *Raft) campaign(campaignType CampaignType) (ans []pb.Message) {
	lastIndex, committed, lastLogTerm := ParseRaftLogIndex(r.RaftLog)
	r.becomeCandidate()
	message := pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		From:    r.id,
		Term:    r.Term,
		Index:   lastIndex,
		Commit:  committed,
		LogTerm: lastLogTerm,
	}
	for peer := range r.Prs {
		message.To = peer
		r.send(message)
	}
	return
}

// handleHeartbeat handle Heartbeat RPC request
// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Attach the commit as minUint64(to.matched, r.committed).
	// When the leader sends out heartbeat message,
	// the receiver(follower) might not be matched with the leader,
	// or it might not have all the committed entries.
	// The leader MUST NOT forward the follower's commit to
	// an unmatched index.
	commit := minUint64(r.Prs[to].Match, r.RaftLog.committed)
	m := pb.Message{
		Term:    r.Term,
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
