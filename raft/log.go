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
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pkg/errors"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated

var ErrInvalidIndex = errors.New("raft: index for entry is invalid")

type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstable logs will be included.
	stabled uint64

	// all entries that have not yet compact. 尚未被回收 (GC) 的Log
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).

	hardState, _, err := storage.InitialState()
	if err != nil {
		log.Fatal("log.newLog failed, cannot initialize state: ", err)
		return nil
	}

	/* TODO: 这里不是很确定 lo的取值, 因为不知道后续这里的entry需要怎么使用
	目前的理解是 entries 就是DB未执行的指令 因此需要全部加载出来 */

	lo, err := storage.FirstIndex()
	if err != nil {
		log.Fatal("log.newLog failed, cannot read firstIndex, ", err)
		return nil
	}

	hi, err := storage.LastIndex()
	if err != nil {
		log.Fatal("log.newLog failed, cannot read lastIndex, ", err)
		return nil
	}

	entries, err := storage.Entries(lo, hi)
	return &RaftLog{
		storage:   storage,
		committed: hardState.Commit,
		applied:   0,                // Applied 是已经被DB执行的指令 所以在recover之后
		stabled:   hardState.Commit, // RaftNode recover 之后, Stabled这个状态应该就等于Commit
		entries:   entries,
	}
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// committed 到最后都是UnStable的
	return l.entries[l.committed:]
}

// AllUnstableEntries returns all the committed but not applied entries
func (l *RaftLog) AllUnstableEntries() (entries []pb.Entry) {
	return l.entries[l.applied : l.committed+1]
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	return l.entries[len(l.entries)-1:][0].Index
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	if i < 0 || i > uint64(len(l.entries)) {
		return -1, ErrInvalidIndex
	}
	return l.entries[i].Term, nil
}
