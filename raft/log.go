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

	// all entries that have not yet compact. 尚未被回收 (GC) 的 Log
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	hardState, _, err := storage.InitialState()
	if err != nil {
		log.Fatal("log.newLog failed, cannot initialize state: ", err)
		return nil
	}

	hi, err := storage.LastIndex()
	if err != nil {
		log.Fatal("log.newLog failed, cannot read lastIndex, ", err)
		return nil
	}

	entries, err := storage.Entries(hi, hardState.Commit+1)
	if err != nil {
		log.Warning("log.newLog failed, cannot read entries, ", err)
		entries = make([]pb.Entry, 0)
	}

	return &RaftLog{
		storage:   storage,
		committed: hardState.Commit,
		applied:   hi,               // Applied 是已经被DB执行的指令 值为保存在Storage中最大的 Index
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
	if len(l.entries) == 0 {
		return 0
	}

	// 兼容初始化entries长度为0的情况
	start := maxInt64(0, int64(len(l.entries)-1))
	return l.entries[start:][0].Index
}

// Term return the term of the entry in the given index
// index 从1开始计算所以entries在取值的时候要减一
func (l *RaftLog) Term(index uint64) (uint64, error) {
	if index < 0 || len(l.entries) == 0 || index > uint64(len(l.entries)) {
		return 0, ErrInvalidIndex
	}
	return l.entries[index-1].Term, nil
}

func (l *RaftLog) LastTerm() uint64 {
	term, _ := l.Term(l.LastIndex())
	return term
}

// helper functions
func (l *RaftLog) append(entries ...pb.Entry) (lastIndex uint64) {
	if len(entries) == 0 {
		lastIndex = l.LastIndex()
		return
	}
	after := entries[0].Index
	switch {
	// 直接Append到末尾
	case after == l.LastIndex()+1:
		l.entries = append(l.entries, entries...)
	default:

	}
	return l.LastIndex()
}

func (l *RaftLog) maybeCommit(maxIndex, term uint64) bool {
	if maxIndex > l.committed && l.zeroTermOnErrCompacted(l.Term(maxIndex)) == term {
		l.commitTo(maxIndex)
		return true
	}
	return false
}
func (l *RaftLog) zeroTermOnErrCompacted(t uint64, err error) uint64 {
	if err == nil {
		return t
	}
	if err == ErrCompacted {
		return 0
	}
	log.Panicf("unexpected error (%v)", err)
	return 0
}

func (l *RaftLog) commitTo(tocommit uint64) {
	// never decrease commit
	if l.committed < tocommit {
		if l.LastIndex() < tocommit {
			log.Panicf("tocommit(%d) is out of range [lastIndex(%d)]. Was the raft log corrupted, truncated, or lost?", tocommit, l.LastIndex())
		}
		l.committed = tocommit
	}
}

func (l *RaftLog) appliedTo(i uint64) {
	if i == 0 {
		return
	}
	if l.committed < i || i < l.applied {
		log.Panicf("applied(%d) is out of range [prevApplied(%d), committed(%d)]", i, l.applied, l.committed)
	}
	l.applied = i
}
