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
	"fmt"
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"os/exec"
	"sort"
	"strings"
	"sync"
	"time"
)

type lockedRand struct {
	mu   sync.Mutex
	rand *rand.Rand
}

var globalRand = &lockedRand{
	rand: rand.New(rand.NewSource(time.Now().UnixNano())),
}

func minUint64(a, b uint64) uint64 {
	if a > b {
		return b
	}
	return a
}

func maxUint64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

// IsEmptyHardState returns true if the given HardState is empty.
func IsEmptyHardState(st pb.HardState) bool {
	return isHardStateEqual(st, pb.HardState{})
}

// IsEmptySnap returns true if the given Snapshot is empty.
func IsEmptySnap(sp *pb.Snapshot) bool {
	if sp == nil || sp.Metadata == nil {
		return true
	}
	return sp.Metadata.Index == 0
}

func mustTerm(term uint64, err error) uint64 {
	if err != nil {
		panic(err)
	}
	return term
}

func nodes(r *Raft) []uint64 {
	nodes := make([]uint64, 0, len(r.Prs))
	for id := range r.Prs {
		nodes = append(nodes, id)
	}
	sort.Sort(uint64Slice(nodes))
	return nodes
}

func diffu(a, b string) string {
	if a == b {
		return ""
	}
	aname, bname := mustTemp("base", a), mustTemp("other", b)
	defer os.Remove(aname)
	defer os.Remove(bname)
	cmd := exec.Command("diff", "-u", aname, bname)
	buf, err := cmd.CombinedOutput()
	if err != nil {
		if _, ok := err.(*exec.ExitError); ok {
			// do nothing
			return string(buf)
		}
		panic(err)
	}
	return string(buf)
}

func mustTemp(pre, body string) string {
	f, err := ioutil.TempFile("", pre)
	if err != nil {
		panic(err)
	}
	_, err = io.Copy(f, strings.NewReader(body))
	if err != nil {
		panic(err)
	}
	f.Close()
	return f.Name()
}

func logToString(l *RaftLog) string {
	s := fmt.Sprintf("committed: %d\n", l.committed)
	s += fmt.Sprintf("applied:  %d\n", l.applied)
	for i, e := range l.entries {
		s += fmt.Sprintf("#%d: %+v\n", i, e)
	}
	return s
}

type uint64Slice []uint64

func (p uint64Slice) Len() int           { return len(p) }
func (p uint64Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p uint64Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func IsLocalMsg(msgt pb.MessageType) bool {
	return msgt == pb.MessageType_MsgHup || msgt == pb.MessageType_MsgBeat
}

func IsResponseMsg(msgt pb.MessageType) bool {
	return msgt == pb.MessageType_MsgAppendResponse || msgt == pb.MessageType_MsgRequestVoteResponse || msgt == pb.MessageType_MsgHeartbeatResponse
}

func isHardStateEqual(a, b pb.HardState) bool {
	return a.Term == b.Term && a.Vote == b.Vote && a.Commit == b.Commit
}

func ParseRaftLogIndex(raftLog *RaftLog) (uint64, uint64, uint64) {
	lastIndex := raftLog.LastIndex()
	committed := raftLog.committed
	lastLogTerm, err := raftLog.Term(lastIndex)
	if err != nil {
		log.Warnf("util.ParseRaftLogIndex Failed, err: %v, lastLogTerm: %d", err, lastLogTerm)
	}

	return lastIndex, committed, lastLogTerm
}

func IsElectionSuccess(prs map[uint64]*Progress, votes map[uint64]bool) bool {
	sucCnt, _ := CountVotes(votes)
	if sucCnt >= (len(prs)+1)/2 {
		return true
	}
	return false
}

func IsElectionFailed(prs map[uint64]*Progress, votes map[uint64]bool) bool {
	_, failedCnt := CountVotes(votes)
	if failedCnt >= (len(prs)+1)/2 {
		return true
	}
	return false
}

func CountVotes(votes map[uint64]bool) (int, int) {
	sucCnt, failedCnt := 0, 0
	for _, vote := range votes {
		if vote {
			sucCnt++
		} else {
			failedCnt++
		}
	}
	return sucCnt, failedCnt
}

func ConvertEntryArrToEntryPntArr(entries []pb.Entry) []*pb.Entry {
	res := make([]*pb.Entry, 0)
	for _, entry := range entries {
		res = append(res, &entry)
	}
	return res
}

func ConvertEntryPntArrToEntryArr(entries []*pb.Entry) []pb.Entry {
	res := make([]pb.Entry, 0)
	for _, entry := range entries {
		res = append(res, *entry)
	}
	return res
}

func ClearVotes(votes map[uint64]bool) map[uint64]bool {
	for pid := range votes {
		delete(votes, pid)
	}
	return votes
}

func isNoopEntry(m pb.Message) bool {
	return m.Entries[0].Data == nil
}
