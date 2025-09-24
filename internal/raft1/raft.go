package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

/*
心跳RPC消息不超过10条
测试程序要求5秒内选主完， 按论文，只有当领导者发送心跳消息的频率远高于每150毫秒一次（例如，每10毫秒一次）时，
这个范围才有意义。由于测试程序限制了心跳消息的频率，您必须使用大于150到300毫秒的选举超时时间，但也不能太大，
否则可能无法在5秒内选举出新的领导者
*/

import (
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/luomingguo/TinyKV/internal/labrpc"
	"github.com/luomingguo/TinyKV/internal/raftapi"

	//	"github.com/luomingguo/TinyKV/internal/labgob"
	tester "github.com/luomingguo/TinyKV/internal/tester1"
)

type role int

const (
	Follower role = iota
	Leader
	Candidate
)

func (r role) String() string {
	switch r {
	case Follower:
		return "Follower"
	case Leader:
		return "Leader"
	case Candidate:
		return "Candidate"
	default:
		return "Unknown"
	}
}

const (
	MinElectionTimeout = 300
	MaxElectionTimeout = 500
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm   int
	votedFor      int // 当前任期把票给谁了
	role          role
	lastHeartBeat time.Time
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term        int
	CandidateId int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool // 是否接受选举
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 比自己小的则丢弃
	if args.Term == rf.currentTerm {
		switch rf.role {
		case Candidate:
			reply.VoteGranted = false
		case Follower:
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
		case Leader:
			reply.VoteGranted = false
			rf.votedFor = -1
		default:
			// 不应该出现，记录日志
			rf.role = Follower // 容错: 退回 Follower
			DPrintf("args.Term is Invalid param, %d", args.Term)
		}
	} else if args.Term < rf.currentTerm {
		reply.VoteGranted = false
	} else {
		// 如果收到更大的 term，更新自己为 Follower
		rf.currentTerm = args.Term
		rf.role = Follower
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
	}
	reply.Term = rf.currentTerm
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here (3A)
		// Check if a leader election should be started.
		timeout := time.Duration(MinElectionTimeout+rand.Intn(MaxElectionTimeout-MinElectionTimeout)) * time.Millisecond
		rf.mu.Lock()
		if time.Since(rf.lastHeartBeat) >= timeout && rf.role != Leader {
			rf.doElection()
		} else {
			rf.mu.Unlock()
		}
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) doElection() {

	rf.currentTerm++
	rf.role = Candidate
	rf.lastHeartBeat = time.Now() // !!!! 重置选举计时器，不然会一直选举
	rf.votedFor = rf.me
	args := &RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
	}
	peerNum := len(rf.peers)
	currentTerm := rf.currentTerm
	rf.mu.Unlock() // rpc请求不要占用锁

	majority := peerNum/2 + 1
	voteChan := make(chan bool, peerNum)
	termChan := make(chan int, peerNum)

	voteGrantedCnt := 1 // 先给自己投一篇
	numRsp := 1         // 请求处理，加上自身
	// TODO 这里不支持成员管理了
	for idx := 0; idx < peerNum; idx++ {
		if idx == rf.me {
			continue
		}

		go func(server int) {
			reply := &RequestVoteReply{}
			ok := rf.sendRequestVote(idx, args, reply)
			if !ok {
				voteChan <- false
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Term < rf.currentTerm {
				// 忽略过期的响应
				voteChan <- false
				return
			} else if reply.Term > rf.currentTerm {
				termChan <- reply.Term
				return
			}
			if reply.VoteGranted {
				voteChan <- true
			} else {
				voteChan <- false
			}
		}(idx)
	}

	for voteGrantedCnt < majority && numRsp < peerNum {
		select {
		case vote := <-voteChan:
			numRsp++
			if !vote {
				continue
			}
			voteGrantedCnt++
			// 中断如果放在循环后面，需要等待所有节点回复或者失败，
			if voteGrantedCnt > len(rf.peers)/2 {
				rf.mu.Lock()
				// 再次检查状态，确保没有被其他goroutine改变
				if rf.role == Candidate && rf.currentTerm == currentTerm {
					rf.role = Leader
					rf.lastHeartBeat = time.Now()
					// TODO发送心跳
				}
				rf.mu.Unlock()
			}
		case higherTerm := <-termChan:
			numRsp++
			rf.mu.Lock()
			if higherTerm > rf.currentTerm {
				rf.currentTerm = higherTerm
				rf.role = Follower
				rf.votedFor = -1
			}
			rf.mu.Unlock()
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.currentTerm = 1
	rf.votedFor = -1
	rf.role = Follower
	rf.lastHeartBeat = time.Now()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
