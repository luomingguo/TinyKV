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
	// Raft use random election timeout, in general (150ms~300ms), but we use more bigger (300~500ms)
	MinElectionTimeout = 600
	MaxElectionTimeout = 900

	HeartBeatTimeout = 200
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh   chan raftapi.ApplyMsg
	// state a Raft server must maintain.
	currentTerm int // latest term server has seen(initialized to 0 on first boot, increases monotonically)
	votedFor    int // candidateId that received vote in current term(or -1 if none)
	// log entries; each entry contains command for state machine, and term when entry was by leader
	logs []Entry

	role          role
	lastHeartBeat time.Time

	// index of highest log entry applied to state machine (initialized to 0, increases monotonically)
	lastApplied int64
	// index of highest log entry known to be committed (initialized to 0, increases monotonically)
	commitIndex int64

	// leader 独有
	// for each server, index of the next log entry to send to that server(initialized to leader last log index + 1)
	nextIndex []int64
	// for each server, index of highest log entry known to be replicated on server (initialized to 0, increase monotonically)
	matchIndex []int64
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

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

// RequestVote RPC arguments structure.
type RequestVoteArgs struct {
	Term         int   // candidate’s term
	LastLogIndex int64 // index of candidate’s last log entry
	LastLogTerm  int   // term of candidate's last log entry
	CandidateId  int   // candidate requesting vote
}

// RequestVote RPC reply structure.
type RequestVoteReply struct {
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// 1. Reply false if the peer has a more up-to-date term & logIndex
	// 2. if voteFor is null or candidateId, and candidate's log is at least as up-to-date as receiver's log, grant vote

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// DPrintf("%d[Candidate]->%d[%s] RequestVote args = [Term: %d, LastLogIndex=%d, LastLogTerm=%d], term = %d, votefor=%d, commitIndex = %d, lastApplied = %d", args.CandidateId, rf.me, rf.role.String(), args.Term, args.LastLogIndex, args.LastLogTerm, rf.currentTerm, rf.votedFor, rf.commitIndex, rf.lastApplied)

	lastEntry := rf.logs[len(rf.logs)-1]
	if args.LastLogTerm < lastEntry.Term {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		// DPrintf("%d[Candidate]<-%d[%s] RequestVote reply = [Term: %d,  Successful: %v], term = %d, votefor=%d", args.CandidateId, rf.me, rf.role.String(), reply.Term, reply.VoteGranted, rf.currentTerm, rf.votedFor)
		return
	} else if args.LastLogTerm > lastEntry.Term {
		// 如果收到更大的 term，更新自己为 Follower
		goto receiveCandidate
	}

	if args.LastLogIndex < int64(len(rf.logs)-1) {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		// DPrintf("%d[Candidate]<-%d[%s] RequestVote reply = [Term: %d,  Successful: %v], term = %d, votefor=%d", args.CandidateId, rf.me, rf.role.String(), reply.Term, reply.VoteGranted, rf.currentTerm, rf.votedFor)
		return
	} else if args.LastLogIndex > int64(len(rf.logs)-1) {
		goto receiveCandidate
	}
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm

	} else if args.Term > rf.currentTerm {
		goto receiveCandidate
	}
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		goto receiveCandidate
	} else {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		// DPrintf("%d[Candidate]<-%d[%s] RequestVote reply = [Term: %d,  Successful: %v], term = %d, votefor=%d", args.CandidateId, rf.me, rf.role.String(), reply.Term, reply.VoteGranted, rf.currentTerm, rf.votedFor)
		return
	}

receiveCandidate:
	rf.currentTerm = args.Term
	rf.role = Follower
	reply.VoteGranted = true
	rf.votedFor = args.CandidateId
	reply.Term = rf.currentTerm
	// DPrintf("%d[Candidate]<-%d[%s] RequestVote reply = [Term: %d,  Successful: %v], term = %d, votefor=%d", args.CandidateId, rf.me, rf.role.String(), reply.Term, reply.VoteGranted, rf.currentTerm, rf.votedFor)
}

type Entry struct {
	Command any
	Term    int // first index is 1
}

// AppendEntriesArgs RPC arguments structure.
type AppendEntriesArgs struct {
	Term         int     // leader's term
	LeaderId     int     // for redirect to Leader for clients
	PrevLogIndex int64   // index of log entry immediately preceding new ones
	PrevLogTerm  int     // term of PrevLogIndex entry
	Entries      []Entry // log entries to store (empty for heartbeat)
	LeaderCommit int64   // Leader's commit index
}

// AppendEntriesReply
type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// 1. Reply false if term < currentTerm
	// 2. Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
	// 3. If an existing entry conflicts with new one (same index but different terms), delete the existing entry
	// 		and all that follow it
	// 4. Append any new entries not already in the log
	// 5. if leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)

	// for follower: If last log index ≥ nextIndex, then send AppendEntries RPC with log entries starting at nextIndex
	//	 if successful: update nextIndex and matchIndex for follower
	//	 if AppendEntries fails bcs of log inconsistency: decrement nextIndex and retry
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false

	// if len(args.Entries) == 0 {
	// 	DPrintf("%d[%s]->%d[%s] HeartBeat args = [Term: %d, LeaderCommit=%d ], term = %d commitIndex = %d, lastApplied = %d", args.LeaderId, Leader.String(), rf.me, rf.role.String(), args.Term, args.LeaderCommit, rf.currentTerm, rf.commitIndex, rf.lastApplied)
	// } else {
	// 	DPrintf("%d[%s]->%d[%s] AppendEntries args = [Term: %d, PrevlogIndex: %d,  PrevLogTerm: %d, len(Entry)=%d, leaderCommit=%d], term = %d commitIndex = %d, lastApplied = %d", args.LeaderId, Leader.String(), rf.me, rf.role.String(), args.Term, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries), args.LeaderCommit, rf.currentTerm, rf.commitIndex, rf.lastApplied)
	// }

	// 1. Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		// DPrintf("%d[%s]<-%d[%s] AppendEntries reply = [Term: %d, Success=%v ], term = %d commitIndex = %d, lastApplied = %d", args.LeaderId, Leader.String(), rf.me, rf.role.String(), reply.Term, reply.Success, rf.currentTerm, rf.commitIndex, rf.lastApplied)
		return
	}

	// 接收到更高或相等的term，转为follower
	if args.Term >= rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = Follower
		rf.votedFor = -1
		rf.lastHeartBeat = time.Now()
		reply.Term = rf.currentTerm
	}

	// 2. Reply false if log doesn't contain an entry at prevLogIndex
	if args.PrevLogIndex >= int64(len(rf.logs)) {
		// DPrintf("%d[%s]<-%d[%s] AppendEntries reply = [Term: %d, Success=%v ] - prevLogIndex too high", args.LeaderId, Leader.String(), rf.me, rf.role.String(), reply.Term, reply.Success)
		return
	}

	// 3. Reply false if log entry at prevLogIndex has different term
	if args.PrevLogIndex >= 0 && rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		// DPrintf("%d[%s]<-%d[%s] AppendEntries reply = [Term: %d, Success=%v ] - term mismatch at prevLogIndex", args.LeaderId, Leader.String(), rf.me, rf.role.String(), reply.Term, reply.Success)
		return
	}

	// 4. 如果存在冲突的日志条目，删除它及其后续所有条目
	insertIndex := args.PrevLogIndex + 1
	for i := 0; i < len(args.Entries); i++ {
		entryIndex := insertIndex + int64(i)
		if entryIndex < int64(len(rf.logs)) {
			if rf.logs[entryIndex].Term != args.Entries[i].Term {
				// 发现冲突，删除此位置及后续所有日志
				rf.logs = rf.logs[:entryIndex]
				break
			}
		} else {
			break
		}
	}

	// 5. 追加新的日志条目
	for i := 0; i < len(args.Entries); i++ {
		entryIndex := insertIndex + int64(i)
		if entryIndex >= int64(len(rf.logs)) {
			rf.logs = append(rf.logs, args.Entries[i])
		}
	}

	reply.Success = true

	// 6. 更新commitIndex
	if args.LeaderCommit > rf.commitIndex {
		newCommitIndex := min(args.LeaderCommit, int64(len(rf.logs)-1))
		if newCommitIndex > rf.commitIndex {
			rf.commitIndex = newCommitIndex
			rf.applyCommittedWithGuardLock()
		}
	}
	// if len(args.Entries) == 0 {
	// 	DPrintf("%d[%s]<-%d[%s] HeartBeat reply = [Term: %d, Success=%v ], term = %d commitIndex = %d, lastApplied = %d", args.LeaderId, Leader.String(), rf.me, rf.role.String(), reply.Term, reply.Success, rf.currentTerm, rf.commitIndex, rf.lastApplied)

	// } else {
	// 	DPrintf("%d[%s]<-%d[%s] AppendEntries reply = [Term: %d, Success=%v ], term = %d commitIndex = %d, lastApplied = %d", args.LeaderId, Leader.String(), rf.me, rf.role.String(), reply.Term, reply.Success, rf.currentTerm, rf.commitIndex, rf.lastApplied)
	// }
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
	// if command received from client: append entry to local log, respond after entry applied to state machine
	// if there exist an N such that N > commitIndex, a major of matchIndex[i] >= N, and log[N].term == currentTerm:
	//   then set commitIndex = N
	rf.mu.Lock()
	defer rf.mu.Unlock()
	newIndex := len(rf.logs) // for new command
	if rf.role != Leader {
		return newIndex, rf.currentTerm, false // 最近提交
	}
	currentTerm := rf.currentTerm
	rf.logs = append(rf.logs, Entry{Command: command, Term: rf.currentTerm})

	// 异步启动日志复制过程
	go rf.replicateLogEntry(newIndex, currentTerm)
	return newIndex, rf.currentTerm, true
}

// 用于收集复制结果
type replicationResult struct {
	serverId int
	success  bool
	term     int
}

// replicate to all other server
func (rf *Raft) replicateLogEntry(logIndex int, term int) {
	numPeers := len(rf.peers)
	majority := numPeers/2 + 1
	// NOTE 如果不能保证每个对端最多回复一个，有死锁风险
	replicationResultChan := make(chan replicationResult, numPeers)

	for id := 0; id < numPeers; id++ {
		if id == rf.me {
			continue
		}
		go rf.replicateToServer(id, logIndex, term, replicationResultChan)
	}
	successCount := 1 // 包括leader自己
	responseCount := 1
	newIdxCommited := false
	for responseCount < numPeers {

		result := <-replicationResultChan
		responseCount++
		if result.term > term { // 粗略确认
			rf.mu.Lock()
			if result.term > rf.currentTerm { // 取最大的
				rf.currentTerm = result.term
				rf.role = Follower
				rf.votedFor = -1
			}
			rf.mu.Unlock()
		}
		if result.success {
			successCount++
			rf.mu.Lock()
			rf.matchIndex[result.serverId] = int64(logIndex)
			rf.nextIndex[result.serverId] = int64(logIndex + 1)
			rf.mu.Unlock()
			if !newIdxCommited && successCount >= majority {
				rf.commitLogEntry(logIndex) // 必须统一以传入term作为日志的term
				newIdxCommited = true
			}
		}
	}
}

func (rf *Raft) replicateToServer(serverId int, newIndex int, term int, resultChan chan<- replicationResult) {
	for {
		rf.mu.Lock()
		if rf.role != Leader || rf.currentTerm != term {
			rf.mu.Unlock()
			return
		}

		nextIdx := rf.nextIndex[serverId]
		if nextIdx == 0 {
			nextIdx = 1
		}

		prevLogIndex := nextIdx - 1
		var entries []Entry

		// 如果需要发送的日志条目超过了新添加的条目，只发送到新条目为止
		if int(nextIdx) <= newIndex {
			endIdx := newIndex + 1
			if endIdx > len(rf.logs) {
				endIdx = len(rf.logs)
			}
			entries = rf.logs[nextIdx:endIdx]
		} else {
			// 已经同步完成
			rf.mu.Unlock()
			resultChan <- replicationResult{
				serverId: serverId,
				success:  true,
				term:     term,
			}
			return
		}

		args := &AppendEntriesArgs{
			Term:         term,
			LeaderId:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  0,
			Entries:      entries,
			LeaderCommit: rf.commitIndex,
		}

		if prevLogIndex >= 0 && prevLogIndex < int64(len(rf.logs)) {
			args.PrevLogTerm = rf.logs[prevLogIndex].Term
		}

		rf.mu.Unlock()

		reply := &AppendEntriesReply{}
		ok := rf.sendAppendEntries(serverId, args, reply)

		if !ok {
			time.Sleep(50 * time.Millisecond)
			continue
		}

		rf.mu.Lock()
		// 检查term是否过期
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.role = Follower
			rf.votedFor = -1
			rf.mu.Unlock()
			resultChan <- replicationResult{
				serverId: serverId,
				success:  false,
				term:     reply.Term,
			}
			return
		}

		if reply.Success {
			// 成功，更新nextIndex和matchIndex
			newNextIndex := prevLogIndex + 1 + int64(len(entries))
			rf.nextIndex[serverId] = newNextIndex
			rf.matchIndex[serverId] = newNextIndex - 1

			// 检查是否完成了目标日志的复制
			if rf.matchIndex[serverId] >= int64(newIndex) {
				rf.mu.Unlock()
				resultChan <- replicationResult{
					serverId: serverId,
					success:  true,
					term:     term,
				}
				return
			}
			rf.mu.Unlock()
			// 继续发送剩余的日志条目
			continue
		} else {
			// 失败，回退nextIndex
			if rf.nextIndex[serverId] > 1 {
				rf.nextIndex[serverId]--
			}
			rf.mu.Unlock()
			// 继续尝试
			time.Sleep(50 * time.Millisecond)
			continue
		}
	}
}

// commitLogEntry 提交指定的日志条目
func (rf *Raft) commitLogEntry(logIndex int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 更新commitIndex
	if int64(logIndex) > rf.commitIndex {
		rf.commitIndex = int64(logIndex)
		rf.applyCommittedWithGuardLock()
	}
}

// 在newIndex之前的日志都可以apply
func (rf *Raft) applyCommittedWithGuardLock() {

	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		entry := rf.logs[rf.lastApplied]
		applyMsg := raftapi.ApplyMsg{
			CommandValid: true,
			Command:      entry.Command,
			CommandIndex: int(rf.lastApplied),
		}
		rf.mu.Unlock()
		rf.applyCh <- applyMsg // 避免阻塞死锁
		rf.mu.Lock()
	}
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

		// Check if a leader election should be started.
		electMs := MinElectionTimeout + (rand.Int63() % (MaxElectionTimeout - MinElectionTimeout))
		timeout := time.Duration(electMs) * time.Millisecond
		rf.mu.Lock()
		if time.Since(rf.lastHeartBeat) >= timeout && rf.role != Leader {
			rf.startElectionLocked()
		} else {
			rf.mu.Unlock()
		}
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// startElectionLocked it has lock locked at the beginning
func (rf *Raft) startElectionLocked() {
	/*
		On conversion to Candidate, start election:
		- increment currentTerm
		- Vote for itself
		- Reset election timer
		- Send RequestVote RPC to all other servers
		If vote received from majority: become leader
		If AppendEntries RPC received from new Leader: covert to folloer
		If election timeout elapses: start new election
	*/
	rf.currentTerm++
	rf.role = Candidate
	rf.lastHeartBeat = time.Now()
	rf.votedFor = rf.me
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogTerm:  rf.logs[len(rf.logs)-1].Term,
		LastLogIndex: int64(len(rf.logs) - 1),
	}
	currentTerm := rf.currentTerm
	rf.mu.Unlock()

	peerNum := len(rf.peers)
	majority := peerNum/2 + 1
	voteChan := make(chan bool, peerNum-1)
	termChan := make(chan int, peerNum-1)

	voteGrantedCnt := 1 // 先给自己投一篇
	numRsp := 1         // 请求处理，加上自身
	// TODO not yet support member managemant
	for idx := 0; idx < peerNum; idx++ {
		if idx == rf.me {
			continue
		}
		go func(server, term int) {
			reply := &RequestVoteReply{}
			ok := rf.sendRequestVote(server, args, reply)
			if !ok {
				voteChan <- false
				return
			}
			if reply.VoteGranted {
				voteChan <- true
				return
			}
			voteChan <- false
			if reply.Term > term {
				termChan <- reply.Term
			}
		}(idx, currentTerm)
	}

	for voteGrantedCnt < majority && numRsp < peerNum {

		select {
		case granted := <-voteChan:
			numRsp++
			if granted {
				voteGrantedCnt++
				if voteGrantedCnt >= majority {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					// 再次检查状态，确保没有被其他goroutine改变
					if rf.role == Candidate && rf.currentTerm == currentTerm {
						rf.startLeaderWithLockGuard()
					}
					return
				}
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
			return
		}
	}
}

// 在成为Leader时启动复制循环
func (rf *Raft) startLeaderWithLockGuard() {
	rf.role = Leader
	// 初始化nextIndex和matchIndex
	for i := range rf.peers {
		rf.nextIndex[i] = int64(len(rf.logs))
		rf.matchIndex[i] = 0
	}
	rf.matchIndex[rf.me] = int64(len(rf.logs) - 1)
	term := rf.currentTerm
	// DPrintf("%d becomes leader at term %d", rf.me, term)
	// 启动复制循环
	// go rf.startReplicationLoop(term)
	// 启动心跳循环
	go rf.startHeartBeatLoop(term)
}

func (rf *Raft) startHeartBeatLoop(term int) {

	for {
		rf.mu.Lock()
		if term != rf.currentTerm || rf.role != Leader {
			rf.mu.Unlock()
			return
		}
		rf.lastHeartBeat = time.Now()
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: int64(len(rf.logs) - 1),
			PrevLogTerm:  rf.logs[len(rf.logs)-1].Term,
			Entries:      make([]Entry, 0),
			LeaderCommit: rf.commitIndex,
		}
		rf.mu.Unlock()
		for idx := 0; idx < len(rf.peers); idx++ {
			if idx == rf.me {
				continue
			}
			go func(server int) {
				reply := &AppendEntriesReply{}
				ok := rf.sendAppendEntries(server, args, reply)
				if ok && reply.Term > args.Term {
					rf.mu.Lock()
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.role = Follower
						rf.votedFor = -1
					}
					rf.mu.Unlock()
				}
			}(idx)
		}
		time.Sleep(time.Duration(HeartBeatTimeout) * time.Millisecond)
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
	rf.applyCh = applyCh
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.role = Follower
	rf.lastHeartBeat = time.Now()

	rf.lastApplied = 0
	rf.commitIndex = 0
	rf.logs = []Entry{
		{
			Command: nil,
			Term:    0,
		},
	} // dummy log entry, so it is 1st-index
	rf.nextIndex = make([]int64, len(peers))
	rf.matchIndex = make([]int64, len(peers))
	for i := range rf.peers {
		rf.nextIndex[i] = 1
		rf.matchIndex[i] = 0
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
