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
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/luomingguo/TinyKV/internal/labgob"
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
	MinElectionTimeout = 700
	MaxElectionTimeout = 1500

	HeartbeatInterval = 200 * time.Millisecond
	// 单次同步最大日志复制
	MaxLogsAppendCnt = 10
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh   chan raftapi.ApplyMsg

	// Volatile State on all servers
	lastApplied   int64      // index of highest log entry applied to state machine (initialized to 0, increases monotonically)
	commitIndex   int64      // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	applyCond     *sync.Cond // commitIndex 推进时唤醒 applier，串行地把日志投递到 applyCh
	electionTimer *time.Timer
	lastHeartbeat time.Time
	role          role

	// Volatile State on leaders
	nextIndex  []int64 // for each server, index of the next log entry to send to that server(initialized to leader last log index + 1)
	matchIndex []int64 // for each server, index of highest log entry known to be replicated on server (initialized to 0, increase monotonically)

	// Non-volatile state on all servers:
	currentTerm int     // latest term server has seen(initialized to 0 on first boot, increases monotonically)
	votedFor    int     // candidateId that received vote in current term(or -1 if none)
	logs        []Entry // log entries; each entry contains command for state machine, and term when entry was by leader
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var voteFor int
	var logs []Entry
	// var numLog int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&voteFor) != nil ||
		d.Decode(&logs) != nil {
		return
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = voteFor
		rf.logs = logs
	}
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

	// Optimization that backs up nextIndex by more than one entry at a time.
	// 失败时，follower 返回冲突信息:
	// 		- XTerm:  冲突条目的 term。若 follower 日志太短，
	// 		          PrevLogIndex 处没有对应条目，则为 -1 (NoXTerm)。
	// 		- XIndex: XTerm 第一次出现的 index (仅冲突时有效)。
	// 		- XLen:   follower 的日志长度 (含 dummy)。XTerm == -1 时使用。
	//  Case 0: 自己 term 更新 -> leader 降级为 follower 并退出。
	//  Case 1: leader 没有 XTerm     -> nextIndex = XIndex
	//	Case 2: leader 拥有 XTerm     -> nextIndex = (leader 侧 XTerm 最后一条 index) + 1
	//	Case 3: follower 日志太短(-1) -> nextIndex = XLen
	XTerm  int
	XIndex int64
	XLen   int64
}

// NoXTerm 是 AppendEntriesReply.XTerm 的哨兵值，表示 follower 在
// PrevLogIndex 处没有对应条目 (日志太短)。
// 真实 term 都 >= 0 (dummy 为 0)，故负值绝不会与冲突 term 相撞。
const NoXTerm = -1

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
	rf.logs = append(rf.logs, Entry{Command: command, Term: rf.currentTerm})
	rf.persist()
	go rf.sendHeartbeats() // 立即触发复制，不必等下一次心跳
	return newIndex, rf.currentTerm, true
}

// applier 是唯一向 applyCh 投递日志的 goroutine。
// 调用方推进 commitIndex 后用 applyCond.Signal() 唤醒它，
// 这样既保证 apply 顺序严格递增，又避免持锁发送 channel 造成阻塞或重入死锁。
func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for !rf.killed() {
		if rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			entry := rf.logs[rf.lastApplied]
			applyMsg := raftapi.ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: int(rf.lastApplied),
			}
			rf.mu.Unlock()
			rf.applyCh <- applyMsg // 发送时不持锁
			rf.mu.Lock()
		} else {
			rf.applyCond.Wait()
		}
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
	rf.applyCond.Broadcast() // 唤醒可能阻塞在 Wait 的 applier，使其检查 killed 退出
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Check if a leader election should be started.
		select {
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			if rf.role != Leader {
				go rf.startElection()
			}
			rf.resetElectionTimer()
			rf.mu.Unlock()
		default:
		}
		rf.mu.Lock()
		if rf.role == Leader && time.Since(rf.lastHeartbeat) >= HeartbeatInterval {
			go rf.sendHeartbeats()
			rf.lastHeartbeat = time.Now()
		}
		rf.mu.Unlock()
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// 1. Reply false if the peer has a more up-to-date term & logIndex
	// 2. if voteFor is null or candidateId, and candidate's log is at least as up-to-date as receiver's log, grant vote
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist() // 任何路径若改了 currentTerm/votedFor 都会被持久化
	DPrintf("%d[Candidate]->%d[%s] RequestVote args = [Term: %d, LastLogIndex=%d, LastLogTerm=%d], term = %d, votefor=undecided, commitIndex = %d, lastApplied = %d", args.CandidateId, rf.me, rf.role.String(), args.Term, args.LastLogIndex, args.LastLogTerm, rf.currentTerm, rf.commitIndex, rf.lastApplied)
	// DPrintf("%d[Candidate]->%d[%s] RequestVote args = [Term: %d, LastLogIndex=%d, LastLogTerm=%d], term = %d, votefor=%d, commitIndex = %d, lastApplied = %d", args.CandidateId, rf.me, rf.role.String(), args.Term, args.LastLogIndex, args.LastLogTerm, rf.currentTerm, rf.votedFor, rf.commitIndex, rf.lastApplied)

	// 1. 对方任期比自己小，拒绝
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		DPrintf("%d[Candidate]<-%d[%s] RequestVote reply = [Term: %d,  Successful: %v], term = %d, votefor=%d", args.CandidateId, rf.me, rf.role.String(), reply.Term, reply.VoteGranted, rf.currentTerm, rf.votedFor)
		return
	}

	// 2. 对方任期更大，无条件更新自己任期，承认leader
	// Note: 任期更新和是否投票是独立判断的
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.role = Follower
	}
	reply.Term = rf.currentTerm
	// 3. 检查是否本任期是否投过票了
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		reply.VoteGranted = false
		DPrintf("%d[Candidate]<-%d[%s] RequestVote reply = [Term: %d,  Successful: %v], term = %d, votefor=%d", args.CandidateId, rf.me, rf.role.String(), reply.Term, reply.VoteGranted, rf.currentTerm, rf.votedFor)
		return
	}
	// 4. 选举限制： candidate 的日志至少和自己的一样新
	lastIndex := len(rf.logs) - 1

	candidateUpToDate := args.LastLogTerm > rf.logs[lastIndex].Term ||
		(args.LastLogTerm == rf.logs[lastIndex].Term && args.LastLogIndex >= int64(lastIndex))
	if !candidateUpToDate {
		reply.VoteGranted = false

		DPrintf("%d[Candidate]<-%d[%s] RequestVote reply = [Term: %d,  Successful: %v], term = %d, votefor=%d", args.CandidateId, rf.me, rf.role.String(), reply.Term, reply.VoteGranted, rf.currentTerm, rf.votedFor)
		return
	}
	rf.votedFor = args.CandidateId
	rf.role = Follower
	rf.resetElectionTimer()
	reply.VoteGranted = true
	DPrintf("%d[Candidate]<-%d[%s] RequestVote reply = [Term: %d,  Successful: %v], term = %d, votefor=%d", args.CandidateId, rf.me, rf.role.String(), reply.Term, reply.VoteGranted, rf.currentTerm, rf.votedFor)
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

	if len(args.Entries) == 0 {
		DPrintf("%d[%s]->%d[%s] HeartBeat args = [Term: %d, PrevlogIndex: %d,  PrevLogTerm: %d, leaderCommit=%d], term = %d commitIndex = %d, lastApplied = %d", args.LeaderId, Leader.String(), rf.me, rf.role.String(), args.Term, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, rf.currentTerm, rf.commitIndex, rf.lastApplied)
	} else {
		DPrintf("%d[%s]->%d[%s] AppendEntries args = [Term: %d, PrevlogIndex: %d,  PrevLogTerm: %d, len(Entry)=%d, leaderCommit=%d], term = %d commitIndex = %d, lastApplied = %d", args.LeaderId, Leader.String(), rf.me, rf.role.String(), args.Term, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries), args.LeaderCommit, rf.currentTerm, rf.commitIndex, rf.lastApplied)
	}

	// 1. 任期比自己小，拒绝
	if args.Term < rf.currentTerm {
		DPrintf("%d[%s]<-%d[%s] AppendEntries reply = [Term: %d, Success=%v], term = %d commitIndex = %d, lastApplied = %d", args.LeaderId, Leader.String(), rf.me, rf.role.String(), reply.Term, reply.Success, rf.currentTerm, rf.commitIndex, rf.lastApplied)
		// NOTE: 这里不处理Xterm等是因为，laeder收到后会退回到follow，保存没有意义
		return
	}
	rf.lastHeartbeat = time.Now()
	// 2. 任期比自己大，承认 leader， 重置计时器
	if args.Term > rf.currentTerm {
		rf.votedFor = -1
		rf.currentTerm = args.Term
		rf.persist()
	}
	rf.role = Follower
	rf.resetElectionTimer()

	// 3. 一致性检查： prevLogIndex 处的条目 term 应该匹配
	// Case 3:
	if args.PrevLogIndex >= int64(len(rf.logs)) {
		reply.XTerm = NoXTerm
		reply.XLen = int64(len(rf.logs)) // including dummy log
		DPrintf("%d[%s]<-%d[%s] AppendEntries reply = [Term: %d, Success=%v, XTerm = %d, XIndex = %d, Xlen = %d] - prevLogIndex too high", args.LeaderId, Leader.String(), rf.me, rf.role.String(), reply.Term, reply.Success, reply.XTerm, reply.XIndex, reply.XLen)
		return
	}

	// Case 1/2: PrevLogIndex
	if args.PrevLogIndex >= 0 && rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.XTerm = rf.logs[args.PrevLogIndex].Term
		reply.XIndex = args.PrevLogIndex
		for i := args.PrevLogIndex; i > 1; i-- {
			if rf.logs[i].Term != reply.XTerm {
				reply.XIndex = int64(i + 1)
				break
			}
		}
		DPrintf("%d[%s]<-%d[%s] AppendEntries reply = [Term: %d, Success=%v, XTerm = %d, XIndex = %d, Xlen = %d] - term mismatch at prevLogIndex", args.LeaderId, Leader.String(), rf.me, rf.role.String(), reply.Term, reply.Success, reply.XTerm, reply.XIndex, reply.XLen)
		return
	}
	// 3. If an existing entry conflicts with new one (same index but different terms), delete the existing entry
	// 		and all that follow it
	// 4. Append any new entries not already in the log
	reply.Success = true
	insertIndex := args.PrevLogIndex + 1
	for i := 0; i < len(args.Entries); i++ {
		entryIndex := insertIndex + int64(i)

		// follower 日志已经没有了，直接追加后续 entries
		if entryIndex >= int64(len(rf.logs)) {
			rf.logs = append(rf.logs, args.Entries[i:]...)
			break
		}

		if rf.logs[entryIndex].Term != args.Entries[i].Term {
			rf.logs = append(rf.logs[:entryIndex], args.Entries[i:]...)
			break
		}
	}
	rf.persist()

	// 5. if leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		newCommitIndex := min(args.LeaderCommit, int64(len(rf.logs)-1))
		if newCommitIndex > rf.commitIndex {
			rf.commitIndex = newCommitIndex
			rf.applyCond.Signal()
		}
	}

	if len(args.Entries) == 0 {
		DPrintf("%d[%s]<-%d[%s] HeartBeat reply = [Term: %d, Success=%v ], term = %d commitIndex = %d, lastApplied = %d", args.LeaderId, Leader.String(), rf.me, rf.role.String(), reply.Term, reply.Success, rf.currentTerm, rf.commitIndex, rf.lastApplied)
	} else {
		DPrintf("%d[%s]<-%d[%s] AppendEntries reply = [Term: %d, Success=%v ], term = %d commitIndex = %d, lastApplied = %d", args.LeaderId, Leader.String(), rf.me, rf.role.String(), reply.Term, reply.Success, rf.currentTerm, rf.commitIndex, rf.lastApplied)
	}
}

// startElectioLocked it has lock locked at the beginning
func (rf *Raft) startElection() {
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

	rf.mu.Lock()
	rf.currentTerm++
	term := rf.currentTerm
	rf.votedFor = rf.me
	lastLogTerm := rf.logs[len(rf.logs)-1].Term
	lastLogIndex := int64(len(rf.logs) - 1)
	rf.role = Candidate
	votes := 1   // 给自己一票
	rf.persist() // 持久化自增后的 term 和给自己的票
	rf.mu.Unlock()

	majority := len(rf.peers)/2 + 1

	var once sync.Once

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(peer int) {
			reply := &RequestVoteReply{}
			args := &RequestVoteArgs{
				Term:         term,
				CandidateId:  rf.me,
				LastLogTerm:  lastLogTerm,
				LastLogIndex: lastLogIndex,
			}
			if !rf.sendRequestVote(peer, args, reply) {
				return
			}
			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.role = Follower
				rf.persist()
				rf.mu.Unlock()
				return
			}
			if reply.VoteGranted && rf.role == Candidate && rf.currentTerm == term {
				votes++
				shouldBeLeader := votes >= majority // 锁内计算，存入局部变量
				rf.mu.Unlock()
				if shouldBeLeader {
					once.Do(func() {
						rf.becomeLeader(term)
					})
				}
			} else {
				rf.mu.Unlock()
			}
		}(i)
	}
}

func (rf *Raft) tryUpdateCommitIndex() {
	// 如果存在N > commitIndex,使得:
	// 1. matchIndex[i] >= N的服务器数量过半
	// 2. log[N].term == currentTerm
	// 则设置commitIndex = N

	for N := int64(len(rf.logs) - 1); N > rf.commitIndex; N-- {
		if rf.logs[N].Term != rf.currentTerm {
			continue
		}

		count := 1 // leader自己
		for i := range rf.peers {
			if i != rf.me && rf.matchIndex[i] >= N {
				count++
			}
		}

		if count > len(rf.peers)/2 {
			rf.commitIndex = N
			rf.applyCond.Signal()
			break
		}
	}
}

// 在成为Leader时的必要动作
func (rf *Raft) becomeLeader(term int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 防止过期的选举结果让我们错误地成为leader
	if rf.role != Candidate || rf.currentTerm != term {
		DPrintf("%d[%s] becomes leader failed at term %d, rf.currentTerm = %d", rf.me, rf.role.String(), term, rf.currentTerm)
		return
	}
	rf.role = Leader
	// 初始化nextIndex和matchIndex
	for i := range rf.peers {
		rf.nextIndex[i] = int64(len(rf.logs))
		rf.matchIndex[i] = 0
	}
	rf.matchIndex[rf.me] = int64(len(rf.logs) - 1)

	DPrintf("%d becomes leader at term %d", rf.me, term)
	// 启动心跳循环
	go rf.sendHeartbeats()
}

// heartbeatAndReplicationLoop, call by leader
func (rf *Raft) sendHeartbeats() {
	// 并发地向所有follower发送AppendEntries，不等待完成
	for i := range rf.peers {

		if i == rf.me {
			continue
		}

		go func(peer int) {
			rf.mu.Lock()
			if rf.role != Leader { // 触发源可能已不是 leader，避免乱发 RPC 干扰收敛
				rf.mu.Unlock()
				return
			}
			prevLogIndex := rf.nextIndex[peer] - 1
			args := &AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  rf.logs[prevLogIndex].Term,
				Entries:      rf.logs[prevLogIndex+1:],
				LeaderCommit: rf.commitIndex,
			}
			entryCnt := len(rf.logs[prevLogIndex+1:])
			rf.mu.Unlock()

			reply := &AppendEntriesReply{}
			if !rf.sendAppendEntries(peer, args, reply) {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.role = Follower
				rf.persist()
				rf.resetElectionTimer()
				return
			}
			// term 已经过期，忽略
			if rf.role != Leader {
				return
			}
			if reply.Success {
				rf.matchIndex[peer] = max(rf.matchIndex[peer], args.PrevLogIndex+int64(entryCnt))
				rf.nextIndex[peer] = rf.matchIndex[peer] + 1
				// 修复2: 尝试更新commitIndex
				if entryCnt != 0 {
					rf.tryUpdateCommitIndex()
				}
				return
			}
			// Case 3: follower 的日志太短，PrevLogIndex 处没有条目
			if reply.XTerm == NoXTerm {
				rf.nextIndex[peer] = reply.XLen
				return
			}
			// Case 1/2: term 冲突，查找 leader 自己日志中 XTerm 的最后一条
			lastIndexOfXTerm := 0
			for i := len(rf.logs) - 1; i >= 1; i-- {
				if rf.logs[i].Term == reply.XTerm {
					lastIndexOfXTerm = i
					break
				}
			}
			if lastIndexOfXTerm > 0 {
				// Case 2: leader 拥有 XTerm
				rf.nextIndex[peer] = int64(lastIndexOfXTerm + 1)
			} else {
				// Case 1: leader 没有 XTerm
				rf.nextIndex[peer] = reply.XIndex
			}
		}(i)
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
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg,
) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.role = Follower
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.electionTimer = time.NewTimer(randomElectionTimeout())
	rf.lastHeartbeat = time.Now()

	rf.lastApplied = 0
	rf.commitIndex = 0
	// dummy log entry, so it is 1st-index
	rf.logs = []Entry{
		{
			Command: nil,
			Term:    0,
		},
	}
	rf.nextIndex = make([]int64, len(peers))
	rf.matchIndex = make([]int64, len(peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	// start applier goroutine to deliver committed entries to applyCh
	go rf.applier()

	return rf
}

// 每次需要重置计时器（收到心跳、开始新选举）时：
func (rf *Raft) resetElectionTimer() {
	if !rf.electionTimer.Stop() {
		select {
		case <-rf.electionTimer.C:
		default:
		}
	}
	rf.electionTimer.Reset(randomElectionTimeout())
}

func randomElectionTimeout() time.Duration {
	return time.Duration(MinElectionTimeout+rand.Intn(MaxElectionTimeout-MinElectionTimeout)) * time.Millisecond // 700 ~ 1400
}
