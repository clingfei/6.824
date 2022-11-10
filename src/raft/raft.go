package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"fmt"
	"math/rand"
	"time"

	//	"bytes"
	"sync"
	"sync/atomic"

	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type State int

const (
	Leader    State = 0
	Follower  State = 1
	Candidate State = 2
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state       State
	currentTerm int
	votedFor    int // CandidateId that received vote in current Term
	// log entries, each entry contains command for state machine, and term when entry was received by leader(first index is 1)
	log []LogEntry
	// index of the highest log entry known to be committed, initialized to 0, increases monotonically
	commitIndex int
	// index of the highest log entry applied
	lastApplied int
	// for each server, index of the next log entry to send to that server(initialized to leader last log index+1
	nextIndex []int
	// for each server, index of highest log entry known to be replicated on server(initialized to 0, increases monotonically
	matchIndex []int

	lastIncludedTerm  int
	lastIncludedIndex int

	// 在server收到heartbeat时将isTimeout置1，在每次睡眠前将timeout置0，睡醒后检查isTimeout,如果是0，则超时，需要重新选举，否则不需要，继续睡眠
	// 这样可以降低系统的复杂度
	isTimeout bool

	applyCh chan ApplyMsg
}

// each entry contains command for state machine, and Term when entry was received by leader, first index is 1
type LogEntry struct {
	// Log中应该有的内容：Term command index
	Term    int
	Index   int
	Command interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var Term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	Term = rf.currentTerm
	isleader = rf.state == Leader
	return Term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm int
	var votedFor int
	var log []LogEntry
	var lastIncludedIndex int
	var lastIncludedTerm int
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil ||
		d.Decode(&lastIncludedIndex) != nil || d.Decode(&lastIncludedTerm) != nil {
		return
	} else {
		rf.mu.Lock()
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
		rf.mu.Unlock()
	}

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

func DEBUG(format string, a ...interface{}) {
	fmt.Printf(format, a...)
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

type AppendEntriesArgs struct {
	// leader's Term
	Term int
	// follower can redirect clients
	LeaderId int
	// index of log entry immediately preceding new ones
	PrevLogIndex int
	// Term of prevLogIndex entry
	PrevLogTerm int
	// log entries to store(empty for heartbeat, may send more than one for efficiency)
	Entries []LogEntry
	// leader's commitIndex
	LeaderCommit int
}

type AppendEntriesReply struct {
	// currentTerm, for leader to update itself
	Term int
	// true if follower contained entry matching prevLogIndex and prevLogTerm
	Success bool
}

//AppendEntries由leader发起，用于在follower上复制日志记录和heartbeat

// resets the election timeout so that other servers don't step forward as leaders when one has already be elected。
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// 如果日志中存在prevLogIndex和prevLogTerm都相等的日志记录，那么success被设置为true
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		fmt.Printf("%d receive outdated heartbeat from %d, Term: %d, currentTerm: %d\n",
			rf.me, args.LeaderId, args.Term, rf.currentTerm)
		reply.Term, reply.Success = rf.currentTerm, false
		return
	}
	if args.Term >= rf.currentTerm {
		//fmt.Printf("%d reset isTimeout\n", rf.me)
		rf.isTimeout = false
	}
	fmt.Printf("%d receive from %d\n", rf.me, args.LeaderId)
	fmt.Printf("PrevLogIndex: %d, PrevLogTerm: %d, Term: %d, LeaderCommit: %d, length: %d, lastIncludedIndex: %d\n",
		args.PrevLogIndex, args.PrevLogTerm, args.Term, args.LeaderCommit, len(args.Entries), rf.lastIncludedIndex)
	if rf.LastIndex() < args.PrevLogIndex ||
		(args.PrevLogIndex >= rf.log[0].Index && rf.LastIndex() >= args.PrevLogIndex && rf.GetLog(args.PrevLogIndex).Term != args.PrevLogTerm) {
		reply.Success, reply.Term = false, rf.currentTerm
		fmt.Printf("currentLogIndex: %d, currentLogTerm: %d\n", rf.log[len(rf.log)-1].Index, rf.log[len(rf.log)-1].Term)
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
	} else if args.Term == rf.currentTerm && rf.state == Candidate {
		rf.votedFor = -1
		rf.state = Follower
	}
	i := 0
	for i < len(args.Entries) && args.Entries[i].Index < rf.log[0].Index {
		i++
	}
	for i < len(args.Entries) && args.Entries[i].Index <= rf.LastIndex() {
		if rf.GetLog(args.Entries[i].Index).Term != args.Entries[i].Term {
			break
		}
		i++
	}
	if i < len(args.Entries) && args.Entries[i].Index <= rf.LastIndex() {
		rf.log = rf.log[:args.Entries[i].Index]
	}
	for i < len(args.Entries) {
		rf.log = append(rf.log, args.Entries[i])
		i++
	}
	reply.Term, reply.Success = rf.currentTerm, true
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = Min(args.LeaderCommit, rf.LastIndex())
	}
	for i := Max(rf.lastApplied, rf.lastIncludedIndex) + 1; i <= rf.commitIndex; i++ {
		var applyMsg ApplyMsg
		applyMsg.Command = rf.GetLog(i).Command
		applyMsg.CommandIndex = i
		applyMsg.CommandValid = true
		rf.applyCh <- applyMsg
	}
	if rf.commitIndex > rf.lastApplied {
		rf.lastApplied = rf.commitIndex
	}
	fmt.Printf("AppendEntries %d's commitIndex is %d, length is %d\n", rf.me, rf.commitIndex, rf.LastLength())
	rf.persist()
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Offset            int
	Data              []byte
	Done              bool
}

type InstallSnapshotReply struct {
	Term int
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	var log []LogEntry
	rf.mu.Lock()
	defer rf.mu.Unlock()
	fmt.Printf("%d: Snapshot: index: %d, lastIncludedIndex: %d, length: %d, lastindex: %d\n",
		rf.me, index, rf.lastIncludedIndex, len(rf.log), rf.log[len(rf.log)-1].Index)
	//rf.lastIncludedIndex = index
	if index < rf.log[0].Index {
		return
	} else if index > rf.log[len(rf.log)-1].Index {
		log = append(log, LogEntry{rf.currentTerm, index, nil})
		rf.lastIncludedTerm = rf.currentTerm
	} else {
		rf.lastIncludedTerm = rf.log[index-rf.lastIncludedIndex].Term
		for i := index; i < len(rf.log)+rf.lastIncludedIndex; i++ {
			log = append(log, rf.log[i-rf.lastIncludedIndex])
		}
		log[0].Command = nil
	}
	rf.log = log
	rf.lastIncludedIndex = index

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	state := w.Bytes()
	// to be fixed
	rf.persister.SaveStateAndSnapshot(state, snapshot)
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	fmt.Printf("%d receive InstallSnapshot from %d: Term: %d, lastIncludedIndex: %d, curIncludedIndex: %d\n",
		rf.me, args.LeaderId, args.Term, args.LastIncludedIndex, rf.lastIncludedIndex)
	var log []LogEntry
	rf.mu.Lock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = args.Term, -1
		rf.state = Follower
		rf.persist()
	} else if args.Term == rf.currentTerm && rf.state == Candidate {
		rf.votedFor = -1
		rf.state = Follower
	}
	reply.Term = rf.currentTerm
	// how to discard any existing or partial snapshot with a smaller index?
	// Reset state machine should in Snapshot?
	rf.isTimeout = false
	if args.LastIncludedIndex <= rf.log[0].Index {
		rf.mu.Unlock()
		return
	} else if args.LastIncludedIndex > rf.log[len(rf.log)-1].Index ||
		rf.log[args.LastIncludedIndex-rf.lastIncludedIndex].Term != args.LastIncludedTerm {
		log = append(log, LogEntry{args.LastIncludedTerm, args.LastIncludedIndex, nil})
		rf.lastIncludedTerm = rf.currentTerm
	} else {
		rf.lastIncludedTerm = rf.log[args.LastIncludedIndex-rf.lastIncludedIndex].Term
		for i := args.LastIncludedIndex; i < len(rf.log)+rf.lastIncludedIndex; i++ {
			log = append(log, rf.log[i-rf.lastIncludedIndex])
		}
		log[0].Command = nil
	}
	rf.log = log
	rf.lastIncludedIndex = args.LastIncludedIndex

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	state := w.Bytes()
	rf.persister.SaveStateAndSnapshot(state, args.Data)
	rf.mu.Unlock()

	applyMsg := ApplyMsg{
		CommandValid:  false,
		Command:       nil,
		SnapshotValid: true,
		Snapshot:      args.Data,
		// to be fixed
		SnapshotIndex: args.LastIncludedIndex,
		SnapshotTerm:  args.LastIncludedTerm,
	}
	rf.mu.Lock()
	rf.applyCh <- applyMsg
	rf.lastApplied = args.LastIncludedIndex
	rf.commitIndex = args.LastIncludedIndex
	rf.mu.Unlock()
	fmt.Printf("InstallSnapshot: %d, lastIncludedIndex: %d, commitIndex: %d\n", rf.me, rf.lastIncludedIndex, rf.commitIndex)
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	// candidate's Term
	Term int
	// candidate requesting vote
	CandidateId int
	// index of candidate's last log entry
	LastLogIndex int
	// Term of candidate's last log entry
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	// currentTerm, for candidate to update itself
	Term int
	// true means candidate received vote
	VoteGranted bool
}

// RequestVote 由candidate发起，用于要求其他follower选举
//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// vote for one another,
	//fmt.Printf("candidateId: %d, Term: %d, me: %d, currentTerm: %d\n", args.CandidateId, args.Term, rf.me, rf.currentTerm)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	fmt.Printf("%d receive RequestVote from: %d, term: %d, LastLogIndex: %d, LastLogTerm: %d, currentTerm: %d\n",
		rf.me, args.CandidateId, args.Term, args.LastLogIndex, args.LastLogTerm, rf.currentTerm)
	if args.Term < rf.currentTerm {
		fmt.Printf("%d receive out-dated RequestVote from: %d, term: %d, currentTerm: %d\n",
			rf.me, args.CandidateId, args.Term, rf.currentTerm)
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = Follower
	}
	reply.Term = rf.currentTerm
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		reply.VoteGranted = false
	} else if rf.log[len(rf.log)-1].Term > args.LastLogTerm ||
		(rf.log[len(rf.log)-1].Term == args.LastLogTerm && rf.LastIndex() > args.LastLogIndex) {
		reply.VoteGranted = false
	} else {
		fmt.Printf("%d's votedFor: %d\n", rf.me, rf.votedFor)
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.isTimeout = false
		rf.persist()
	}
	fmt.Printf(" %d voteFor: %d, currentTerm: %d\n", rf.me, rf.votedFor, rf.currentTerm)
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

//
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
// Term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	index := -1
	// Your code here (2B).
	if rf.state != Leader {
		rf.mu.Unlock()
		return index, -1, false
	}
	fmt.Printf("%d is Leader, start AppendEntries\n", rf.me)
	entry := LogEntry{rf.currentTerm, rf.LastLength(), command}
	rf.log = append(rf.log, entry)
	index = entry.Index
	rf.persist()
	rf.mu.Unlock()
	flag := rf.BroadCast()
	time.Sleep(10 * time.Millisecond)
	rf.mu.Lock()
	rf.isTimeout = false
	rf.mu.Unlock()
	if !flag {
		return -1, -1, false
	}
	rf.Apply()
	return index, rf.currentTerm, true
}

func (rf *Raft) BroadCast() bool {
	flag := true
	for peer := range rf.peers {
		if peer != rf.me {
			go func(peer int) {
				rf.mu.Lock()
				if rf.nextIndex[peer] <= rf.lastIncludedIndex {
					args := &InstallSnapshotArgs{
						Term:              rf.currentTerm,
						LeaderId:          rf.me,
						LastIncludedIndex: rf.lastIncludedIndex,
						LastIncludedTerm:  rf.lastIncludedTerm,
						Offset:            0,
						Data:              rf.persister.snapshot,
						Done:              true,
					}
					rf.mu.Unlock()
					reply := &InstallSnapshotReply{}
					if ok := rf.sendInstallSnapshot(peer, args, reply); !ok {
						return
					}
					rf.mu.Lock()
					if reply.Term > rf.currentTerm {
						rf.state = Follower
						rf.votedFor = -1
						rf.isTimeout = false
						flag = false
						rf.persist()
					} else {
						rf.matchIndex[peer] = rf.lastIncludedIndex
						rf.nextIndex[peer] = rf.matchIndex[peer] + 1
					}
					rf.mu.Unlock()
				} else {
					fmt.Printf("%d's lastIncludedIndex: %d, commitIndex: %d\n", rf.me, rf.lastIncludedIndex, rf.commitIndex)
					args := &AppendEntriesArgs{
						Term:         rf.currentTerm,
						LeaderId:     rf.me,
						PrevLogIndex: rf.nextIndex[peer] - 1,
						PrevLogTerm:  rf.log[rf.nextIndex[peer]-1-rf.lastIncludedIndex].Term,
						Entries:      rf.log[rf.nextIndex[peer]-rf.lastIncludedIndex:],
						LeaderCommit: rf.commitIndex,
					}
					rf.mu.Unlock()
					reply := &AppendEntriesReply{}
					if ok := rf.sendAppendEntries(peer, args, reply); !ok {
						return
					}
					if !reply.Success {
						rf.mu.Lock()
						if reply.Term > rf.currentTerm {
							rf.state = Follower
							rf.votedFor = -1
							rf.isTimeout = false
							flag = false
							rf.persist()
						} else {
							fmt.Printf("%d doesn't contain an entry at prevLogIndex whose term matches prevLogTerm\n", peer)
							if rf.matchIndex[peer] > 0 {
								rf.nextIndex[peer] = rf.matchIndex[peer]
								rf.matchIndex[peer]--
							} else {
								rf.nextIndex[peer] = rf.lastIncludedIndex
							}
						}
						rf.mu.Unlock()
					} else {
						rf.mu.Lock()
						fmt.Printf("%d's length: %d\n", rf.me, rf.log[len(rf.log)-1].Index)
						rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
						rf.nextIndex[peer] = rf.matchIndex[peer] + 1
						rf.mu.Unlock()
					}
				}
			}(peer)
		}
	}
	time.Sleep(10 * time.Millisecond)
	return flag
}

func (rf *Raft) Apply() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	lastcommitIdx := rf.commitIndex
	n := rf.commitIndex + 1
	for n <= rf.log[len(rf.log)-1].Index && rf.log[n-rf.lastIncludedIndex].Term != rf.currentTerm {
		n++
	}
	//n := int(math.Max(float64(rf.commitIndex), float64(rf.currentLogIndex)) + 1)
	fmt.Printf("%d's commitIndex is %d\n", rf.me, rf.commitIndex)
	for n <= rf.log[len(rf.log)-1].Index {
		counter := 1
		fmt.Printf("n: %d\n", n)
		for peer := range rf.peers {
			if peer != rf.me {
				fmt.Printf("%d's matchIndex: %d\n", peer, rf.matchIndex[peer])
				if rf.matchIndex[peer] >= n {
					counter++
				}
			}
		}
		if counter*2 > len(rf.peers) {
			rf.commitIndex = n
			n++
		} else {
			break
		}
	}
	fmt.Printf("%d's commitIndex is %d\n", rf.me, rf.commitIndex)
	for i := lastcommitIdx + 1; i <= rf.commitIndex; i++ {
		applyMsg := ApplyMsg{
			CommandValid:  true,
			Command:       rf.log[i-rf.lastIncludedIndex].Command,
			CommandIndex:  i,
			SnapshotValid: false,
		}
		rf.applyCh <- applyMsg
	}
	if rf.commitIndex > rf.lastApplied {
		rf.lastApplied = rf.commitIndex
	}
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// 如果收到来自新的leader的AppendEntries RPC, 那么状态切换到follower
func (rf *Raft) startElection() {
	fmt.Printf("%d start election\n", rf.me)
	rf.mu.Lock()
	if rf.state == Candidate {
		rf.currentTerm++
		rf.votedFor = rf.me
		rf.persist()
	}
	//rf.isTimeout = false
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.log[len(rf.log)-1].Index,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}
	rf.mu.Unlock()
	wg := sync.WaitGroup{}
	wg.Add(len(rf.peers) - 1)
	voters := 1
	for peer := range rf.peers {
		if peer != rf.me {
			go func(peer int) {
				fmt.Printf("%d send RequestVote to %d\n", rf.me, peer)
				defer wg.Done()
				rf.mu.Lock()
				if rf.state != Candidate {
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()
				reply := &RequestVoteReply{}
				if ok := rf.sendRequestVote(peer, args, reply); !ok {
					return
				}
				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					rf.state = Follower
					rf.isTimeout = false
					rf.persist()
				} else if reply.VoteGranted {
					voters++
					if rf.state == Candidate && voters*2 > len(rf.peers) {
						rf.state = Leader
						rf.isTimeout = false
						for i := 0; i < len(rf.peers); i++ {
							rf.nextIndex[i] = rf.log[len(rf.log)-1].Index + 1
							rf.matchIndex[i] = 0
						}
						rf.persist()
						fmt.Printf("%d becomes leader\n", rf.me)
						go rf.heartBeat()
					}
				}
				rf.mu.Unlock()
			}(peer)
		}
	}
	wg.Wait()
}

func (rf *Raft) heartBeat() {
	for rf.killed() == false {
		time.Sleep(time.Millisecond * 100)
		rf.mu.Lock()
		if rf.state != Leader {
			//rf.state = Follower
			//rf.votedFor = -1
			rf.isTimeout = false
			//rf.persist()
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		flag := rf.BroadCast()
		time.Sleep(10 * time.Millisecond)
		rf.mu.Lock()
		rf.isTimeout = false
		rf.mu.Unlock()
		if flag {
			rf.Apply()
		}
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		// Make sure the election timeouts in different peers don't always fire at the same time,
		// or else all peers will vote only for themselves and no one will become the leader.
		rand.Seed(time.Now().UnixNano())
		sleepInterval := rand.Intn(150) + 250
		time.Sleep(time.Millisecond * time.Duration(sleepInterval))

		rf.mu.Lock()
		if rf.isTimeout {
			fmt.Printf("%d convert Candidate\n", rf.me)
			rf.state = Candidate
			go rf.startElection()
		} else {
			if rf.state == Follower {
				fmt.Printf("%d set isTimeout to True\n", rf.me)
				rf.isTimeout = true
			}
		}
		rf.mu.Unlock()
	}
}

//
//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rand.Seed(time.Now().UnixNano())
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.log = append(rf.log, LogEntry{0, 0, nil})
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.state = Follower
	rf.votedFor = -1
	rf.currentTerm = 0
	rf.matchIndex = make([]int, len(peers))
	rf.nextIndex = make([]int, len(peers))
	rf.isTimeout = true
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0
	rf.applyCh = applyCh

	for i := 0; i < len(peers); i++ {
		rf.matchIndex[i] = 0
		rf.nextIndex[i] = 1
	}

	// Your initialization code here (2A, 2B, 2C).

	// create a background goroutine that will kick off leader election periodically by sending
	// out RequestVote RPCs when it hasn't heard from another peer for a while.
	// this way a peer will learn who is the leader, if there is already a leader or become the leader itself.

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

func (rf *Raft) LastIndex() int {
	return len(rf.log) - 1 + rf.lastIncludedIndex
}

func (rf *Raft) LastLength() int {
	return rf.LastIndex() + 1
}

func (rf *Raft) GetLog(index int) LogEntry {
	if index >= rf.log[0].Index && index <= rf.LastIndex() {
		return rf.log[index-rf.lastIncludedIndex]
	} else {
		fmt.Printf("Unexpected index: %d\n", index)
		return LogEntry{}
	}
}

func Max(a, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}

func Min(a, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}
