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
	"fmt"
	"math/rand"
	"time"

	//	"bytes"
	"sync"
	"sync/atomic"

	//	"6.824/labgob"
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

const interval = 100 * time.Millisecond

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex // Lock to protect shared access to this peer's state
	condLock  sync.Mutex
	cond      *sync.Cond
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	interval time.Duration
	lastbeat time.Time
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

	// 在server收到heartbeat时将isTimeout置1，在每次睡眠前将timeout置0，睡醒后检查isTimeout,如果是0，则超时，需要重新选举，否则不需要，继续睡眠
	// 这样可以降低系统的复杂度
	isTimeout bool
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
	fmt.Printf("id: %d, currentTerm: %d, votedFor: %d\n", rf.me, rf.currentTerm, rf.votedFor)
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
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
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
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
	}
	if args.Term >= rf.currentTerm {
		reply.Success = true
		reply.Term = rf.currentTerm
		rf.isTimeout = false
	} else {
		reply.Success = false
		reply.Term = rf.currentTerm
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

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
	fmt.Printf("candidateId: %d, Term: %d, me: %d, currentTerm: %d\n", args.CandidateId, args.Term, rf.me, rf.currentTerm)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
		rf.isTimeout = false
	}
	idx := rf.matchIndex[rf.me]
	term := 0
	if idx > 0 {
		term = rf.log[idx-1].Term
	}
	// we set votedFor negative to representative null
	// if votedFor is null or CandidateId
	// and candidate's log is at least as up-to-date as receiver's log, grant vote
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		// 也就是说candidate的log一定要比receiver的日志更新
		// 日志中含有不同时间片的条目，则时间片更大的日志更新
		// 若来自相同的时间片，那么更长的日志更新
		// receiver的时间片指的是什么
		if args.LastLogTerm > term || (args.LastLogTerm == term && args.LastLogIndex >= idx) {
			reply.VoteGranted = true
			reply.Term = args.Term
			rf.votedFor = args.CandidateId
		} else {
			reply.VoteGranted = false
			reply.Term = rf.currentTerm
		}
	}
	fmt.Printf("RequestVote: me: %d, votedFor: %d, candidateId: %d\n", rf.me, rf.votedFor, args.CandidateId)
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
	index := -1
	Term := -1
	isLeader := true

	// Your code here (2B).

	return index, Term, isLeader
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

type electionChan struct {
	flag bool
	idx  int
}

// 如果收到来自新的leader的AppendEntries RPC, 那么状态切换到follower
func (rf *Raft) startElection() {
	fmt.Printf("%d start election\n", rf.me)
	args := RequestVoteArgs{}
	ch := make(chan electionChan)
	rf.mu.Lock()
	rf.currentTerm++
	rf.state = Candidate
	rf.votedFor = rf.me
	rf.isTimeout = false
	args.LastLogIndex = rf.matchIndex[rf.me]
	args.LastLogTerm = 0
	if args.LastLogIndex != 0 {
		args.LastLogTerm = rf.log[args.LastLogIndex-1].Term
	}
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	rf.mu.Unlock()
	reply := make([]RequestVoteReply, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		j := i
		if j != rf.me {
			go func() {
				ch <- electionChan{
					rf.sendRequestVote(j, &args, &reply[j]),
					j}
			}()
		}
	}
	counter := 1
	// 如果Call超时，则进行下一轮选举，如果reply.Term > currentTerm，则自愿放弃选举，二者应该分开讨论
	for i := 0; i < len(rf.peers)-1; i++ {
		v := <-ch
		if v.flag && reply[v.idx].VoteGranted {
			counter++
		} else if v.flag && reply[v.idx].Term > rf.currentTerm {
			rf.mu.Lock()
			rf.state = Follower
			rf.currentTerm = reply[v.idx].Term
			rf.mu.Unlock()
			return
		}
	}
	if counter*2 > len(rf.peers) {
		rf.mu.Lock()
		rf.state = Leader

		for i := 0; i < len(rf.peers); i++ {
			rf.matchIndex[i] = len(rf.log)
			rf.nextIndex[i] = len(rf.log) + 1
		}
		rf.mu.Unlock()
		go rf.heartBeat()
	}
}

func (rf *Raft) sleep() {
	interval := rf.interval - time.Now().Sub(rf.lastbeat)
	time.Sleep(interval)
}

type AppendEntriesReplyChan struct {
	idx  int
	flag bool
}

func (rf *Raft) heartBeat() {
	for rf.killed() == false && rf.state == Leader {
		rf.mu.Lock()
		args := AppendEntriesArgs{
			rf.currentTerm, rf.me, len(rf.log) - 1,
			rf.log[len(rf.log)-1].Term, nil, rf.commitIndex,
		}
		ch := make(chan AppendEntriesReplyChan)
		reply := make([]AppendEntriesReply, len(rf.peers))
		for i := 0; i < len(rf.peers); i++ {
			j := i
			if j != rf.me {
				go func() {
					ch <- AppendEntriesReplyChan{j, rf.sendAppendEntries(j, &args, &reply[j])}
				}()
			}
		}
		for i := 0; i < len(rf.peers)-1; i++ {
			v := <-ch
			if !reply[v.idx].Success && reply[v.idx].Term > rf.currentTerm {
				rf.currentTerm = reply[v.idx].Term
				rf.state = Follower
				rf.mu.Unlock()
				return
			}
		}
		rf.mu.Unlock()
		time.Sleep(interval)
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		// 如何判断是否超时？
		// 如何设置超时时间？
		// Make sure the election timeouts in different peers don't always fire at the same time,
		// or else all peers will vote only for themselves and no one will become the leader.
		rand.Seed(time.Now().UnixNano())
		sleepInterval := rand.Intn(500) + 500
		time.Sleep(time.Millisecond * time.Duration(sleepInterval))
		rf.mu.Lock()
		isTimeout := rf.isTimeout
		state := rf.state
		rf.mu.Unlock()
		if state == Leader {
			fmt.Printf("current Leader: %d\n", rf.me)
			return
		}
		if isTimeout {
			go rf.startElection()
		} else {
			rf.mu.Lock()
			rf.isTimeout = true
			rf.mu.Unlock()
		}
		//if rf.state == Follower {
		//	if time.Now().After(rf.lastbeat.Add(rf.interval)) {
		//		// 如果选举失败，继续选举
		//		for !rf.startElection() {
		//		}
		//		for i := 0; i < len(rf.peers); i++ {
		//			rf.matchIndex[i] = 0
		//			rf.nextIndex[i] = len(rf.log)
		//		}
		//	}
		//}
		//if rf.state == Leader {
		//	rf.heartBeat()
		//	//如果是leader，则开启协程发送heartbeat，否则
		//	// 有两种情况会唤醒ticker：一个是睡过了时间，另一个是收到了heartbeat
		//	// 但是如果状态是leader，则不应该有这两部分，所以leader直接阻塞不返回即可
		//}
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
	rf.cond = sync.NewCond(&rf.condLock)
	rf.log = append(rf.log, LogEntry{0, 0, nil})
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.lastbeat = time.Now()
	rf.interval = time.Duration(300+rand.Intn(150)) * time.Millisecond
	rf.state = Follower
	rf.votedFor = -1
	rf.currentTerm = 0
	rf.matchIndex = make([]int, len(peers))
	rf.nextIndex = make([]int, len(peers))
	rf.isTimeout = true
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

// tester require your Raft to elect a new leader within five seconds of the failure of the old leader
// leader election may require multiple rounds in case of a split vote, so must pick election timeouts that an election will
// complete in less than five seconds even if it requires multiple rounds
// paper mentions election timeouts in the range of 150 to 300 milliseconds, tester limits 10 heart beats per second, so
// timeouts must larger than 150 to 300.    another question, what's the relation between interval of heartbeat and timeout?
// the easiest way to takes actions periodically of after delays in time is to create a goroutine with a loop that calls time.Sleep
// implement GetState
// check rf.killed in each loop to avoid having dead Raft instances print confusing messages
//

// 需要想明白的几个问题：
// leader是如何使用AppendEntries的？ 我的理解是，leader请求每个server的AppendEntries RPC，每个follower接收到来自leader的请求后重置定时器
// 对应的问题：leader在哪里调用AppendEntries？ follower的定时器如何定义？超时如何通知？如何重置？
// 如何选举？
// 要求选举的超时时间必须不一致，因此要生成随机数    也就是说在选举时也要设置一个定时器，如果在定时器耗时结束时，选举没有完成，那么重新开始一轮选举
// Call自带计时器，return false说明超时，也就是说election timeout，只要有一个超时就应该重新选举，那么应该对于不同的server采用不同的协程发送？
// 选举的时间限制？ heartbeat的时间限制？（每秒发送不超过10次heartbeat）
//
// 收到heartbeat后如何重置定时器？
// tick需要Sleep(Interval), 那就有两个问题：Interval如何选择？Interval在make时采用随机值
//在收到heartbeat后如何更新？
//使用条件变量，ticker在这上面wait，一个协程Sleep，睡醒时BroadCast，AppendEntries来到时也BroadCast
// Sleep的协程睡醒后，首先判断有没有超时，如果超时，重新选举。如果没有超时，判断下次超时的时间，然后继续睡
// 如果使用mu作为条件变量，那么所有的RPC都将被锁住？

// 几个时间：
// heartbeat：每秒不超过10次
// election timeouts: 五秒内选举出一个leader 略大于150~300ms，不同的peer具有不同的interval，
//

// call只能是并行的，那么如何判断有没有哪个call超时呢？
// 使用管道，如果有一个是false，那么就开启下一轮，而且正好也是要等到所有的election timeout，完美