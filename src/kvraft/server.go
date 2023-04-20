package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Operator    string
	Key         string
	Value       string
	ClientId    int64
	SequenceNum int64
}

type Notify struct {
	sequenceNum int64
	term        int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	database map[string]string
	// 用于记录已经完成的请求的响应和序列号
	lastSequence map[int64]int64
	channel      map[int]chan Notify
	//termMap           map[int]int
	indexMap          map[int64]int
	snapshotLastIndex int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	DPrintf("%d receive Get from %d, sequence: %d, key: %v\n", kv.me, args.ClientId, args.SequenceNum, args.Key)
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	} else {
		kv.mu.Lock()
		lastSequence, ok := kv.lastSequence[args.ClientId]
		if ok && args.SequenceNum <= lastSequence {
			reply.Err, reply.Value = OK, kv.database[args.Key]
			kv.mu.Unlock()
			return
		}
		Command := Op{
			"Get", args.Key, "", args.ClientId, args.SequenceNum,
		}
		kv.mu.Unlock()
		idx, term, isLeader := kv.rf.Start(Command)
		if !isLeader {
			reply.Err = ErrWrongLeader
			return
		}
		kv.mu.Lock()
		ch, ok := kv.channel[idx]
		if !ok {
			kv.channel[idx] = make(chan Notify, 1)
			ch = kv.channel[idx]
		}
		kv.mu.Unlock()
		DPrintf("%d wait on channel %d\n", kv.me, idx)
		select {
		case <-time.After(500 * time.Millisecond):
			{
				kv.mu.Lock()
				_, isLeader = kv.rf.GetState()
				lastSequence, ok := kv.lastSequence[args.ClientId]
				if isLeader && ok && args.SequenceNum <= lastSequence {
					reply.Err, reply.Value = OK, kv.database[args.Key]
				} else {
					reply.Err = ErrWrongLeader
				}
				DPrintf("%d delete channel %d\n", kv.me, idx)
				delete(kv.channel, idx)
				kv.mu.Unlock()
				break
			}
		case notify := <-ch:
			{
				DPrintf("%d wake on channel %d\n", kv.me, idx)
				kv.mu.Lock()
				if notify.sequenceNum == args.SequenceNum && term == notify.term {
					//if applyCommand.SequenceNum == args.SequenceNum {
					if value, ok := kv.database[args.Key]; !ok {
						reply.Err = ErrNoKey
						kv.lastSequence[args.ClientId] = args.SequenceNum
					} else {
						kv.lastSequence[args.ClientId] = args.SequenceNum
						reply.Value, reply.Err = value, OK
					}
				} else {
					reply.Err = ErrWrongLeader
				}
				DPrintf("delete channel %d\n", idx)
				delete(kv.channel, idx)
				kv.mu.Unlock()
				break
			}
		}
	}
	DPrintf("Get from %d end, seq[%d] : Err: %v, Value: %v\n",
		args.ClientId, args.SequenceNum, reply.Err, reply.Value)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	DPrintf("S[%d] receive PutAppend: clientid: %d, sequence: %d, Op: %v, Key: %v, Value: %v\n", kv.me, args.ClientId, args.SequenceNum, args.Op, args.Key, args.Value)
	// Your code here.
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	} else {
		if lastSequence, ok := kv.lastSequence[args.ClientId]; ok && args.SequenceNum <= lastSequence {
			reply.Err = OK
			kv.mu.Unlock()
			return
		}
		Command := Op{
			args.Op, args.Key, args.Value, args.ClientId, args.SequenceNum,
		}
		kv.mu.Unlock()
		idx, term, isLeader := kv.rf.Start(Command)
		if !isLeader {
			reply.Err = ErrWrongLeader
			return
		}
		kv.mu.Lock()
		ch, ok := kv.channel[idx]
		if !ok {
			kv.channel[idx] = make(chan Notify, 1)
			ch = kv.channel[idx]
		}
		kv.mu.Unlock()
		DPrintf("L[%d] wait on channel %d\n", kv.me, idx)
		select {
		case <-time.After(500 * time.Millisecond):
			{
				kv.mu.Lock()
				_, isLeader := kv.rf.GetState()
				lastSequence, ok := kv.lastSequence[args.ClientId]
				if isLeader && ok && args.SequenceNum <= lastSequence {
					reply.Err = OK
				} else {
					reply.Err = ErrWrongLeader
				}
				DPrintf("outdated %d delete channel %d\n", kv.me, idx)
				delete(kv.channel, idx)
				kv.mu.Unlock()
				break
			}
		case notify := <-ch:
			{
				DPrintf("%d wake on channel %d\n", kv.me, idx)
				kv.mu.Lock()
				if notify.sequenceNum == args.SequenceNum && term == notify.term {
					//if applyCommand.SequenceNum == args.SequenceNum {
					reply.Err = OK
				} else {
					reply.Err = ErrWrongLeader
				}
				DPrintf("%d delete channel %d\n", kv.me, idx)
				delete(kv.channel, idx)
				kv.mu.Unlock()
				break
			}
		}
	}
	DPrintf("PutAppend from C[%d] seq[%d] end Err: %v\n", kv.me, args.SequenceNum, reply.Err)
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) apply() {
	for !kv.killed() {
		applyMsg := <-kv.applyCh
		if applyMsg.CommandValid {
			DPrintf("applyMsg: Snapshot: %v, %d, Command: %v, %d\n",
				applyMsg.SnapshotValid, applyMsg.SnapshotIndex, applyMsg.CommandValid, applyMsg.CommandIndex)
			command := (applyMsg.Command).(Op)
			kv.mu.Lock()
			DPrintf("S[%d] applyMsg: isValid: %v, CommandIndex: %d, SequenceNum: %d, Value: %v\n",
				kv.me, applyMsg.CommandValid, applyMsg.CommandIndex, command.SequenceNum, command.Value)
			lastSequence, ok := kv.lastSequence[command.ClientId]
			if command.Operator == "Put" {
				if !ok || lastSequence < command.SequenceNum {
					kv.database[command.Key] = command.Value
					kv.lastSequence[command.ClientId] = command.SequenceNum
				}
			} else if command.Operator == "Append" {
				if !ok || lastSequence < command.SequenceNum {
					value, ok := kv.database[command.Key]
					if !ok {
						kv.database[command.Key] = command.Value
					} else {
						kv.database[command.Key] = value + command.Value
					}
					kv.lastSequence[command.ClientId] = command.SequenceNum
				}
			}
			DPrintf("CommandIndex: %d\n", applyMsg.CommandIndex)
			ch, ok := kv.channel[applyMsg.CommandIndex]
			//kv.termMap[applyMsg.CommandIndex] = applyMsg.CommandTerm
			notify := Notify{command.SequenceNum, applyMsg.CommandTerm}
			kv.mu.Unlock()
			if ok {
				ch <- notify
				DPrintf("%d send to channel %d\n", kv.me, applyMsg.CommandIndex)
			} else {
				DPrintf("%d cannot find channel %d\n", kv.me, applyMsg.CommandIndex)
			}
			// 判断是否需要Snapshot
			if kv.maxraftstate != -1 {
				kv.mu.Lock()
				if kv.rf.Persister.RaftStateSize() >= kv.maxraftstate {
					kv.snapshotLastIndex = applyMsg.CommandIndex
					kv.StartSnapshot()
				}
				kv.mu.Unlock()
			}
		} else {
			snapshot := applyMsg.Snapshot
			kv.mu.Lock()
			kv.RestoreFromSnapshot(snapshot)
			kv.snapshotLastIndex = applyMsg.SnapshotIndex
			kv.CleanChannel()
			kv.mu.Unlock()
		}
		//kv.mu.Unlock()
	}
}

func (kv *KVServer) CleanChannel() {
	for idx := range kv.channel {
		// 说明之前的idx的操作已经在leader上完成了，这些channel都可以删除
		if idx <= kv.snapshotLastIndex {
			delete(kv.channel, idx)
		}
	}
}

func (kv *KVServer) StartSnapshot() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.database)
	e.Encode(kv.lastSequence)
	e.Encode(kv.snapshotLastIndex)
	data := w.Bytes()
	kv.rf.Snapshot(kv.snapshotLastIndex, data)
}

func (kv *KVServer) RestoreFromSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var database map[string]string
	var lastSequence map[int64]int64
	var snapshotLastIndex int
	if d.Decode(&database) != nil || d.Decode(&lastSequence) != nil || d.Decode(&snapshotLastIndex) != nil {
		DPrintf("ReadSnapshot failed\n")
	} else {
		kv.database = database
		kv.lastSequence = lastSequence
		kv.snapshotLastIndex = snapshotLastIndex
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	// You may need initialization code here.
	kv.database = make(map[string]string)
	kv.lastSequence = make(map[int64]int64)
	kv.channel = make(map[int]chan Notify)
	//kv.termMap = make(map[int]int)
	kv.indexMap = make(map[int64]int)
	kv.snapshotLastIndex = 0

	snapshot := persister.ReadSnapshot()
	if len(snapshot) >= 0 {
		kv.mu.Lock()
		kv.RestoreFromSnapshot(snapshot)
		kv.mu.Unlock()
	}
	go kv.apply()
	return kv
}

// kv节点通过一个channel收到raft节点成功的响应。因此需要一个协程来监听channel,在收到rf通过channel发来的apply后通知对应地处理协程
// 如何区分处理来自客户端的不同的请求的协程？
// 防止处理重复请求：每个client有一个单调递增的序列号，server记录下最近一次完成的请求序列号，如果序列号小于最新的，则说明该请求以前处理过了，直接忽略
// 最新的结果记录下来，重复的进行返回
// 与请求相关的属性应该作为日志的一部分复制到raft的每个节点

// 每个Start的请求创建一个新的协程以将处理协程阻塞，
// 那么这些管道是否需要复制? 可能某些服务器上没有创建channel？channel应该是仅在leader上创建的，因此应当将channel作为Op的参数来复制？

// 如果maxraftstate == -1,不需要snapshot
// 如果persister.RaftStateSize() >= maxraftstate，则调用Snapshot()
// 哪些属性需要Snapshot？
