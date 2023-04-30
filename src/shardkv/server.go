package shardkv

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

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

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	mck    *shardctrler.Clerk
	config shardctrler.Config

	lastSequence      map[int64]int64
	database          map[string]string
	channel           map[int]chan Notify
	indexMap          map[int64]int
	dead              int32
	snapshotLastIndex int
}

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

// 检查是否是Leader
// 检查对应的key是否应该由自己处理，
func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	DPrintf("S[%d] receive Get RPC from C[%d], seq[%d], key: %v\n",
		kv.me, args.ClientId, args.SequenceId, args.Key)
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	} else {
		// 要不要加锁
		shard := key2shard(args.Key)
		kv.mu.Lock()
		if kv.config.Shards[shard] != kv.gid {
			reply.Err = ErrWrongGroup
			kv.mu.Unlock()
			return
		}
		lastSequence, ok := kv.lastSequence[args.ClientId]
		if ok && args.SequenceId <= lastSequence {
			reply.Err, reply.Value = OK, kv.database[args.Key]
			kv.mu.Unlock()
			return
		}
		command := Op{
			"Get", args.Key, "", args.ClientId, args.SequenceId,
		}
		kv.mu.Unlock()
		idx, term, isLeader := kv.rf.Start(command)
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

		select {
		case <-time.After(500 * time.Millisecond):
			{
				kv.mu.Lock()
				_, isLeader = kv.rf.GetState()
				lastSequence, ok := kv.lastSequence[args.ClientId]
				if isLeader && ok && args.SequenceId <= lastSequence {
					// 在等待apply的过程中，可能有相同序列号的请求被处理了
					reply.Err, reply.Value = OK, kv.database[args.Key]
				} else {
					reply.Err = ErrWrongLeader
				}
				delete(kv.channel, idx)
				kv.mu.Unlock()
				break
			}
		case notify := <-ch:
			{
				kv.mu.Lock()
				if notify.sequenceNum == args.SequenceId && term == notify.term {
					if value, ok := kv.database[args.Key]; !ok {
						reply.Err = ErrNoKey
						kv.lastSequence[args.ClientId] = args.SequenceId
					} else {
						kv.lastSequence[args.ClientId] = args.SequenceId
						reply.Value, reply.Err = value, OK
					}
				} else {
					reply.Err = ErrWrongLeader
				}
				delete(kv.channel, idx)
				kv.mu.Unlock()
				break
			}
		}
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	DPrintf("S[%d] receive PutAppend RPC from C[%d], seq[%d], key: %v\n",
		kv.me, args.ClientId, args.SequenceId, args.Key)
	if kv.killed() {
		reply.Err = ErrWrongLeader
		DPrintf("Seq[%d] ErrWrongLeader\n", args.SequenceId)
		return
	}
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		DPrintf("Seq[%d] ErrWrongLeader\n", args.SequenceId)
		return
	} else {
		shard := key2shard(args.Key)
		kv.mu.Lock()
		if kv.config.Shards[shard] != kv.gid {
			reply.Err = ErrWrongGroup
			kv.mu.Unlock()
			DPrintf("Seq[%d] ErrWrongGroup\n", args.SequenceId)
			return
		}
		if lastSequence, ok := kv.lastSequence[args.ClientId]; ok && args.SequenceId <= lastSequence {
			reply.Err = OK
			DPrintf("Seq[%d] Duplicate request\n", args.SequenceId)
			kv.mu.Unlock()
			return
		}
		command := Op{
			args.Op, args.Key, args.Value, args.ClientId, args.SequenceId,
		}
		kv.mu.Unlock()
		DPrintf("seq[%d] start\n", args.SequenceId)
		idx, term, isLeader := kv.rf.Start(command)
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
		select {
		case <-time.After(500 * time.Millisecond):
			{
				kv.mu.Lock()
				_, isLeader := kv.rf.GetState()
				lastSequence, ok := kv.lastSequence[args.ClientId]
				if isLeader && ok && args.SequenceId <= lastSequence {
					reply.Err = OK
				} else {
					reply.Err = ErrWrongLeader
				}
				delete(kv.channel, idx)
				kv.mu.Unlock()
				break
			}
		case notify := <-ch:
			{
				DPrintf("S[%d] wake on channel %d\n", kv.me, idx)
				kv.mu.Lock()
				if notify.sequenceNum == args.SequenceId && term == notify.term {
					reply.Err = OK
				} else {
					reply.Err = ErrWrongLeader
				}
				delete(kv.channel, idx)
				kv.mu.Unlock()
				break
			}
		}
	}
	DPrintf("Response for PutAppend from C[%d] seq[%d]: %v", args.ClientId, args.SequenceId, reply.Err)
}

func (kv *ShardKV) apply() {
	for !kv.killed() {
		applyMsg := <-kv.applyCh
		if applyMsg.CommandValid {
			DPrintf("S[%d] applyMsg: Snapshot: %v, %d, Command: %v, %d\n",
				kv.me, applyMsg.SnapshotValid, applyMsg.SnapshotIndex, applyMsg.CommandValid, applyMsg.CommandIndex)
			command := (applyMsg.Command).(Op)
			kv.mu.Lock()
			DPrintf("S[%d] applyMsg: isValid: %v, CommandIndex: %d, SequenceNum: %d, Value: %v, Operator: %v\n",
				kv.me, applyMsg.CommandValid, applyMsg.CommandIndex, command.SequenceNum, command.Value, command.Operator)
			lastSequence, ok := kv.lastSequence[command.ClientId]
			//			notify := Notify{command.SequenceNum, applyMsg.CommandTerm}
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
			ch, ok := kv.channel[applyMsg.CommandIndex]
			notify := Notify{command.SequenceNum, applyMsg.CommandTerm}
			kv.mu.Unlock()
			if ok {
				DPrintf("S[%d] wake on channel %d\n", kv.me, applyMsg.CommandIndex)
				ch <- notify
			} else {
				DPrintf("S[%d] channel %d not found\n", kv.me, applyMsg.CommandIndex)
			}
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
	}
}

func (kv *ShardKV) CleanChannel() {
	for idx := range kv.channel {
		// 说明之前的idx的操作已经在leader上完成了，这些channel都可以删除
		if idx <= kv.snapshotLastIndex {
			delete(kv.channel, idx)
		}
	}
}

// TO FIX
func (kv *ShardKV) StartSnapshot() {
	DPrintf("S[%d] start Snapshot\n", kv.me)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.database)
	e.Encode(kv.lastSequence)
	e.Encode(kv.snapshotLastIndex)
	data := w.Bytes()
	kv.rf.Snapshot(kv.snapshotLastIndex, data)
}

func (kv *ShardKV) RestoreFromSnapshot(snapshot []byte) {
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
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *ShardKV) GetConfig() {
	for {
		newConfig := kv.mck.Query(-1)
		kv.mu.Lock()
		kv.config = newConfig
		kv.mu.Unlock()
		// DPrintf("current config: %v\n", kv.config)
		time.Sleep(100 * time.Millisecond)
	}
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.database = make(map[string]string)
	kv.lastSequence = make(map[int64]int64)
	kv.channel = make(map[int]chan Notify)
	kv.indexMap = make(map[int64]int)
	kv.snapshotLastIndex = 0

	snapshot := persister.ReadSnapshot()
	if len(snapshot) >= 0 {
		kv.mu.Lock()
		kv.RestoreFromSnapshot(snapshot)
		kv.mu.Unlock()
	}
	go kv.GetConfig()
	go kv.apply()

	return kv
}

// 对于server来说，如何获知自己应该处理哪个shard？
// 通过Query(-1)获得当前最新的

// 从Config中可以得到每个shard对应的gid，通过key2Shard将key转换成对应的shard，然后判断是否应该由自己处理
