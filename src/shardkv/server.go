package shardkv

import (
	"6.824/labrpc"
	"sync/atomic"
	"time"
)
import "6.824/raft"
import "sync"
import "6.824/labgob"

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
	lastSequence map[int64]int64
	database     map[string]string
	channel      map[int]chan Notify
	indexMap     map[int64]int
	dead         int32
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	command := Op{"Get", args.Key, "", args.ClientId, args.SequenceId}
	reply.Err = kv.ApplyCommand(args.ClientId, args.SequenceId, command)
	kv.mu.Lock()
	if reply.Err == OK {
		if value, ok := kv.database[args.Key]; ok {
			reply.Value = value
		} else {
			reply.Err = ErrNoKey
		}
	}
	kv.mu.Unlock()
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
}

func (kv *ShardKV) ApplyCommand(clientId, sequence int64, command Op) (Err Err) {
	kv.mu.Lock()
	if _, isLeader := kv.rf.GetState(); !isLeader {
		kv.mu.Unlock()
		Err = ErrWrongLeader
	} else {
		lastSequence, ok := kv.lastSequence[clientId]
		if ok && lastSequence >= sequence {
			Err = OK
			kv.mu.Unlock()
			return
		}
		kv.mu.Unlock()
		idx, term, isLeader := kv.rf.Start(command)
		if !isLeader {
			Err = ErrWrongLeader
			return
		}
		kv.mu.Lock()
		ch, ok := kv.channel[idx]
		if !ok {
			ch = make(chan Notify, 1)
			kv.channel[idx] = ch
		}
		kv.mu.Unlock()
		select {
		case notify := <-ch:
			{
				if notify.sequenceNum != sequence || term != notify.term {
					Err = ErrWrongLeader
				} else {
					Err = OK
				}
				break
			}
		case <-time.After(500 * time.Millisecond):
			{
				_, isLeader := kv.rf.GetState()
				if !isLeader {
					Err = ErrWrongLeader
				} else {
					Err = OK
				}
				break
			}
		}
		kv.mu.Lock()
		delete(kv.channel, idx)
		kv.mu.Unlock()
	}
	return
}

func (kv *ShardKV) apply() {
	for !kv.killed() {
		applyMsg := <-kv.applyCh
		if applyMsg.CommandValid {
			command := (applyMsg.Command).(Op)
			kv.mu.Lock()
			lastSequence, ok := kv.lastSequence[command.ClientId]
			notify := Notify{command.SequenceNum, applyMsg.CommandTerm}
			if !ok || lastSequence < command.SequenceNum {
				switch command.Operator {
				case "Put":
					{
						kv.database[command.Key] = command.Value
					}
				case "Append":
					{
						if value, ok := kv.database[command.Key]; ok {
							kv.database[command.Key] = value + command.Value
						} else {
							kv.database[command.Key] = command.Value
						}
					}
				}
				ch, ok := kv.channel[applyMsg.CommandIndex]
				if ok {
					ch <- notify
				}

			}
		}
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

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	return kv
}
