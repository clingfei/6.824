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
	Operator     string
	Key          string
	Value        string
	ClientId     int64
	SequenceNum  int64
	DataBase     map[string]string
	LastSequence map[int64]int64
	Num          int
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
	ck     *Clerk
	config shardctrler.Config

	lastSequence      map[int64]int64
	database          map[string]string
	channel           map[int]chan Notify
	indexMap          map[int64]int
	dead              int32
	snapshotLastIndex int
	migrating         bool
	sequenceId        int64

	migrateIn map[int]int
	migMap    map[int]int
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
	DPrintf("G[%d] S[%d] receive Get RPC from C[%d], seq[%d], key: %v\n",
		kv.gid, kv.me, args.ClientId, args.SequenceId, args.Key)
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
		if kv.migrating {
			if len(kv.migrateIn) > 0 {
				reply.Err = ErrWrongLeader
				DPrintf("G[%d] S[%d] is migrating, Seq[%d]\n", kv.gid, kv.me, args.SequenceId)
				DPrintf("%v\n", kv.migrateIn)
				kv.mu.Unlock()
				return
			} else {
				kv.migrating = false
			}
		}
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
			"Get", args.Key, "", args.ClientId, args.SequenceId, nil, nil, 0,
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
				if kv.config.Shards[shard] != kv.gid {
					reply.Err = ErrWrongGroup
				} else if isLeader && ok && args.SequenceId <= lastSequence {
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
				if kv.config.Shards[shard] != kv.gid {
					reply.Err = ErrWrongGroup
				} else if notify.sequenceNum == args.SequenceId && term == notify.term {
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
	DPrintf("G[%d] S[%d] Response for Get from C[%d] seq[%d]: Err: %v, Value: %v",
		kv.gid, kv.me, args.ClientId, args.SequenceId, reply.Err, reply.Value)
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	DPrintf("G[%d] S[%d] receive PutAppend RPC from C[%d], seq[%d], key: %s, value: %s\n",
		kv.gid, kv.me, args.ClientId, args.SequenceId, args.Key, args.Value)
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
		if kv.migrating {
			if len(kv.migrateIn) > 0 {
				reply.Err = ErrWrongLeader
				DPrintf("G[%d] S[%d] is migrating, Seq[%d], Config[%d], In[%v]\n", kv.gid, kv.me, args.SequenceId, kv.config.Num, kv.migrateIn)
				kv.mu.Unlock()
				return
			} else {
				kv.migrating = false
			}
		}
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
			args.Op, args.Key, args.Value, args.ClientId, args.SequenceId, nil, nil, 0,
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
				if kv.config.Shards[shard] != kv.gid {
					reply.Err = ErrWrongGroup
				} else if isLeader && ok && args.SequenceId <= lastSequence {
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
				if kv.config.Shards[shard] != kv.gid {
					reply.Err = ErrWrongGroup
				} else if notify.sequenceNum == args.SequenceId && term == notify.term {
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
	DPrintf("G[%d] S[%d] Response for PutAppend from C[%d] seq[%d]: %v",
		kv.gid, kv.me, args.ClientId, args.SequenceId, reply.Err)
}

func (kv *ShardKV) apply() {
	for !kv.killed() {
		applyMsg := <-kv.applyCh
		if applyMsg.CommandValid {
			//DPrintf("S[%d] applyMsg: Snapshot: %v, %d, Command: %v, %d\n",
			//	kv.me, applyMsg.SnapshotValid, applyMsg.SnapshotIndex, applyMsg.CommandValid, applyMsg.CommandIndex)
			command := (applyMsg.Command).(Op)
			kv.mu.Lock()
			DPrintf("G[%d] S[%d] applyMsg: isValid: %v, CommandIndex: %d, SequenceNum: %d, Value: %v, Operator: %v\n",
				kv.gid, kv.me, applyMsg.CommandValid, applyMsg.CommandIndex, command.SequenceNum, command.Value, command.Operator)
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
			} else if command.Operator == "Migrate" {
				if !ok || lastSequence < command.SequenceNum {
					for k, v := range command.DataBase {
						kv.database[k] = v
					}
					for clientId, seq := range command.LastSequence {
						if cur, ok := kv.lastSequence[clientId]; !ok || cur < seq {
							kv.lastSequence[clientId] = seq
						}
					}
					kv.lastSequence[command.ClientId] = command.SequenceNum
				}
			}
			ch, ok := kv.channel[applyMsg.CommandIndex]
			notify := Notify{command.SequenceNum, applyMsg.CommandTerm}
			kv.mu.Unlock()
			if ok {
				DPrintf("G[%d] S[%d] wake on channel %d\n", kv.gid, kv.me, applyMsg.CommandIndex)
				ch <- notify
			} else {
				DPrintf("G[%d] S[%d] channel %d not found\n", kv.gid, kv.me, applyMsg.CommandIndex)
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

func (kv *ShardKV) Migrate(args *MigrateArgs, reply *MigrateReply) {
	DPrintf("G[%d] S[%d] receive Migrate RPC from G[%d], seq[%d], value: %v\n",
		kv.gid, kv.me, args.Gid, args.SequenceId, args.Database)
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	} else {
		kv.mu.Lock()
		if lastSequence, ok := kv.lastSequence[args.ClientId]; ok && args.SequenceId <= lastSequence {
			reply.Err = OK
			DPrintf("Seq[%d] Duplicate migration\n", args.SequenceId)
			kv.mu.Unlock()
			return
		}
		command := Op{
			"Migrate", "", "", args.ClientId, args.SequenceId, args.Database, args.LaseSequence, args.Num,
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
					reply.Err = OK
					kv.migMap[args.Gid] = args.Num
					if num, ok := kv.migrateIn[args.Gid]; ok && args.Num >= num {
						delete(kv.migrateIn, args.Gid)
						DPrintf("Remove migrateIn %d\n", args.Gid)
					}
					if len(kv.migrateIn) == 0 {
						kv.migrating = false
					}
				} else {
					reply.Err = ErrWrongLeader
				}
				delete(kv.channel, idx)
				kv.mu.Unlock()
				break
			}
		case notify := <-ch:
			{
				DPrintf("G[%d] S[%d] wake on channel %d, Gid[%d]\n", kv.gid, kv.me, idx, args.Gid)
				kv.mu.Lock()
				if notify.sequenceNum == args.SequenceId && term == notify.term {
					DPrintf("%v, %v\n", kv.migrateIn, kv.migrating)
					reply.Err = OK
					kv.migMap[args.Gid] = args.Num
					if num, ok := kv.migrateIn[args.Gid]; ok && args.Num >= num {
						delete(kv.migrateIn, args.Gid)
						DPrintf("Remove migrateIn %d\n", args.Gid)
					}
					if len(kv.migrateIn) == 0 {
						kv.migrating = false
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
	DPrintf("G[%d] S[%d] Response for Migrate from G[%d] seq[%d]: %v", kv.gid, kv.me, args.Gid, args.SequenceId, reply.Err)
}

func deepCopy(groups map[string]string) map[string]string {
	newGroups := make(map[string]string)
	for key, value := range groups {
		newGroups[key] = value
	}
	return newGroups
}

func deepCopySeq(seq map[int64]int64) map[int64]int64 {
	newSeq := make(map[int64]int64)
	for key, value := range seq {
		newSeq[key] = value
	}
	return newSeq
}

// 现在的问题是迁移的时候没有迁移全
func (kv *ShardKV) StartMigrate(newConfig shardctrler.Config) {
	// 扫描所有的key，判断key对应的shard，由于之前迁移的数据没有删除，因此首先判断这个数据是不是自己的，然后判断这个数据是不是应该迁移，然后根据shard获取对应的gid，如果gid不一致的话，则开始迁移
	kv.mu.Lock()
	KV2Migrate := make(map[int]map[string]string)
	for key, value := range kv.database {
		shard := key2shard(key)
		DPrintf("G[%d] S[%d]: \n", kv.gid, kv.me)
		DPrintf("%v, %v, Gid: %d\n", key, value, newConfig.Shards[shard])
		if targetGid := newConfig.Shards[shard]; targetGid != kv.gid && kv.config.Shards[shard] == kv.gid {
			if KV2Migrate[targetGid] == nil {
				KV2Migrate[targetGid] = make(map[string]string)
			}
			KV2Migrate[targetGid][key] = value
			//delete(kv.database, key)
		}
	}
	// 遍历newConfig，看到原本应该由自己负责的shard，在新的shard里面不由自己负责了
	for shard, gid := range kv.config.Shards {
		if targetGid := newConfig.Shards[shard]; gid == kv.gid && targetGid != kv.gid {
			if _, ok := KV2Migrate[targetGid]; !ok {
				// 如果没有需要迁移到目标group的数据，则创建一个空的map
				KV2Migrate[targetGid] = make(map[string]string)
			}
		}
	}

	for shard, gid := range newConfig.Shards {
		// 在新的配置中，应该由自己处理，但是在原本的配置中不应该自己处理的，记录下来
		if gid == kv.gid && kv.config.Shards[shard] != kv.gid {
			// 记录迁移的src
			if num, ok := kv.migMap[kv.config.Shards[shard]]; ok && num >= newConfig.Num {
				DPrintf("Data has migrated from G[%d] before!\n", kv.config.Shards[shard])
			} else {
				kv.migrateIn[kv.config.Shards[shard]] = newConfig.Num
			}
		}
	}
	curConfig := kv.config.Num
	kv.mu.Unlock()
	DPrintf("G[%d]'s KV2Migrate: %v\n", kv.gid, KV2Migrate)
	DPrintf("G[%d]'s MigMap: %v\n", kv.gid, kv.migMap)
	for gid, database := range KV2Migrate {
		targetServers := newConfig.Groups[gid]
		go func(targetServers []string, gid int, database map[string]string) {
			args := MigrateArgs{ClientId: kv.ck.me, SequenceId: kv.ck.sequenceId, Gid: kv.gid, Num: newConfig.Num}
			args.Database = deepCopy(database)
			kv.mu.Lock()
			args.LaseSequence = deepCopySeq(kv.lastSequence)
			kv.mu.Unlock()
			DPrintf("G[%d] S[%d] sending migration to G[%d], seq[%d], config[%d]\n", args.Gid, kv.me, gid, kv.ck.sequenceId, curConfig)
			kv.ck.sequenceId++
			for {
				for si := 0; si < len(targetServers); si++ {
					srv := kv.ck.make_end(targetServers[si])
					var reply MigrateReply
					ok := srv.Call("ShardKV.Migrate", &args, &reply)
					if ok && reply.Err == OK {
						return
					} else {
						// not ok, or ErrWrongLeader
					}
				}
				time.Sleep(100 * time.Millisecond)
			}
		}(targetServers, gid, database)
	}
}

func (kv *ShardKV) GetConfig() {
	for !kv.killed() {
		// 只有Leader需要周期性GetConfig，因此在Query之前首先检查一下自己的状态
		// 如果不是Leader，等待100 Millisecond之后，再次检查
		if _, isLeader := kv.rf.GetState(); isLeader {
			newConfig := kv.mck.Query(-1)
			kv.mu.Lock()
			if kv.config.Num != newConfig.Num {
				// 说明配置发生了变化，判断是否进行迁移
				DPrintf("G[%d] S[%d] set migrating\n", kv.gid, kv.me)
				DPrintf("newconfig: %v\n", newConfig)
				DPrintf("oldconfig: %v\n", kv.config)
				kv.migrating = true
				kv.mu.Unlock()
				kv.StartMigrate(newConfig)
				kv.mu.Lock()
			}
			kv.config = newConfig
			kv.mu.Unlock()
		}
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
	kv.ck = MakeClerk(kv.ctrlers, make_end)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.database = make(map[string]string)
	kv.lastSequence = make(map[int64]int64)
	kv.channel = make(map[int]chan Notify)
	kv.indexMap = make(map[int64]int)
	kv.snapshotLastIndex = 0
	kv.migrating = false
	kv.sequenceId = nrand()
	kv.migrateIn = make(map[int]int)
	kv.migMap = make(map[int]int)

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
// server不断检查配置变化，当检查到配置变化时，启动shard迁移过程
// 如果在新的配置中，replica group失去了某个shard，那么对client发送的对这个shard的请求发送WrongShard，然后将这个shard对应的数据迁移到新的服务器上
// 如果replica group得到一个shard，在响应这个shard之前必须完成数据的迁移过程
// 需要解决的问题：
// 1. 服务器之间如何通信
// 2. 如何判断当前是否已经迁移完成

// 两个group，称为A和B，A持有锁，调用B的RPC Put，同时B持有锁，调用A的RPC Put，而RPC一直进入不了这个锁，然后就会导致死锁
// 如果先对要迁移的数据进行备份，然后再发送，可以避免上面的死锁问题，但是如果在迁移的过程中，收到来自client的请求，就可以进入锁，而此时迁移过程未完成，不应该处理这一请求
// 是不是可以为迁移过程单开一个锁？在开启迁移时，mu.Lock是锁住的，而自己接受的来自其他的server的迁移不受影响，但是apply过程需要mu这个锁？
// 设置一个标志位，0， 1两种状态，0表示正常，没有在迁移，1表示正在迁移，返回一个ErrNoLeader，
// 如果检查到新的配置与当前配置不一致，则将标志位设置为1，开始迁移，在迁移完之后，将标志位设置回0
// 如何判断当前是否已经迁移完？检查在新的配置中是否存在某个shard，不存在？

// 如果每个服务器都周期性地获取最新配置的话，可能会导致一个group中的configuration不一致
// 每个group只有leader获取配置，获取之后通过apply在所有的server上达成共识
// 也只有leader需要向其他的服务器迁移shard，follower不需要
// 在从follower切换到leader时，如何启动该线程？

// 可线性化的问题，t1接收到来自client的Get，t2时检测到配置改变，然后把get的数据给了另一个group，然后t3时经过Raft达成共识，
// 根源在于，delete操作没有经过Raft，
// 在接到apply之后，进行操作之前，再检查一遍是不是自己负责的shard，如果不是的话，就放弃？

// 旧的未删除的shard会覆盖新的，在加入KV2Migrate之前先过滤一遍

// 出现死锁的原因在于，G1和G2两个查询的时间不同步，在G2查询到新的配置之前，G1已经迁移到G2了，那么在G1迁移到G2之后其实G2就没有必要再等待G1了
// 设置一个map，记录每次迁移时，group 和config Num之间的对应关系，当查询到新的配置时，设置等待group时，依次判断当前的config num是否已经传输过了
// 在删除MigrateIn中的记录时，也要比较一下等待的Config Num与当前的大小关系，只有新来的比它大时，才能够reset migrating
