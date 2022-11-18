package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"log"
	"sync"
	"sync/atomic"
)

const Debug = true

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

type Request struct {
	sequenceNum int64
	Err         Err
	Value       string
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
	requestMap map[int64]Request
	channel    map[int]chan interface{}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	DPrintf("%d receive Get from %d, sequence: %d, key: %v\n", kv.me, args.ClientId, args.SequenceNum, args.Key)
	kv.mu.Lock()
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	} else {
		request, ok := kv.requestMap[args.ClientId]
		if ok && args.SequenceNum <= request.sequenceNum {
			// 说明该请求已经处理过了，requestMap中应该包含序列号和返回结果
			if args.SequenceNum == request.sequenceNum {
				reply.Err, reply.Value = request.Err, request.Value
			}
			kv.mu.Unlock()
			return
		}
		Command := Op{
			"Get", args.Key, "", args.ClientId, args.SequenceNum,
		}
		idx, _, isLeader := kv.rf.Start(Command)
		if !isLeader {
			reply.Err = ErrWrongLeader
			kv.mu.Unlock()
			return
		}
		DPrintf("Start end: idx: %d\n", idx)
		//var ch chan interface{}
		ch := make(chan interface{})
		kv.channel[idx] = ch
		//ch, ok := kv.channel[kv.taskId]
		//if !ok {
		//	ch = make(chan interface{})
		//	kv.channel[kv.taskId] = ch
		//}
		kv.mu.Unlock()
		DPrintf("%d wait on channel\n", kv.me)
		_ = <-ch
		DPrintf("%d wake on channel\n", kv.me)
		kv.mu.Lock()
		if _, isLeader := kv.rf.GetState(); !isLeader {
			reply.Err = ErrWrongLeader
			kv.mu.Unlock()
			return
		}
		if request := kv.requestMap[args.ClientId]; request.sequenceNum == args.SequenceNum {
			if value, ok := kv.database[args.Key]; !ok {
				reply.Err = ErrNoKey
				kv.requestMap[args.ClientId] = Request{
					sequenceNum: args.SequenceNum, Value: "", Err: ErrNoKey,
				}
			} else {
				kv.requestMap[args.ClientId] = Request{
					sequenceNum: args.SequenceNum, Value: value, Err: OK,
				}
				reply.Value, reply.Err = value, OK
			}
		} else {
			DPrintf("Unexpected branch: clientid: %d, sequence: %d\n", args.ClientId, args.SequenceNum)
		}
		kv.mu.Unlock()
	}
	DPrintf("Get end: Err: %v, Value: %v\n", reply.Err, reply.Value)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	DPrintf("%d receive PutAppend: clientid: %d, sequence: %d, Op: %v, Key: %v, Value: %v\n", kv.me, args.ClientId, args.SequenceNum, args.Op, args.Key, args.Value)
	// Your code here.
	kv.mu.Lock()
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	} else {
		if request, ok := kv.requestMap[args.ClientId]; ok && args.SequenceNum <= request.sequenceNum {
			if args.SequenceNum == request.sequenceNum {
				reply.Err = request.Err
			}
			kv.mu.Unlock()
			return
		}
		Command := Op{
			args.Op, args.Key, args.Value, args.ClientId, args.SequenceNum,
		}
		idx, _, isLeader := kv.rf.Start(Command)
		if !isLeader {
			reply.Err = ErrWrongLeader
			kv.mu.Unlock()
			return
		}
		DPrintf("Start end: idx: %d\n", idx)
		ch := make(chan interface{})
		kv.channel[idx] = ch
		//ch, ok := kv.channel[kv.taskId]
		//if !ok {
		//	ch = make(chan interface{})
		//	kv.channel[kv.taskId] = ch
		//}
		kv.mu.Unlock()
		DPrintf("wait on channel\n")
		_ = <-ch
		DPrintf("wake on channel\n")
		kv.mu.Lock()
		if _, isLeader := kv.rf.GetState(); !isLeader {
			reply.Err = ErrWrongLeader
			kv.mu.Unlock()
			return
		}
		if request := kv.requestMap[args.ClientId]; request.sequenceNum == args.SequenceNum {
			if value, ok := kv.database[args.Key]; args.Op == "Append" && ok {
				kv.database[args.Key] = value + args.Value
			} else {
				kv.database[args.Key] = args.Value
			}
			kv.requestMap[args.ClientId] = Request{args.SequenceNum, OK, ""}
			reply.Err = OK
		} else {
			DPrintf("Unexpected branch: clientid: %d, sequence: %d, idx: %d\n", args.ClientId, args.SequenceNum, idx)
		}
		kv.mu.Unlock()
	}
	DPrintf("PutAppend end: Err: %v\n", reply.Err)
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
		command := (applyMsg.Command).(Op)
		DPrintf("applyMsg: isValid: %v, CommandIndex: %d, SequenceNum: %d, Value: %v\n",
			applyMsg.CommandValid, applyMsg.CommandIndex, command.SequenceNum, command.Value)
		if applyMsg.CommandValid {
			kv.mu.Lock()
			request := Request{}
			request.sequenceNum = command.SequenceNum
			kv.requestMap[command.ClientId] = request
			ch, ok := kv.channel[applyMsg.CommandIndex]
			kv.mu.Unlock()
			if ok {
				ch <- 1
				DPrintf("send to channel")
			}
		}
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
	kv.requestMap = make(map[int64]Request)
	kv.channel = make(map[int]chan interface{})
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
