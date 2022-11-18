package kvraft

import (
	"6.824/labrpc"
	"sync"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	curLeader  int
	me         int64
	mu         sync.Mutex
	sequenceId int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.curLeader = 0
	ck.me = nrand()
	ck.sequenceId = nrand()
	return ck
}

func (ck *Clerk) SendGet(key string, ch chan string) {
	ck.mu.Lock()
	args := GetArgs{
		Key:         key,
		ClientId:    ck.me,
		SequenceNum: ck.sequenceId,
	}
	reply := GetReply{}
	for {
		DPrintf("%d send Get to %d key: %v, sequence: %d \n", ck.me, ck.curLeader, args.Key, args.SequenceNum)
		if !ck.sendGet(ck.curLeader, &args, &reply) || reply.Err == ErrWrongLeader {
			DPrintf("%d receive Err: %v, value: %v from %d \n", ck.me, reply.Err, reply.Value, ck.curLeader)
			ck.curLeader = (ck.curLeader + 1) % len(ck.servers)
			continue
		}
		DPrintf("Call succeed receive %d receive Err: %v, value: %v from %d\n", ck.me, reply.Err, reply.Value, ck.curLeader)
		ck.sequenceId++
		ck.mu.Unlock()
		ch <- reply.Value
		break
	}

}

func (ck *Clerk) SendPutAppend(key string, value string, op string, ch chan string) {
	ck.mu.Lock()
	args := PutAppendArgs{
		Key:         key,
		Value:       value,
		Op:          op,
		ClientId:    ck.me,
		SequenceNum: ck.sequenceId,
	}
	reply := PutAppendReply{}
	for {
		DPrintf("%d send %v to %d, key: %v, value: %v, sequence: %d\n", ck.me, op, ck.curLeader, args.Key, args.Value, args.SequenceNum)
		if !ck.sendPutAppend(ck.curLeader, &args, &reply) || reply.Err == ErrWrongLeader {
			DPrintf("%d receive Err: %v from %d \n", ck.me, reply.Err, ck.curLeader)
			ck.curLeader = (ck.curLeader + 1) % len(ck.servers)
			continue
		}
		DPrintf("Call succeed, %d receive Err: %v from %d \n", ck.me, reply.Err, ck.curLeader)
		ck.sequenceId++
		ck.mu.Unlock()
		ch <- ""
		break
	}
}

func (ck *Clerk) sendGet(server int, args *GetArgs, reply *GetReply) bool {
	ok := ck.servers[server].Call("KVServer.Get", args, reply)
	return ok
}

func (ck *Clerk) sendPutAppend(server int, args *PutAppendArgs, reply *PutAppendReply) bool {
	ok := ck.servers[server].Call("KVServer.PutAppend", args, reply)
	return ok
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	ch := make(chan string)
	go ck.SendGet(key, ch)
	return <-ch
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ch := make(chan string)
	//A Clerk sometimes doesn't know which kvserver is the Raft leader.
	//If the Clerk sends an RPC to the wrong kvserver, or if it cannot reach the kvserver,
	//the Clerk should re-try by sending to a different kvserver.
	go ck.SendPutAppend(key, value, op, ch)
	<-ch
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

// 需要想明白一个问题：如何保证一个操作只执行一次？请求中带一个编号，
