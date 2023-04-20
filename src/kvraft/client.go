package kvraft

import (
	"6.824/labrpc"
	"sync"
	"time"
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
	args := &GetArgs{key, ck.me, ck.sequenceId}
	ck.sequenceId++
	for {
		for _, srv := range ck.servers {
			var reply GetReply
			ok := srv.Call("KVServer.Get", args, &reply)
			if ok && reply.Err != ErrWrongLeader {
				DPrintf("C[%d]'s Get sed[%d] receive response\n", ck.me, ck.sequenceId)
				return reply.Value
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
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
	args := &PutAppendArgs{key, value, op, ck.me, ck.sequenceId}
	ck.sequenceId++
	//A Clerk sometimes doesn't know which kvserver is the Raft leader.
	//If the Clerk sends an RPC to the wrong kvserver, or if it cannot reach the kvserver,
	//the Clerk should re-try by sending to a different kvserver.
	for {
		for _, srv := range ck.servers {
			var reply PutAppendReply
			ok := srv.Call("KVServer.PutAppend", args, &reply)
			if ok && reply.Err != ErrWrongLeader {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

// 需要想明白一个问题：如何保证一个操作只执行一次？请求中带一个编号，
