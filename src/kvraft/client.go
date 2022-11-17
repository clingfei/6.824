package kvraft

import "6.824/labrpc"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	curLeader  int
	me         int64
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
	args := GetArgs{
		Key:         key,
		ClientId:    ck.me,
		SequenceNum: ck.sequenceId,
	}
	ck.sequenceId++
	reply := GetReply{}
	// You will have to modify this function.
	//DPrintf("%d send Get to %d key: %v, sequence: %d \n", ck.me, ck.curLeader, args.Key, args.SequenceNum)
	//if ok := ck.servers[ck.curLeader].Call("KVServer.Get", &args, &reply); !ok || (ok && reply.Err != OK && reply.Err != ErrNoKey) {
	//	for i := (ck.curLeader + 1) % len(ck.servers); i != ck.curLeader; i = (i + 1) % len(ck.servers) {
	//		DPrintf("%d send Get to %d key: %v, sequence: %d \n", ck.me, i, args.Key, args.SequenceNum)
	//		if ok := ck.servers[i].Call("KVServer.Get", &args, &reply); ok && (reply.Err == OK || reply.Err == ErrNoKey) {
	//			DPrintf("%d receive Err: %v, value: %v from %d \n", ck.me, reply.Err, reply.Value, i)
	//			ck.curLeader = i
	//			return reply.Value
	//		}
	//	}
	//} else {
	//	DPrintf("%d receive Err: %v, value: %v from %d \n", ck.me, reply.Err, reply.Value, ck.curLeader)
	//	return reply.Value
	//}
	//return ""
	for i := ck.curLeader; ; i = (i + 1) % len(ck.servers) {
		DPrintf("%d send Get to %d key: %v, sequence: %d \n", ck.me, i, args.Key, args.SequenceNum)
		if ok := ck.servers[i].Call("KVServer.Get", &args, &reply); ok && (reply.Err == OK || reply.Err == ErrNoKey) {
			DPrintf("%d receive Err: %v, value: %v from %d \n", ck.me, reply.Err, reply.Value, i)
			ck.curLeader = i
			return reply.Value
		} else if ok {
			DPrintf("%d receive Err: %v from %d\n", ck.me, reply.Err, i)
		} else {
			DPrintf("No response\n")
		}

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
	args := PutAppendArgs{
		Key:         key,
		Value:       value,
		Op:          op,
		ClientId:    ck.me,
		SequenceNum: ck.sequenceId,
	}
	ck.sequenceId++
	reply := PutAppendReply{}
	//A Clerk sometimes doesn't know which kvserver is the Raft leader.
	//If the Clerk sends an RPC to the wrong kvserver, or if it cannot reach the kvserver,
	//the Clerk should re-try by sending to a different kvserver.
	// DPrintf("%d send %v to %d, key: %v, value: %v\n", ck.me, op, ck.curLeader, args.Key, args.Value)
	//if ok := ck.servers[ck.curLeader].Call("KVServer.PutAppend", &args, &reply); !ok || (ok && reply.Err != OK) {
	//	for i := (ck.curLeader + 1) % len(ck.servers); i != ck.curLeader; i = (i + 1) % len(ck.servers) {
	//		DPrintf("%d send %v to %d, key: %v, value: %v\n", ck.me, op, i, args.Key, args.Value)
	//		if ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply); ok && reply.Err == OK {
	//			ck.curLeader = i
	//			return
	//		}
	//		DPrintf("%d receive Err: %v from %d \n", ck.me, reply.Err, i)
	//	}
	//} else {
	//	DPrintf("%d receive Err: %v from %d \n", ck.me, reply.Err, ck.curLeader)
	//}
	for i := ck.curLeader; ; i = (i + 1) % len(ck.servers) {
		DPrintf("%d send %v to %d, key: %v, value: %v, sequence: %d\n", ck.me, op, i, args.Key, args.Value, args.SequenceNum)
		if ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply); ok && reply.Err == OK {
			ck.curLeader = i
			return
		}
		DPrintf("%d receive Err: %v from %d \n", ck.me, reply.Err, i)
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

// 需要想明白一个问题：如何保证一个操作只执行一次？请求中带一个编号，
