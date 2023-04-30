package shardctrler

import (
	"6.824/raft"
	"sort"
	"sync/atomic"
	"time"
)
import "6.824/labrpc"
import "sync"
import "6.824/labgob"

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32

	// Your data here.
	configs      []Config // indexed by config num
	lastSequence map[int64]int64
	channel      map[int]chan Reply
	lastIndex    int
}

type Op struct {
	// Your data here.
	Operator    string
	ClientId    int64
	Sequence    int64
	Groups      map[int][]string // arguments for Join
	GIDS        []int            // arguments for Leave
	QueryNumber int              // arguments for Query
	GID         int              // arguments for Move
	Shard       int              // arguments for Move
}

type Reply struct {
	WrongLeader bool
	Err         Err
	Sequence    int64
}

// Join的作用是加入与GID相对应的一组服务器，shardctrler创建包含这些服务器的新配置
// 新配置将shards平均分配到不同的组中，尽可能少地移动shards
func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	Debug("S[%d] receive Join from %d, Sequence: %d\n", sc.me, args.ClientId, args.Sequence)
	// Your code here.
	Command := Op{
		"Join", args.ClientId, args.Sequence, args.Servers, nil, -1, -1, -1,
	}
	reply.WrongLeader, reply.Err = sc.ApplyCommand(args.ClientId, args.Sequence, Command)
}

// Config[1]和Config[2]的区别是什么？
// Config[1]和Config[2]的区别是，在Join了新的Groups之后，从Config[1]生成Config[2]，shards不会发生改变，
// shards中存放的是不同的shards到gid的映射，再从gid映射到servers上
// shards [10]int，是一共有10个shard，每个shards的值代表GID，对应的下标代表shard？
func (sc *ShardCtrler) executeJoin(Groups map[int][]string) Err {
	lastConfig := sc.configs[len(sc.configs)-1]
	newconfig := Config{len(sc.configs), lastConfig.Shards, deepCopy(lastConfig.Groups)}
	lastlength := len(newconfig.Groups)
	for gid, servers := range Groups {
		if _, ok := newconfig.Groups[gid]; !ok {
			newservers := make([]string, len(servers))
			copy(newservers, servers)
			newconfig.Groups[gid] = newservers
		}
	}
	Debug("Leave %d Groups, current %d Groups\n", len(newconfig.Groups)-lastlength, len(newconfig.Groups))
	// g2s[gid]的长度就是gid这个组对应的shards
	g2s := groupToShards(newconfig)
	for {
		maxGid := getMaxNumShardByGid(g2s)
		minGid := getMinNumShardByGid(g2s)
		if maxGid != 0 && len(g2s[maxGid])-len(g2s[minGid]) <= 1 {
			break
		}
		//fmt.Printf("maxGid: %d, minGid: %d, %d, %d\n", maxGid, minGid, len(g2s[maxGid]), len(g2s[minGid]))
		g2s[minGid] = append(g2s[minGid], g2s[maxGid][0])
		g2s[maxGid] = g2s[maxGid][1:]
		//fmt.Printf("maxGid: %d, minGid: %d, %d, %d\n", maxGid, minGid, len(g2s[maxGid]), len(g2s[minGid]))
	}
	var newShards [NShards]int
	for gid, shards := range g2s {
		for _, shard := range shards {
			newShards[shard] = gid
		}
	}
	newconfig.Shards = newShards
	sc.configs = append(sc.configs, newconfig)
	Debug("S[%d]'s Config has changed to: %v\n", sc.me, newconfig)
	return OK
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	Debug("S[%d] receive Leave from %d, Sequence: %d\n", sc.me, args.ClientId, args.Sequence)
	// Your code here.
	Command := Op{
		"Leave", args.ClientId, args.Sequence, nil, args.GIDs, -1, -1, -1,
	}
	reply.WrongLeader, reply.Err = sc.ApplyCommand(args.ClientId, args.Sequence, Command)
}

func (sc *ShardCtrler) executeLeave(GIDs []int) Err {
	lastConfig := sc.configs[len(sc.configs)-1]
	newConfig := Config{
		len(sc.configs), lastConfig.Shards, deepCopy(lastConfig.Groups),
	}
	lastlength := len(newConfig.Groups)
	g2s := groupToShards(newConfig)
	for _, gid := range GIDs {
		if gid == 0 {
			continue
		}
		if _, ok := newConfig.Groups[gid]; ok {
			delete(newConfig.Groups, gid)
		}
		if shards, ok := g2s[gid]; ok {
			g2s[0] = append(g2s[0], shards...)
			delete(g2s, gid)
		}
	}
	Debug("Leave %d Groups, current %d Groups\n", len(newConfig.Groups)-lastlength, len(newConfig.Groups))
	for {
		maxGid := getMaxNumShardByGid(g2s)
		minGid := getMinNumShardByGid(g2s)
		if (maxGid != 0 && len(g2s[maxGid])-len(g2s[minGid]) <= 1) || minGid == 0 {
			break
		}
		g2s[minGid] = append(g2s[minGid], g2s[maxGid][0])
		g2s[maxGid] = g2s[maxGid][1:]
	}
	var newShards [NShards]int
	for gid, shards := range g2s {
		for _, shard := range shards {
			newShards[shard] = gid
		}
	}
	newConfig.Shards = newShards
	sc.configs = append(sc.configs, newConfig)
	Debug("S[%d]'s Config has changed to: %v\n", sc.me, newConfig)
	return OK
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	Debug("S[%d] receive Move from %d, Sequence: %d\n", sc.me, args.ClientId, args.Sequence)
	// Your code here.
	Command := Op{
		"Move", args.ClientId, args.Sequence, nil, nil, -1, args.GID, args.Shard,
	}
	reply.WrongLeader, reply.Err = sc.ApplyCommand(args.ClientId, args.Sequence, Command)
}

func (sc *ShardCtrler) executeMove(GID, shard int) Err {
	lastConfig := sc.configs[len(sc.configs)-1]
	newConfig := Config{
		len(sc.configs), lastConfig.Shards, deepCopy(lastConfig.Groups),
	}
	newConfig.Shards[shard] = GID
	sc.configs = append(sc.configs, newConfig)
	Debug("S[%d]'s Config has changed to: %v\n", sc.me, newConfig)
	return OK
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	//Debug("S[%d] receive Query from %d, Sequence: %d, Num: %d\n", sc.me, args.ClientId, args.Sequence, args.Num)
	// Your code here.
	Command := Op{
		"Query", args.ClientId, args.Sequence, nil, nil, args.Num, -1, -1,
	}
	reply.WrongLeader, reply.Err = sc.ApplyCommand(args.ClientId, args.Sequence, Command)
	//Debug("com[Query] seq[%d] from c[%d] Reply: %v %s\n",
	//	args.Sequence, args.ClientId, reply.WrongLeader, reply.Err)
	if reply.WrongLeader {
		return
	}
	sc.mu.Lock()
	length := len(sc.configs)
	reply.WrongLeader, reply.Err = false, OK
	if args.Num == -1 || args.Num >= length {
		reply.Config = sc.configs[length-1]
	} else {
		reply.Config = sc.configs[args.Num]
	}
	sc.lastSequence[args.ClientId] = args.Sequence
	sc.mu.Unlock()
	//Debug("S[%d] reply Query to %d, config: %v\n", sc.me, args.ClientId, reply.Config)
	return
}

func (sc *ShardCtrler) ApplyCommand(clientId int64, sequence int64, command Op) (WrongLeader bool, Err Err) {
	if _, isLeader := sc.rf.GetState(); !isLeader {
		WrongLeader = true
	} else {
		sc.mu.Lock()
		lastSequence, ok := sc.lastSequence[clientId]
		if ok && lastSequence >= sequence {
			WrongLeader, Err = false, OK
			sc.mu.Unlock()
			Debug("L[%d] receive outdated Sequence: %d, lastSequence: %d\n", sc.me, sequence, lastSequence)
			return
		}
		sc.mu.Unlock()
		idx, _, isLeader := sc.rf.Start(command)
		if !isLeader {
			WrongLeader = true
			return
		}
		sc.mu.Lock()
		ch, ok := sc.channel[idx]
		if !ok {
			ch = make(chan Reply, 1)
			sc.channel[idx] = ch
		}
		sc.mu.Unlock()
		Debug("S[%d] wait on channel %d\n", sc.me, idx)
		select {
		case reply := <-ch:
			{
				if reply.Sequence == sequence {
					WrongLeader, Err = reply.WrongLeader, reply.Err
				} else {
					WrongLeader = true
				}
				Debug("S[%d] com[%s] seq[%d] from c[%d] Reply: %v %s, idx: %d\n",
					sc.me, command.Operator, sequence, clientId, reply.WrongLeader, reply.Err, idx)
			}
		case <-time.After(500 * time.Millisecond):
			{
				_, isLeader := sc.rf.GetState()
				WrongLeader, Err = isLeader, OK
				Debug("S[%d] wait timeout on channel %d\n", sc.me, idx)
			}
		}
		sc.mu.Lock()
		delete(sc.channel, idx)
		sc.mu.Unlock()
	}
	return
}

func (sc *ShardCtrler) apply() {
	for !sc.killed() {
		applyMsg := <-sc.applyCh
		Debug("S[%d] receive applyMsg: %d, CommandValid: %v, Command: %v\n",
			sc.me, applyMsg.CommandIndex, applyMsg.CommandValid, applyMsg.Command)
		if applyMsg.CommandIndex <= sc.lastIndex {
			continue
		} else {
			sc.lastIndex = applyMsg.CommandIndex
		}
		if applyMsg.CommandValid {
			command := (applyMsg.Command).(Op)
			sc.mu.Lock()
			lastSequence, ok := sc.lastSequence[command.ClientId]
			reply := Reply{Sequence: command.Sequence}
			if !ok || lastSequence < command.Sequence {
				switch command.Operator {
				case "Join":
					{
						reply.WrongLeader, reply.Err = false, sc.executeJoin(command.Groups)
						sc.lastSequence[command.ClientId] = command.Sequence
						break
					}
				case "Leave":
					{
						reply.WrongLeader, reply.Err = false, sc.executeLeave(command.GIDS)
						sc.lastSequence[command.ClientId] = command.Sequence
						break
					}
				case "Move":
					{
						reply.WrongLeader, reply.Err = false, sc.executeMove(command.GID, command.Shard)
						sc.lastSequence[command.ClientId] = command.Sequence
						break
					}
				case "Query":
					reply.WrongLeader, reply.Err = false, OK
				}
			} else {
				reply.WrongLeader, reply.Err = false, OK
			}
			ch, ok := sc.channel[applyMsg.CommandIndex]
			sc.mu.Unlock()
			if ok {
				//Debug("s[%d] send on channel %d : %v\n", sc.me, applyMsg.CommandIndex, reply.WrongLeader)
				ch <- reply
			} else {
				Debug("S[%d] channel %d not found\n", sc.me, applyMsg.CommandIndex)
			}
		} else {
			go sc.CleanChannel(applyMsg.SnapshotIndex)
		}
	}
}

func (sc *ShardCtrler) CleanChannel(SnapShotIndex int) {
	sc.mu.Lock()
	for idx, ch := range sc.channel {
		if idx <= SnapShotIndex {
			go func(ch chan Reply) {
				ch <- Reply{WrongLeader: true}
			}(ch)
		}
	}
	sc.mu.Unlock()
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	// Your code here, if desired.
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.lastSequence = make(map[int64]int64)
	sc.channel = make(map[int]chan Reply)
	sc.lastIndex = 0

	go sc.apply()
	return sc
}

func getMinNumShardByGid(g2s map[int][]int) int {
	gids := make([]int, 0)
	for key := range g2s {
		gids = append(gids, key)
	}
	sort.Ints(gids)
	if len(gids) == 1 {
		return 0
	}
	gids = gids[1:]
	min, index := len(g2s[gids[0]]), gids[0]
	for _, gid := range gids {
		if len(g2s[gid]) <= min {
			min, index = len(g2s[gid]), gid
		}
	}
	return index
}

func getMaxNumShardByGid(g2s map[int][]int) int {
	// gid为0表示无效配置，所有的shards都应该被分配给其他的group
	if shards, ok := g2s[0]; ok && len(shards) > 0 {
		return 0
	}
	gids := make([]int, 0)
	for key := range g2s {
		gids = append(gids, key)
	}
	sort.Ints(gids)
	max, index := -1, -1
	// 按照gid从小到大的顺序遍历gids
	for _, gid := range gids {
		if len(g2s[gid]) > max {
			max, index = len(g2s[gid]), gid
		}
	}
	return index
}

func groupToShards(config Config) map[int][]int {
	g2s := make(map[int][]int)
	for gid := range config.Groups {
		g2s[gid] = make([]int, 0)
	}
	for shardId, gid := range config.Shards {
		g2s[gid] = append(g2s[gid], shardId)
	}
	return g2s
}

func deepCopy(groups map[int][]string) map[int][]string {
	newGroups := make(map[int][]string)
	for gid, servers := range groups {
		newServers := make([]string, len(servers))
		copy(newServers, servers)
		newGroups[gid] = newServers
	}
	return newGroups
}

// 每个configuration描述了replica groups集合，
// shardctrl管理一系列带有编号的configuration
// 当configuration和replica groups的对应关系需要修改时，shardctrl创建新的configuration
// clients/server向shardctrler请求当前的配置信息

// Join RPC用于添加新的replica groups，参数是GID到服务器名的映射，shardctrler创建包含新的replica groups的新的配置文件。
//				新的配置文件应该尽可能平均划分整个replica group. 如果GID不是当前配置得一部分，则允许重用，也就是说某个GID对应的server可以先Join再Leave再Join
// Leave RPC的参数是已经加入到group中的GID列表，shardctrler创建新的不包含这些group的列表，并将这些组的分片分配给剩余的组，分片的分配应当尽可能平均。
// Move RPC参数是shard number和GID。创建新的configuration，将shard分配给对应的group。Move用于测试，Move后的Join和Leave可能会撤销Move， 由于负载均衡
// Query RPC参数是configuration number。shardctrler返回包含该number的configuration.
// 配置编号从0开始，不包含任何groups，并且所有的shards都应该分配给GID zero。
