package shardctrler

import (
	"fmt"
	"sync"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

type DupTableEntry struct {
	Result Config
	ReqNum int
	Error  Err
}

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	configs     []Config                // indexed by config num
	dupTable    map[int64]DupTableEntry // maps client ID to duptable entry for duplication and waiting
	lastApplied int                     // index of last applied, set in ticker
	gidToShard  map[int][]int           // latest config's mapping of gid to shard id
}

type Op struct {
	// your data here
	ClientID   int64
	RequestNum int
	Command    string // join, leave, move, query

	QueryNum    int              // desired config number
	JoinServers map[int][]string // new GID -> servers mappings
	LeaveGIDs   []int            //list of GIDs of previously joined groups
	MoveShard   int              // shard to move
	MoveGID     int              // new GID to create & move shard to
}

func (sc *ShardCtrler) stallAndCheck(op Op) bool {

	index, _, isLeader := sc.rf.Start(op)

	if !isLeader {
		// fmt.Printf("1!\n")
		return false
	}

	for {
		sc.mu.Lock()
		lastApplied := sc.lastApplied
		_, stillLeader := sc.rf.GetState()
		sc.mu.Unlock()

		if !stillLeader {
			// fmt.Printf("2!\n")
			return false
		}

		if index <= lastApplied {
			sc.mu.Lock()
			dupEntry, ok := sc.dupTable[op.ClientID]
			sc.mu.Unlock()

			if ok {
				// if duplicate table's requestNumber matches, then return true
				// otherwise, leader has changed and return false
				out := dupEntry.ReqNum == op.RequestNum
				return out
			}
		}
	}
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	entry := Op{
		ClientID:    args.ClientID,
		RequestNum:  args.RequestNum,
		Command:     "Join",
		JoinServers: args.Servers,
	}

	ok := sc.stallAndCheck(entry)

	sc.mu.Lock()
	defer sc.mu.Unlock()

	if !ok {
		reply.WrongLeader = true
		return
	}

	reply.Err = OK
	reply.WrongLeader = false
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	// fmt.Printf("Got Leave! ------------\n")
	entry := Op{
		ClientID:   args.ClientID,
		RequestNum: args.RequestNum,
		Command:    "Leave",
		LeaveGIDs:  args.GIDs,
	}

	ok := sc.stallAndCheck(entry)

	sc.mu.Lock()
	defer sc.mu.Unlock()

	if !ok {
		reply.WrongLeader = true
		return
	}

	reply.Err = OK
	reply.WrongLeader = false
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	// fmt.Printf("Got Move! ------------\n")
	entry := Op{
		ClientID:   args.ClientID,
		RequestNum: args.RequestNum,
		Command:    "Move",
		MoveShard:  args.Shard,
		MoveGID:    args.GID,
	}

	ok := sc.stallAndCheck(entry)

	sc.mu.Lock()
	defer sc.mu.Unlock()

	if !ok {
		reply.WrongLeader = true
		return
	}

	reply.Err = OK
	reply.WrongLeader = false
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	entry := Op{
		ClientID:   args.ClientID,
		RequestNum: args.RequestNum,
		Command:    "Query",
		QueryNum:   args.Num,
	}

	ok := sc.stallAndCheck(entry)

	sc.mu.Lock()
	defer sc.mu.Unlock()

	if !ok {
		reply.WrongLeader = true
		return
	}

	reply.Err = OK
	reply.WrongLeader = false
	reply.Config = sc.dupTable[args.ClientID].Result
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func (sc *ShardCtrler) noDups(op Op) bool {
	entry, ok := sc.dupTable[op.ClientID]
	lastReqNum := entry.ReqNum
	if ok {
		return lastReqNum < op.RequestNum
	}
	return true
}

func (sc *ShardCtrler) getMaxAndMin(mapping map[int][]int) (int, int, int, int) {
	// helper function called by rebalanceShards
	// input: gid -> list of Shards
	// output: gidMax, max Shards, gidMin, min Shards

	gidMax := -1
	maxShards := 0
	gidMin := -1
	minShards := NShards + 1

	if len(mapping) == 0 {
		return 0, 0, 0, 0
	}

	for gid, shardList := range mapping {
		if len(shardList) >= maxShards {
			if len(shardList) > maxShards || (len(shardList) == maxShards && gid > gidMax) {
				gidMax = gid
				maxShards = len(shardList)
			}
		}
		if gidMin == -1 || len(shardList) < minShards || (len(shardList) == minShards && gid < gidMin) {
			gidMin = gid
			minShards = len(shardList)
		}
	}
	if maxShards == minShards {
		// All shards are of equal length, return smallest gid val
		minGid := sc.findMinKey(mapping)
		return minGid, len(mapping[minGid]), minGid, len(mapping[minGid])
	}
	return gidMax, maxShards, gidMin, minShards
}

func (sc *ShardCtrler) findMinKey(m map[int][]int) int {
	// helper function called by getMaxandMin to return smallest gid
	// in gidToShards map in case everything is equal
	var minKey int
	for key := range m {
		if minKey == 0 || key < minKey {
			minKey = key
		}
	}
	return minKey
}

func (sc *ShardCtrler) rebalanceShards(lastConfig Config, servers []int, join bool) [NShards]int {
	// helper function that rebalances shards given mapping of new gid to shard
	// such that division is as even as possible and moves as few shards as possible
	// if join, then used for doJoin. Otherwise, used for doLeave
	// called by both makeShards and removeShards, outputs new mapping of shard to gid

	newShardMap := lastConfig.Shards // output

	// map gids to list of shards in lastConfig
	gidToShard := make(map[int][]int)
	// first make entry for every group
	for gid := range lastConfig.Groups {
		gidToShard[gid] = make([]int, 0)
	}
	for shard, gid := range lastConfig.Shards {
		if gid != 0 {
			// if shard is assigned, add 1 to assgined gid in gidToShard
			gidToShard[gid] = append(gidToShard[gid], shard)
		}
	}

	// add or remove gids from gidToShard based on join
	if join {
		for _, gid := range servers {
			gidToShard[gid] = make([]int, 0)
		}
	} else {
		for _, gid := range servers {
			// first modify newShardMap by setting the shard's gid to 0
			for _, shard := range gidToShard[gid] {
				newShardMap[shard] = 0
			}
			// then remove that gid from my gidToShard mapping
			delete(gidToShard, gid)
		}
	}

	// first go through all the shards and assign each unassigned to the min gid each time
	for i := 0; i < NShards; i++ {
		if newShardMap[i] == 0 {
			_, _, gidMin, _ := sc.getMaxAndMin(gidToShard)
			newShardMap[i] = gidMin
			// modify gidToShard
			gidToShard[gidMin] = append(gidToShard[gidMin], i)
		}
	}

	// then while loop to make sure the diff between max & min is at most 1
	gidMax, maxShards, gidMin, minShards := sc.getMaxAndMin(gidToShard)
	for maxShards-minShards > 1 {
		// in each round, give first shard in gidMax to minGid in newShardMap
		// and remove that shard in gidMax & add that shard in gidMIn in gidToShard
		firstShard := gidToShard[gidMax][0]
		newShardMap[firstShard] = gidMin

		gidToShard[gidMax] = gidToShard[gidMax][1:]
		gidToShard[gidMin] = append(gidToShard[gidMin], firstShard)

		// recalculate values
		gidMax, maxShards, gidMin, minShards = sc.getMaxAndMin(gidToShard)
	}

	return newShardMap
}

func (sc *ShardCtrler) deepCopyGroups(groups map[int][]string) map[int][]string {
	// creates a deep copy of groups
	// called by doquery, dojoin, doleave, and domove

	out := make(map[int][]string)

	for gid, group := range groups {
		out[gid] = group
	}

	return out
}

func (sc *ShardCtrler) doQuery(op Op) Config {
	// either returns config at configNum
	// or returns config[len(config)-1]
	queryNum := op.QueryNum
	if queryNum == -1 || (queryNum > len(sc.configs)-1) {
		return sc.configs[len(sc.configs)-1]
	}
	return sc.configs[queryNum]
}

func (sc *ShardCtrler) doJoin(op Op) {
	// create new config with mapping of gid to lists of server names
	// from mapping make shards such that division is as even as possible
	// and moves as few shards as possible

	lastConfig := sc.configs[len(sc.configs)-1]
	servers := op.JoinServers
	var joinGids []int
	newConfig := Config{
		Num:    lastConfig.Num + 1,
		Groups: sc.deepCopyGroups(lastConfig.Groups),
	}
	// then add new servers
	for gid, server := range servers {
		newConfig.Groups[gid] = server
		joinGids = append(joinGids, gid)
	}

	// then add new shards
	newConfig.Shards = sc.rebalanceShards(lastConfig, joinGids, true)

	// then add new config to sc.configs
	sc.configs = append(sc.configs, newConfig)

}

func (sc *ShardCtrler) doLeave(op Op) {
	// leave arg is a list of GIDs of previously joined groups

	// create a new configuration that does not include those group
	// The new configuration should divide the shards as evenly
	// among the groups, and should move as few shards to achieve that goal.

	leaveGIDs := op.LeaveGIDs
	lastConfig := sc.configs[len(sc.configs)-1]
	newConfig := Config{
		Num: lastConfig.Num + 1,
	}

	// first, remove leaveGIDs from lastConfig.Groups
	newGroup := sc.deepCopyGroups(lastConfig.Groups)
	for _, gid := range leaveGIDs {
		delete(newGroup, gid)
	}
	newConfig.Groups = newGroup

	// then remove and rebalance shard assignment
	newConfig.Shards = sc.rebalanceShards(lastConfig, leaveGIDs, false)
	fmt.Printf("IN LEAVE, GROUP IS %+v\n", newConfig.Groups)

	// then add new config to sc.configs
	sc.configs = append(sc.configs, newConfig)
}

func (sc *ShardCtrler) doMove(op Op) {
	// create a new configuration
	// where shard is assigned to the group
	shard, gid := op.MoveShard, op.MoveGID

	lastConfig := sc.configs[len(sc.configs)-1]
	newShards := lastConfig.Shards
	newShards[shard] = gid

	newConfig := Config{
		Num:    lastConfig.Num + 1,
		Shards: newShards,
		Groups: sc.deepCopyGroups(lastConfig.Groups),
	}

	sc.configs = append(sc.configs, newConfig)
}

func (sc *ShardCtrler) handleOp(op Op) DupTableEntry {
	// helper function called by ticker
	// handles each action case
	// returns entry to the duptable

	dupEntry := DupTableEntry{
		ReqNum: op.RequestNum,
	}
	switch op.Command {
	case "Query":
		result := sc.doQuery(op)
		// add result to dupEntry
		dupEntry.Result = result
	case "Join":
		sc.doJoin(op)
	case "Leave":
		sc.doLeave(op)
	case "Move":
		sc.doMove(op)
	}
	return dupEntry
}

func (sc *ShardCtrler) Ticker() {
	for {
		msg := <-sc.applyCh
		sc.mu.Lock()
		if msg.SnapshotValid {
			// sc.decodeSnapshot(msg.Snapshot)
			// fmt.Printf("I got a snapshot from channel!\n")
		} else {
			op := msg.Command.(Op)
			if sc.noDups(op) {
				// fmt.Printf("msg: %+v\n", msg)
				dupEntry := sc.handleOp(op)
				// since we don't have gets now, just add
				// everything to duptable with Error = OK
				sc.dupTable[op.ClientID] = dupEntry
			}
			// set lastApplied regardless of dup or not
			sc.lastApplied = msg.CommandIndex
		}
		sc.mu.Unlock()
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.mu.Lock()
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.dupTable = make(map[int64]DupTableEntry)
	sc.lastApplied = 0
	sc.gidToShard = make(map[int][]int)
	sc.mu.Unlock()

	go sc.Ticker()
	return sc
}
