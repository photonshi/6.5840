package shardkv

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientID   int64
	RequestNum int
	Command    string // get, put, append, config
	Key        string
	Value      string
	Config     shardctrler.Config
	// tentative design
	// MigrateConfigData []MigrateDataEntry
	// MigrateConfigNum  int
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
	config        shardctrler.Config
	mck           *shardctrler.Clerk
	dupTable      map[int]map[int64]DupTableEntry // maps NShard to map of client id to entry
	kvTable       map[int]map[string]string       // maps NShards to map of key to value
	lastApplied   int
	dead          int32                   // set by Kill()
	tempConfigNum int                     // this is also used to detect outdated requests
	needList      []int                   // list of shard numbers to request kv pairs from
	kvBuffer      map[string]string       // temporary buffer used by migrate shards to make op atomic
	dupBuffer     map[int64]DupTableEntry // temporary buffer used by migrate shards to make op atomic
}

type DupTableEntry struct {
	Result string
	ReqNum int
	Error  Err
}

func (kv *ShardKV) stallAndCheck(op Op) bool {

	index, _, isLeader := kv.rf.Start(op)
	shard := key2shard(op.Key)

	if !isLeader {
		return false
	}

	for {
		kv.mu.Lock()
		lastApplied := kv.lastApplied
		_, stillLeader := kv.rf.GetState()
		kv.mu.Unlock()

		if !stillLeader {
			return false
		}

		if index <= lastApplied {
			kv.mu.Lock()
			dupEntry, ok := kv.dupTable[shard][op.ClientID]
			kv.mu.Unlock()

			if ok {
				// if duplicate table's requestNumber matches, then return true
				// otherwise, leader has changed and return false
				out := dupEntry.ReqNum == op.RequestNum
				return out
			}
		}
	}
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	entry := Op{}
	entry.Command = "Get"
	entry.Key = args.Key
	entry.ClientID = args.ClientID
	entry.RequestNum = args.RequestNum
	shard := key2shard(args.Key)

	ok := kv.stallAndCheck(entry)
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if !ok {
		reply.Err = ErrWrongLeader
		return
	}
	reply.Err = kv.dupTable[shard][args.ClientID].Error
	if kv.dupTable[shard][args.ClientID].Error == OK {
		// obtain get result from dup table
		// to ensure it is linearlizable
		reply.Value = kv.dupTable[shard][args.ClientID].Result
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	entry := Op{}
	entry.Command = args.Op
	entry.Key = args.Key
	entry.Value = args.Value
	entry.ClientID = args.ClientID
	entry.RequestNum = args.RequestNum

	ok := kv.stallAndCheck(entry)
	if !ok {
		reply.Err = ErrWrongLeader
		return
	}
	shard := key2shard(args.Key)
	reply.Err = kv.dupTable[shard][args.ClientID].Error
}

// MIGRATE SHARD RPC & HANDLERS ------------------------------------------------------

func (kv *ShardKV) updateDuptable(incomingTable map[int]map[int64]DupTableEntry) {
	// helper function used by MigrateShard that keeps the duptable entry with the highest reqNum
	// for each client
	for shard, table := range incomingTable {
		kv.dupTable[shard] = table
	}
}

func (kv *ShardKV) sendMigrateShardHelper(op Op) {
	// helper function that makes args & reply for a given GID
	// by pulling all shards associated with that gid from kv.gainList
	// and calls sendMigrateShard in a loop

	kv.mu.Lock()
	gidToShards := make(map[int][]int) // used to track which shards to request from a gid

	for _, shard := range kv.needList {
		// we need to request kvs from that shard
		// so we find the corresponding GID

		latestGidHolder := kv.config.Shards[shard] // this might be incorrect
		gidToShards[latestGidHolder] = append(gidToShards[latestGidHolder], shard)

	}
	var wg sync.WaitGroup
	kv.mu.Unlock()

	// then make sendMigrateShard entries & request for each GID in gidToShard
	for gid := range gidToShards {
		arg := MigrateShardRPC{
			GID:           kv.gid,
			Shards:        gidToShards[gid],
			ConfigNum:     kv.tempConfigNum,
			RequestNum:    op.RequestNum,
			RequestConfig: kv.config,
		}

		// then call sendMigrateShard for each server
		if servers, ok := kv.config.Groups[gid]; ok {
			wg.Add(1)
			go kv.sendMigrateShard(servers, &wg, &arg)
		}
	}
	wg.Wait()
}

func (kv *ShardKV) sendMigrateShard(servers []string, wg *sync.WaitGroup, arg *MigrateShardRPC) {
	// RPC call that sends MigrateShard RPC to given gid & process reply
	// gid obtained from migrateTicker that pulls latest gid with shard
	// and shards pulled from kv.gainList with that GID

	// i might get a put operation that get deleted while this is

	defer wg.Done()

	for {
		for _, server := range servers {
			reply := MigrateShardReply{}
			srv := kv.make_end(server)
			ok := srv.Call("ShardKV.MigrateShard", arg, &reply)
			if ok && reply.Err == OK {

				kv.mu.Lock()
				defer kv.mu.Unlock()
				// add reply's data to my shard's kv
				kvList := reply.KVTable

				fmt.Printf("IN MIGRATION #################### \n %+v in GID %v \n ARGS:%+v\n REPLY: %+v\n ######################\n ", kv.me, kv.gid, arg, reply)

				// TODO need to make this operation atomic
				for shard, kvList := range kvList {
					for key, value := range kvList {
						kv.kvTable[shard][key] = value
					}
				}

				// then keep the duptable of whichever that has the largest sequence number
				kv.updateDuptable(reply.DupTable)
				return
			} else {
				fmt.Printf("MigrateShard request failed, reply is %+v\n request is %+v\n\n", reply, arg)
			}
		}
	}
}

func (kv *ShardKV) MigrateShard(arg *MigrateShardRPC, reply *MigrateShardReply) {
	// Migration handler
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if arg.ConfigNum > kv.tempConfigNum {
		fmt.Printf("%v in gid %v got Wrong config! reject! \n Arg: %+v \n Temp: %v\n", kv.me, kv.gid, arg, kv.tempConfigNum)
		reply.Err = ErrWrongConfig
		return
	}

	// then for each shard is requested, I copy my kv mapping of that shard into it
	replyMap := make(map[int]map[string]string)
	replyDupTable := make(map[int]map[int64]DupTableEntry)
	for _, shard := range arg.Shards {
		keyVal, ok := kv.kvTable[shard]
		if !ok {
			panic("got requested a shard I don't have!\n")
		}

		// otherwise copy the mapping into replyMap and dupMap
		replyMap[shard] = keyVal
		myDupForthisShard := kv.dupTable[shard]

		dupMap := make(map[int64]DupTableEntry)
		for clientID, DupEntry := range myDupForthisShard {
			dupMap[clientID] = DupEntry
		}
		replyDupTable[shard] = dupMap
	}

	reply.KVTable = replyMap
	reply.ConfigNum = kv.tempConfigNum
	reply.Err = OK
	reply.GID = kv.gid
	reply.DupTable = replyDupTable

	fmt.Printf("%v in gid %v repsonse in migrate shard is %+v\n", kv.me, kv.gid, reply)
}

// ------------------------------------------------------------------------------------

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}
func (kv *ShardKV) decodeSnapshot(snapshot []byte) {
	// helper function called by ticker, invoked when mst is a snapshot

	if snapshot == nil {
		fmt.Printf("no data in snapshot!\n")
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var kvTable map[int]map[string]string
	var dupTable map[int]map[int64]DupTableEntry
	var lastApplied int
	var config shardctrler.Config

	// snapshot contains kvTable, dupTable, and lastSnapshot
	if d.Decode(&kvTable) != nil ||
		d.Decode(&dupTable) != nil ||
		d.Decode(&lastApplied) != nil ||
		d.Decode(&config) != nil {
		fmt.Printf("error in decoding snapshot \n")
		return
	} else {
		kv.kvTable = kvTable
		kv.dupTable = dupTable
		kv.lastApplied = lastApplied
		kv.config = config
	}
}
func (kv *ShardKV) encodeSnapshot() []byte {
	// helper function that encodes kvTable, dupTable, and lastSnapshotInd to snapshot
	// called by ticker
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.kvTable)
	e.Encode(kv.dupTable)
	e.Encode(kv.lastApplied)
	e.Encode(kv.config)
	return w.Bytes()
}

func (kv *ShardKV) noDups(op Op, shard int, isShardMine bool) bool {
	// Helper function that determins whether the incoming op is a duplicate or not
	// if command type is not config, then duplication is determined by op.RequestNum
	// otherwise, duplicate is determined by configNum

	if op.Command != "Config" {
		entry, ok := kv.dupTable[shard][op.ClientID]
		lastReqNum := entry.ReqNum
		if ok {
			if entry.Error != ErrWrongGroup {
				return lastReqNum < op.RequestNum
			} else {
				// if it is a wrong group, then that means the command was not executed
				// so we should just return whether the shard is mine
				if entry.ReqNum-1 < op.RequestNum {
					return isShardMine
				} else {
					return false
				}
			}
		}
		return true
	} else {
		return op.Config.Num > kv.config.Num
	}

}

func (kv *ShardKV) handleOp(op Op, shard int) {
	switch op.Command {
	case "Put":
		kv.kvTable[shard][op.Key] = op.Value
	case "Append":
		kv.kvTable[shard][op.Key] += op.Value
	}
	fmt.Printf("%v in gid %v 's kvtable after handle op: %+v\n", kv.me, kv.gid, kv.kvTable)
}

func (kv *ShardKV) HandleConfig(op Op) {
	// set temp config num: currentConfigNum should always be 1 ahead!

	if op.Config.Num != kv.config.Num+1 {
		panic("Fucked this up!")
	}

	kv.tempConfigNum = op.Config.Num
	newShardtoGid := op.Config.Shards
	currentShardtoGid := kv.config.Shards

	for i := 0; i < shardctrler.NShards; i++ {
		newGid := newShardtoGid[i]
		oldGid := currentShardtoGid[i]
		if newGid != oldGid && oldGid != 0 && newGid == kv.gid {
			// I gained kvs associated with newGid
			// and lost kvs associated with oldGid
			kv.needList = append(kv.needList, i)
			// CONFIRM is this the only condition?
		}
	}
	fmt.Printf("%v in gid %v STARTING MIGRATION IN HANDLECONFIG! NEEDLIST IS %+v\n", kv.me, kv.gid, kv.needList)
	// call sendMigrationHelper to do reconfiguration
	kv.mu.Unlock()
	kv.sendMigrateShardHelper(op)
	kv.mu.Lock()

	// after migration, update configs
	// if I received a config for the first time, set kv.Config = config
	// and kv.previuosConfig as config
	// if (len(kv.config.Groups) == 0) && len(op.Config.Groups) != 0 {
	// 	kv.config = op.Config
	// } else {
	// 	kv.prevConfig = kv.config
	// 	kv.config = op.Config
	// }
	kv.config = op.Config

	// then I remove everything in my need list
	kv.needList = make([]int, 0)

	fmt.Printf("%v in gid %v FINISHED MIGRATION!!!!!!\n current Config: %+v \n kvtable %+v\n", kv.me, kv.gid, kv.config, kv.kvTable)
}

func (kv *ShardKV) isThisShardMine(shard int, config shardctrler.Config) bool {
	// helper function called by handleOp that is used to determine
	// whether this shard is mine or not from my current? config
	currentShardToGid := config.Shards
	return currentShardToGid[shard] == kv.gid
}

func (kv *ShardKV) ApplyChTicker() {
	for {
		msg := <-kv.applyCh
		// if msg is snapshot, apply snapshot with helper function
		// if lastApplied - lastSnapshotInd > maxRaftstate, send snapshot
		// Inside snapshot, contains everything in our database & duplicate table
		kv.mu.Lock()
		if msg.SnapshotValid {
			if msg.SnapshotIndex <= kv.lastApplied {
				panic("FUCK. Snapshot at , lastapplied is\n")
			}
			kv.decodeSnapshot(msg.Snapshot)
		}
		if msg.CommandValid {
			fmt.Printf("Command from channel is %+v\n", msg.Command)
			if msg.Command == "INIT" {
				continue
			}
			op := msg.Command.(Op)
			shard := key2shard(op.Key)
			shardIsMine := kv.isThisShardMine(shard, kv.config)
			fmt.Printf(" in apply ch ------------- \n  %v in gid %v's kvtable is %+v\n  %v in gid %v's current config: %+v \n   %v in gid %v's dupmap is %+v\n  %v in gid %v's incoming op is %+v \n  %v in gid %v's shard is %v\n------------\n", kv.me, kv.gid, kv.kvTable, kv.me, kv.gid, kv.config, kv.me, kv.gid, kv.dupTable, kv.me, kv.gid, op, kv.me, kv.gid, shard)

			if kv.noDups(op, shard, shardIsMine) {
				fmt.Printf("No dups, proceed!\n")
				// then initialize entry in duplicate table
				dupEntry := DupTableEntry{}
				dupEntry.ReqNum = op.RequestNum
				dupEntry.Error = OK

				if op.Command == "Config" {
					kv.HandleConfig(op)
				} else if shardIsMine {
					fmt.Printf("THIS SHARD IS MINE!\n")
					kv.handleOp(op, shard)
				} else {
					fmt.Printf("THIS SHARD IS NOT MINE!\n")
					dupEntry.Error = ErrWrongGroup
				}

				if op.Command == "Get" {
					_, ok := kv.kvTable[shard][op.Key]
					if !ok && dupEntry.Error != ErrWrongGroup {
						// if result for get is not in table but shard is assigned to my group
						// then the error is errNoKey
						dupEntry.Error = ErrNoKey
					} else {
						dupEntry.Result = kv.kvTable[shard][op.Key]
					}
				}
				// i add to the dupentry only if the shard is mine?
				kv.dupTable[shard][op.ClientID] = dupEntry
			} else {
				fmt.Printf("%v in gid %v has a FUCKING DUP!\n\n", kv.me, kv.gid)
			}
			// set lastApplied regardless of dup or not
			kv.lastApplied = msg.CommandIndex

		}
		// fmt.Printf("Finished apply ticker!\n")
		kv.mu.Unlock()
	}
}
func (kv *ShardKV) SnapTicker() {
	// Ticker that periodicaly goes off & checks whether a snapshot is needed
	// if so, call kv.rf.snapshot after endoding in
	for !kv.killed() {
		time.Sleep(10 * time.Millisecond)
		kv.mu.Lock()
		if kv.maxraftstate >= 0 && kv.rf.Persister.RaftStateSize() >= kv.maxraftstate {
			// call raft's snapshot
			fmt.Printf("about to call Snapshot from server %d!\n", kv.me)
			kv.rf.Snapshot(kv.lastApplied, kv.encodeSnapshot())
		}
		kv.mu.Unlock()
	}

}

func (kv *ShardKV) ConfigTicker() {
	// ticker function that contiually pulls config from shardcontroller
	// kv only updates kv.config & kv.currentConfigNum in Ticker

	for !kv.killed() {
		time.Sleep(10 * time.Millisecond)
		kv.mu.Lock()
		// pull new config from mck
		newConfig := kv.mck.Query(kv.config.Num + 1)

		// if new config is more up-to-date than my current config
		// send it through my raft
		if newConfig.Num == kv.config.Num+1 {
			configOp := Op{
				Command: "Config",
				Config:  newConfig,
			}
			_, isLeader := kv.rf.GetState()
			if isLeader {
				// update configNum to show that I am reconfiguring, but in newest config
				kv.rf.Start(configOp)
			}
		}
		// fmt.Printf("Finished config ticker\n")
		kv.mu.Unlock()
	}
}

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
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.

	labgob.Register(Op{})
	kv := new(ShardKV)
	kv.mu.Lock()
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.
	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.lastApplied = 0
	kv.kvTable = make(map[int]map[string]string)
	kv.dupTable = make(map[int]map[int64]DupTableEntry)

	for i := 0; i < shardctrler.NShards; i++ {
		kv.kvTable[i] = make(map[string]string)
		kv.dupTable[i] = make(map[int64]DupTableEntry)
	}
	kv.tempConfigNum = 0
	kv.needList = make([]int, 0)

	// initialize configs with empty
	kv.config = shardctrler.Config{}

	// instantiate buffers

	kv.mu.Unlock()

	go kv.ApplyChTicker()
	go kv.SnapTicker()
	go kv.ConfigTicker()

	return kv
}
