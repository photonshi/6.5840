package shardkv

import "6.5840/shardctrler"

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrWrongConfig = "ErrWrongConfig"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientID   int64
	RequestNum int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientID   int64
	RequestNum int
}

type GetReply struct {
	Err   Err
	Value string
}

type MigrateShardRPC struct {
	GID           int                // requester GID
	Shards        []int              // list of Shards I need KV for
	ConfigNum     int                // config num in shardcontroller, used for detecting stale requests
	RequestNum    int                // RequestNum, used for duplicate detection
	RequestConfig shardctrler.Config // what i think the mapping is, used for debug
}

type MigrateShardReply struct {
	GID       int
	Err       Err
	ConfigNum int                       // config num in shardcontroller, used for detecting stale requests
	KVTable   map[int]map[string]string // map of shard to key & value
	DupTable  map[int]map[int64]DupTableEntry
}
