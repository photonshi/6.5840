package kvraft

import (
	"log"
	"sync"
	"sync/atomic"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type DupTableEntry struct {
	Error     Err
	Operation Op
	ReqNum    int
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	// inclues type, client, key, and value
	Type       string
	ClientID   int64 // clientID? Do I need it?
	Key        string
	Value      string
	Command    string // get, put, append
	ID         int64  // unique identifier that serves as key in dictionary
	RequestNum int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvTable     map[string]string       // kv database
	dupTable    map[int64]DupTableEntry // maps client ID to duptable entry for duplication and waiting
	lastApplied int                     // index of last applied, set in ticker
}

func (kv *KVServer) stallAndCheck(op Op) bool {

	index, _, isLeader := kv.rf.Start(op)

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
			dupEntry, ok := kv.dupTable[op.ClientID]
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

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	entry := Op{}
	entry.Command = "Get"
	entry.Key = args.Key
	entry.ClientID = args.ClientID
	entry.RequestNum = args.RequestNum

	ok := kv.stallAndCheck(entry)
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if !ok {
		reply.Err = ErrWrongLeader
		return
	}
	reply.Err = kv.dupTable[args.ClientID].Error
	if kv.dupTable[args.ClientID].Error == OK {
		// obtain get result from dup table
		// to ensure it is linearlizable
		reply.Value = kv.dupTable[args.ClientID].Operation.Value
	}

}
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
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
	reply.Err = OK
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) handleOp(op Op) {
	if op.Command == "Put" {
		kv.kvTable[op.Key] = op.Value
	} else if op.Command == "Append" {
		kv.kvTable[op.Key] += op.Value
	}
}

func (kv *KVServer) noDups(op Op) bool {
	entry, ok := kv.dupTable[op.ClientID]
	lastReqNum := entry.ReqNum
	if ok {
		return lastReqNum < op.RequestNum
	}
	return true
}

func (kv *KVServer) Ticker() {
	for {
		msg := <-kv.applyCh
		op := msg.Command.(Op)

		kv.mu.Lock()

		if kv.noDups(op) {
			kv.handleOp(op)
			dupEntry := DupTableEntry{}
			dupEntry.ReqNum = op.RequestNum
			dupEntry.Operation = op
			dupEntry.Error = OK
			if op.Command == "Get" {
				_, ok := kv.kvTable[op.Key]
				if !ok {
					// if result for get is not in table, error is ErrNoKey
					dupEntry.Error = ErrNoKey
				} else {
					dupEntry.Operation.Value = kv.kvTable[op.Key]
				}
			}
			kv.dupTable[op.ClientID] = dupEntry
		}
		// set lastApplied regardless of dup or not
		kv.lastApplied = msg.CommandIndex
		kv.mu.Unlock()
	}
}

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
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.mu.Lock()
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.kvTable = make(map[string]string)
	kv.dupTable = make(map[int64]DupTableEntry)
	kv.lastApplied = 0
	kv.mu.Unlock()
	go kv.Ticker()
	return kv
}
