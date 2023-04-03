package kvraft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

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

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	// inclues type, client, key, and value
	Type     string
	ClientID int64 // clientID? Do I need it?
	Key      string
	Value    string
	Command  string // get, put, append
	ID       int64  // unique identifier that serves as key in dictionary
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvData     map[string]string  // map of kv pairs
	OpDone     map[int64]chan int // maps Op.ID to whether it's done or not. 0 if not, 1 if yes
	replyTable map[int64]string   // maps Op.ID to reply value, used to make get operations linearlizable
	Timeout    int
}

func (kv *KVServer) IsLeader(op Op) bool {
	// helper function called by Get and PutAppend that determins what Reply.Err should be
	_, _, isLeader := kv.rf.Start(op)
	// fmt.Printf("Sending op to raft %+v \n", op)
	opChan := kv.OpDone[op.ID]
	select {
	case <-time.After(time.Duration(kv.Timeout) * time.Millisecond):
		// fmt.Printf("\n Timed out! \n")
		return false
	case out := <-opChan: // case we received something
		// fmt.Printf("\n received out! \n")
		return (out == 1) && isLeader
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// your code here
	// first construct the op to check with IsLeader
	kv.mu.Lock()
	op := Op{}
	op.ClientID = args.ClientID
	op.Command = "Get"
	op.Key = args.Key
	op.ID = nrand()
	// make channel for that op
	kv.OpDone[op.ID] = make(chan int)
	kv.mu.Unlock()

	rightLeader := kv.IsLeader(op)

	kv.mu.Lock()
	defer kv.mu.Unlock()
	if rightLeader && kv.replyTable[op.ID] != "no key" {
		reply.Err = OK
		reply.Value = kv.replyTable[op.ID]
		return
	} else if !rightLeader {
		reply.Err = ErrWrongLeader
		return
	} else if kv.replyTable[op.ID] == "no key" {
		reply.Err = ErrNoKey
		return
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// first construct the op to check with IsLeader
	kv.mu.Lock()
	op := Op{}
	op.ClientID = args.ClientID
	op.Command = args.Op
	op.Key = args.Key
	op.Value = args.Value
	op.ID = nrand()
	// make channel for that op
	kv.OpDone[op.ID] = make(chan int)
	kv.mu.Unlock()

	rightLeader := kv.IsLeader(op)

	kv.mu.Lock()
	defer kv.mu.Unlock()
	if rightLeader {
		reply.Err = OK
		return
	} else {
		reply.Err = ErrWrongLeader
		return
	}
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

func (kv *KVServer) HandleOp(op Op) {
	// helper function that is called by Ticker
	// fills in kvData mapping in accordance with raft commits
	// TODO figure out what should be done on "get"

	kv.mu.Lock()
	defer kv.mu.Unlock()
	commandType := op.Command
	// fmt.Printf("\n got op %+v \n\n", op)
	// first, set opDict to true for a given op

	if commandType == "Get" {
		// modify reply table
		_, ok := kv.kvData[op.Key]
		if ok {
			kv.replyTable[op.ID] = kv.kvData[op.Key]
		} else {
			kv.replyTable[op.ID] = "no key"
		}

	} else if commandType == "Put" {
		// fmt.Printf("got put!\n")
		kv.kvData[op.Key] = op.Value
	} else if commandType == "Append" {
		// fmt.Printf("got append!\n")
		v, ok := kv.kvData[op.Key]
		if ok {
			kv.kvData[op.Key] = v + op.Value
		} else {
			kv.kvData[op.Key] = op.Value
		}
	}
	// fmt.Printf("----------------\nKV table is now %+v\n --------------------", kv.kvData)
	// fmt.Printf("----------------\nget table is now %+v\n --------------------", kv.replyTable)
}

func (kv *KVServer) Ticker() {
	// Perpetual ticker that pulls applyMsg from raft
	for {
		applyMsg := <-kv.applyCh
		op := applyMsg.Command.(Op)
		// print for debug --------------
		// fmt.Printf("Op is %+v\n", op)
		// ------------------------------
		kv.HandleOp(op)
		opChan, ok := kv.OpDone[op.ID]
		// notify that i received this op back
		if ok {
			opChan <- 1
		} // TODO otherwise what should I do here?
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
	kv.mu.Lock()
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.kvData = make(map[string]string)
	kv.OpDone = make(map[int64]chan int)
	kv.replyTable = make(map[int64]string)
	kv.Timeout = 1000
	kv.mu.Unlock()

	// You may need initialization code here.
	go kv.Ticker()
	return kv
}
