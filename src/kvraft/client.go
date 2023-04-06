package kvraft

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"sync"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	mu         sync.Mutex
	clientId   int64
	requestNum int
	leader     int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.mu.Lock()
	ck.servers = servers
	// You'll have to add code here.
	ck.clientId = nrand()
	ck.requestNum = 0 // initializes to 0
	ck.leader = 0
	ck.mu.Unlock()
	return ck
}

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
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	// Plan: iterate over all servers

	arg := GetArgs{}
	arg.ClientID = ck.clientId
	arg.Key = key
	arg.RequestNum = ck.requestNum
	ck.requestNum += 1
	reply := GetReply{}
	for {
		server := ck.servers[ck.leader]
		ok := server.Call("KVServer.Get", &arg, &reply)
		if ok && reply.Err == OK {
			return reply.Value
		}
		if reply.Err == ErrNoKey {
			return ""
		} else {
			ck.mu.Lock()
			ck.leader = (ck.leader + 1) % len(ck.servers)
			ck.mu.Unlock()
		}
	}
}

// }

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	// same iteration logic as Get
	// but here we just return

	arg := PutAppendArgs{}
	arg.ClientID = ck.clientId
	arg.Key = key
	arg.Value = value
	arg.Op = op // put or append
	arg.RequestNum = ck.requestNum
	ck.requestNum += 1
	reply := PutAppendReply{}

	// iterate through each server
	for {
		server := ck.servers[ck.leader]
		ok := server.Call("KVServer.PutAppend", &arg, &reply)
		if ok && reply.Err == OK {
			return
		} else {
			// update leader
			ck.mu.Lock()
			ck.leader = (ck.leader + 1) % len(ck.servers)
			ck.mu.Unlock()
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	fmt.Printf("Client put value is %v \n", value)
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	fmt.Printf("Client append value is %v \n", value)
	ck.PutAppend(key, value, "Append")
}
