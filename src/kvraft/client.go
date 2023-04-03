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
	mu       sync.Mutex
	clientId int64
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
	// TODO: remember who is leader
	for {
		arg := GetArgs{}
		arg.ClientID = ck.clientId
		arg.Key = key
		reply := GetReply{}
		for _, server := range ck.servers {
			ok := server.Call("KVServer.Get", &arg, &reply)
			if ok && reply.Err != ErrWrongLeader {
				// design: in server, if wrong leader, set Err = "wrongLeader"
				// and if key does not exist, return '' as reply.Value in server
				return reply.Value
			}
		}

	}
}

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
	for {
		arg := PutAppendArgs{}
		arg.ClientID = ck.clientId
		arg.Key = key
		arg.Value = value
		arg.Op = op // put or append
		reply := PutAppendReply{}

		// iterate through each server
		for _, server := range ck.servers {
			ok := server.Call("KVServer.PutAppend", &arg, &reply)
			if ok && reply.Err != ErrWrongLeader {
				// TODO think about what other errs we may encounter
				return
			}
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
