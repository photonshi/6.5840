package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	clientId   int64
	requestNum int
	leader     int // is this necessary?
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
	// Your code here.
	ck.clientId = nrand()
	ck.requestNum = 0
	ck.leader = 0
	return ck
}

func (ck *Clerk) Query(num int) Config {

	// Your code here.
	arg := QueryArgs{
		Num:        num,
		ClientID:   ck.clientId,
		RequestNum: ck.requestNum,
	}

	ck.requestNum += 1

	for {
		reply := QueryReply{}

		server := ck.servers[ck.leader]
		ok := server.Call("ShardCtrler.Query", &arg, &reply)
		if ok && reply.Err == OK && !reply.WrongLeader {
			// if everything checks out, return config
			return reply.Config
		} else {
			// otherwise, increment leader and try again
			ck.leader = (ck.leader + 1) % len(ck.servers)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	// Your code here.
	arg := JoinArgs{
		Servers:    servers,
		ClientID:   ck.clientId,
		RequestNum: ck.requestNum,
	}

	ck.requestNum += 1

	for {
		reply := JoinReply{}
		server := ck.servers[ck.leader]
		ok := server.Call("ShardCtrler.Join", &arg, &reply)
		if ok && reply.Err == OK && !reply.WrongLeader {
			// if everything checks out, return
			return
		} else {
			// otherwise, increment leader and try again
			ck.leader = (ck.leader + 1) % len(ck.servers)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	arg := LeaveArgs{
		GIDs:       gids,
		ClientID:   ck.clientId,
		RequestNum: ck.requestNum,
	}

	ck.requestNum += 1
	for {
		reply := LeaveReply{}
		server := ck.servers[ck.leader]
		ok := server.Call("ShardCtrler.Leave", &arg, &reply)
		if ok && reply.Err == OK && !reply.WrongLeader {
			// if everything checks out, return
			return
		} else {
			// otherwise, increment leader and try again
			ck.leader = (ck.leader + 1) % len(ck.servers)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	arg := MoveArgs{
		GID:        gid,
		Shard:      shard,
		ClientID:   ck.clientId,
		RequestNum: ck.requestNum,
	}

	ck.requestNum += 1

	for {
		reply := MoveReply{}
		server := ck.servers[ck.leader]
		ok := server.Call("ShardCtrler.Move", &arg, &reply)
		if ok && reply.Err == OK && !reply.WrongLeader {
			// if everything checks out, return
			return
		} else {
			// otherwise, increment leader and try again
			ck.leader = (ck.leader + 1) % len(ck.servers)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
