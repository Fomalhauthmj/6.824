package kvraft

import (
	"crypto/rand"
	"math/big"

	"../labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leader     int
	clerkId    int64
	opId       int
	reSendFlag bool
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
	ck.leader = 0
	ck.clerkId = nrand()
	ck.opId = 0
	ck.reSendFlag = false
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
	// You will have to modify this function.
	args := GetArgs{
		ClerkId: ck.clerkId,
		Key:     key,
	}
	if !ck.reSendFlag {
		ck.opId++
	}
	args.OpId = ck.opId
	reply := GetReply{}
	ok := ck.servers[ck.leader].Call("KVServer.Get", &args, &reply)
	if ok {
		switch reply.Err {
		case OK:
			ck.reSendFlag = false
			return reply.Value
		case ErrNoKey:
			ck.reSendFlag = false
			return ""
		case ErrWrongLeader:
			ck.reSendFlag = true
			ck.leader = (ck.leader + 1) % len(ck.servers)
			return ck.Get(key)
		}
	} else {
		ck.reSendFlag = true
		ck.leader = (ck.leader + 1) % len(ck.servers)
		return ck.Get(key)
	}
	return ""
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
		ClerkId: ck.clerkId,
		Key:     key,
		Value:   value,
		Op:      op,
	}
	if !ck.reSendFlag {
		ck.opId++
	}
	args.OpId = ck.opId
	reply := PutAppendReply{}
	ok := ck.servers[ck.leader].Call("KVServer.PutAppend", &args, &reply)
	if ok {
		switch reply.Err {
		case OK:
			ck.reSendFlag = false
		case ErrWrongLeader:
			ck.reSendFlag = true
			ck.leader = (ck.leader + 1) % len(ck.servers)
			ck.PutAppend(key, value, op)
		}
	} else {
		ck.reSendFlag = true
		ck.leader = (ck.leader + 1) % len(ck.servers)
		ck.PutAppend(key, value, op)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
