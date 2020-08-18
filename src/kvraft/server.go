package kvraft

import (
	"log"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
)

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	log.SetFlags(log.Lmicroseconds)
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpId   int64
	OpName string
	Key    string
	Value  string
}
type Reply struct {
}
type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	database map[string]string
	requests map[int64]string
	applyMsg raft.ApplyMsg

	receiveFinishCond   *sync.Cond
	receiveStartCond    *sync.Cond
	receiveNextApplyMsg bool
	waitReply           int
	applyErr            Err
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	DPrintf("[%v] Get", kv.me)
	op := Op{
		OpId:   args.Id,
		OpName: "Get",
		Key:    args.Key,
	}
	index, _, isLeader := kv.rf.Start(op)
	if isLeader {
		kv.receiveFinishCond.L.Lock()
		kv.waitReply++
		for kv.applyMsg.CommandIndex < index {
			kv.receiveFinishCond.Wait()
		}
		if kv.applyMsg.Command.(Op).OpId == args.Id {
			DPrintf("[%v] match applyMsg %v", kv.me, DebugMsg(kv.applyMsg))
			reply.Err = kv.applyErr
			reply.Value = kv.database[args.Key]
		} else {
			reply.Err = ErrWrongLeader
		}
		kv.receiveNextApplyMsg = true
		kv.waitReply--
		kv.receiveFinishCond.L.Unlock()
		kv.receiveStartCond.Broadcast()
	} else {
		reply.Err = ErrWrongLeader
		DPrintf("[%v] ErrWrongLeader", kv.me)
	}
	DPrintf("[%v] Get reply", kv.me)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	DPrintf("[%v] PutAppend", kv.me)
	op := Op{
		OpId:   args.Id,
		OpName: args.Op,
		Key:    args.Key,
		Value:  args.Value,
	}
	index, _, isLeader := kv.rf.Start(op)
	if isLeader {
		kv.receiveFinishCond.L.Lock()
		kv.waitReply++
		for kv.applyMsg.CommandIndex < index {
			kv.receiveFinishCond.Wait()
		}
		if kv.applyMsg.Command.(Op).OpId == args.Id {
			DPrintf("[%v] match applyMsg %v", kv.me, DebugMsg(kv.applyMsg))
			reply.Err = kv.applyErr
		} else {
			reply.Err = ErrWrongLeader
		}
		kv.receiveNextApplyMsg = true
		kv.waitReply--
		kv.receiveFinishCond.L.Unlock()
		kv.receiveStartCond.Broadcast()
	} else {
		reply.Err = ErrWrongLeader
		DPrintf("[%v] ErrWrongLeader", kv.me)
	}
	DPrintf("[%v] PutAppend reply", kv.me)
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
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
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.database = make(map[string]string)
	kv.requests = make(map[int64]bool)
	kv.mu = sync.Mutex{}
	kv.receiveStartCond = sync.NewCond(&kv.mu)
	kv.receiveFinishCond = sync.NewCond(&kv.mu)
	kv.waitReply = 0
	kv.receiveNextApplyMsg = true
	go kv.ReceiveApplyMsg()
	// You may need initialization code here.
	return kv
}

func (kv *KVServer) ReceiveApplyMsg() {
	for !kv.killed() {
		kv.receiveStartCond.L.Lock()
		if kv.waitReply == 0 {
			kv.receiveNextApplyMsg = true
		}
		for !kv.receiveNextApplyMsg {
			DPrintf("condition false continue wait")
			kv.receiveStartCond.Wait()
		}
		kv.applyMsg = <-kv.applyCh
		DPrintf("[%v] ReceiveApplyMsg %v", kv.me, DebugMsg(kv.applyMsg))
		kv.applyErr = kv.Apply()
		kv.receiveNextApplyMsg = false
		kv.receiveStartCond.L.Unlock()
		kv.receiveFinishCond.Broadcast()
		time.Sleep(10 * time.Millisecond)
	}
}
func (kv *KVServer) Apply() Err {
	DPrintf("[%v] apply %v", kv.me, DebugMsg(kv.applyMsg))
	if kv.applyMsg.CommandValid {
		op := kv.applyMsg.Command.(Op)
		_, exist := kv.requests[op.OpId]
		if exist {
			return ErrDuplicateRequest
		} else {
			kv.requests[op.OpId] = true
		}
		switch op.OpName {
		case "Get":
			_, ok := kv.database[op.Key]
			if ok {
				return OK
			} else {
				return ErrNoKey
			}
		case "Put":
			kv.database[op.Key] = op.Value
			return OK
		case "Append":
			kv.database[op.Key] += op.Value
			return OK
		}
	}
	return OK
}
func DebugMsg(msg raft.ApplyMsg) string {
	result := "CommandValid: " + strconv.FormatBool(msg.CommandValid) + " CommandIndex: " + strconv.Itoa(msg.CommandIndex) + "\n"
	op := msg.Command.(Op)
	result += "OpName: " + op.OpName + "\n"
	result += "Key: " + op.Key + " Value: " + op.Value
	return result
}
