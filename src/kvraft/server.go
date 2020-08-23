package kvraft

import (
	"bytes"
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
	ClerkId int64
	OpId    int
	OpName  string
	Key     string
	Value   string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	database       map[string]string
	savedErr       map[int64]Err
	savedValue     map[int64]string
	clerkMaxOpId   map[int64]int
	appliedOpTerm  int
	appliedOpIndex int

	isLeader bool
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		ClerkId: args.ClerkId,
		OpId:    args.OpId,
		OpName:  "Get",
		Key:     args.Key,
	}
	kv.mu.Lock()
	if kv.DuplicateOp(&op) {
		reply.Err = kv.GetSavedErr(&op)
		if reply.Err != ErrNoKey {
			reply.Value = kv.GetSavedValue(&op)
		}
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	index, term, isLeader := kv.rf.Start(op)
	reply.Err = ErrWrongLeader
	if isLeader {
		kv.mu.Lock()
		kv.isLeader = true
		kv.mu.Unlock()

		waitCount := 0
		waitLimit := 20
		for !kv.killed() {
			kv.mu.Lock()
			if !kv.isLeader {
				kv.mu.Unlock()
				return
			}

			if kv.DuplicateOp(&op) {
				if kv.SameOp(index, term) {
					reply.Err = kv.GetSavedErr(&op)
					if reply.Err != ErrNoKey {
						reply.Value = kv.GetSavedValue(&op)
					}
				} else {
					kv.isLeader = false
				}
				kv.mu.Unlock()
				return
			}
			kv.mu.Unlock()

			time.Sleep(10 * time.Millisecond)
			waitCount++
			if waitCount >= waitLimit {
				return
			}
		}
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		ClerkId: args.ClerkId,
		OpId:    args.OpId,
		OpName:  args.Op,
		Key:     args.Key,
		Value:   args.Value,
	}
	kv.mu.Lock()
	if kv.DuplicateOp(&op) {
		reply.Err = kv.GetSavedErr(&op)
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	index, term, isLeader := kv.rf.Start(op)
	reply.Err = ErrWrongLeader
	if isLeader {
		kv.mu.Lock()
		kv.isLeader = true
		kv.mu.Unlock()

		waitCount := 0
		waitLimit := 20
		for !kv.killed() {
			kv.mu.Lock()
			if !kv.isLeader {
				kv.mu.Unlock()
				return
			}

			if kv.DuplicateOp(&op) {
				if kv.SameOp(index, term) {
					reply.Err = kv.GetSavedErr(&op)
				} else {
					kv.isLeader = false
				}
				kv.mu.Unlock()
				return
			}
			kv.mu.Unlock()

			time.Sleep(10 * time.Millisecond)
			waitCount++
			if waitCount >= waitLimit {
				return
			}
		}
	}
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
	kv.database = make(map[string]string)
	kv.savedErr = make(map[int64]Err)
	kv.savedValue = make(map[int64]string)
	kv.clerkMaxOpId = make(map[int64]int)
	kv.isLeader = false
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	go kv.ReceiveApplyMsg()
	go kv.DetectRaftState(persister)
	// You may need initialization code here.
	return kv
}

func (kv *KVServer) ReceiveApplyMsg() {
	for !kv.killed() {
		applyMsg := <-kv.applyCh
		kv.mu.Lock()
		kv.Apply(&applyMsg)
		kv.mu.Unlock()
	}
}

func (kv *KVServer) DuplicateOp(op *Op) bool {
	maxOpId, exist := kv.clerkMaxOpId[op.ClerkId]
	if exist && maxOpId >= op.OpId {
		return true
	} else {
		return false
	}
}

func (kv *KVServer) GetSavedValue(op *Op) string {
	return kv.savedValue[op.ClerkId]
}

func (kv *KVServer) SetSavedValue(op *Op, value string) {
	kv.clerkMaxOpId[op.ClerkId] = op.OpId
	kv.savedValue[op.ClerkId] = value
}

func (kv *KVServer) GetSavedErr(op *Op) Err {
	return kv.savedErr[op.ClerkId]
}

func (kv *KVServer) SetSavedErr(op *Op, err Err) {
	kv.clerkMaxOpId[op.ClerkId] = op.OpId
	kv.savedErr[op.ClerkId] = err
}

func (kv *KVServer) SameOp(clerkId, term int) bool {
	return kv.appliedOpTerm == term
}

func (kv *KVServer) Apply(applyMsg *raft.ApplyMsg) {
	if applyMsg.CommandValid {
		DPrintf("[%v] apply %v", kv.me, applyMsg.CommandIndex)
		kv.appliedOpTerm = applyMsg.CommandTerm
		kv.appliedOpIndex = applyMsg.CommandIndex

		op := applyMsg.Command.(Op)
		if kv.DuplicateOp(&op) {
			return
		}
		switch op.OpName {
		case "Get":
			value, ok := kv.database[op.Key]
			if ok {
				kv.SetSavedErr(&op, OK)
				kv.SetSavedValue(&op, value)
			} else {
				kv.SetSavedErr(&op, ErrNoKey)
			}
		case "Put":
			kv.database[op.Key] = op.Value
			kv.SetSavedErr(&op, OK)
		case "Append":
			kv.database[op.Key] += op.Value
			kv.SetSavedErr(&op, OK)
		}
	} else {
		DPrintf("[%v] receive snapshot", kv.me)
		kv.ReadSnapshot(applyMsg.Snapshot)
	}
}

func (kv *KVServer) DetectRaftState(persister *raft.Persister) {
	if kv.maxraftstate == -1 {
		return
	}
	for !kv.killed() {
		raftStateSize := persister.RaftStateSize()
		if raftStateSize >= kv.maxraftstate {
			DPrintf("[%v] before SaveSnapshot:%v", kv.me, raftStateSize)
			kv.rf.SaveSnapshot(kv.MakeSnapshot())
			DPrintf("[%v] after SaveSnapshot:%v", kv.me, persister.RaftStateSize())
		}
	}
}

func (kv *KVServer) MakeSnapshot() (int, int, []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.database)
	e.Encode(kv.savedErr)
	e.Encode(kv.savedValue)
	e.Encode(kv.clerkMaxOpId)
	e.Encode(kv.appliedOpTerm)
	e.Encode(kv.appliedOpIndex)
	data := w.Bytes()
	return kv.appliedOpIndex, kv.appliedOpTerm, data
}

func (kv *KVServer) ReadSnapshot(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var Database map[string]string
	var SavedErr map[int64]Err
	var SavedValue map[int64]string
	var ClerkMaxOpId map[int64]int
	var appliedOpTerm int
	var appliedOpIndex int
	if d.Decode(&Database) != nil || d.Decode(&SavedErr) != nil || d.Decode(&SavedValue) != nil || d.Decode(&ClerkMaxOpId) != nil || d.Decode(&appliedOpTerm) != nil || d.Decode(&appliedOpIndex) != nil {
		DPrintf("[%v] snapshot Decode error", kv.me)
	} else {
		kv.database = Database
		kv.savedErr = SavedErr
		kv.savedValue = SavedValue
		kv.clerkMaxOpId = ClerkMaxOpId
		kv.appliedOpTerm = appliedOpTerm
		kv.appliedOpIndex = appliedOpIndex
		DPrintf("[%v] restore from Snapshot success(appliedOpIndex:%v appliedOpTerm:%v)", kv.me, kv.appliedOpIndex, appliedOpTerm)
	}
}

func DebugMsg(msg *raft.ApplyMsg) string {
	result := "CommandValid: " + strconv.FormatBool(msg.CommandValid) + " CommandIndex: " + strconv.Itoa(msg.CommandIndex)
	switch msg.Command.(type) {
	case Op:
		op := msg.Command.(Op)
		result += " OpName: " + op.OpName + " Key: " + op.Key + " Value: " + op.Value
	}
	return result
}
