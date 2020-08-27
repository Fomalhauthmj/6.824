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

	cond1       *sync.Cond
	cond2       *sync.Cond
	nextFlag    bool
	waitReply   map[int]int
	currentTerm int
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
		//DPrintf("Leader[%v] PutAppend args(%v)", kv.me, args)
		kv.cond2.L.Lock()
		kv.waitReply[index]++
		for kv.appliedOpIndex < index && term == kv.currentTerm {
			kv.cond2.Wait()
		}
		if term == kv.currentTerm && kv.SameOp(index, term) {
			reply.Err = kv.GetSavedErr(&op)
			if reply.Err != ErrNoKey {
				reply.Value = kv.GetSavedValue(&op)
			}
			//DPrintf("Leader[%v] Get reply(%v)", kv.me, reply)
		}
		kv.waitReply[index]--
		if kv.waitReply[index] == 0 {
			delete(kv.waitReply, index)
		}
		kv.nextFlag = true
		kv.cond2.L.Unlock()
		kv.cond1.Broadcast()
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
		//DPrintf("Leader[%v] PutAppend args(%v)", kv.me, args)
		kv.cond2.L.Lock()
		kv.waitReply[index]++
		for kv.appliedOpIndex < index && term == kv.currentTerm {
			kv.cond2.Wait()
		}
		if term == kv.currentTerm && kv.SameOp(index, term) {
			reply.Err = kv.GetSavedErr(&op)
			//DPrintf("Leader[%v] PutAppend reply(%v)", kv.me, reply)
		}
		kv.waitReply[index]--
		if kv.waitReply[index] == 0 {
			delete(kv.waitReply, index)
		}
		kv.nextFlag = true
		kv.cond2.L.Unlock()
		kv.cond1.Broadcast()
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
	kv.applyCh = make(chan raft.ApplyMsg, 1024)

	kv.database = make(map[string]string)
	kv.savedErr = make(map[int64]Err)
	kv.savedValue = make(map[int64]string)
	kv.clerkMaxOpId = make(map[int64]int)

	kv.waitReply = make(map[int]int)
	kv.cond1 = sync.NewCond(&kv.mu)
	kv.cond2 = sync.NewCond(&kv.mu)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	// You may need initialization code here.
	kv.ApplySnapshot(persister.ReadSnapshot())
	go kv.ReceiveApplyMsg()
	go kv.DetectTerm()
	go kv.DetectSize(persister)
	return kv
}

func (kv *KVServer) ReceiveApplyMsg() {
	for !kv.killed() {
		applyMsg := <-kv.applyCh
		kv.mu.Lock()
		kv.Apply(&applyMsg)
		_, exist := kv.waitReply[kv.appliedOpIndex]
		if exist {
			kv.nextFlag = false
			kv.cond2.Broadcast()
		} else {
			kv.nextFlag = true
		}
		kv.mu.Unlock()

		kv.cond1.L.Lock()
		for !kv.nextFlag {
			kv.cond1.Wait()
		}
		kv.cond1.L.Unlock()
	}
}

func (kv *KVServer) DuplicateOp(op *Op) bool {
	maxOpId, exist := kv.clerkMaxOpId[op.ClerkId]
	if exist && maxOpId == op.OpId {
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

func (kv *KVServer) SameOp(index, term int) bool {
	return kv.appliedOpIndex == index && kv.appliedOpTerm == term
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
		kv.ApplySnapshot(applyMsg.Snapshot)
	}
}

func (kv *KVServer) DetectTerm() {
	for !kv.killed() {
		kv.mu.Lock()
		kv.currentTerm, _ = kv.rf.GetState()
		kv.cond2.Broadcast()
		kv.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}
func (kv *KVServer) DetectSize(persister *raft.Persister) {
	if kv.maxraftstate == -1 {
		return
	}
	for !kv.killed() {
		size := persister.RaftStateSize()
		if size >= kv.maxraftstate {
			kv.rf.SaveSnapshot(kv.MakeSnapshot())
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *KVServer) MakeSnapshot() (int, int, []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
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

func (kv *KVServer) ApplySnapshot(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var Database map[string]string
	var SavedErr map[int64]Err
	var SavedValue map[int64]string
	var ClerkMaxOpId map[int64]int
	var appliedOpTerm int
	var appliedOpIndex int
	if d.Decode(&Database) != nil || d.Decode(&SavedErr) != nil || d.Decode(&SavedValue) != nil || d.Decode(&ClerkMaxOpId) != nil || d.Decode(&appliedOpTerm) != nil || d.Decode(&appliedOpIndex) != nil {
		DPrintf("[%v] restore Snapshot error", kv.me)
	} else {
		kv.database = Database
		kv.savedErr = SavedErr
		kv.savedValue = SavedValue
		kv.clerkMaxOpId = ClerkMaxOpId
		kv.appliedOpTerm = appliedOpTerm
		kv.appliedOpIndex = appliedOpIndex
		DPrintf("[%v] restore Snapshot success(appliedOpIndex:%v appliedOpTerm:%v)", kv.me, kv.appliedOpIndex, appliedOpTerm)
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
