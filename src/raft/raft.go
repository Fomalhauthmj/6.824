package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}
type LogEntry struct {
	Command      interface{}
	ReceivedTerm int
}

//
// A Go object implementing a single Raft peer.
//
const (
	Follower  = "Follower"
	Candidate = "Candidate"
	Leader    = "Leader"
)

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//Persistent state on all servers
	currentTerm int
	votedFor    int
	log         []LogEntry
	//Volatile state on all servers
	commitIndex int
	lastApplied int
	//Volatile state on leaders
	nextIndex  []int
	matchIndex []int

	lastHeardTime time.Time
	role          string
	voteCount     int
	applyCh       chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	if rf.role == Leader {
		isleader = true
	} else {
		isleader = false
	}
	DPrintf("[%v] GetState %v %v", rf.me, term, isleader)
	rf.mu.Unlock()
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
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
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	reply.VoteGranted = false
	rf.mu.Lock()
	//valid request
	if args.Term >= rf.currentTerm {
		DPrintf("[%v] receive NEW RequestVote from [%v] at term %v", rf.me, args.CandidateId, args.Term)
		if args.Term > rf.currentTerm {
			rf.ConvertToFollower(args.Term)
		}
		if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
			logLen := len(rf.log)
			lastLogTerm := rf.currentTerm
			if logLen > 0 {
				lastLogTerm = rf.log[logLen-1].ReceivedTerm
			}
			if args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= logLen) {
				reply.VoteGranted = true
				reply.Term = rf.currentTerm
				rf.votedFor = args.CandidateId
			}
		} else {
			DPrintf("[%v] has voted for [%v]", rf.me, rf.votedFor)
		}
	} else {
		DPrintf("[%v] receive STALE RequestVote from [%v] at term %v", rf.me, args.CandidateId, args.Term)
		reply.Term = rf.currentTerm
	}
	rf.mu.Unlock()
}

//
//AppendEntries RPC
//
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	reply.Success = false
	DPrintf("Solving")
	rf.mu.Lock()
	//valid request
	if args.Term >= rf.currentTerm {
		DPrintf("[%v] receive NEW AppendEntries from [%v] at term %v", rf.me, args.LeaderId, args.Term)
		if args.Term > rf.currentTerm {
			rf.ConvertToFollower(args.Term)
		}
		rf.lastHeardTime = time.Now()
		//heartbeat or AppendEntries
		if len(rf.log) >= args.PrevLogIndex {
			if args.PrevLogIndex == 0 || rf.log[args.PrevLogIndex-1].ReceivedTerm == args.PrevLogTerm {
				DPrintf("[%v] check pass", rf.me)
				minLen := Min(len(rf.log)-args.PrevLogIndex, len(args.Entries))
				conflictFlag := false
				for i := 0; i < minLen; i++ {
					//conflict
					if rf.log[args.PrevLogIndex+i].ReceivedTerm != args.Entries[i].ReceivedTerm {
						rf.log = rf.log[:args.PrevLogIndex+i]
						rf.log = append(rf.log, args.Entries[i:]...)
						conflictFlag = true
						break
					}
				}
				if !conflictFlag && minLen < len(args.Entries) {
					rf.log = append(rf.log, args.Entries[minLen:]...)
				}
				reply.Success = true
				reply.Term = rf.currentTerm
				if args.LeaderCommit > rf.commitIndex {
					rf.commitIndex = Min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
					DPrintf("Follower[%v] new commitIndex: %v", rf.me, rf.commitIndex)
				}
			}
		}
	} else {
		DPrintf("[%v] receive STALE AppendEntries from [%v] at term %v", rf.me, args.LeaderId, args.Term)
		reply.Term = rf.currentTerm
	}
	rf.mu.Unlock()
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	DPrintf("[%v] sendRequestVote to [%v] %v", rf.me, server, args)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	DPrintf("[%v] sendAppendEntries to [%v] %v", rf.me, server, args)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role == Leader {
		DPrintf("[%v] Start %v", rf.me, command)
		rf.log = append(rf.log, LogEntry{
			Command:      command,
			ReceivedTerm: rf.currentTerm,
		})
		index = len(rf.log)
		term = rf.currentTerm
		isLeader = true
		DPrintf("[%v] log %v: %v(%v)", rf.me, index, rf.log[index-1].Command, rf.log[index-1].ReceivedTerm)
		go rf.Agreement()
	}
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.ConvertToFollower(0)
	go rf.ElectionTimer()
	go rf.ApplyChan()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

func (rf *Raft) ConvertToCandidate() {
	rf.role = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.lastHeardTime = time.Now()
	rf.voteCount = 1
}
func (rf *Raft) ConvertToLeader() {
	rf.role = Leader
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	lastLogIndex := len(rf.log)
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = lastLogIndex + 1
		rf.matchIndex[i] = 0
	}
}
func (rf *Raft) ConvertToFollower(newTerm int) {
	rf.role = Follower
	rf.currentTerm = newTerm
	rf.votedFor = -1
}

//
func (rf *Raft) Heartbeat() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.role == Leader {
			args := &AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: rf.commitIndex,
				LeaderCommit: rf.commitIndex,
			}
			if args.PrevLogIndex > 0 {
				args.PrevLogTerm = rf.log[args.PrevLogIndex-1].ReceivedTerm
			}
			for idx, _ := range rf.peers {
				if idx == rf.me {
					continue
				}
				go func(server int, args *AppendEntriesArgs) {
					rf.mu.Lock()
					if rf.role != Leader {
						rf.mu.Unlock()
						return
					}
					rf.mu.Unlock()
					reply := &AppendEntriesReply{}
					ok := rf.sendAppendEntries(server, args, reply)
					if ok {
						rf.mu.Lock()
						defer rf.mu.Unlock()
						if reply.Term > rf.currentTerm {
							rf.ConvertToFollower(reply.Term)
							return
						}
						if !reply.Success && rf.nextIndex[server] > 1 {
							rf.nextIndex[server]--
							go rf.SingleServerAgreement(server)
						}
					}
				}(idx, args)
			}
		}
		rf.mu.Unlock()
		time.Sleep(500 * time.Millisecond)
	}
}

func (rf *Raft) Election() {
	rf.mu.Lock()
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log),
		LastLogTerm:  rf.currentTerm,
	}
	if args.LastLogIndex > 0 {
		args.LastLogTerm = rf.log[args.LastLogIndex-1].ReceivedTerm
	}
	for idx, _ := range rf.peers {
		if idx == rf.me {
			continue
		}
		go func(server int, args *RequestVoteArgs) {
			rf.mu.Lock()
			if rf.role != Candidate {
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
			reply := &RequestVoteReply{}
			ok := rf.sendRequestVote(server, args, reply)
			if ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.Term > rf.currentTerm {
					rf.ConvertToFollower(reply.Term)
					return
				}
				if reply.VoteGranted {
					rf.voteCount++
					DPrintf("[%v] get vote from [%v] at term %v %v/%v", rf.me, server, args.Term, rf.voteCount, len(rf.peers))
					if rf.role == Candidate && rf.voteCount > len(rf.peers)/2 {
						rf.ConvertToLeader()
						go rf.Heartbeat()
					}
				}
			}
		}(idx, args)
	}
	rf.mu.Unlock()
}

func (rf *Raft) ElectionTimer() {
	rf.mu.Lock()
	rf.lastHeardTime = time.Now()
	rf.mu.Unlock()
	electionTime := time.Duration(500+rand.Intn(5)*30) * time.Millisecond
	for !rf.killed() {
		time.Sleep(electionTime)
		rf.mu.Lock()
		if rf.role != Leader {
			duration := time.Since(rf.lastHeardTime)
			if duration >= electionTime {
				if rf.role == Candidate || (rf.role == Follower && rf.votedFor == -1) {
					DPrintf("[%v] election timeout, start election", rf.me)
					rf.ConvertToCandidate()
					go rf.Election()
				}
				electionTime = time.Duration(500+rand.Intn(5)*30) * time.Millisecond
			}
		}
		rf.mu.Unlock()
	}
}
func (rf *Raft) ApplyChan() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.commitIndex > rf.lastApplied {
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastApplied].Command,
				CommandIndex: rf.lastApplied + 1,
			}
			rf.lastApplied++
			rf.applyCh <- applyMsg
		}
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) Agreement() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for idx, _ := range rf.peers {
		if idx == rf.me {
			continue
		}
		go rf.SingleServerAgreement(idx)
	}
}
func (rf *Raft) SingleServerAgreement(server int) {
	rf.mu.Lock()
	if rf.role != Leader || len(rf.log) < rf.nextIndex[server] {
		rf.mu.Unlock()
		return
	}
	prevLogIndex := rf.nextIndex[server] - 1
	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		LeaderCommit: rf.commitIndex,
	}
	args.Entries = make([]LogEntry, len(rf.log[prevLogIndex:]))
	copy(args.Entries, rf.log[prevLogIndex:])
	if prevLogIndex > 0 {
		args.PrevLogTerm = rf.log[prevLogIndex-1].ReceivedTerm
	}
	rf.mu.Unlock()
	reply := &AppendEntriesReply{}
	ok := rf.sendAppendEntries(server, args, reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if reply.Term > rf.currentTerm {
			rf.ConvertToFollower(reply.Term)
			return
		}
		if reply.Success {
			//update
			rf.nextIndex[server] += len(args.Entries)
			rf.matchIndex[server] = rf.nextIndex[server] - 1
			//
			N := rf.commitIndex + 1
			for N <= len(rf.log) {
				matchCount := 0
				for idx, val := range rf.matchIndex {
					if idx == rf.me || val >= N {
						matchCount++
					}
				}
				DPrintf("[%v] N: %v %v/%v", rf.me, N, matchCount, len(rf.peers))
				if matchCount > len(rf.peers)/2 {
					if rf.log[N-1].ReceivedTerm == rf.currentTerm {
						rf.commitIndex = N
						DPrintf("Leader[%v] new commitIndex: %v", rf.me, rf.commitIndex)
					}
					N++
				} else {
					break
				}
			}
		} else {
			if rf.nextIndex[server] > 1 {
				rf.nextIndex[server]--
				go rf.SingleServerAgreement(server)
			}
		}
	}
}
func Min(x, y int) int {
	if x < y {
		return x
	} else {
		return y
	}
}
