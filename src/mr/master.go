package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	Unstarted = 0
	Working   = 1
	Finished  = 2
	Mapping   = 3
	Reducing  = 4
)

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	log.SetFlags(log.Lmicroseconds)
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Master struct {
	// Your definitions here.
	stateMutex  sync.Mutex
	jobState    int
	nReduce     int
	mapMutex    sync.Mutex
	reduceMutex sync.Mutex
	mapTable    map[string]int
	reduceTable map[int]int
	inputFiles  []string
}

func (m *Master) MapTaskSolved(tasknumber int) {
	defer m.mapMutex.Unlock()
	defer m.stateMutex.Unlock()
	m.mapMutex.Lock()
	m.stateMutex.Lock()
	m.mapTable[m.inputFiles[tasknumber]] = Finished
	flag := true
	for _, file := range m.inputFiles {
		if m.mapTable[file] != Finished {
			flag = false
			break
		}
	}
	if flag {
		m.jobState = Reducing
	}
	DPrintf("MapTaskSolved %v", tasknumber)
}
func (m *Master) ReduceTaskSolved(tasknumber int) {
	defer m.reduceMutex.Unlock()
	defer m.stateMutex.Unlock()
	m.reduceMutex.Lock()
	m.stateMutex.Lock()
	m.reduceTable[tasknumber] = Finished
	flag := true
	for i := 0; i < m.nReduce; i++ {
		if m.reduceTable[i] != Finished {
			flag = false
			break
		}
	}
	if flag {
		m.jobState = Finished
	}
	DPrintf("ReduceTaskSolved %v", tasknumber)
}
func (m *Master) AllocateMapTask() int {
	defer m.mapMutex.Unlock()
	m.mapMutex.Lock()
	for i, file := range m.inputFiles {
		if m.mapTable[file] == Unstarted {
			m.mapTable[file] = Working
			DPrintf("AllocateMapTask %v", i)
			return i
		}
	}
	return -1
}
func (m *Master) AllocateReduceTask() int {
	defer m.reduceMutex.Unlock()
	m.reduceMutex.Lock()
	for i := 0; i < m.nReduce; i++ {
		if m.reduceTable[i] == Unstarted {
			m.reduceTable[i] = Working
			DPrintf("AllocateReduceTask %v", i)
			return i
		}
	}
	return -1
}
func (m *Master) CheckMapTask(tasknumber int) {
	time.Sleep(10 * time.Second)
	defer m.mapMutex.Unlock()
	m.mapMutex.Lock()
	if m.mapTable[m.inputFiles[tasknumber]] == Working {
		m.mapTable[m.inputFiles[tasknumber]] = Unstarted
		DPrintf("MapTask not finished %v", tasknumber)
	}
}
func (m *Master) CheckReduceTask(tasknumber int) {
	time.Sleep(10 * time.Second)
	defer m.reduceMutex.Unlock()
	m.reduceMutex.Lock()
	if m.reduceTable[tasknumber] == Working {
		m.reduceTable[tasknumber] = Unstarted
		DPrintf("ReduceTask not finished %v", tasknumber)
	}
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) Request(args *RequestArgs, reply *RequestReply) error {
	switch args.Info {
	case Aak_Task:
		m.stateMutex.Lock()
		switch m.jobState {
		case Mapping:
			tasknumber := m.AllocateMapTask()
			if tasknumber != -1 {
				reply.Info = Map_Task
				reply.Filename = m.inputFiles[tasknumber]
				reply.Tasknumber = tasknumber
				reply.Nreduce = m.nReduce
				go m.CheckMapTask(tasknumber)
			} else {
				reply.Info = Wait_Task
			}
		case Reducing:
			tasknumber := m.AllocateReduceTask()
			if tasknumber != -1 {
				reply.Info = Reduce_Task
				reply.Tasknumber = tasknumber
				reply.Nmap = len(m.inputFiles)
				go m.CheckReduceTask(tasknumber)
			} else {
				reply.Info = Wait_Task
			}
		case Finished:
			reply.Info = Exit_Task
		}
		m.stateMutex.Unlock()
	case Complete_Task:
		switch args.Taskkind {
		case Map_Task:
			m.MapTaskSolved(args.Tasknumber)
			reply.Info = Wait_Task
		case Reduce_Task:
			m.ReduceTaskSolved(args.Tasknumber)
			reply.Info = Wait_Task
		}
	}
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	defer m.stateMutex.Unlock()
	m.stateMutex.Lock()
	ret := false
	// Your code here.
	if m.jobState == Finished {
		ret = true
	}
	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	DPrintf("Make a Master %v", os.Getpid())
	m := Master{}
	m.nReduce = nReduce
	m.inputFiles = files
	m.jobState = Mapping
	m.mapTable = make(map[string]int)
	m.reduceTable = make(map[int]int)
	for _, file := range m.inputFiles {
		m.mapTable[file] = Unstarted
	}
	for i := 0; i < m.nReduce; i++ {
		m.reduceTable[i] = Unstarted
	}
	// Your code here.
	m.server()
	return &m
}
