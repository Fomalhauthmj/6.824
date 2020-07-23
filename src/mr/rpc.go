package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
/*
worker:
	ask for a task

	complete a task
		args: Taskkind Tasknumber
master:
	map task
		args: Tasknumber Filename Nreduce

	reduce task
		args:  Tasknumber Nmap

	exit task

	wait task
*/
const (
	Aak_Task      = 0
	Complete_Task = 1
	Map_Task      = 2
	Reduce_Task   = 3
	Exit_Task     = 4
	Wait_Task     = 5
)

type RequestArgs struct {
	Info       int
	Taskkind   int
	Tasknumber int
}
type RequestReply struct {
	Info       int
	Filename   string
	Nreduce    int
	Nmap       int
	Tasknumber int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
