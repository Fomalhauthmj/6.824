package raft

import (
	"log"
	"strconv"
)

// Debugging
const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	log.SetFlags(log.Lmicroseconds)
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func DebugArgs(args *AppendEntriesArgs) string {
	return strconv.Itoa(args.Term) + " " + strconv.Itoa(args.PrevLogIndex) + " " + strconv.Itoa(args.PrevLogTerm) + " " + strconv.Itoa(len(args.Entries)) + " " + strconv.Itoa(args.LeaderCommit)
}

func Min(x, y int) int {
	if x < y {
		return x
	} else {
		return y
	}
}

func Max(x, y int) int {
	if x > y {
		return x
	} else {
		return y
	}
}
