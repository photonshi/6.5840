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

// type for when worker requests task from coordinator

type TaskRequest struct {
}

type Ack struct {
}

// type for when worker hands finished task to coordinator

type CompletedTask struct {
	MapID    int
	ReduceID int
	MapTask  bool // True if map, False otherwise
	// QUESTION: Do i need a list of file paths?? or can I just regex match for it
}

// type for coordinator's task assignment to worker

type TaskAssignment struct {
	ID       int
	Maptask  int      // 1 or 2
	Filelist []string // single string if Maptask
	// can also add how many map tasks there are (number of mappers in total)
	NReduce     int // necesary to perform modulo operation
	AllMapTasks int // how many map tasks there are
}

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
