package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

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

type GetTaskReply struct {
	Filename string
	NReduce  int
	Task     TaskType
	TaskId   int
	MapCount int
}

type ReportArgs struct {
	Task     TaskType
	WorkerId int
	FileName string
}

type ReduceReport struct {
	Rno int
}

type MapReport struct {
	Mno int
}

type UNUSED struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
