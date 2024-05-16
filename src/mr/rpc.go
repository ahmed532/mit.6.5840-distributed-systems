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

type AskForFileArgs struct {
	Dummy int
}

type AskForFileReply struct {
	Name     string
	State    int
	ReducerI int
}

type GetNReduceArgs struct {
	Dummy int
}

type GetNReduceReply struct {
	N int
}

type CommitFileArgs struct {
	Name string
}

type CommitFileReply struct {
	Dummy int
}

type CommitOutputArgs struct {
	I int
}

type CommitOutputReply struct {
	Dummy int
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
