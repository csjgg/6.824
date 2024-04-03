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

type TaskType int

const (
	Map TaskType = iota
	Reduce
	End
)

// rpc args and reply types.
type Args struct {
	Tasknum int
	Succeed bool
}

type Reply struct {
	Task      TaskType
	Filename  string
	Reducenum int // for a mapper, this will be the Nreduce; for a Reducer, this will be the num of reduce
	Mappernum int // same as Reduce
	Tasknum   int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
