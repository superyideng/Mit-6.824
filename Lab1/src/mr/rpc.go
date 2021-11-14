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

type WorkerRequestTask struct {

}

type MasterReplyTask struct {
	TaskType string // 0 for Map Task and 1 for Reduce Task

	FileName string
	TotalReduce int
	CurMapNum int

	CurReduceId int
	TotalMap int
}

type WorkerSubmitTask struct {
	FileName string
	ReduceId int
}

type MasterAckSubmission struct {

}


// Add your RPC definitions here.


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}