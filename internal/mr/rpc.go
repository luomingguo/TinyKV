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

// example to show how to declare the arguments
// and reply for an RPC.
//
// Add your RPC definitions here.
type AskTaskArgs struct {
	MachineID string
}

type AskTaskReply struct {
	TaskType  TaskType
	TaskKey   int32
	NumReduce int32
	FileName  string // map task
	MapNum    int32  // 总的Map任务数量，用于Reduce任务
}

type ReportTaskArgs struct {
	TaskType TaskType
	State    TaskState
	TaskKey  int32
}

type ReportTaskReply struct {
	// TODO 优化如果report任务成功，直接获取下一个任务
}

// 最小化任务状态
type TaskState int32

const (
	TaskIdle TaskState = iota
	TaskRunning
	TaskDone
	TaskFailed
)

type TaskType string

const (
	TaskWait   TaskType = "wait" // 无任务
	TaskMap    TaskType = "map"
	TaskReduce TaskType = "reduce"
	TaskExit   TaskType = "exit"
)

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
