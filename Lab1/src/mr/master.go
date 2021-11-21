package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "time"

import "sync"

//import "fmt"
type Master struct {
	// Your definitions here.
	Phase string
	TotalReduce int
	TotalMap int

	MapTasks []string
	MapTaskInfo map[string]TaskInfo // Key: File, Value: TaskInfo

	ReduceTasks []int
	ReduceTaskInfo []string

	//NeedsWait bool // true means a worker needs to wait when request
	CurIndex int
}

type TaskInfo struct {
	Id int
	Status string // Three status for each map task: "queuing", "in progress" and "finished"
}

var mu sync.Mutex

func (m *Master) TaskAllocation(args *WorkerRequestTask, reply *MasterReplyTask) error {
	mu.Lock()
	defer mu.Unlock()

	if (m.Phase == "Map" && m.CurIndex == len(m.MapTasks)) || m.Phase == "Reduce" && m.CurIndex == len(m.ReduceTasks) {
		// when phase is still "Map" but have reached to the end of task queue
		reply.TaskType = "Wait"
	} else if m.Phase == "Map" {
		// allocate map task
		reply.TaskType = "Map"
		reply.CurMapNum = m.MapTaskInfo[m.MapTasks[m.CurIndex]].Id
		reply.FileName = m.MapTasks[m.CurIndex]
		reply.TotalReduce = m.TotalReduce

		// update Task Status
		tempInfo := m.MapTaskInfo[m.MapTasks[m.CurIndex]]
		tempInfo.Status = "in progress"
		m.MapTaskInfo[m.MapTasks[m.CurIndex]] = tempInfo

		// move CurIndex
		m.CurIndex += 1

		// new thread to catch out time-out task
		go m.moniterMapTimeOut(reply.FileName)

	} else {
		// after map finished, allocate reduce task
		reply.TaskType = "Reduce"
		reply.TotalMap = m.TotalMap
		reply.CurReduceId = m.ReduceTasks[m.CurIndex]

		// update Task Status
		m.ReduceTaskInfo[m.ReduceTasks[m.CurIndex]] = "in progress"

		m.CurIndex += 1

		// new thread to catch out time-out task
		go m.moniterReduceTimeOut(reply.CurReduceId)
	}

	return nil
}

func (m *Master) TaskSubmission(args *WorkerSubmitTask, reply *MasterAckSubmission) error {
	mu.Lock()
	defer mu.Unlock()

	// if master receives a Map task at Reduce phase, ignore it
	if m.Phase != args.TaskType {
		return nil
	}

	if m.Phase == "Map" {
		// update the Task status
		fileName := args.FileName

		// if this file is already "finished", then do nothing
		if m.MapTaskInfo[fileName].Status == "finished" {
			return nil
		}

		tempInfo := TaskInfo{}
		tempInfo.Id = m.MapTaskInfo[fileName].Id
		tempInfo.Status = "finished"

		m.MapTaskInfo[fileName] = tempInfo

		if m.hasMapPhaseFinished() {
			m.Phase = "Reduce"
			m.CurIndex = 0
		}
	} else {
		reduceId := args.ReduceId

		// if this task is already "finished", then do nothing
		if m.ReduceTaskInfo[reduceId] == "finished" {
			return nil
		}

		// update the Task status
		m.ReduceTaskInfo[reduceId] = "finished"

		if m.hasReducePhaseFinished() {
			m.Phase = "Finished"
			return nil
		}
	}
	return nil
}


func (m *Master) moniterMapTimeOut(filename string) {
	//mu.Lock()
	for i := 0; i < 10; i++ {
		time.Sleep(time.Second)
		if m.MapTaskInfo[filename].Status == "finished" {
			//mu.Unlock()
			return
		}
	}
	mu.Lock()

	m.MapTasks = append(m.MapTasks, filename)

	tempInfo := TaskInfo{}
	tempInfo.Id = m.MapTaskInfo[filename].Id
	tempInfo.Status = "queuing"

	m.MapTaskInfo[filename] = tempInfo
	mu.Unlock()
}

/// read map and lock
func (m *Master) moniterReduceTimeOut(id int) {
	//mu.Lock()
	for i := 0; i < 10; i++ {
		time.Sleep(time.Second)
		if m.ReduceTaskInfo[id] == "finished" {
			//mu.Unlock()
			return
		}
	}
	mu.Lock()
	m.ReduceTasks = append(m.ReduceTasks, id)
	m.ReduceTaskInfo[id] = "queuing"
	mu.Unlock()
}

func (m *Master) hasMapPhaseFinished() bool {
	for _, maptask := range m.MapTaskInfo {
		if maptask.Status != "finished" {
			return false
		}
	}
    return true
}

func (m *Master) hasReducePhaseFinished() bool {
	for _, reducetask := range m.ReduceTaskInfo {
		if reducetask != "finished" {
			return false
		}
	}
	return true
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
	mu.Lock()
	defer mu.Unlock()
	ret := m.Phase == "Finished"

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.TotalReduce = nReduce
	m.TotalMap = len(files)

	m.MapTasks = make([]string, 0)
	m.MapTaskInfo = make(map[string]TaskInfo)


	m.ReduceTasks = make([]int, 0)
	m.ReduceTaskInfo = make([]string, nReduce)

	for i, file := range files {
		m.MapTasks = append(m.MapTasks, file)

		task := TaskInfo{}
		task.Id = i
		task.Status = "queuing"
		m.MapTaskInfo[file] = task

		for i := 0; i < nReduce; i++ {
			m.ReduceTasks = append(m.ReduceTasks, i)
			m.ReduceTaskInfo[i] = "queuing"
		}
	}


	m.Phase = "Map"

	//m.NeedsWait = false
	m.CurIndex = 0

	m.server()
	return &m
}
