package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
)

type Master struct {
	// Your definitions here.
	nMap    int
	nReduce int
	nMapRemain int
	nReduceRemain int
	mapTasks   []string
	reduceTasks []int
	workers	[]int
	mapTaskStates map[string]TaskStateType
	reduceTaskStates map[int]TaskStateType
	workerStates map[int]WorkerStateType
	bigLock sync.Mutex
}

type TaskStateType int32
const (
	TaskState_Unassigned TaskStateType = 0
	TaskState_Running TaskStateType = 1
	TaskState_Completed TaskStateType = 2
)

type WorkerStateType int32
const (
	WorkerState_Idle WorkerStateType = 0
	WorkerState_Runing WorkerStateType = 1
	WorkerState_Crashed WorkerStateType = 2
)

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Master) AssignTask(args *AssignTaskArgs, reply *AssignTaskReply) error {
	workerPid := args.WorkerPid
	m.bigLock.Lock()
	defer m.bigLock.Unlock()
	// Map tasks not finished
	if m.nMapRemain != 0 {
		// Find an unassigned map task
		for task, taskState := range m.mapTaskStates {
			if taskState == TaskState_Unassigned {
				log.Printf("Assign map task %s to worker %d\n", task, workerPid)
				reply.TaskName = task
				reply.TaskType = "map"
				m.mapTaskStates[task] = TaskState_Running
				m.workerStates[workerPid] = WorkerState_Runing
				return nil
			}
		}
		// All tasks are assigned, let the worker wait
		log.Printf("Assign waiting task to worker %d\n", workerPid)
		reply.TaskName = ""
		reply.TaskType = "wait"
		m.workerStates[workerPid] = WorkerState_Idle
		return nil
	} else if m.nReduceRemain != 0 {
		// Find an unassigned reduce task
		for task, taskState := range m.reduceTaskStates {
			if taskState == TaskState_Unassigned {
				log.Printf("Assign reduce task %d to worker %d\n", task, workerPid)
				reply.TaskName = strconv.Itoa(task)
				reply.TaskType = "reduce"
				m.reduceTaskStates[task] = TaskState_Running
				m.workerStates[workerPid] = WorkerState_Runing
				return nil
			}
		}
		log.Printf("Assign waiting task to worker %d\n", workerPid)
		reply.TaskName = ""
		reply.TaskType = "wait"
		m.workerStates[workerPid] = WorkerState_Idle
		return nil
	} else {
		log.Printf("Assign exiting task to worker %d\n", workerPid)
		reply.TaskName = ""
		reply.TaskType = "exit"
		m.workerStates[workerPid] = WorkerState_Idle
		return nil
	}
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
	ret := false

	// Your code here.

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
	log.SetFlags(log.Lmicroseconds)
	
	m.bigLock.Lock()
	m.nReduce = nReduce
	m.nMap = len(files)
	m.nMapRemain = m.nMap
	copy(files, m.mapTasks)
	m.mapTaskStates = make(map[string]TaskStateType)
	m.reduceTaskStates = make(map[int]TaskStateType)
	m.workerStates = make(map[int]WorkerStateType)
	for _, task := range files {
		m.mapTaskStates[task] = TaskState_Unassigned
	}
	for i := 0; i < nReduce; i++ {
		m.reduceTaskStates[i] = TaskState_Unassigned
	}
	m.bigLock.Unlock()
	
	m.server()
	return &m
}
