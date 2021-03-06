package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
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

	// for crash checking
	mapTaskBeginTime map[string]time.Time
	reduceTaskBeginTime map[int]time.Time
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
	reply.Y = args.X + args.B
	reply.C = args.X*2
	return nil
}

func (m *Master) AssignTask(args *AssignTaskArgs, reply *AssignTaskReply) error {
	workerPid := args.WorkerPid
	m.bigLock.Lock()
	defer m.bigLock.Unlock()
	reply.TaskNReduce = m.nReduce

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
				m.mapTaskBeginTime[task] = time.Now()
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
				m.reduceTaskBeginTime[task] = time.Now()
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

func (m *Master) ReportTaskComplete(args *ReportTaskCompleteArgs, reply *ReportTaskCompleteReply) error {
	// workerPid := args.WorkerPid
	taskType := args.TaskType
	taskName := args.TaskName
	m.bigLock.Lock()
	defer m.bigLock.Unlock()
	if taskType == "map" {
		state, exists := m.mapTaskStates[taskName]
		if !exists {
			reply.Msg = "Report map task that doesn't exist."
		} else if state == TaskState_Running {
			reply.Msg = "Report map task success."
			m.mapTaskStates[taskName] = TaskState_Completed
			m.nMapRemain--
			if m.nMapRemain == 0 {
				log.Printf("Map tasks Done.")
			}
		} else if state == TaskState_Unassigned {
			reply.Msg = "Report unassigned map task."
		} else if state == TaskState_Completed {
			reply.Msg = "Report map task that already completed."
		}
	} else if taskType == "reduce" {
		taskId, _ := strconv.Atoi(taskName)
		state, exists := m.reduceTaskStates[taskId]
		if !exists {
			reply.Msg = "Report reduce task that doesn't exist."
		} else if state == TaskState_Running {
			reply.Msg = "Report reduce task success."
			m.reduceTaskStates[taskId] = TaskState_Completed
			m.nReduceRemain--
			if m.nReduceRemain == 0 {
				log.Printf("Reduce tasks Done.")
			}
		} else if state == TaskState_Unassigned {
			reply.Msg = "Report unassigned reduce task."
		} else if state == TaskState_Completed {
			reply.Msg = "Report reduce task that already completed."
		}
	}
	return nil
}

func (m *Master) checkCrash() {
	for {
		log.Printf("Crash checking...")
		m.bigLock.Lock()
		for task, taskState := range m.mapTaskStates {
			if taskState == TaskState_Running && time.Since(m.mapTaskBeginTime[task]) > time.Second {
				log.Printf("Find map task %s overtime", task)
				m.mapTaskStates[task] = TaskState_Unassigned
			}
		}
		for task, taskState := range m.reduceTaskStates {
			if taskState == TaskState_Running && time.Since(m.reduceTaskBeginTime[task]) > time.Second {
				log.Printf("Find reduce task %d overtime", task)
				m.reduceTaskStates[task] = TaskState_Unassigned
			}
		}
		m.bigLock.Unlock()
		time.Sleep(10*time.Second)
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
	if m.nReduceRemain == 0 {
		ret = true
	}
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
	logFile, _ := os.Create(fmt.Sprintf("Master-%d.log", os.Getpid()))
	log.SetOutput(logFile)
	
	m.bigLock.Lock()
	m.nReduce = nReduce
	m.nMap = len(files)
	m.nMapRemain = m.nMap
	m.nReduceRemain = nReduce
	copy(files, m.mapTasks)
	m.mapTaskStates = make(map[string]TaskStateType)
	m.reduceTaskStates = make(map[int]TaskStateType)
	m.workerStates = make(map[int]WorkerStateType)
	m.mapTaskBeginTime = make(map[string]time.Time)
	m.reduceTaskBeginTime = make(map[int]time.Time)
	for _, task := range files {
		m.mapTaskStates[task] = TaskState_Unassigned
	}
	for i := 0; i < nReduce; i++ {
		m.reduceTaskStates[i] = TaskState_Unassigned
	}
	m.bigLock.Unlock()

	go m.checkCrash()
	
	m.server()
	return &m
}
