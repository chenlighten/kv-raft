package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	log.SetFlags(log.Lmicroseconds)
	logFile, _ := os.Create(fmt.Sprintf("Worker-%d.log", os.Getpid()))
	log.SetOutput(logFile)

	// uncomment to send the Example RPC to the master.
	// CallExample()
	
	for i := 0; i < 100; i++{
		// Asking for a task
		args := AssignTaskArgs{}
		reply := AssignTaskReply{}
		args.WorkerPid = os.Getpid()
		log.Printf("Worker %d asking for a task", args.WorkerPid)
		if !call("Master.AssignTask", &args, &reply) {
			log.Printf("Lost connection to master.")
			return
		}
		log.Printf("Worker %d recieving %s task %s, with nReduce=%v", 
			args.WorkerPid, reply.TaskType, reply.TaskName, reply.TaskNReduce)

		// Handle that task
		taskName := reply.TaskName
		taskType := reply.TaskType
		nReduce := reply.TaskNReduce
		if taskType == "map" {
			doMap(mapf, taskName, nReduce)
			args := ReportTaskCompleteArgs{}
			args.WorkerPid = os.Getpid()
			args.TaskType = "map"
			args.TaskName = taskName
			reply := ReportTaskCompleteReply{}
			log.Printf("Worker %d report map task %s completed.", args.WorkerPid, args.TaskName)
			call("Master.ReportTaskComplete", &args, &reply)
			log.Printf("Worker %d get reporting feedback: %s", args.WorkerPid, reply.Msg)
		} else if taskType == "reduce" {
			log.Printf("Begin a reduce task")
			taskId, _ := strconv.Atoi(taskName)
			doReduce(reducef, taskId, nReduce)
			args := ReportTaskCompleteArgs{}
			args.WorkerPid = os.Getpid()
			args.TaskType = "reduce"
			args.TaskName = taskName
			reply := ReportTaskCompleteReply{}
			log.Printf("Worker %d report reduce task %s completed.", args.WorkerPid, args.TaskName)
			call("Master.ReportTaskComplete", &args, &reply)
			log.Printf("Worker %d get reporting feedback: %s", args.WorkerPid, reply.Msg)
		} else if taskType == "wait" {
			log.Printf("Worker %d get waiting task.", os.Getpid())
			time.Sleep(time.Millisecond)
		} else if taskType == "exit" {
			log.Printf("Worker %d get exiting task.", os.Getpid())
			break
		}
	}
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99
	args.B = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\nreply.C %v", reply.Y, reply.C)
}

func doMap(mapf func (string, string) []KeyValue, fileName string, nReduce int) {
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("Can not open file %s because %e", fileName, err)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("Can not read file %s because %e", fileName, err)
	}
	file.Close()

	intermidiate := mapf(fileName, string(content))
	interSplit := make([][]KeyValue, nReduce)
	for _, kv := range intermidiate {
		i := ihash(kv.Key) % nReduce
		interSplit[i] = append(interSplit[i], kv)	
	}
	for i := 0; i < nReduce; i++ {
		newFileName := fmt.Sprintf("mr-%s-%d", fileName[6:10], i)
		file, err = os.Create(newFileName)
		if err != nil {
			log.Fatalf("Can not create file %s because %e", newFileName, err)
		}
		enc := json.NewEncoder(file)
		for _, kv := range interSplit[i] {
			enc.Encode(kv)
		}
		file.Close()
	}
}

func doReduce(reducef func(string, []string) string, taskId int, nReduce int) {
	intermidiate := []KeyValue{}
	log.Printf("Begin reading files...")
	allFiles, err := ioutil.ReadDir("./")
	if err != nil {
		log.Fatalf("Error reading working directory")
	}
	fileNames := []string{}
	for _, file := range allFiles {
		splitFileName := strings.Split(file.Name(), "-")
		if id, _ := strconv.Atoi(splitFileName[len(splitFileName) - 1]); id == taskId {
			fileNames = append(fileNames, file.Name())
		}
	}
	for _, fileName := range fileNames {
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("Can not open file %s in reduce task", fileName)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermidiate = append(intermidiate, kv)
		}
	}
	log.Printf("Reading files done, size %d", len(intermidiate))
	log.Printf("Begin sorting...")
	sort.Sort(ByKey(intermidiate))
	log.Printf("Sorting done")
	log.Printf("Begin doing reduce...")
	results := []KeyValue{}
	for i, j := 0, 0; j < len(intermidiate); {
		values := []string{}
		for ; j < len(intermidiate) && intermidiate[i].Key == intermidiate[j].Key; j++ {
			values = append(values, intermidiate[j].Value)
		}
		results = append(results, KeyValue{intermidiate[i].Key, reducef(intermidiate[i].Key, values)})
		i = j
	}
	log.Printf("Doing reduce done")
	
	log.Printf("Begin writing result")
	file, err := os.Create(fmt.Sprintf("mr-out-%d", taskId))
	if err != nil {
		log.Fatalf("Can not create file %s", fmt.Sprintf("mr-out-%d", taskId))
	}
	for _, kv := range results {
		fmt.Fprintf(file, "%v %v\n", kv.Key, kv.Value)
	}
	file.Close()
	log.Printf("Writing result done")
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
