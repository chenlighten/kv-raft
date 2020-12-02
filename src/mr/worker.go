package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

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

	// uncomment to send the Example RPC to the master.
	// CallExample()
	
	for i := 0; i < 3; i++{
		// Asking for a task
		args := AssignTaskArgs{}
		reply := AssignTaskReply{}
		args.WorkerPid = os.Getpid()
		log.Printf("Worker %d asking for a task", args.WorkerPid)
		call("Master.AssignTask", &args, &reply)
		log.Printf("Worker %d recieving %s task %s, with nReduce=%v", 
			args.WorkerPid, reply.TaskType, reply.TaskName, reply.TaskNReduce)
		
		// Handle that task
		if reply.TaskType == "map" {
			doMap(mapf, reply.TaskName, reply.TaskNReduce)
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
		file, err = os.Create(fmt.Sprintf("mr-%d-%d", os.Getpid(), i))
		if err != nil {
			log.Fatalf("Can not create file %s because %e", fmt.Sprintf("mr-%d-%d", os.Getpid(), i), err)
		}
		enc := json.NewEncoder(file)
		for _, kv := range interSplit[i] {
			enc.Encode(kv)
		}
		file.Close()
	}
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
