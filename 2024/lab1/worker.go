package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	for {
		if task, err := RpcGetTask(); err == nil {
			if task.Type == MapTask {
				Map(task, mapf)
			}

			if task.Type == MapTask {
				Reduce(task, reducef)
			}

			RpcCompleteTask(task)
		} else {
			break
		}
	}
}

func RpcGetTask() (Task, error) {
	args := GetTaskArgs{}
	reply := GetTaskReply{}

	if !call("Coordinator.GetTask", &args, &reply) {
		return reply.Task, errors.New("no more tasks")
	}

	return reply.Task, nil
}

func RpcCompleteTask(task Task) {
	args := CompleteTaskArgs{Task: task}
	reply := CompleteTaskReply{}
	ok := call("Coordinator.CompleteTask", &args, &reply)

	if !ok {
		fmt.Println("Something went wrong during CompleteTask")
	}
}

func Map(task Task, mapf func(string, string) []KeyValue) {
	// read file
	// mapf
	// iterate over kv, build a map indexed by reduce task id (ihash(key) % nReduce) with kv
	// iterate over map, write intermediate file mr-{task.Id}-{map index}

	ofile, _ := os.Create(fmt.Sprintf("mr-, a ...any))
	enc := json.NewEncoder()

	for _, filename := range task.Filenames {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := mapf(filename, string(content))
		

		for _, kv := range kva {

		}




	}

	// sort

	// save to file
}

func Reduce(task Task, reducef func(string, []string) string) {
	oname := fmt.Sprintf("mr-out-%d", task.Id)
	ofile, _ := os.Create(oname)

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()

}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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
