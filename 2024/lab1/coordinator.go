package mr

import (
	"errors"
	"fmt"
	"log"
	"math"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type Coordinator struct {
	tasks []Task
}

type Task struct {
	Id        int
	Filenames []string
	Scheduled bool
	Done      bool
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	for _, task := range c.tasks {
		if !task.Scheduled && !task.Done {
			task.Scheduled = true
			reply.Task = task

			return nil
		}
	}

	return errors.New("no more tasks")
}

func (c *Coordinator) NotifyAboutTaskCompletion(args *NotifyAboutTaskCompletionArgs, reply *NotifyAboutTaskCompletionReply) error {
	for i := 0; i < len(c.tasks); i++ {
		task := &c.tasks[i]
		if task.Id == args.Task.Id {
			task.Done = true
			reply.Ack = true

			return nil
		}
	}

	return fmt.Errorf("task %d not found", args.Task.Id)
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	for _, task := range c.tasks {
		if !task.Done {
			return false
		}
	}

	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	if nReduce < 1 {
		panic("nReduce must be greater than 0")
	}

	var tasks []Task
	chunkSize := int(math.Ceil(float64(len(files)) / float64(nReduce)))

	for i := 0; i < len(files); i += chunkSize {
		tasks = append(tasks, Task{Id: i, Filenames: files[i : i+chunkSize]})
	}

	c := Coordinator{tasks: tasks}
	c.server()
	return &c
}
