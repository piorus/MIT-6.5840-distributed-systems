package mr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	tasks []Task
	mutex sync.Mutex
}

type TaskType int

const (
	UnknownTaskType TaskType = iota
	MapTaskType
	ReduceTaskType
	//SleepTaskType
)

type Task interface {
	Schedule()
	Reschedule()
	Complete()
	Id() int
	Type() TaskType
	Equals(task Task) bool
	IsScheduled() bool
	IsCompleted() bool
}

type BaseTask struct {
	id        int
	scheduled bool
	completed bool
}

type MapTask struct {
	BaseTask
	Filename string
	NReduce  int
}

type ReduceTask struct {
	BaseTask
}

func (t BaseTask) Schedule() {
	t.scheduled = true
}

func (t BaseTask) Reschedule() {
	t.scheduled = false
}

func (t BaseTask) Complete() {
	t.completed = true
}

func (t BaseTask) Id() int {
	return t.id
}

func (t BaseTask) Type() TaskType {
	return UnknownTaskType
}

func (t BaseTask) Equals(task Task) bool {
	return t.Type() == task.Type() && t.Id() == task.Id()
}

func (t BaseTask) IsScheduled() bool {
	return t.scheduled
}

func (t BaseTask) IsCompleted() bool {
	return t.completed
}

func (t MapTask) Type() TaskType {
	return MapTaskType
}

func (t ReduceTask) Type() TaskType {
	return ReduceTaskType
}

func IsMappingComplete(tasks []Task) bool {
	for _, task := range tasks {
		if task.Type() == MapTaskType && !task.IsCompleted() {
			return false
		}
	}

	return true
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	for i, task := range c.tasks {
		if task.Type() == MapTaskType && !task.IsCompleted() {
			reply.Task = task

			go func() {
				time.Sleep(10 * time.Second)

				if !task.IsCompleted() {
					c.tasks[i].Reschedule()
				}
			}()

			return nil
		}

		if task.Type() == ReduceTaskType && !task.IsCompleted() && !IsMappingComplete(c.tasks) {
			// wait until mapping ends
			time.Sleep(time.Second)

			return c.GetTask(args, reply)
		} else if task.Type() == ReduceTaskType && !task.IsCompleted() {
			reply.Task = task

			return nil
		}
	}

	return errors.New("no more tasks")
}

func (c *Coordinator) CompleteTask(args *CompleteTaskArgs, reply *CompleteTaskReply) error {
	for i := 0; i < len(c.tasks); i++ {
		task := c.tasks[i]

		if task.Equals(args.Task) {
			c.mutex.Lock()
			c.tasks[i].Complete()
			c.mutex.Unlock()

			reply.Ack = true

			return nil
		}
	}

	return fmt.Errorf("task %d not found", args.Task.Id)
}

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
		if !task.IsCompleted() {
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

	for i, filename := range files {
		tasks = append(tasks, MapTask{BaseTask: BaseTask{id: i}, Filename: filename, NReduce: nReduce})
	}

	for i := 0; i < nReduce; i++ {
		tasks = append(tasks, ReduceTask{BaseTask: BaseTask{id: i}})
	}

	c := Coordinator{tasks: tasks}
	c.server()
	return &c
}
