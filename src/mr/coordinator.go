package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// The master keeps several data structures. For each map
// task and reduce task, it stores the state (idle, in-progress,
// or completed), and the identity of the worker machine
// (for non-idle tasks).
// The master is the conduit through which the location
// of intermediate file regions is propagated from map tasks
// to reduce tasks. Therefore, for each completed map task,
// the master stores the locations and sizes of the R intermediate file regions produced by the map task. Updates
// to this location and size information are received as map
// tasks are completed. The information is pushed incrementally to workers that have in-progress reduce tasks.

type TaskStatus string

const TaskStatusIdle TaskStatus = "idle"
const TaskStatusInProgress TaskStatus = "in-progress"
const TaskStatusCompleted TaskStatus = "completed"

type TaskType string

const TaskTypeMap TaskType = "map"
const TaskTypeReduce TaskType = "reduce"

type Task struct {
	Id                    int
	Status                TaskStatus
	Type                  TaskType
	Locations             []string // the location of the intermediate file regions (it should always be of size R)
	InputFile             string   // the input file for the map task
	IntermediateFileNames []string
	StartedAt             time.Time
}

type Coordinator struct {
	nReduce     int
	MapTasks    []*Task
	ReduceTasks []*Task
	mu          sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, mapTask := range c.MapTasks {
		if mapTask.Status == TaskStatusIdle {
			mapTask.Status = TaskStatusInProgress
			mapTask.StartedAt = time.Now()
			reply.Task = *mapTask
			reply.NReduce = c.nReduce
			return nil
		}
	}
	for _, reduceTask := range c.ReduceTasks {
		if reduceTask.Status == TaskStatusIdle {
			reduceTask.Status = TaskStatusInProgress
			reduceTask.StartedAt = time.Now()
			reply.Task = *reduceTask
			reply.NReduce = c.nReduce
			return nil
		}
	}
	return nil
}

func (c *Coordinator) UpdateTaskStatus(args *UpdateTaskStatusArgs, reply *UpdateTaskStatusReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	// find the task
	// update the status
	// if the task is a map task, update the locations of the reduce task
	if args.TaskType == TaskTypeMap {
		c.MapTasks[args.TaskId].Status = args.TaskStatus
		if args.TaskStatus == TaskStatusCompleted {
			for _, intermediateFile := range args.IntermediateFileNames {
				// pattern is mr-taskId-reduceId
				// extract reduceId
				parts := strings.Split(intermediateFile, "-")
				lastItem := parts[len(parts)-1]
				partition, err := strconv.Atoi(lastItem)
				if err != nil {
					fmt.Println("Error converting partition")
				}
				c.ReduceTasks[partition].Locations = append(c.ReduceTasks[partition].Locations, intermediateFile)
			}
		}
		return nil
	} else {
		c.ReduceTasks[args.TaskId].Status = args.TaskStatus
		return nil
	}
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
	ret := false
	for _, mapTask := range c.MapTasks {
		if mapTask.Status != TaskStatusCompleted {
			return ret
		}
	}
	for _, reduceTask := range c.ReduceTasks {
		if reduceTask.Status != TaskStatusCompleted {
			return ret
		}
	}
	ret = true
	return ret
}

func (c *Coordinator) CheckStaleTasks() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, mapTask := range c.MapTasks {
		if mapTask.Status != TaskStatusInProgress && time.Since(mapTask.StartedAt) > 10*time.Second {
			mapTask.Status = TaskStatusIdle

		}
	}
	for _, reduceTask := range c.ReduceTasks {
		if reduceTask.Status != TaskStatusCompleted && time.Since(reduceTask.StartedAt) > 10*time.Second {
			reduceTask.Status = TaskStatusIdle
		}
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{nReduce: nReduce}
	go func() {
		for {
			c.CheckStaleTasks()
			time.Sleep(1 * time.Second)
		}
	}()
	c.MapTasks = make([]*Task, len(files))
	c.ReduceTasks = make([]*Task, nReduce)
	for i := 0; i < len(files); i++ {
		c.MapTasks = append(c.MapTasks, &Task{Id: i, Status: TaskStatusIdle, Type: TaskTypeMap, InputFile: files[i]})
		i++
	}
	for i := 0; i < nReduce; i++ {
		c.ReduceTasks = append(c.ReduceTasks, &Task{Id: i, Status: TaskStatusIdle, Type: TaskTypeReduce})
		i++
	}

	c.server()
	return &c
}
