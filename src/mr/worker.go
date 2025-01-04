package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// worker starts by asking the coordinator for a task
	// if there's no assigned task it keeps asking periodically
	// if the task is a map task, it reads the file, calls the map function
	// and writes the intermediate files to disk, it also emits the
	// intermediary file paths are structured as "mr-X-Y" where X is the map task number and Y is the reduce task number
	for {
		task, nReduce := requestTask()
		if task == nil {
			os.Exit(1)
		}
		if task.Type == TaskTypeMap {
			intermediateFileNames, err := processMapTask(task, mapf, nReduce)
			task.IntermediateFileNames = intermediateFileNames
			if err != nil {
				continue
			}
		} else {
			processReduceTask(task, reducef)
		}
		updateTaskStatus(task, TaskStatusCompleted)
		time.Sleep(1 * time.Second)
	}
}

func processMapTask(task *Task, mapf func(string, string) []KeyValue, nReduce int) ([]string, error) {
	intermediateFileNames := make([]string, 0)
	bytes, err := os.ReadFile(task.InputFile)
	if err != nil {
		return intermediateFileNames, err
	}
	content := string(bytes)
	kva := mapf(task.InputFile, content)
	tmpFiles := make([]*os.File,0)
	for _, kv := range kva {
		partition := ihash(kv.Key) % nReduce
		if partition > len(tmpFiles)-1 {
			tmpFileName := fmt.Sprintf("mr-%d-*", task.Id)
			tmpFile, err := os.CreateTemp("", tmpFileName)
			if err != nil {
				return intermediateFileNames, fmt.Errorf("failed to create temporary file: %w", err)
			}
			defer os.Remove(tmpFile.Name())
			tmpFiles = append(tmpFiles, tmpFile)
		}
		enc := json.NewEncoder(tmpFiles[partition])
		if err := enc.Encode(&kv); err != nil {
			return intermediateFileNames, err
		}
		if err := tmpFiles[partition].Sync(); err != nil {
			tmpFiles[partition].Close()
			return intermediateFileNames, fmt.Errorf("failed to sync temporary file: %w", err)
		}

		// Close the file to flush the buffers
		if err := tmpFiles[partition].Close(); err != nil {
			return intermediateFileNames, fmt.Errorf("failed to close temporary file: %w", err)
		}
		intermediateFile := fmt.Sprintf("mr-%d-%d", task.Id, partition)
		// Atomically rename the temporary file to the target filename
		if err := os.Rename(tmpFiles[partition].Name(), intermediateFile); err != nil {
			return intermediateFileNames, fmt.Errorf("failed to rename temporary file: %w", err)
		}
		intermediateFileNames = append(intermediateFileNames, intermediateFile)
	}

	return intermediateFileNames, nil
}

func processReduceTask(task *Task, reducef func(string, []string) string) error {
	// read each of the intermediate files associated to this task
	// group the values by key
	// call the reduce function for each key and it's associated values
	outputFilePath := fmt.Sprintf("mr-out-%d", task.Id)
	outputFile, err := os.Create(outputFilePath)
	if err != nil {
		return err
	}
	decoders := make([]*json.Decoder, len(task.Locations))
	for i := 0; i < len(task.Locations); i++ {
		filePath := task.Locations[i]
		file, err := os.Open(filePath)
		if err != nil {
			return err
		}
		decoders[i] = json.NewDecoder(file)
	}

	intermediate := make([]KeyValue, 0)
	for _, dec := range decoders {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			return err
		}
		intermediate = append(intermediate, kv)
	}

	sort.Sort(ByKey(intermediate))
	i := 0
	values := make([]string, 0)
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[i].Key == intermediate[j].Key {
			j += 1
		}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
			result := reducef(intermediate[k].Key, values)
			line := fmt.Sprintf("%v %v", intermediate[k].Key, result)
			bytesWritten, err := outputFile.Write([]byte(line))
			if err != nil || bytesWritten != len(line) {
				return err
			}
		}

	}
	return nil

}

func requestTask() (*Task, int) {

	args := RequestTaskArgs{}
	reply := RequestTaskReply{}
	ok := call("Coordinator.RequestTask", &args, &reply)
	if ok {
		return &reply.Task, reply.NReduce
	}
	return nil, 0
}

func updateTaskStatus(task *Task, status TaskStatus) {
	args := UpdateTaskStatusArgs{TaskId: task.Id, TaskType: task.Type, TaskStatus: status, IntermediateFileNames: task.IntermediateFileNames}
	reply := UpdateTaskStatusReply{}
	call("Coordinator.UpdateTaskStatus", &args, &reply)
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
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
