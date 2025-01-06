package mr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
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

func EnsureDir(dirName string) error {
	err := os.MkdirAll(dirName, os.ModePerm)
	if err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}
	return nil
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	workerId := fmt.Sprintf("%d", os.Getpid())
	// worker starts by asking the coordinator for a task
	// if there's no assigned task it keeps asking periodically
	// if the task is a map task, it reads the file, calls the map function
	// and writes the intermediate files to disk, it also emits the
	// intermediary file paths are structured as "mr-X-Y" where X is the map task number and Y is the reduce task number
	for {
		task, nReduce := requestTask(workerId)
		if task == nil {
			os.Exit(1)
		}
		if task.Type == TaskTypeMap {
			intermediateFileNamesMap, err := processMapTask(task, mapf, nReduce)
			intermediateFileNames := make([]string, 0)
			for _, v := range intermediateFileNamesMap {
				intermediateFileNames = append(intermediateFileNames, v)
			}

			task.IntermediateFileNames = intermediateFileNames
			if err != nil {
				log.Fatalf("failed to process map task: %v", err)
			}
		} else {
			processReduceTask(task, reducef)
		}
		updateTaskStatus(task, TaskStatusCompleted)
		time.Sleep(1 * time.Second)
	}
}

func processMapTask(task *Task, mapf func(string, string) []KeyValue, nReduce int) (map[int]string, error) {
	println("Processing map task")
	intermediateFileNames := make(map[int]string, 0)
	bytes, err := os.ReadFile(task.InputFile)
	if err != nil {
		log.Fatalf("failed to read input file: %v", err)
	}
	content := string(bytes)
	kva := mapf(task.InputFile, content)
	println("Kva: ", len(kva))
	tmpFiles := make([]*os.File, nReduce)
	for _, kv := range kva {
		partition := ihash(kv.Key) % nReduce
		if tmpFiles[partition] == nil {
			tmpFileName := fmt.Sprintf("mr-%d-*", task.Id)
			tmpFile, err := os.CreateTemp("", tmpFileName)
			if err != nil {
				log.Fatalf("failed to create tmp file: %v", err)
			}
			defer tmpFile.Close()
			defer os.Remove(tmpFile.Name())
			tmpFiles[partition] = tmpFile
		}

		println("Partition: ", partition)
		println("TmpFiles: ", len(tmpFiles))
		enc := json.NewEncoder(tmpFiles[partition])
		if tmpFiles[partition] == nil {
			log.Fatalf("tmpFiles[%d] is nil", partition)
		}
		if err := enc.Encode(&kv); err != nil {
			log.Fatalf("failed to encode key value: %v", err)
		}

		intermediateFile := fmt.Sprintf("mr-output/intermediate/mr-%d-%d", task.Id, partition)
		intermediateFileNames[partition] = intermediateFile
	}

	for idx, tmpFile := range tmpFiles {
		// Atomically rename the temporary file to the target filename
		if tmpFile != nil {
			if err := tmpFile.Sync(); err != nil {
				log.Fatalf("failed to sync temporary file: %v", err)
			}
			dir := filepath.Dir(intermediateFileNames[idx])
			if err := EnsureDir(dir); err != nil {
				return intermediateFileNames, fmt.Errorf("failed to ensure directory: %w", err)
			}

			if err := os.Rename(tmpFile.Name(), intermediateFileNames[idx]); err != nil {
				return intermediateFileNames, fmt.Errorf("failed to rename temporary file: %w", err)
			}
		}

	}
	log.Printf("Intermediate file names: %v", intermediateFileNames)
	return intermediateFileNames, nil
}

func processReduceTask(task *Task, reducef func(string, []string) string) error {
	println("Processing reduce task")
	// read each of the intermediate files associated to this task
	// group the values by key
	// call the reduce function for each key and it's associated values
	outputFilePath := fmt.Sprintf("mr-output/final/mr-out-%d", task.Id)
	dir := filepath.Dir(outputFilePath)
	if err := EnsureDir(dir); err != nil {
		return fmt.Errorf("failed to ensure directory: %w", err)
	}
	outputFile, err := os.Create(outputFilePath)
	if err != nil {
		return err
	}
	defer outputFile.Close()
	log.Println("Reading from Locations: ", task.Locations)
	intermediate, err := readIntermediateFiles(task.Locations)
	if err != nil {
		log.Fatalf("failed to read intermediate files: %v", err)
	}
	sort.Sort(ByKey(intermediate))
	values := make([]string, 0)
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[i].Key == intermediate[j].Key {
			j += 1
		}

		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		result := reducef(intermediate[i].Key, values)
		line := fmt.Sprintf("%v %v\n", intermediate[i].Key, result)
		bytesWritten, err := outputFile.Write([]byte(line))
		if err != nil || bytesWritten != len(line) {
			return err
		}
		i = j + 1

	}
	return nil

}

func readIntermediateFiles(fileNames []string) ([]KeyValue, error) {
	intermediateFileContent := make([]KeyValue, 0)
	for _, fileName := range fileNames {
		f, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("failed to open file: %v", err)
		}
		defer f.Close()
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			var kv KeyValue
			if err := json.Unmarshal(scanner.Bytes(), &kv); err != nil {
				log.Fatalf("failed to unmarshal key value: %v", err)
			}
			intermediateFileContent = append(intermediateFileContent, kv)
		}
		if scanner.Err() != nil {
			log.Fatalf("failed to scan file: %v", scanner.Err())
		}
	}
	log.Printf("Found %d kv pairs to reduce", len(intermediateFileContent))
	return intermediateFileContent, nil

}

func requestTask(workerId string) (*Task, int) {

	args := RequestTaskArgs{WorkerId: workerId}
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
