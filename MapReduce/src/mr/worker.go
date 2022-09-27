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
)

// each worker will process will ask the coordinator for a task, read the task's input from one
// or more files, execute the task, and write the task's output to one or more files.

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type worker struct {
	id      int
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
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

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	w := worker{}
	w.mapf = mapf
	w.reducef = reducef
	w.RegisterId()

	w.Run()

}

func (w *worker) Run() {
	for {
		reply := w.GetTask()
		if reply.Task == Map {
			w.MapWorker(&reply)
		} else if reply.Task == Reduce {
			w.ReduceWorker(&reply)
		} else {
			log.Fatalf("unsupport task type: %v", reply.Task)
		}
	}
}

func (w *worker) RegisterId() {
	var arg UNUSED
	var reply int
	if ok := call("Coordinator.Register", &arg, &reply); !ok {
		os.Exit(0)
	}
	w.id = reply
}

func (w *worker) GetTask() GetTaskReply {
	reply := GetTaskReply{}
	if ok := call("Coordinator.Get", w.id, &reply); !ok {
		os.Exit(0)
	}
	if reply.Task == Map {
		fmt.Printf("Get Map Task, taskid: %v\n", reply.TaskId)
	} else {
		fmt.Printf("Get Reduce Task, taskid: %v\n", reply.TaskId)
	}

	return reply
}

// MapWorker worker ask for a map task, then coordinator respond with the filename.
// worker should read this file and call the application Map function.
// refer to mrsequential.go.
func (w *worker) MapWorker(reply *GetTaskReply) {
	file, err := os.Open(reply.Filename)
	defer file.Close()
	if err != nil {
		log.Fatalf("cannot open %v", reply.Filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", reply.Filename)
	}

	kva := w.mapf(reply.Filename, string(content))
	reduces := make([][]KeyValue, reply.NReduce)

	for _, kv := range kva {
		idx := ihash(kv.Key) % reply.NReduce
		reduces[idx] = append(reduces[idx], kv)
	}
	//The worker should put intermediate Map output in files in the current directory,
	//where your worker can later read them as input to Reduce tasks.
	curDir, _ := os.Getwd()
	for i := 0; i < reply.NReduce; i++ {
		sort.Sort(ByKey(reduces[i]))
		//To ensure that nobody observes partially written files in the presence of crashes,
		//the MapReduce paper mentions the trick of using a temporary file and
		//atomically renaming it once it is completely written.
		//You can use ioutil.TempFile to create a temporary file and os.Rename to atomically rename it.
		tempFile, err := ioutil.TempFile(curDir, "*")
		if err != nil {
			log.Fatalf("cannot open tempFile: %v", err)
		}
		//The worker's map task code will need a way to store intermediate key/value pairs in files in a way
		//that can be correctly read back during reduce tasks.
		//One possibility is to use Go's encoding/json package.
		enc := json.NewEncoder(tempFile)
		for _, kv := range reduces[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("encode error: %v", err)
			}
		}
		intermediate := fmt.Sprintf("mr-%d-%d", reply.TaskId, i+1)
		os.Rename(tempFile.Name(), curDir+string(os.PathSeparator)+intermediate)
	}
	w.MapReport(reply.TaskId)
}

func (w *worker) ReduceWorker(reply *GetTaskReply) {
	curDir, _ := os.Getwd()
	var kva []KeyValue
	for i := 1; i <= reply.MapCount; i++ {
		filename := fmt.Sprintf("mr-%d-%d", i, reply.TaskId)
		fmt.Println(curDir + string(os.PathSeparator) + filename)
		file, err := os.Open(curDir + string(os.PathSeparator) + filename)
		if err != nil {
			log.Fatalf("open file %s error: %v", curDir+string(os.PathSeparator)+filename, err)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}
	target, _ := os.Create(fmt.Sprintf("mr-out-%d", reply.TaskId))
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := w.reducef(kva[i].Key, values)
		fmt.Fprintf(target, "%v %v\n", kva[i].Key, output)
		i = j
	}
	target.Close()
	w.ReduceReport(reply.TaskId)
}

func (w *worker) ReduceReport(Rno int) {
	args := ReduceReport{Rno}
	if ok := call("Coordinator.ReduceReport", args, nil); !ok {
		os.Exit(0)
	}
}

func (w *worker) MapReport(Mno int) {
	args := MapReport{Mno}
	if ok := call("Coordinator.MapReport", args, nil); !ok {
		os.Exit(0)
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

// if the worker fails to contact the coordinator, it can assume that the coordinator has exited.
// therfore this worker should exit.
//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
