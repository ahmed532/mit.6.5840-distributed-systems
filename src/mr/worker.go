package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
)

// Map functions return a slice of KeyValue.
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

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(
	mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for {
		filename, state := AskForFile()
		if filename == "" {
			break
		}
		switch state {
		case 0:
			mapWorker(filename, mapf)
		case 1:
			reduceWorker(reducef)
		case 2:
			println("done")
		default:
			log.Fatal("what?")
		}
	}

}

func reduceWorker(reducef func(string, []string) string) {
	filenames, err := os.ReadDir(".")
	if err != nil {
		log.Fatal("cannot ls dir")
	}
	var intermediateFiles []string
	for _, f := range filenames {
		if f.Name()[:9] == "mr-inter-" {
			intermediateFiles = append(intermediateFiles, f.Name())
		}
	}

	var allKva [][]KeyValue
	for _, f := range intermediateFiles {
		file, err := os.Open(f)
		if err != nil {
			log.Fatal("Cannot open intermediate file in reduce")
		}
		dec := json.NewDecoder(file)
		allKva = append(allKva, []KeyValue{})
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				log.Fatal("Cannot read intermediate kv in reduce")
			}
			allKva[len(allKva)-1] = append(allKva[len(allKva)-1], kv)
		}
	}
	for {

	}
}

func mapWorker(filename string, mapf func(string, string) []KeyValue) {
	content, err := os.ReadFile(filename)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	kva := mapf(filename, string(content))
	sort.Sort(ByKey(kva))

	oname := fmt.Sprintf("mr-inter-%s", filename[3:])
	ofile, err := os.Create(oname)
	if err != nil {
		log.Fatalf("cannot create %v", oname)
	}
	defer ofile.Close()
	enc := json.NewEncoder(ofile)
	for _, kv := range kva {
		err := enc.Encode(&kv)
		if err != nil {
			log.Fatalf("cannot write key val %v", filename)
		}
	}
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

func AskForFile() (string, int) {
	args := AskForFileArgs{}
	reply := AskForFileReply{}
	ok := call("Coordinator.AskForFile", &args, &reply)
	if ok {
		return reply.Name, reply.State
	} else {
		fmt.Printf("call failed!\n")
		panic("bad file")
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
