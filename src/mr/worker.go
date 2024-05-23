package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strings"
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
	nReduce := GetNReduce()
	for {
		filename, state, reduceI := AskForFile()
		if filename == "" && state == 2 {
			break
		}
		if filename == "wait" {
			continue
		}
		switch state {
		case 0:
			mapWorker(filename, mapf, nReduce)
		case 1:
			reduceWorker(reducef, reduceI)
		case 2:
			println("done")
		default:
			log.Fatal("what?")
		}
	}

}

func reduceWorker(reducef func(string, []string) string, reduceI int) {
	filenames, err := os.ReadDir(".")
	if err != nil {
		log.Fatal("cannot ls dir")
	}
	var allKva []KeyValue
	for _, f := range filenames {
		if !strings.HasSuffix(f.Name(), fmt.Sprint(reduceI)) {
			continue
		}
		file, err := os.Open(f.Name())
		if err != nil {
			log.Fatal("Cannot open intermediate file in reduce")
		}
		defer file.Close()
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			allKva = append(allKva, kv)
		}
	}
	sort.Sort(ByKey(allKva))

	oname := fmt.Sprintf("t-mr-out-%d", reduceI)
	ofile, _ := os.Create(oname)
	i := 0
	for i < len(allKva) {
		j := i + 1
		for j < len(allKva) && allKva[j].Key == allKva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, allKva[k].Value)
		}
		output := reducef(allKva[i].Key, values)
		// println("output: ", output)
		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", allKva[i].Key, output)

		i = j
	}
	ofile.Close()
	os.Rename(fmt.Sprintf("t-mr-out-%d", reduceI), fmt.Sprintf("mr-out-%d", reduceI))
	CommitOutput(reduceI)
}

func mapWorker(filename string, mapf func(string, string) []KeyValue, nReduce int) {
	content, err := os.ReadFile(filename)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	kva := mapf(filename, string(content))

	var intermediateFileEncoders []*json.Encoder
	for i := 0; i < nReduce; i++ {
		oname := fmt.Sprintf("mr-inter-%s-%d", filename[3:], i)
		ofile, err := os.Create(oname)
		if err != nil {
			log.Fatalf("cannot create %v", oname)
		}
		defer ofile.Close()
		enc := json.NewEncoder(ofile)
		intermediateFileEncoders = append(intermediateFileEncoders, enc)
	}

	for _, kv := range kva {
		r := ihash(kv.Key) % nReduce
		err := intermediateFileEncoders[r].Encode(&kv)
		if err != nil {
			log.Fatalf("cannot write key val %v", filename)
		}
	}
	CommitFile(filename)
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

func AskForFile() (string, int, int) {
	args := AskForFileArgs{}
	reply := AskForFileReply{}
	ok := call("Coordinator.AskForFile", &args, &reply)
	if ok {
		return reply.Name, reply.State, reply.ReducerI
	} else {
		return "wait", -1, -1
	}
}

func GetNReduce() int {
	args := GetNReduceArgs{}
	reply := GetNReduceReply{}
	ok := call("Coordinator.GetNReduce", &args, &reply)
	if ok {
		return reply.N
	} else {
		log.Fatal("GetNReduce Call failed")
	}
	return -1
}

func CommitFile(file string) {
	args := CommitFileArgs{}
	reply := CommitFileReply{}
	args.Name = file
	ok := call("Coordinator.CommitFile", &args, &reply)
	if !ok {
		log.Fatal("GetNReduce Call failed")
	}
}

func CommitOutput(r int) {
	args := CommitOutputArgs{}
	reply := CommitOutputReply{}
	args.I = r
	ok := call("Coordinator.CommitOutput", &args, &reply)
	if !ok {
		log.Fatal("GetNReduce Call failed")
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
