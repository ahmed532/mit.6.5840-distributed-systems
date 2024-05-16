package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type InputFile struct {
	file      string
	processed int
}

type Coordinator struct {
	// Your definitions here.
	InputFiles []InputFile
	nReduce    int
	nReduceI   int
	lock       sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) AskForFile(args *AskForFileArgs, reply *AskForFileReply) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	for i, f := range c.InputFiles {
		if f.processed == 0 {
			reply.Name = f.file
			reply.State = 0
			c.InputFiles[i].processed = 1
			return nil
		}
	}

	if c.nReduceI < c.nReduce {
		reply.Name = "reducer"
		reply.State = 1
		reply.ReducerI = c.nReduceI
		c.nReduceI++
	} else {
		reply.Name = ""
		reply.State = 2
		reply.ReducerI = -1
	}
	return nil
}

func (c *Coordinator) GetNReduce(args *GetNReduceArgs, reply *GetNReduceReply) error {
	reply.N = c.nReduce
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
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.nReduceI == c.nReduce
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	i := []InputFile{}
	for _, f := range files {
		i = append(i, InputFile{
			file:      f,
			processed: 0,
		})
	}
	c := Coordinator{
		InputFiles: i,
		nReduce:    nReduce,
		nReduceI:   0,
	}

	// Your code here.

	c.server()
	return &c
}
