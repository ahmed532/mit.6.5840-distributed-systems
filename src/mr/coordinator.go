package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type InputFile struct {
	file      string
	processed int
}

type Coordinator struct {
	// Your definitions here.
	InputFiles []InputFile
	nReduce    int
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
	for i, f := range c.InputFiles {
		if f.processed == 0 {
			reply.Name = f.file
			reply.State = 0
			c.InputFiles[i].processed = 1
			return nil
		}
	}

	for i, f := range c.InputFiles {
		if f.processed == 1 {
			reply.Name = "reducer"
			reply.State = 1
			c.InputFiles[i].processed = 2
			return nil
		}
	}
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
	ret := false

	// Your code here.

	return ret
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
	}

	// Your code here.

	c.server()
	return &c
}
