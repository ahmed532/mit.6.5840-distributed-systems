package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type InputFile struct {
	file      string
	processed int
	started   time.Time
}

type Coordinator struct {
	// Your definitions here.
	InputFiles  []InputFile
	nReduce     int
	reduces     []int
	reduceStart []time.Time
	lock        sync.Mutex
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
	s10, _ := time.ParseDuration("10s")
	for i, f := range c.InputFiles {
		if f.processed == 0 {
			reply.Name = f.file
			reply.State = 0
			c.InputFiles[i].processed = 1
			c.InputFiles[i].started = time.Now()
			return nil
		} else if f.processed == 1 {
			if time.Since(f.started) > s10 {
				reply.Name = f.file
				reply.State = 0
				c.InputFiles[i].processed = 1
				c.InputFiles[i].started = time.Now()
				return nil
			}
		}
	}

	for i := 0; i < c.nReduce; i++ {
		if c.reduces[i] == 0 {
			reply.Name = "reducer"
			reply.State = 1
			reply.ReducerI = i
			c.reduces[i] = 1
			c.reduceStart[i] = time.Now()
			return nil
		} else if c.reduces[i] == 1 {
			if time.Since(c.reduceStart[i]) > s10 {
				reply.Name = "reducer"
				reply.State = 1
				reply.ReducerI = i
				c.reduces[i] = 1
				c.reduceStart[i] = time.Now()
				return nil
			}
		}
	}

	for s := range c.reduces {
		if s != 2 {
			reply.Name = "wait"
			reply.State = 3
			reply.ReducerI = -1
			return nil
		}
	}
	reply.Name = ""
	reply.State = 2
	reply.ReducerI = -1
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
	// for _, s := range c.reduces {
	// 	print(s, " ")
	// }
	// println()
	for _, s := range c.reduces {
		if s != 2 {
			return false
		}
	}
	return true
	// if _, err := os.Stat(fmt.Sprintf("t-mr-out-%d", c.nReduce-1)); err == nil {
	// 	for i := 0; i < c.nReduce; i++ {

	// 	}
	// 	return true
	// } else if errors.Is(err, os.ErrNotExist) {
	// 	return false
	// } else {
	// 	log.Fatal("Big finish error")
	// }
	// return false
}

func (c *Coordinator) CommitFile(args *CommitFileArgs, reply *CommitFileReply) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	for i, f := range c.InputFiles {
		if f.file == args.Name {
			c.InputFiles[i].processed = 2
		}
	}
	return nil
}

func (c *Coordinator) CommitOutput(args *CommitOutputArgs, reply *CommitOutputReply) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.reduces[args.I] = 2
	return nil
}

// create a Coordeinator.
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
		InputFiles:  i,
		nReduce:     nReduce,
		reduces:     make([]int, nReduce),
		reduceStart: make([]time.Time, nReduce),
	}

	// Your code here.

	c.server()
	return &c
}
