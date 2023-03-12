package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type MapTask struct {
	ID       int
	Filepath string // map only works with one file
	Finish   bool   // true if completed, false otherwise
	// clock for when the task started. Implement timeout behavior later
	TimeStart time.Time // type is time
	Assigned  bool
}

type ReduceTask struct {
	ID int
	// Filepaths []string // reduce works with many files (for mod nreduce)
	Finish bool // true if completed, false otherwise
	// clock for when the task started. Implement timeout behavior later
	TimeStart time.Time // type is time
	Assigned  bool
}

type Coordinator struct {
	// values for number of mappers and number of reducers
	numMapper   int
	numReducer  int
	allMappers  []MapTask    // mapID to mapTask
	allReducers []ReduceTask //nReduce to reduceTask
	lock        sync.Mutex   // lock for main coordinator thread
	allMapTasks int
	nReduce     int
}

// function for coordinator to assign tasks to idle workers
// iterate through active mappers / reducers to assign expired tasks first

func (c *Coordinator) GetTask(args *TaskRequest, reply *TaskAssignment) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	// reduceNow := (c.numMapper == 0)
	// get current time
	now := time.Now()
	latest_start := now.Add(-time.Second * 10)
	// fmt.Printf("%s \n", latest_start)
	// switch reduceNow {
	// if we still have mappers left
	if c.numMapper != 0 {
		for id := range c.allMappers {
			if !c.allMappers[id].Finish && c.allMappers[id].TimeStart.Before(latest_start) && c.allMappers[id].Assigned {
				// then set mt timer to current
				c.allMappers[id].TimeStart = now
				c.allMappers[id].Assigned = true
				fmt.Printf("assigned map task %d \n", id)
				reply.ID = id
				reply.Maptask = 1
				reply.AllMapTasks = c.allMapTasks
				reply.Filelist = []string{c.allMappers[id].Filepath}
				reply.NReduce = c.nReduce

				return nil
			} else if !c.allMappers[id].Assigned {
				fmt.Printf("assigned map task %d \n", id)
				c.allMappers[id].TimeStart = now
				c.allMappers[id].Assigned = true
				reply.ID = id
				reply.Maptask = 1
				reply.AllMapTasks = c.allMapTasks
				reply.Filelist = []string{c.allMappers[id].Filepath}
				reply.NReduce = c.nReduce

				return nil
			}
		}
	} else if c.numMapper == 0 && c.numReducer > 0 {
		// we are in reduce task
		// all maps should be zero
		for id := range c.allReducers {
			if !c.allReducers[id].Finish && c.allReducers[id].TimeStart.Before(latest_start) && c.allReducers[id].Assigned {
				fmt.Printf("assigned reduce task %d \n", id)
				// assign stale mapper task to worker
				c.allReducers[id].TimeStart = now
				c.allReducers[id].Assigned = true

				reply.Maptask = 2
				reply.AllMapTasks = c.allMapTasks
				reply.NReduce = c.nReduce
				reply.ID = id

				return nil

			} else if !c.allReducers[id].Assigned {
				fmt.Printf("assigned reduce task %d \n", id)
				c.allReducers[id].TimeStart = now
				c.allReducers[id].Assigned = true

				reply.Maptask = 2
				reply.AllMapTasks = c.allMapTasks
				reply.NReduce = c.nReduce
				reply.ID = id

				return nil
			}

		}
	} else if c.numMapper == 0 && c.numReducer == 0 {
		return nil
	}

	return nil // is this how I return???
}

// can I have a no response for rpc?
func (c *Coordinator) ImDone(args *CompletedTask, reply *Ack) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	switch args.MapTask {
	case true:
		// this is a mapTask
		// we should set the mapTask corresponding to mapid to finished
		index := args.MapID

		if !c.allMappers[index].Finish {
			// fmt.Printf("before -----------------------%v \n", c.allMappers[index])
			c.allMappers[index].Finish = true
			// fmt.Printf("after -----------------------%v \n", c.allMappers[index])
			c.numMapper--
		}

	case false:

		index := args.ReduceID
		if !c.allReducers[index].Finish { // this line gives the error of index out of range
			c.allReducers[index].Finish = true
			c.numReducer--
		}

	}
	return nil
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
	// ret := false

	// Your code here.
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.numMapper == 0 && c.numReducer == 0 {
		return true
	}
	return false
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// could potentially delete
	c.numReducer = nReduce
	c.numMapper = len(files)
	// -------------------------
	c.allMapTasks = len(files)
	if nReduce != 0 {
		c.nReduce = nReduce
	} else {
		os.Exit(1)
	}

	c.allMappers = make([]MapTask, len(files))
	c.allReducers = make([]ReduceTask, nReduce)
	// Your code here.
	// initialize map and reduce tasks
	for index, filepath := range files {
		// make maptasks and add them to c
		mt := MapTask{}
		mt.ID = index
		mt.Filepath = filepath
		mt.Finish = false
		mt.Assigned = false

		c.allMappers[index] = mt
	}

	// how do I instantiate the reducetasks?
	// in the beginning, let reduceTasks be empty list

	for i := 0; i < nReduce; i++ {
		rt := ReduceTask{}
		rt.ID = i
		rt.Finish = false
		rt.Assigned = false

		c.allReducers[i] = rt
	}
	c.server()
	return &c
}
