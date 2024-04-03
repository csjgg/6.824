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

type task struct {
	tasktype  TaskType
	Mapperid  int
	Reducerid int
	starttime time.Time
}
type Coordinator struct {
	// Your definitions here.
	files       []string
	Nmapper     int
	Nreduce     int
	Reducenum   int
	Mappernum   int
	tasknum     int
	tasklist    map[int]task
	mapfinished bool
	mutex       sync.Mutex
}

// Distribute tasks to workers
// check if task in tasklist time out, reassign it to another worker
// check if Mapper task is done, if not assign mapper task
// eles assign reducer task
// if all task done, return End task
func (c *Coordinator) Gettask(args *Args, reply *Reply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	for {
		// check the tasklist
		for tasknum, t := range c.tasklist {
			if time.Since(t.starttime) > 10*time.Second {
				reply.Task = t.tasktype
				if reply.Task == Map {
					reply.Filename = c.files[t.Mapperid]
				}
				reply.Reducenum = t.Reducerid
				reply.Mappernum = t.Mapperid
				reply.Tasknum = c.tasknum
				c.tasknum++
				delete(c.tasklist, tasknum)
				c.tasklist[reply.Tasknum] = task{
					tasktype:  reply.Task,
					Mapperid:  reply.Mappernum,
					Reducerid: reply.Reducenum,
					starttime: time.Now(),
				}
				if reply.Task == Map {
					log.Print("Reassign task ", tasknum, " to worker ", reply.Mappernum)
				} else {
					log.Print("Reassign task ", tasknum, " to worker ", reply.Reducenum)
				}
				return nil
			}
		}
		// check if all mapper task done
		if c.Mappernum < c.Nmapper {
			reply.Task = Map
			reply.Filename = c.files[c.Mappernum]
			reply.Reducenum = c.Nreduce
			reply.Mappernum = c.Mappernum
			reply.Tasknum = c.tasknum
			c.Mappernum++
			c.tasknum++
			c.tasklist[reply.Tasknum] = task{
				tasktype:  reply.Task,
				Mapperid:  reply.Mappernum,
				Reducerid: reply.Reducenum,
				starttime: time.Now(),
			}
			log.Print("Assign Map task ", reply.Tasknum, " to worker ", reply.Mappernum)
			return nil
		}
		// check if all mapper task done
		if !c.mapfinished {
			if c.Checkmapfinished() {
				c.mapfinished = true
			} else {
				c.mutex.Unlock()
				time.Sleep(2 * time.Second)
				c.mutex.Lock()
				continue
			}
		}
		// assign reducer task
		if c.Reducenum < c.Nreduce {
			reply.Task = Reduce
			reply.Reducenum = c.Reducenum
			reply.Mappernum = c.Nmapper
			reply.Tasknum = c.tasknum
			c.Reducenum++
			c.tasknum++
			c.tasklist[reply.Tasknum] = task{
				tasktype:  reply.Task,
				Mapperid:  reply.Mappernum,
				Reducerid: reply.Reducenum,
				starttime: time.Now(),
			}
			log.Print("Assign Reduce task ", reply.Tasknum, " to worker ", reply.Reducenum)
			return nil
		}
		// if no task left, return End
		if len(c.tasklist) > 0 {
			c.mutex.Unlock()
			time.Sleep(2 * time.Second)
			c.mutex.Lock()
			continue
		}
		reply.Task = End
		return nil
	}
}

// check wheather all mapper task done
// should call under lock
func (c *Coordinator) Checkmapfinished() bool {
	finished := true
	for _, t := range c.tasklist {
		if t.tasktype == Map {
			finished = false
			break
		}
	}
	return finished
}

// Find the right task and remove it from the tasklist
func (c *Coordinator) Finishtask(args *Args, reply *Reply) error {
	if args.Succeed {
		c.mutex.Lock()
		defer c.mutex.Unlock()
		delete(c.tasklist, args.Tasknum)
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
	c.mutex.Lock()
	defer c.mutex.Unlock()
	ret := false
	if c.Mappernum == c.Nmapper && c.Reducenum == c.Nreduce && len(c.tasklist) == 0 {
		ret = true
	}
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:       files,
		Nmapper:     len(files),
		Nreduce:     nReduce,
		Mappernum:   0,
		Reducenum:   0,
		tasknum:     0,
		tasklist:    map[int]task{},
		mapfinished: false,
		mutex:       sync.Mutex{},
	}
	c.server()
	return &c
}
