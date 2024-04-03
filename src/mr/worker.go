package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
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
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		ok, task, filename, Reducenum, Workernum, tasknum := Calltask()
		if ok {
			succeed := false
			if task == End {
				break
			} else if task == Map {
				succeed = Mapwork(&filename, Reducenum, Workernum, mapf)
			} else if task == Reduce {
				succeed = Reducework(Reducenum, Workernum, reducef)
			} else {
				log.Printf("Worker: undefined task type")
			}
			Finishtask(tasknum, succeed)
		} else {
			break
		}
	}

}

// Mapwork
// Read form the file, call map function, write into buckets
// write buckets into files
func Mapwork(filename *string, NReduce int, Workernum int, mapf func(string, string) []KeyValue) bool {
	// make NReducenum buckets
	buckets := make([][]KeyValue, NReduce)
	for i := 0; i < NReduce; i++ {
		buckets[i] = []KeyValue{}
	}
	// read from file
	file, err := os.Open(*filename)
	defer func() {
		if err := file.Close(); err != nil {
			log.Print(err)
		}
	}()
	if err != nil {
		log.Printf("Worker: cannot open %v", filename)
		log.Print(err)
		return false
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("Worker: cannot read %v", filename)
		log.Print(err)
		return false
	}
	// Call mapf and divide kval into buckets
	kva := mapf(*filename, string(content))
	for _, kv := range kva {
		bucketnum := ihash(kv.Key) % NReduce
		buckets[bucketnum] = append(buckets[bucketnum], kv)
	}
	// write into json files
	for index, bucket := range buckets {
		ok := writeintofile(index, Workernum, bucket)
		if !ok {
			return false
		}
	}
	log.Printf("Worker %d: map finished", Workernum)
	return true
}

// For mapper
// write into json files
func writeintofile(reducenum int, workernum int, bucket []KeyValue) bool {
	// create file
	filename := fmt.Sprintf("mr-%d-%d", workernum, reducenum)
	file, err := os.CreateTemp(".", "tmp")
	if err != nil {
		log.Printf("Worker: cannot create temp file")
		log.Print(err)
		return false
	}
	enc := json.NewEncoder(file)
	defer os.Remove(file.Name())
	defer os.Rename(file.Name(), filename)
	defer file.Close()
	for _, kv := range bucket {
		err := enc.Encode(&kv)
		if err != nil {
			log.Printf("Worker: cannot encode kv")
			log.Print(err)
			return false
		}
	}
	return true
}

// Reduce work
// Read from files, call reduce function, write into json file
func Reducework(Reducenum int, Nworker int, reducef func(string, []string) string) bool {
	immfilename := "mr-%d-%d"
	bucket := []KeyValue{}

	for i := 0; i < Nworker; i++ {
		filename := fmt.Sprintf(immfilename, i, Reducenum)
		file, err := os.Open(filename)
		defer os.Remove(file.Name())
		defer func() {
			if err := file.Close(); err != nil {
				if err != io.EOF {
					log.Print("Close file error")
					log.Print(err)
				}
			}
		}()
		if err != nil {
			log.Printf("Worker: cannot open %v", filename)
			log.Print(err)
			return false
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				if err == io.EOF {
					break
				}
				log.Printf("Worker: cannot decode kv")
				log.Print(err)
				return false
			}
			bucket = append(bucket, kv)
		}
	}

	// sort
	sort.Sort(ByKey(bucket))
	// call Reduce on each distinct key in bucket[],
	// and print the result to mr-out-reducenum.
	oname := fmt.Sprintf("mr-out-%d", Reducenum)
	ofile, _ := os.CreateTemp(".", "tmp")
	defer os.Remove(ofile.Name())
	defer os.Rename(ofile.Name(), oname)
	defer ofile.Close()
	i := 0
	for i < len(bucket) {
		j := i + 1
		for j < len(bucket) && bucket[j].Key == bucket[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, bucket[k].Value)
		}
		output := reducef(bucket[i].Key, values)
		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", bucket[i].Key, output)
		i = j
	}
	log.Printf("Worker %d: reduce finished", Reducenum)
	return true
}

// Tell the master job finished
func Finishtask(tasknum int, succeed bool) {
	args := Args{
		tasknum,
		succeed,
	}
	reply := Reply{}
	call("Coordinator.Finishtask", &args, &reply)
}

// make an RPC call to the coordinator asking for a task
func Calltask() (bool, TaskType, string, int, int, int) {
	args := Args{}
	reply := Reply{}
	ok := call("Coordinator.Gettask", &args, &reply)
	if ok {
		return true, reply.Task, reply.Filename, reply.Reducenum, reply.Mappernum, reply.Tasknum
	}
	return false, 0, "", 0, 0, 0
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
