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

// for generating worker id

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// copied from mrsequential
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
	// first instantiates rpc call
	for {
		var newRequest TaskRequest
		newAssignment := GetTask(newRequest)

		switch newAssignment.Maptask {
		// if mapTask
		case 1:

			// call function to handle maptask
			file := newAssignment.Filelist[0]
			mapper_id := newAssignment.ID
			nReduce := newAssignment.NReduce
			key_val_pair := doMap(file, mapf)
			doIntermediate(key_val_pair, mapper_id, nReduce)

			// then send rpc call to coordinator to signal done
			var doneNotice CompletedTask
			doneNotice.MapID = mapper_id
			doneNotice.MapTask = true
			fmt.Printf("finishing map task %d \n", newAssignment.ID)
			ImDone(doneNotice) // send rpc call to coordinator to say im done
		case 2:
			// we have a reduce task
			// fmt.Printf("starting reduce task %d \n", newAssignment.ID)
			mapcount := newAssignment.AllMapTasks
			reduceID := newAssignment.ID
			kvPairs := decodeReduce(reduceID, mapcount)
			doReduce(kvPairs, reduceID, reducef)
			// send done Notice
			var doneNotice CompletedTask
			doneNotice.ReduceID = reduceID
			doneNotice.MapTask = false
			// fmt.Printf("Finished reduce task %d", reduceID)
			ImDone(doneNotice)

		}
	}

}

// map function to perform maptask
func doMap(filename string, mapf func(string, string) []KeyValue) []KeyValue {
	// copied from sequential implementation
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	// fmt.Printf("%v key value \n", kva)
	return kva

}

// function for intermediate step: handeling keyvalue output of map
// sorting, hashing, mod by nReduce, write to file,
// then send rpc call to coordinator

func doIntermediate(kvp []KeyValue, map_id int, nReduce int) {
	// iterate through intermediate values to hash id, then mod by nreduce
	// create new mapping of nreduce to kv pair for reduce task
	// for each item in the mapping, create new file

	// var reduceMapping map[int][]KeyValue // maps reduceNumber to list of KeyValue

	reduceMapping := make(map[int][]KeyValue) // extract value from kvpair's value

	// bug fix: create entry for every i up to nReduce this way files are actually created
	var Empty []KeyValue
	for i := 0; i < nReduce; i++ {
		reduceMapping[i] = Empty
	}

	for _, kvPair := range kvp {
		// use helper function
		hashedval := ihash(kvPair.Key)
		reduceNumber := hashedval % nReduce
		reduceMapping[reduceNumber] = append(reduceMapping[reduceNumber], kvPair)

	}

	// then we iterate through kvpairs for each nReduce number and write them to file
	for reduceNumber, kvPairList := range reduceMapping {
		oname := fmt.Sprintf("mr-%d-%d", map_id, reduceNumber)         // how the fuck do I name the string after a variable
		file, error := os.OpenFile(oname, os.O_RDWR|os.O_CREATE, 0755) // creates the file

		// from gosamples
		if error != nil {
			log.Fatal(error)
			fmt.Printf("error in creating intermediate file")
			os.Exit(1)
		}
		enc := json.NewEncoder(file)
		defer file.Close()

		for _, kvPair := range kvPairList {
			// dump kvPair into the file
			err := enc.Encode(&kvPair) // How do i dump this information into this file???
			if err != nil {
				log.Fatal(err)
				fmt.Printf("error in writing to intermediate file")
			}
		}
		// fmt.Printf("done with file %s", oname)
	}

}

func decodeReduce(nReduce int, mapCount int) []KeyValue {
	// first ingest files that match the nReduce number
	// list of files that matches the nReduce number
	// bad

	// if nReduce == 0 {
	// 	fmt.Printf("doing reduce task 1")
	// }

	var reduceKVPairs []KeyValue

	// iterate through all mappers to generate list of matches
	var matches []string

	for i := 0; i < mapCount; i++ {
		reduceFileName := fmt.Sprintf("mr-%d-%d", i, nReduce)
		matches = append(matches, reduceFileName)
	}

	// then iterate through file list and call reduce
	for _, file := range matches {

		file_content, err := os.Open(file)
		if err != nil {

			log.Fatalf("error in opening file %s \n", file)
			os.Exit(1)
		}
		defer file_content.Close()

		// how do I read the content of a file ???
		dec := json.NewDecoder(file_content)
		// After extracting kv pairs in reduce files, iterate through each file and append
		for {
			var kvPair KeyValue
			if err := dec.Decode(&kvPair); err != nil {
				break
			}
			reduceKVPairs = append(reduceKVPairs, kvPair)
		}

	}

	sort.Sort(ByKey(reduceKVPairs))
	// if nReduce == 0 {
	// 	fmt.Printf("Here are the reduceKVPairs for 0 %v \n", reduceKVPairs)
	// }
	return reduceKVPairs
}

func doReduce(intermediate []KeyValue, nReduce int, reducef func(string, []string) string) {
	// main reduce function that iterates over list of key values and returns string, save to file

	// var reduceResult string
	reduceResult := fmt.Sprintf("mr-out-%d", nReduce)
	ofile, _ := os.Create(reduceResult)
	defer ofile.Close()

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	ofile.Close()

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

// test function for sending request to coordinator

func GetTask(tr TaskRequest) TaskAssignment {

	taskReply := TaskAssignment{} // Empty Response from Coordinator
	ok := call("Coordinator.GetTask", &tr, &taskReply)
	if !ok {
		// reply.Y should be 100.
		fmt.Printf("call failed!\n")
		os.Exit(1)

	}
	// } else {
	// 	return fmt.Printf("call failed!\n")
	// }
	return taskReply
}

// test function for sending request to coordinator

func ImDone(dr CompletedTask) {

	taskReply := Ack{} // Empty Response from Coordinator
	ok := call("Coordinator.ImDone", &dr, &taskReply)
	if !ok {
		// reply.Y should be 100.
		fmt.Printf("call failed!\n")
		os.Exit(1)

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
