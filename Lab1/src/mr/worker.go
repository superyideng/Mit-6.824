package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "plugin"
import "os"
import "io/ioutil"
import "sort"
import "encoding/json"
import "time"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue
// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % TotalReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	mapf, reducef = loadPlugin(os.Args[1])

	for true {
		// worker request for a task via rpc
		reply, connectionSucceeded := RequestMaster()
		if connectionSucceeded == false {
			os.Exit(1)
			return
		}

		// do map task when reply.TaskType == 0
		if reply.TaskType == "Map" {
			kva := MapAFile(reply.FileName, mapf)

			mapIntermediate := GenerateIntermediateKVMap(reply.TotalReduce, kva)

			CreateIntermediateFile(reply.CurMapNum, reply.TotalReduce, mapIntermediate)

			// submit the task via rpc
			//submitReply, submitSucceeded := SubmitTask(reply.FileName)

		} else if reply.TaskType == "Reduce" { // do reduce task when reply.TaskType == 1
			kva := ReadIntermediateFile(reply.TotalMap, reply.CurReduceId)

			DoReduceAndGenerateOutputFile(kva, reply.CurReduceId, reducef)

		} else if reply.TaskType == "Wait" {
			time.Sleep(time.Second)
			continue
		}

		// submit the task via rpc
		//submitReply, submitSucceeded := SubmitTask(reply.FileName, reply.CurReduceId)
		SubmitTask(reply.TaskType, reply.FileName, reply.CurReduceId)

		//////////////////////////////////////////////////////// discuss the submit status

		time.Sleep(time.Second)

	}
}

// Send a request via RPC call to the master.
func RequestMaster() (MasterReplyTask, bool) {
	// declare an argument structure.
	args := WorkerRequestTask{}

	// declare a reply structure.
	reply := MasterReplyTask{}

	// send the RPC request, wait for the reply.
	connectionSucceeded := call("Master.TaskAllocation", &args, &reply)

	return reply, connectionSucceeded
}

// Submit the task to master via RPC.
func SubmitTask(taskType string, fileName string, curReduceId int) (MasterAckSubmission, bool) {
	// declare an argument structure.
	args := WorkerSubmitTask{}
	args.TaskType = taskType
	args.FileName = fileName
	args.ReduceId = curReduceId

	// declare a reply structure.
	reply := MasterAckSubmission{}

	// send the RPC request, wait for the reply.
	submissionSucceeded := call("Master.TaskSubmission", &args, &reply)

	return reply, submissionSucceeded
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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


// do map task on input file
func MapAFile(filename string, mapf func(string, string) []KeyValue) []KeyValue {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", file)
	}
	file.Close()
	kva := mapf(filename, string(content))
	return kva
}

// generate intermediate map to store each KeyValue pair
func GenerateIntermediateKVMap(totalReduce int, kva []KeyValue) map[int][]KeyValue {
	mapIntermediate := make(map[int][]KeyValue)
	for i := 0; i < totalReduce; i++ {
		bucket := make([]KeyValue, 0)
		mapIntermediate[i] = bucket
	}

	for _, kv := range kva {
		//k: word; val: "1"
		word := kv.Key
		numReduce := ihash(word) % totalReduce
		mapIntermediate[numReduce] = append(mapIntermediate[numReduce], kv)
	}

	// sort the intermediate map by key
	for i := 0; i < totalReduce; i++ {
		sort.Sort(ByKey(mapIntermediate[i]))
	}

	return mapIntermediate
}

// create intermediate files, which is output of a map task
func CreateIntermediateFile(curMapNum int, totalReduce int, mapIntermediate map[int][]KeyValue) {
	for i := 0; i < totalReduce; i++ {
		tempFileName := fmt.Sprintf("temp-%d-%d", curMapNum, i)
		tempFile, err := ioutil.TempFile("./", tempFileName)
		if err != nil {
			log.Fatalf("cannot create temp intermediate file %v", tempFileName)
		}
		enc := json.NewEncoder(tempFile)
		for _, kv := range mapIntermediate[i] {
			errEnc := enc.Encode(&kv)
			if errEnc != nil {
				log.Fatalf("cannot encode")
			}		
		}
		//tempFile.Close()
		intermediateFileName := fmt.Sprintf("mr-%d-%d", curMapNum, i)
		os.Rename(tempFile.Name(), intermediateFileName)
		tempFile.Close()

		// intermediateFileName := fmt.Sprintf("mr-%d-%d", curMapNum, i)
		// file, err := os.Create(intermediateFileName)
		// if err != nil {
		// 	log.Fatalf("cannot open %v", intermediateFileName)
		// }
		// enc := json.NewEncoder(file)
		// for _, kv := range mapIntermediate[i] {
		// 	errEnc := enc.Encode(&kv)
		// 	if errEnc != nil {
		// 		log.Fatalf("cannot encode")
		// 	}		
		// }
		// file.Close()
	}
}


// read intermediate files from map task and store KeyValue pair in kva
func ReadIntermediateFile(totalMap int, reduceId int) []KeyValue {
	kva := []KeyValue{}
	for i := 0; i < totalMap; i++ {
		intermediateFileName := fmt.Sprintf("mr-%d-%d", i, reduceId)

		///////////////////////////// discuss whether this file exist

		file, err := os.Open(intermediateFileName)
		if err != nil {
			log.Fatalf("cannot open %v", intermediateFileName)
		}

		dec := json.NewDecoder(file)
		for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				kva = append(kva, kv)
			}
			file.Close()
	}

	sort.Sort(ByKey(kva))
	return kva
}

// Dp reduce task and generate output file
func DoReduceAndGenerateOutputFile(kva []KeyValue, reduceId int, reducef func(string, []string) string) {
	oname := fmt.Sprintf("mr-out-%d", reduceId)
	ofile, _ := os.Create(oname)

	// call Reduce on each distinct key in intermediate[]
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	ofile.Close()
}

// load the application Map and Reduce functions from a plugin file, e.g. ../mrapps/wc.so
func loadPlugin(filename string) (func(string, string) []KeyValue, func(string, []string) string) {
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("cannot load plugin %v", filename)
	}
	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("cannot find Map in %v", filename)
	}
	mapf := xmapf.(func(string, string) []KeyValue)
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("cannot find Reduce in %v", filename)
	}
	reducef := xreducef.(func(string, []string) string)

	return mapf, reducef
}
