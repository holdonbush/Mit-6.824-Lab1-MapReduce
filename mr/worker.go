package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "io/ioutil"
import "strconv"
import "encoding/json"
import "sort"


// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for(true) {
		reply := CallForTask(MsgForTask,"")
		if(reply.TaskType == "") {
			break
		}
		switch(reply.TaskType) {
		case "map":
			mapInWorker(&reply, mapf)	
		case "reduce":
			reduceInWorker(&reply, reducef)
		}
	}
}

//
// mapInWorker : workers do the map phase
func mapInWorker(reply *MyReply,mapf func(string, string) []KeyValue) {
	file, err := os.Open(reply.Filename)
	defer file.Close()
	if err != nil {
		log.Fatalf("cannot open %v", reply.Filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", reply.Filename)
	}

	// map function, get intermediate keyvalue pairs
	kva := mapf(reply.Filename, string(content))
	// partition function. finish the partition task
	kvas := Partition(kva, reply.NReduce)
	// write to temp local file
	for i := 0; i<reply.NReduce; i++ {
		filename := WriteToJSONFile(kvas[i], reply.MapNumAllocated, i)
		_ = SendInterFiles(MsgForInterFileLoc, filename, i)
	}
	_ = CallForTask(MsgForFinishMap, reply.Filename)
}

// reduceInWroker : workers do the reduce phase
func reduceInWorker(reply *MyReply, reducef func(string, []string) string) {
	intermediate := []KeyValue{}
	for _,v := range reply.ReduceFileList {
		// fmt.Println(v)
		file, err := os.Open(v)
		defer file.Close()
		if err != nil {
			log.Fatalf("cannot open %v", v)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}
	sort.Sort(ByKey(intermediate))
	oname := "mr-out-"+strconv.Itoa(reply.ReduceNumAllocated)
	ofile, _ := os.Create(oname)

	i := 0
	for i < len(intermediate) {
		j := i+1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	_ = CallForTask(MsgForFinishReduce, strconv.Itoa(reply.ReduceNumAllocated))
}


// CallForTask : my RPC call function
func CallForTask(msgType int,msgCnt string) MyReply {
	args := MyArgs{}
	args.MessageType = msgType
	args.MessageCnt = msgCnt

	reply := MyReply{}

	// call
	res := call("Master.MyCallHandler", &args, &reply)
	if !res {
		return MyReply{TaskType:""}
	}
	return reply
}

// SendInterFiles : send intermediate files' location (filenames here) to master
func SendInterFiles(msgType int, msgCnt string, nReduceType int) MyReply {
	args := MyIntermediateFile{}
	args.MessageType = msgType
	args.MessageCnt = msgCnt
	args.NReduceType = nReduceType

	repley := MyReply{}

	res := call("Master.MyInnerFileHandler", &args, &repley)
	if !res {
		fmt.Println("error sending intermediate files' location")
	}
	return repley
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

	if(err.Error() != "unexpected EOF") {
		fmt.Println("error: ",err)
	}
	return false
}

// WriteToJSONFile : write intermediate KeyValue pairs to a Json file
func WriteToJSONFile(intermediate []KeyValue, mapTaskNum, reduceTaskNUm int) string {
	filename := "mr-"+strconv.Itoa(mapTaskNum)+"-"+strconv.Itoa(reduceTaskNUm)
	jfile, _ := os.Create(filename)

	enc := json.NewEncoder(jfile)
	for _, kv := range intermediate {
		err := enc.Encode(&kv)
		if(err != nil) {
			log.Fatal("error: ",err)
		}
	}
	return filename
}


// WriteToReduceOutput : write to final file
func WriteToReduceOutput(key, values string, nReduce int) {
	filename := "mr-out-"+strconv.Itoa(nReduce)
	ofile, err := os.Open(filename)
	if err != nil {
		fmt.Println("no such file")
		ofile, _ = os.Create(filename)
	}

	fmt.Fprintf(ofile, "%v %v\n", key, values)
}


// Partition : divide intermedia keyvalue pairs into nReduce buckets
func Partition(kva []KeyValue, nReduce int) [][]KeyValue {
	kvas := make([][]KeyValue,nReduce)
	for _,kv := range kva {
		v := ihash(kv.Key) % nReduce
		kvas[v] = append(kvas[v], kv)
	}
	return kvas
}