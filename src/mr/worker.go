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
	"strconv"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
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
var w RequestReply

func MapTask(mapf func(string, string) []KeyValue) {
	fmt.Printf("%v %v start MapTask %v\n", time.Now().Format(time.StampMilli), os.Getpid(), w.Filename)
	inputfile, err := os.Open(w.Filename)
	if err != nil {
		log.Fatalf("cannot open %v", w.Filename)
	}
	content, err := ioutil.ReadAll(inputfile)
	if err != nil {
		log.Fatalf("cannot read %v", w.Filename)
	}
	inputfile.Close()
	kva := mapf(w.Filename, string(content))
	tempfiles := make([]*os.File, w.Nreduce)
	encs := make([]*json.Encoder, w.Nreduce)
	for i := 0; i < w.Nreduce; i++ {
		tempfiles[i], err = ioutil.TempFile("", "mrintermediate")
		if err != nil {
			log.Fatalf("cannot create tempfile %v-%v", w.Filename, i)
		}
		encs[i] = json.NewEncoder(tempfiles[i])
	}
	for _, kv := range kva {
		err := encs[ihash(kv.Key)%w.Nreduce].Encode(&kv)
		if err != nil {
			log.Fatalf("cannot write tempfile %v", w.Filename)
		}
	}
	for i := 0; i < w.Nreduce; i++ {
		tempfiles[i].Close()
		os.Rename(tempfiles[i].Name(), "mr-"+strconv.Itoa(w.Tasknumber)+"-"+strconv.Itoa(i))
	}
	args := RequestArgs{}
	args.Info = Complete_Task
	args.Taskkind = Map_Task
	args.Tasknumber = w.Tasknumber
	fmt.Printf("%v %v will report MapTask finished %v\n", time.Now().Format(time.StampMilli), os.Getpid(), w.Filename)
	w = Call(&args)
}
func ReduceTask(reducef func(string, []string) string) {
	fmt.Printf("%v %v start ReduceTask %v\n", time.Now().Format(time.StampMilli), os.Getpid(), w.Tasknumber)
	decs := make([]*json.Decoder, w.Nmap)
	for i := 0; i < w.Nmap; i++ {
		inputfile, err := os.Open("mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(w.Tasknumber))
		if err != nil {
			log.Fatalf("cannot open %v", "mr-"+strconv.Itoa(i)+"-"+strconv.Itoa(w.Tasknumber))
		}
		decs[i] = json.NewDecoder(inputfile)
	}
	intermediate := []KeyValue{}
	for i := 0; i < w.Nmap; i++ {
		for {
			var kv KeyValue
			if err := decs[i].Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}
	sort.Sort(ByKey(intermediate))
	oname := "mr-out-" + strconv.Itoa(w.Tasknumber)
	ofile, _ := os.Create(oname)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
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
	args := RequestArgs{}
	args.Info = Complete_Task
	args.Taskkind = Reduce_Task
	args.Tasknumber = w.Tasknumber
	fmt.Printf("%v %v will report ReduceTask finished %v\n", time.Now().Format(time.StampMilli), os.Getpid(), w.Tasknumber)
	w = Call(&args)
}
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	fmt.Printf("%v Make a Worker %v\n", time.Now().Format(time.StampMilli), os.Getpid())
	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()
	args := RequestArgs{}
	finish := false
	w.Info = Wait_Task
	for true {
		if w.Info == Wait_Task {
			args.Info = Aak_Task
			w = Call(&args)
			switch w.Info {
			case Map_Task:
				MapTask(mapf)
			case Reduce_Task:
				ReduceTask(reducef)
			case Exit_Task:
				finish = true
			case Wait_Task:
			}
		}
		if finish {
			break
		}
		time.Sleep(2 * time.Second)
	}
}

//
// example function to show how to make an RPC call to the master.
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
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

func Call(args *RequestArgs) RequestReply {
	reply := RequestReply{}
	for {
		flag := call("Master.Request", args, &reply)
		if flag {
			break
		}
		time.Sleep(time.Second)
	}
	return reply
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
