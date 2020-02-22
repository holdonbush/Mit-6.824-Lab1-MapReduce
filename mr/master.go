package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "strconv"
import "sync"
import "time"


// tasks' status
// UnAllocated    ---->     UnMaped
// Allocated      ---->     be maped to a worker
// Finished       ---->     worker finish the map task
const (
	UnAllocated = iota
	Allocated
	Finished
)

var maptasks chan string          // chan for map task
var reducetasks chan int          // chan for reduce task

// Master struct
type Master struct {
	// Your definitions here.
	AllFilesName        map[string]int
	MapTaskNumCount     int
	NReduce             int               // n reduce task
	InterFIlename       [][]string        // intermediate file
	MapFinished         bool
	ReduceTaskStatus    map[int]int      // about reduce tasks' status
	ReduceFinished      bool              // Finish the reduce task
	RWLock              *sync.RWMutex
}

// MyCallHandler func
// Your code here -- RPC handlers for the worker to call.
func (m *Master) MyCallHandler(args *MyArgs, reply *MyReply) error {
	msgType := args.MessageType
	switch(msgType) {
	case MsgForTask:
		select {
		case filename := <- maptasks:
			// allocate map task
			reply.Filename = filename
			reply.MapNumAllocated = m.MapTaskNumCount
			reply.NReduce = m.NReduce
			reply.TaskType = "map"

			m.RWLock.Lock()
			m.AllFilesName[filename] = Allocated
			m.MapTaskNumCount++
			m.RWLock.Unlock()
			go m.timerForWorker("map",filename)
			return nil

		case reduceNum := <- reducetasks:
			// allocate reduce task
			reply.TaskType = "reduce"
			reply.ReduceFileList = m.InterFIlename[reduceNum]
			reply.NReduce = m.NReduce
			reply.ReduceNumAllocated = reduceNum
			
			m.RWLock.Lock()
			m.ReduceTaskStatus[reduceNum] = Allocated
			m.RWLock.Unlock()
			go m.timerForWorker("reduce", strconv.Itoa(reduceNum))
			return nil
		}
	case MsgForFinishMap:
		m.RWLock.Lock()
		defer m.RWLock.Unlock()
		m.AllFilesName[args.MessageCnt] = Finished
	case MsgForFinishReduce:
		index, _ := strconv.Atoi(args.MessageCnt)
		m.RWLock.Lock()
		defer m.RWLock.Unlock()
		m.ReduceTaskStatus[index] = Finished
	}
	return nil
}

// timerForWorker : monitor the worker
func (m *Master)timerForWorker(taskType, identify string) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if taskType == "map" {
				m.RWLock.Lock()
				m.AllFilesName[identify] = UnAllocated
				m.RWLock.Unlock()
				maptasks <- identify
			} else if taskType == "reduce" {
				index, _ := strconv.Atoi(identify)
				m.RWLock.Lock()
				m.ReduceTaskStatus[index] = UnAllocated
				m.RWLock.Unlock()
				reducetasks <- index
			}
			return
		default:
			if taskType == "map" {
				m.RWLock.RLock()
				if m.AllFilesName[identify] == Finished {
					m.RWLock.RUnlock()
					return
				} else {
					m.RWLock.RUnlock()
				}
			} else if taskType == "reduce" {
				index, _ := strconv.Atoi(identify)
				m.RWLock.RLock()
				if m.ReduceTaskStatus[index] == Finished {
					m.RWLock.RUnlock()
					return
				} else {
					m.RWLock.RUnlock()
				}
			}
		}
	}
}



// MyInnerFileHandler : intermediate files' handler
func (m *Master) MyInnerFileHandler(args *MyIntermediateFile, reply *MyReply) error {
	nReduceNUm := args.NReduceType;
	filename := args.MessageCnt;

	m.InterFIlename[nReduceNUm] = append(m.InterFIlename[nReduceNUm], filename)
	return nil
}



//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	maptasks = make(chan string, 5)
	reducetasks = make(chan int, 5)
	rpc.Register(m)
	rpc.HandleHTTP()
	go m.generateTask()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// GenerateTask : create tasks
func (m *Master) generateTask() {
	for k,v := range m.AllFilesName {
		if v == UnAllocated {
			maptasks <- k
		}
	}
	ok := false
	for !ok {
		ok = checkAllMapTask(m)
	}
	
	m.MapFinished = true

	for k,v := range m.ReduceTaskStatus {
		if v == UnAllocated {
			reducetasks <- k
		}
	}

	ok = false
	for !ok {
		ok = checkAllReduceTask(m)
	}
	m.ReduceFinished = true
}

// checkAllMapTask : check if all map tasks are finished
func checkAllMapTask(m *Master) bool {
	m.RWLock.RLock()
	defer m.RWLock.RUnlock()
	for _,v := range m.AllFilesName {
		if v != Finished {
			return false
		}
	}
	return true
}

func checkAllReduceTask(m *Master) bool {
	m.RWLock.RLock()
	defer m.RWLock.RUnlock()
	for _, v := range m.ReduceTaskStatus {
		if v != Finished {
			return false
		}
	}
	return true
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false
	// Your code here.
	ret = m.ReduceFinished

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.AllFilesName = make(map[string]int)
	m.MapTaskNumCount = 0
	m.NReduce = nReduce
	m.MapFinished = false
	m.ReduceFinished = false
	m.ReduceTaskStatus = make(map[int]int)
	m.InterFIlename = make([][]string, m.NReduce)
	m.RWLock = new(sync.RWMutex)
	for _,v := range files {
		m.AllFilesName[v] = UnAllocated
	}
	
	for i := 0; i<nReduce; i++ {
		m.ReduceTaskStatus[i] = UnAllocated
	}

	m.server()
	return &m
}
