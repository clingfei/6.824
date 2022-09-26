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

const ScheduleInterval = time.Millisecond * 500

type FileStatus int32

const (
	UNPROCESS  FileStatus = 0
	PROCESSING FileStatus = 1
	FINISHED   FileStatus = 2
)

type TaskType int32

const (
	Map    TaskType = 0
	Reduce TaskType = 1
)

type TaskStatus int32

const (
	UNSTARTED TaskStatus = 0
	RUNNING   TaskStatus = 1
	FAILED    TaskStatus = 2
	END       TaskStatus = 4
)

type TaskStat struct {
	WorkerId  int
	Status    TaskStatus
	StartTime time.Time
}

type TaskPhase int

const (
	MapPhase    TaskPhase = 0
	ReducePhase TaskPhase = 1
)

type Queue struct {
	items []int
	lock  sync.RWMutex
}

func (q *Queue) New() *Queue {
	q.items = []int{}
	return q
}

func (q *Queue) Push(item int) {
	q.items = append(q.items, item)
}

func (q *Queue) Pop() int {
	if len(q.items) == 0 {
		return -1
	}
	ret := q.items[0]
	q.items = q.items[1:]
	return ret
}

type Coordinator struct {
	// Your definitions here.
	mu      sync.Mutex
	nReduce int

	// 记录剩余未被处理的文件
	fileMap  map[string]FileStatus
	fileTask map[int]string

	// 记录worker，用于register时分配id
	WorkerCounter int

	// 记录MapTask的状态
	MapTask map[int]TaskStat
	// 记录ReduceTask的状态
	ReduceTask map[int]TaskStat
	// 处于等待状态的worker
	WaitingQueue Queue
	// Map or Reduce
	taskPhase TaskPhase
	// 用于同步
	ChanMap map[int]chan bool

	rCounter int
	mCounter int
}

// coordinator should notice if a worker hasn't completed its task in 10 seconds.
// and then give this task to a different worker.

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// when a worker send an RPC to ask for a task, coordinator should respond with the filename of an as-yet-unstarted
// map task.

//
// start a thread that listens for RPCs from worker.go
//
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

//for the relevant RPC handler in the coordinator to have a loop that waits, either with time.Sleep() or sync.Cond.
//Go runs the handler for each RPC in its own thread, so the fact that one handler is waiting won't prevent the coordinator from processing other RPCs.
//coordinator wait for ten seconds; after that the coordinator should
//assume the worker has died (of course, it might not have).
func (c *Coordinator) Get(args int, reply *GetTaskReply) error {
	reply.NReduce = c.nReduce
	c.checkTask(args, reply)
	return nil
}

func (c *Coordinator) checkTask(workerid int, reply *GetTaskReply) {
	c.mu.Lock()
	if c.taskPhase == MapPhase {
		c.mCounter++
		for filename, status := range c.fileMap {
			if status == UNPROCESS {
				c.fileTask[c.mCounter] = filename
				c.fileMap[filename] = PROCESSING
				c.MapTask[c.mCounter] = TaskStat{workerid, RUNNING, time.Now()}
				c.mu.Unlock()
				reply.Task = Map
				reply.Filename = filename
				reply.TaskId = c.mCounter
				return
			}
		}
		// 将这个worker加入等待队列
		c.WaitingQueue.Push(workerid)
		c.mu.Unlock()
		if ok := <-c.ChanMap[workerid]; ok {
			c.checkTask(workerid, reply)
		}
	} else {
		if c.rCounter >= c.nReduce {
			for rCounter, status := range c.ReduceTask {
				if status.Status == FAILED || status.Status == UNSTARTED {
					reply.TaskId = rCounter
					reply.MapCount = len(c.fileMap)
					reply.Task = Reduce
					c.ReduceTask[rCounter] = TaskStat{workerid, RUNNING, time.Now()}
					c.mu.Unlock()
					return
				}
			}
			// 我们可以这样设计，worker id用于标识每一个worker，但是使用rCounter和wCounter来标识不同的task
			c.WaitingQueue.Push(workerid)
			c.mu.Unlock()
			if ok := <-c.ChanMap[workerid]; ok {
				c.checkTask(workerid, reply)
			}
		} else {
			c.rCounter++
			reply.TaskId = c.rCounter
			reply.MapCount = len(c.fileMap)
			reply.Task = Reduce
			c.ReduceTask[c.rCounter] = TaskStat{workerid, RUNNING, time.Now()}
		}
	}
	c.mu.Unlock()
}

func (c *Coordinator) Register(args interface{}, reply *int) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.WorkerCounter++
	*reply = c.WorkerCounter
	return nil
}

func (c *Coordinator) ReduceReport(args ReduceReport, reply interface{}) {
	// 如何判断某一个reduce task有没有执行完
	c.mu.Lock()
	status := c.ReduceTask[args.Rno]
	status.Status = END
	c.MapTask[args.Rno] = status
	c.mu.Unlock()
}

func (c *Coordinator) MapReport(args MapReport, reply interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	status := c.MapTask[args.Mno]
	status.Status = END
	c.MapTask[args.Mno] = status
	filename := c.fileTask[args.Mno]
	c.fileMap[filename] = FINISHED
	for _, v := range c.fileMap {
		if v != FINISHED {
			return
		}
	}
	c.taskPhase = ReducePhase
	c.awakeRoutine()
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := true
	// Your code here.
	if c.taskPhase == ReducePhase {
		for _, v := range c.ReduceTask {
			if v.Status != END {
				ret = false
				break
			}
		}
	} else {
		ret = false
	}
	return ret
}

// Lock是对于goroutine来说的，而不是对于一个函数来说的
// 考虑如下执行路径：schedule->taskReschedule->唤醒阻塞的协程->return
func (c *Coordinator) schedule() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.Done() {
		return
	}
	var taskmap map[int]TaskStat
	if c.taskPhase == MapPhase {
		taskmap = c.MapTask
	} else {
		taskmap = c.ReduceTask
	}
	for _, t := range taskmap {
		if t.Status == RUNNING {
			if time.Now().After(t.StartTime.Add(time.Second * 10)) {
				t.Status = FAILED
				c.taskReschedule(t.WorkerId)
			}
		}
	}
}

func (c *Coordinator) taskReschedule(workerId int) {
	if c.taskPhase == MapPhase {
		filename := c.fileTask[workerId]
		c.fileMap[filename] = UNPROCESS
	}
	// Reduce failed如何处理？
	c.awakeRoutine()
}

func (c *Coordinator) awakeRoutine() {
	// 选择一个处于WAITING状态的worker唤醒，唤醒后由其自行选择一个需要的任务来完成。
	waitingWorker := c.WaitingQueue.Pop()
	if waitingWorker != -1 {
		c.ChanMap[waitingWorker] <- true
	}
}

func (c *Coordinator) tickSchedule() {
	for !c.Done() {
		go c.schedule()
		time.Sleep(ScheduleInterval)
	}
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	//if len(files) < 2 {
	//	fmt.Fprintf(os.Stderr, "usage: MapReduce xxx.so inputfiles...\n")
	//	os.Exit(1)
	//}
	c := Coordinator{}
	// 应该记录所有待处理的文件，文件的状态包括未处理，处理中，和已处理完成，还要保存正在处理的worker id。
	// 如何区分不同的worker？
	// Your code here.
	c.nReduce = nReduce
	for _, filename := range files {
		c.fileMap[filename] = UNPROCESS
	}
	c.taskPhase = MapPhase
	for i := 0; i <= nReduce; i++ {
		c.ReduceTask[i] = TaskStat{0, UNSTARTED, time.Now()}
	}
	go c.tickSchedule()
	c.server()
	return &c
}

// 调度器轮转，检查map中每个任务的时间，如果超时，那么将这个worker设置为failed。如果结束，直接将这个删掉？
